"""
Finalize 2019 Ops. Add metadata.
Created by: Axb
Created on: 06/30/2021
"""
from pathlib import Path
import pandas as pd
from airportei.utilis import connect_to_mariadb, get_snake_case_dict, PATH_INTERIM


if __name__ == "__main__":
    # Corrected county based on the spatial join. Output from
    # 01_explore_nfdc_facilities_counties.py and some manual imputations by
    # Apoorb.
    path_cor_county = Path.home().joinpath(
        PATH_INTERIM, "county_correction", "nfdc_vs_arpt_city_comp_filled.xlsx")
    path_madhu_2019_fin = Path.home().joinpath(
        PATH_INTERIM, "madhu_ops_fleetmix", "madhu_qaqc_2019Operations.xlsx"
    )
    cor_counties_tx = pd.read_excel(
        path_cor_county,
        sheet_name="filled_data",
        usecols=["facility_id", "city_arpt", "county_arpt", "fips_tx_boundar", "district_tx_boundar"])
    # Madhu developed the 2019 ops data
    # (r"C:\Users\a-bibeka\Texas A&M Transportation Institute\Venugopal, "
    #  r"Madhusudhan - Airport Activity\Tasks\Task 4 Data Analysis and Quality "
    #  r"Assurance\QAQC")
    # File: 2019Operations.xlsx---Madhu compared operations from ERG, TFMSC,
    # TAF, 5010, and AirNav data to
    ops2019 = pd.read_excel(
        path_madhu_2019_fin,
        "2019Ops",
        usecols=[
            "Facility ID",
            "Facility Name",
            "Facility Group",
            "Annual Operations",
            "Summer Daily",
        ],
    )
    # Will add the metadata from FY21TCEQ_AirportEI.texas_facilities table to
    # the ops2019. Removing the old metadata.
    ops2019_1 = (
        ops2019.rename(columns=get_snake_case_dict(ops2019))
        .drop(columns=["facility_group", "facility_name"])
        .assign(
            facility_id=lambda df: df.facility_id.astype(str),
            annual_operations=lambda df: df.annual_operations.replace(
                "-", "0", regex=False
            ).astype(float),
            summer_daily=lambda df: df.summer_daily.astype(float),
        )
    )
    # Get metadata from FY21TCEQ_AirportEI.texas_facilities
    conn = connect_to_mariadb("FY21TCEQ_AirportEI")
    tx_fac_df = pd.read_sql("SELECT * FROM texas_facilities", conn)
    conn.close()
    rename_col_map = get_snake_case_dict(tx_fac_df)
    rename_col_map = {
        key: val.replace("2020", "y2020")
        .replace("2019", "y2019")
        .replace("2017", "y2017")
        for key, val in rename_col_map.items()
    }
    tx_fac_df_1 = tx_fac_df.rename(columns=rename_col_map)
    # Only get the following metadata columns.
    meta_cols = [
        "facility_id",
        "facility_name",
        "facility_type",
        "airport_status_code",
        "ownership",
        "used",
        "medical_use",
        "otherservices",
        "fuel_types",
        "military_joint_use",
        "tx_dot_group",
        "facility_group",
        "farmor_ranch",
    ]
    tx_fac_df_2 = tx_fac_df_1.filter(items=meta_cols)
    # Add metadata to ops. There are some new airports
    # in FY21TCEQ_AirportEI.texas_facilities for which we do not have
    # operations data.
    ops2019_meta = tx_fac_df_2.merge(ops2019_1, on="facility_id", how="left")
    # Get the aiports with missing ops data.
    miss_ops2019_facility_ids = ops2019_meta.loc[
        lambda df: df.summer_daily.isna(), "facility_id"
    ].values
    miss_ops2019 = tx_fac_df_1.loc[
        lambda df: df.facility_id.isin(miss_ops2019_facility_ids)
    ]
    # Impute ops
    miss_ops2019["ops_impute_annual"] = miss_ops2019["y2017_erg_ops"]

    miss_ops2019_1 = miss_ops2019.filter(
        items=["facility_id", "facility_name", "ops_impute_annual", "facility_type",
        "facility_group"]
    )
    # Impute ops for medical HELIPORT
    miss_ops2019_1.loc[
        lambda df: (df.facility_type == "HELIPORT")
                   & (df.facility_group == "Medical")
        ,
        "ops_impute_annual"
    ] = 156 # Based on Madhu's qaqc sheet.

    # Impute ops for private heliports
    miss_ops2019_1.loc[
        lambda df: (df.ops_impute_annual.isna())
                   & (df.facility_type == "HELIPORT")
        ,
        "ops_impute_annual"
    ] = 110 # Based on ERG defaults

    miss_ops2019_1.loc[
        lambda df: (df.facility_group == "Farm/Ranch")
        ,
        "ops_impute_annual"
    ] = 2 # Based on Farm/Ranch Data. 2 looked like a conservative estimate
    # for small airport.

    miss_ops2019_1.loc[
        lambda df: (df.facility_group.isin(
            ["Other_PU_Airports", "Other_PR_Airports"]))
        ,
        "ops_impute_annual"
    ] = 110 # Based on Other private airport data. 110 looked like a
    # conservative estimate for small airport.



    # Get annual to summer conversion factors from Madhu's QAQC 2019 Ops
    # spreadsheet.
    annual_to_summer = pd.DataFrame(
        {
            "facility_group": [
                "Medical",
                "Other_PR_Airports",
                "Farm/Ranch",
                "Other_PR_Heliports",
                "Other_PU_Airports",
            ],
            "annual_to_summer": [
                0.217264143,
                0.217264143,
                0.217264143,
                0.217264143,
                0.217264143,
            ],
        }
    )

    miss_ops2019_2 = miss_ops2019_1.merge(
        annual_to_summer, on="facility_group", how="left"
    ).assign(ops_impute_summer=lambda df: df.ops_impute_annual * df.annual_to_summer)
    assert all(miss_ops2019_2.ops_impute_summer > 0), "Need more imputation"

    miss_ops2019_2_fil = miss_ops2019_2.filter(
        items=["facility_id", "ops_impute_annual", "ops_impute_summer"]
    )

    # Impute missing Ops
    ops2019_meta_imputed = ops2019_meta.merge(
        miss_ops2019_2_fil, on=["facility_id"], how="left"
    )
    mask = ops2019_meta_imputed.annual_operations.isna()
    ops2019_meta_imputed.loc[mask, "annual_operations"] = ops2019_meta_imputed.loc[
        mask, "ops_impute_annual"
    ]
    ops2019_meta_imputed.loc[mask, "summer_daily"] = ops2019_meta_imputed.loc[
        mask, "ops_impute_summer"
    ]
    assert all(ops2019_meta_imputed.annual_operations > 0), (
        "Need more imputation")

    # Add corrected county, city, fips, and district.
    ops2019_meta_imputed_1 = pd.merge(
        ops2019_meta_imputed.assign(
            facility_id=lambda df: df.facility_id.str.lower()),
        cor_counties_tx,
        on="facility_id",
        how="left"
    )

    ops2019_meta_imputed_2 = (
        ops2019_meta_imputed_1
        .filter(
            items=[
                "county_arpt",
                "city_arpt",
                "district_tx_boundar",
                "fips_tx_boundar",
                "facility_id",
                "facility_name",
                "facility_group",
                "annual_operations",
                "summer_daily",
                "tx_dot_group",
                "medical_use",
                "military_joint_use",
                "otherservices",
                "fuel_types",
                "ownership",
                "used",
                "airport_status_code"
                "farmor_ranch"
            ]
        )
    )
    assert all(~ ops2019_meta_imputed_1.fips_tx_boundar.isna()), (
        "Check for missing counties.")

    path_out_imputed_ops = Path.home().joinpath(
        PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx")
    ops2019_meta_imputed_2.to_excel(path_out_imputed_ops)

