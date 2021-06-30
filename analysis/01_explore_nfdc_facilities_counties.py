import os
import pandas as pd
import geopandas as gpd
from airportei.utilis import PATH_RAW, PATH_INTERIM, get_snake_case_dict


if __name__ == "__main__":
    path_nfdc = os.path.join(PATH_RAW, "madhu_files",
                                   "FAA_NFDC_Facilities.csv")
    nfdc_facilities = pd.read_csv(path_nfdc)
    nfdc_rename_dict = get_snake_case_dict(nfdc_facilities)
    # path_airport = os.path.join(PATH_RAW, "Airports.csv")
    # airport = pd.read_csv(path_airport)
    path_airport_shp = os.path.join(PATH_RAW, "Airports", "Airports.shp")
    airport = gpd.read_file(path_airport_shp)
    path_counties_shp = os.path.join(PATH_RAW,
                                 "Texas_County_Boundaries_Detailed-shp",
                                 "County.shp")
    counties_tx = gpd.read_file(path_counties_shp)
    counties_tx_1 = (
        counties_tx.rename(columns=get_snake_case_dict(counties_tx))
        .rename(columns={"cnty_nm": "county", "dist_nm": "district_tx_boundar",
                         "cnty_fips": "fips_tx_boundar"})
        .assign(
            county=lambda df: (
                df.county.str.strip().str.lower().str.replace("'", "")
            ),
        )
        .filter(items=["county", "district_tx_boundar", "fips_tx_boundar",
                       "geometry"])
    )

    nfdc_facilities_1 = (
        nfdc_facilities.rename(columns=nfdc_rename_dict)
        .filter(
            items=[
                "location_id",
                "facility_name",
                "effective_date",
                "city",
                "county",
                "state",
                "type",
                "airport_status_code",
                "ownership",
                "use",
                "medical_use",
                "other_services",
                "fuel_types",
                "military_joint_use",
                "single_engine_ga",
                "multi_engine_ga",
                "jet_engine_ga",
                "helicopters_ga",
                "gliders_operational",
                "military_operational",
                "ultralights",
                "operations_commercial",
                "operations_commuter",
                "operations_air_taxi",
                "operations_ga_local",
                "operations_ga_itin",
                "operations_military",
                "operations_date",
            ]
        )
        .rename(columns={"location_id": "facility_id", "type": "facility_type"})
        .assign(
            total_ops=lambda df: (
                df.operations_commercial.fillna(0)
                + df.operations_commuter.fillna(0)
                + df.operations_air_taxi.fillna(0)
                + df.operations_ga_local.fillna(0)
                + df.operations_ga_itin.fillna(0)
                + df.operations_military.fillna(0)
            )
        )
    )

    airport_tx = airport.rename(columns=get_snake_case_dict(airport)).loc[
        lambda df: df.state == "TX"
    ]

    nfdc_facilities_1_fil = nfdc_facilities_1.filter(
        items=["facility_id", "facility_name", "city", "county"]
    ).assign(
        county=lambda df: (
            df.county.str.strip().str.lower().str.replace("'", "")
        ),
        facility_id=lambda df: (
            df.facility_id.str.strip().str.lower().str.replace("'", "")
        ),
        facility_name=lambda df: (
            df.facility_name.str.strip().str.lower().str.replace("'", "")
        ),
    )

    airport_tx_fil_counties = (
        gpd.sjoin(
            airport_tx,
            counties_tx_1,
            how="left",
            op="intersects")
        .rename(
            columns={
                "ident": "facility_id",
                "name": "facility_name",
                "operstatus": "operstatus_arpt",
                "latitude": "lat_arpt",
                "longitude": "long_arpt",
                "servcity": "city",
            }
        )
        .assign(
            facility_id=lambda df: (
                df.facility_id.str.strip().str.lower().str.replace("'", "")
            ),
            facility_name=lambda df: (
                df.facility_name.str.strip().str.lower().str.replace("'", "")
            ),
        )
        .assign(
            facility_id=lambda df: df.facility_id.replace(
                ["1e4", "1e2", "1e7", "2e5", "2e7", "3e0"],
                [str(int(i)) for i in [1e4, 1e2, 1e7, 2e5, 2e7, 3e0]])
    )
        .filter(
            items=[
                "facility_id",
                "facility_name",
                "city",
                "county",
                "district_tx_boundar",
                "fips_tx_boundar",
                "operstatus_arpt",
                "lat_arpt",
                "long_arpt",
                "geometry"
            ]
        )
    )

    nfdc_arpt_mrg = pd.merge(
        nfdc_facilities_1_fil,
        airport_tx_fil_counties,
        on=["facility_id"],
        suffixes=["_nfdc", "_arpt"],
        how="outer",
    )

    nfdc_arpt_mrg["qc"] = (nfdc_arpt_mrg.county_arpt
                           == nfdc_arpt_mrg.county_nfdc).astype(int)

    path_out = os.path.join(PATH_INTERIM, "nfdc_vs_arpt_city_comp.xlsx")
    nfdc_arpt_mrg.to_excel(path_out)
    path_out_tx_counties = os.path.join(PATH_INTERIM, "tx_counties.csv")
    counties_tx_1.drop("geometry", axis=1).to_csv(
        path_out_tx_counties)

    miss_nfdc = nfdc_arpt_mrg.loc[lambda df: df.city_nfdc.isna()]

    miss_arpt = nfdc_arpt_mrg.loc[lambda df: df.city_arpt.isna()]

    mask = (nfdc_arpt_mrg.county_nfdc.isna()) | (nfdc_arpt_mrg.county_arpt.isna())
    assert all(
        nfdc_arpt_mrg.loc[~mask, "county_nfdc"] == nfdc_arpt_mrg.loc[~mask, "county_arpt"]
    )
    operation_date = nfdc_facilities_1.operations_date.value_counts().sort_index()
