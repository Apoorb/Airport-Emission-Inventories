"""
Develop projection factors.
Created on: 7/08/2021
Created by: Apoorba Bibeka
"""
from pathlib import Path
import pandas as pd
import numpy as np
from airportei.utilis import PATH_RAW, PATH_INTERIM, get_snake_case_dict


def fill_tasp_arpts_prj(taf_ops2019_tasp_):
    """
    Use TASP and Military airports in the same district to fill the TASP and
    Military airport projection factors data.

    Parameters
    ----------
    taf_ops2019_tasp_

    Returns
    -------

    """
    taf_ops2019_tasp_na = taf_ops2019_tasp_.loc[
        lambda df: df.proj_fac.isna()
    ].sort_values(["district_tx_boundar", "facility_id", "sysyear"])

    taf_ops2019_tasp_non_na = taf_ops2019_tasp_.loc[
        lambda df: ~df.proj_fac.isna()
    ].sort_values(["district_tx_boundar", "facility_id", "sysyear"])
    # Check if we can use the airports from the same district to fill the
    # TASP data.
    assert (
        set(taf_ops2019_tasp_na.district_tx_boundar.unique())
        - set(taf_ops2019_tasp_non_na.district_tx_boundar.unique())
    ) == set(), "We cannot use districts to fill fleetmix."
    list_fill_df = []
    for indx, row in taf_ops2019_tasp_na.iterrows():
        df_fil_district = taf_ops2019_tasp_non_na.loc[
            lambda df: df.district_tx_boundar == row.district_tx_boundar
        ]

        df_fil_district_fil_ops = (
            df_fil_district.assign(
                ops_diff=lambda df: abs(df.annual_operations - row.annual_operations),
                ops_per_diff=lambda df: df.ops_diff * 100 / row.annual_operations,
            )
            .loc[lambda df: df.ops_diff == min(df.ops_diff)]
            .loc[lambda df: df.facility_id == df.facility_id.iloc[0]]
            .rename(columns={"facility_id": "filled_from_facility_id"})
            .filter(
                items=[
                    "district_tx_boundar",
                    "sysyear",
                    "proj_fac",
                    "ops_diff",
                    "ops_per_diff",
                    "filled_from_facility_id",
                ]
            )
        )
        fill_row = row.to_frame().T

        fill_row.drop(columns=["sysyear", "proj_fac"], inplace=True)

        fill_df = fill_row.merge(df_fil_district_fil_ops, on="district_tx_boundar")
        list_fill_df.append(fill_df)

    arpt_fill_dfs = pd.concat(list_fill_df)

    taf_ops2019_tasp_1 = pd.concat([taf_ops2019_tasp_non_na, arpt_fill_dfs])
    return taf_ops2019_tasp_1


if __name__ == "__main__":
    path_aeo_proj_fac = Path.home().joinpath(
        PATH_RAW, "com_jet_com_mil_aeo_projections.xlsx"
    )
    path_com_2020_ops = Path.home().joinpath(
        PATH_INTERIM, "commercial_airport_ops2019_20.xlsx"
    )
    path_ops2019_clean = Path.home().joinpath(
        PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx"
    )
    path_taf_2020 = Path.home().joinpath(PATH_RAW, "TAFDetailed_2020.txt")
    path_tfmsc = Path.home().joinpath(PATH_RAW, "madhu_files", "FAA_2019TFMSC.csv")
    path_fleetmix_clean = Path.home().joinpath(
        PATH_INTERIM, "fleetmix_axb_07_05_2021.xlsx"
    )
    ops2019 = pd.read_excel(
        path_ops2019_clean,
        usecols=[
            "facility_id",
            "facility_name",
            "facility_group",
            "facility_type",
            "county_arpt",
            "district_tx_boundar",
            "fips_tx_boundar",
            "medical_use",
            "military_joint_use",
            "otherservices",
            "fuel_types",
            "ownership",
            "used",
            "annual_operations",
        ],
    )
    taf_2020 = pd.read_csv(path_taf_2020, sep="\t")
    taf_2020_1 = (
        taf_2020.rename(columns=get_snake_case_dict(taf_2020))
        .rename(columns={"loc_id": "facility_id"})
        .assign(facility_id=lambda df: df.facility_id.str.lower().str.strip())
        .loc[:, ["facility_id", "sysyear", "scenario", "t_aops"]]
        .loc[lambda df: df.t_aops >= 1]
    )
    tfmsc_df = pd.read_csv(path_tfmsc)
    tfmsc_df_1 = (
        tfmsc_df.rename(columns=get_snake_case_dict(tfmsc_df))
        .rename(columns={"location_id": "facility_id"})
        .assign(facility_id=lambda df: df.facility_id.str.lower().str.strip())
    )
    fleetmix_x1 = pd.ExcelFile(path_fleetmix_clean)
    # Test if the list of facilities match between the TFMSC and TAF datasets.
    len(set(tfmsc_df_1.facility_id))
    len(set(taf_2020_1.facility_id))
    len(set(tfmsc_df_1.facility_id) - set(taf_2020_1.facility_id))

    # Get facility level projection factors
    taf_base_yr = (
        taf_2020_1.loc[lambda df: df.sysyear == 2019]
        .rename(columns={"t_aops": "t_aops_2019"})
        .filter(items=["facility_id", "t_aops_2019"])
    )

    taf_prj_fac = taf_2020_1.merge(taf_base_yr, on="facility_id", how="left").assign(
        proj_fac=lambda df: df.t_aops / df.t_aops_2019
    )

    # Find facility with projection factors.
    mask = taf_prj_fac.groupby("facility_id").sysyear.transform("count") == 35
    missing_fac_df = taf_prj_fac.loc[~mask]
    missing_fac_df.facility_id.unique()

    fill_yrs = range(2011, 2016)
    fill_missing_skf = pd.DataFrame(
        {
            "facility_id": ["skf"] * len(fill_yrs),
            "sysyear": fill_yrs,
            "proj_fac": [1] * len(fill_yrs),
        }
    )
    taf_prj_fac = pd.concat([taf_prj_fac, fill_missing_skf])
    assert all(
        taf_prj_fac.groupby("facility_id").sysyear.transform("count") == 35
    ), "Got project factors for all years."

    # Add projection factors to the ops data.
    taf_ops2019 = ops2019.merge(taf_prj_fac, on="facility_id", how="left").sort_values(
        by=["facility_id", "sysyear"]
    )

    # Commercial airport projection factors.
    taf_ops2019_commercial = taf_ops2019.loc[
        lambda df: df.facility_group == "Commercial"
    ]
    assert all(
        ~taf_ops2019_commercial.proj_fac.isna().values
    ), "Check for missing projection factors for commercial."

    # Update 2020 projection factors based on the OPSNET and airport website
    # data.
    pd.ExcelFile(path_com_2020_ops).sheet_names
    obs_opsnet_com_2020_ops = pd.read_excel(
        path_com_2020_ops, "airport_ops_opsnet", usecols=["facility_id", "proj_fac"]
    )
    obs_opsnet_com_2020_ops_1 = obs_opsnet_com_2020_ops.rename(
        columns={"proj_fac": "proj_fac_obs_opsnet_2020"}
    ).assign(sysyear=2020)
    taf_ops2019_commercial_1 = taf_ops2019_commercial.merge(
        obs_opsnet_com_2020_ops_1, on=["facility_id", "sysyear"], how="left"
    )
    taf_ops2019_commercial_1.loc[
        lambda df: ~df.proj_fac_obs_opsnet_2020.isna(), "proj_fac"
    ] = taf_ops2019_commercial_1.loc[
        lambda df: ~df.proj_fac_obs_opsnet_2020.isna(), "proj_fac_obs_opsnet_2020"
    ]
    # Reliever airport projection factors.
    taf_ops2019_rel = taf_ops2019.loc[lambda df: df.facility_group == "Reliever"]
    assert all(
        ~taf_ops2019_rel.proj_fac.isna().values
    ), "Check for missing projection factors for relievers."

    # TASP airports
    taf_ops2019_tasp = taf_ops2019.loc[lambda df: df.facility_group == "TASP"]
    taf_ops2019_tasp_filled = fill_tasp_arpts_prj(taf_ops2019_tasp)
    assert all(
        ~taf_ops2019_tasp_filled.proj_fac.isna().values
    ), "Check for missing projection factors for TASP."

    taf_ops2019_mil = ops2019.loc[lambda df: (df.facility_group == "Military")]
    taf_ops2019_med = ops2019.loc[lambda df: (df.facility_group == "Medical")]

    taf_ops2019_oth = ops2019.loc[
        lambda df: (
            ~df.facility_group.isin(
                ["Commercial", "Reliever", "TASP", "Medical", "Military"]
            )
        )
    ]

    # Get the aeo projection factors for remaining facility groups.
    aeo_proj_fac = pd.read_excel(
        path_aeo_proj_fac,
        "proj_fac",
        dtype={"Year": "float", "mil_proj_fac": "float", "com_av_proj_fac": "float"},
    )

    taf_ops2019_med_1 = (
        taf_ops2019_med.assign(sysyear=[range(2011, 2051)] * len(taf_ops2019_med))
        .explode("sysyear")
        .assign(proj_fac=1)
    )
    taf_ops2019_mil_1 = (
        taf_ops2019_mil.assign(merge_col="x")
        .merge(aeo_proj_fac.assign(merge_col="x"), on="merge_col", how="left")
        .assign(proj_fac=lambda df: df.mil_proj_fac)
        .drop(columns=["merge_col", "mil_proj_fac", "com_av_proj_fac"])
        .rename(columns={"Year": "sysyear"})
    )
    taf_ops2019_oth_1 = (
        taf_ops2019_oth.assign(merge_col="x")
        .merge(aeo_proj_fac.assign(merge_col="x"), on="merge_col", how="left")
        .assign(proj_fac=lambda df: df.com_av_proj_fac)
        .drop(columns=["merge_col", "mil_proj_fac", "com_av_proj_fac"])
        .rename(columns={"Year": "sysyear"})
    )

    taf_df_ops2019_1 = pd.concat(
        [
            taf_ops2019_commercial_1,
            taf_ops2019_rel,
            taf_ops2019_tasp_filled,
            taf_ops2019_med_1,
            taf_ops2019_mil_1,
            taf_ops2019_oth_1,
        ]
    )

    # Fill 2046 to 2050 projection factors.
    fac_with_46_50_data = taf_df_ops2019_1.loc[
        lambda df: (df.sysyear == 2046), "facility_id"
    ].values
    taf_df_ops2019_46_50_prst = taf_df_ops2019_1.loc[
        lambda df: (df.facility_id.isin(fac_with_46_50_data))
    ]

    taf_df_ops_fill_46_50 = taf_df_ops2019_1.loc[
        lambda df: ~(df.facility_id.isin(fac_with_46_50_data))
    ]
    fill_values_46_50 = (
        taf_df_ops_fill_46_50.loc[lambda df: df.sysyear == 2045]
        .assign(
            sysyear=[range(2045, 2051)]
            * len(taf_df_ops_fill_46_50.facility_id.unique())
        )
        .explode("sysyear")
    )

    filled_values_45_50 = pd.concat(
        [taf_df_ops_fill_46_50.loc[lambda df: df.sysyear < 2045], fill_values_46_50]
    )

    taf_df_ops2019_46_50_all = pd.concat(
        [taf_df_ops2019_46_50_prst, filled_values_45_50]
    )

    taf_df_ops2019_46_50_all.sort_values(["facility_id", "sysyear"], inplace=True)

    assert (
        len(taf_df_ops2019_46_50_all.facility_id.unique()) == 2037
    ), "There should be 2037 facilities."
    assert all(
        np.ravel(~taf_df_ops2019_46_50_all[["proj_fac"]].isna().values)
    ), "Check for na values."
    assert all(
        (taf_df_ops2019_46_50_all.groupby(["facility_id"]).sysyear.count() == 40).values
    ), "Check for missing years."

    taf_df_ops2019_grp = taf_df_ops2019_46_50_all.groupby("facility_group")

    path_proj_fac_clean = Path.home().joinpath(
        PATH_INTERIM, "proj_fac_axb_07_11_2021.xlsx"
    )
    writer = pd.ExcelWriter(path_proj_fac_clean, engine="xlsxwriter")
    for shnm, df in taf_df_ops2019_grp:
        df.to_excel(writer, shnm.replace("/", "_"), index=False)
    writer.save()
