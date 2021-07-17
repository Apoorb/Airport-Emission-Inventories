"""
Develop fleet mix from TFMSC data.
Created on: 7/08/2021
Created by: Apoorba Bibeka
"""
from pathlib import Path
import pandas as pd
import numpy as np
from airportei.utilis import PATH_RAW, PATH_INTERIM, get_snake_case_dict, \
    connect_to_sql_server


def clean_tfmsc(tfmsc_, tfmsc_aedt_map_):
    """
    Remove special characters from the TFMSC data.
    Parameters
    ----------
    tfmsc_

    Returns
    -------

    """
    tfmsc_1_ = (
        tfmsc_.rename(columns=get_snake_case_dict(tfmsc_df))
        .rename(columns={"location_id": "facility_id"})
        .loc[
            lambda df: ~(
                df.aircraft_type_id.isna()
                | df.aircraft_type.str.contains("unknown")
                | df.aircraft_type.str.contains("#VALUE!")
            )
        ]
        .assign(
            aircraft_id=lambda df: df.aircraft_id.str.lower().str.strip(),
            facility_id=lambda df: df.facility_id.str.strip().str.lower(),
        )
        .groupby(["year_id", "facility_id", "airport", "aircraft_id"])
        .agg(total_ops_by_craft=("total_ops", "sum"))
        .reset_index()
    )

    tfmsc_2_ = tfmsc_1_.merge(tfmsc_aedt_map_, on="aircraft_id").assign(
        total_ops_by_arpt=lambda df: df.groupby(
            ["year_id", "facility_id", "airport"]
        ).total_ops_by_craft.transform("sum"),
        fleetmix=lambda df: df.total_ops_by_craft / df.total_ops_by_arpt,
    )

    after_ops = tfmsc_2_.total_ops_by_craft.sum()
    before_ops = tfmsc_1_.total_ops_by_craft.sum()
    percent_ops_lost = 100 * (before_ops - after_ops) / before_ops
    print(
        f"Removing {percent_ops_lost} percent operations due to unknown "
        f"aircraft ids/ types in AEDT."
    )

    merge_dat_loss_ = pd.merge(
        tfmsc_1_.groupby("facility_id").total_ops_by_craft.sum(),
        tfmsc_2_.groupby("facility_id").total_ops_by_craft.sum(),
        left_index=True,
        right_index=True,
        how="outer",
        suffixes=["_before", "_after"],
    )
    merge_dat_loss_["ops_lost"] = (
        merge_dat_loss_.total_ops_by_craft_before
        - merge_dat_loss_.total_ops_by_craft_after
    )
    merge_dat_loss_ = merge_dat_loss_.assign(
        ops_lost=lambda df: (
            df.total_ops_by_craft_before - df.total_ops_by_craft_after
        ),
        percent_ops_lost=lambda df: 100 * (df.ops_lost / df.total_ops_by_craft_before),
    )
    assert (
        set(tfmsc_1_.facility_id.values) - set(tfmsc_2_.facility_id.values)
    ) == set(), "All facilities are retained."

    tfmsc_3_ = tfmsc_2_.assign(
        aircraft_type_id=lambda df: (
            df.aircraft_type_id.replace(
                ["Myst�re", "A�rospatiale", "Br�guet", "#NAME?"],
                ["Mystere", "Aerospatiale", "Breguet", -999],
                regex=True,
            )
        ),
        aircraft_type=lambda df: (
            df.aircraft_type.replace(
                ["Myst�re", "A�rospatiale", "Br�guet", "- unknown"],
                ["Mystere", "Aerospatiale", "Breguet", -999],
                regex=True,
            )
        ),
    )
    return [tfmsc_3_, merge_dat_loss_]


def fill_heli_fleet(tfmsc_df_ops2019_):
    heli_fleetmix = pd.DataFrame(
        {
            "merge_key": ["x", "x", "x"],
            "aircraft_id": [5238, 5271, 5331],
            "closest_airframe_id_aedt": [5238, 5271, 5331],
            "aircraft_type": ["agusta a119", "eurocopter as 350 b1", "bell 429"],
            "closest_airframe_type_aedt": [
                "agusta a119",
                "eurocopter as 350 b1",
                "bell 429",
            ],
            "fleetmix": [1 / 3, 1 / 3, 1 / 3],
            "source": [
                "https://www.helis.com/database/org/Texas-Helicopters/",
                "https://www.helis.com/database/org/Texas-Helicopters/",
                "https://www.helis.com/database/org/Texas-Helicopters/",
            ],
        }
    )
    tfmsc_df_ops2019_cat_na_heli = tfmsc_df_ops2019_.loc[
        lambda df: df.facility_type == "HELIPORT"
    ]
    tfmsc_df_ops2019_cat_na_heli_ = (
        tfmsc_df_ops2019_cat_na_heli.assign(merge_key="x")
        .drop(
            columns=[
                "aircraft_id",
                "aircraft_type_id",
                "aircraft_type",
                "closest_airframe_id_aedt",
                "closest_airframe_type_aedt",
                "fleetmix",
            ]
        )
        .merge(heli_fleetmix, on="merge_key")
        .drop(columns=["source", "merge_key"])
    )
    return tfmsc_df_ops2019_cat_na_heli_


def fill_tasp_mil_arpts(tfmsc_df_ops2019_cat_):
    """
    Use TASP and Military airports in the same district to fill the TASP and
    Military airport fleet mix data.
    Parameters
    ----------
    tfmsc_df_ops2019_cat_

    Returns
    -------

    """
    tfmsc_df_ops2019_cat_na = tfmsc_df_ops2019_cat_.loc[
        lambda df: df.fleetmix.isna()
    ].sort_values(["district_tx_boundar", "facility_id", "aircraft_id"])

    tfmsc_df_ops2019_cat_non_na = tfmsc_df_ops2019_cat_.loc[
        lambda df: ~df.fleetmix.isna()
    ].sort_values(["district_tx_boundar", "facility_id", "aircraft_id"])

    tfmsc_df_ops2019_cat_non_na_temp = tfmsc_df_ops2019_cat_non_na.copy()

    tfmsc_df_ops2019_cat_na_arpt = tfmsc_df_ops2019_cat_na.loc[
        lambda df: df.facility_type != "HELIPORT"
    ]

    if all(tfmsc_df_ops2019_cat_non_na_temp.facility_group == "Military"):
        bexar_ops = tfmsc_df_ops2019_cat_non_na_temp.loc[
            lambda df: df.county_arpt == "bexar"
        ].assign(county_arpt="travis", district_tx_boundar="Austin")
        travis_ops = bexar_ops
        tfmsc_df_ops2019_cat_non_na_temp = pd.concat(
            [tfmsc_df_ops2019_cat_non_na_temp, travis_ops]
        )
    # Check if we can use the airports from the same district to fill the
    # TASP data.
    assert (
        set(tfmsc_df_ops2019_cat_na_arpt.district_tx_boundar.unique())
        - set(tfmsc_df_ops2019_cat_non_na.district_tx_boundar.unique())
    ) == set(), "We cannot use districts to fill fleetmix."
    list_fill_df = []
    for indx, row in tfmsc_df_ops2019_cat_na_arpt.iterrows():
        df_fil_district = tfmsc_df_ops2019_cat_non_na_temp.loc[
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
                    "aircraft_id",
                    "aircraft_type_id",
                    "aircraft_type",
                    "closest_airframe_id_aedt",
                    "closest_airframe_type_aedt",
                    "fleetmix",
                    "ops_diff",
                    "ops_per_diff",
                    "filled_from_facility_id",
                ]
            )
        )
        fill_row = row.to_frame().T

        fill_row.drop(
            columns=[
                "aircraft_id",
                "aircraft_type_id",
                "aircraft_type",
                "closest_airframe_id_aedt",
                "closest_airframe_type_aedt",
                "fleetmix",
            ],
            inplace=True,
        )

        fill_df = fill_row.merge(df_fil_district_fil_ops, on="district_tx_boundar")
        list_fill_df.append(fill_df)

    arpt_fill_dfs = pd.concat(list_fill_df)

    tfmsc_df_ops2019_tasp_1 = pd.concat([tfmsc_df_ops2019_cat_non_na, arpt_fill_dfs])
    return tfmsc_df_ops2019_tasp_1


def fill_farm_arpts(tfmsc_df_ops2019_farmranch_):
    """
    Use the one farm and ranch airport with fleet mix to fill fleetmix for
    other airports.
    Parameters
    ----------
    tfmsc_df_ops2019_farmranch_

    Returns
    -------

    """
    farmranch_filler = tfmsc_df_ops2019_farmranch_.loc[
        lambda df: ~df.aircraft_type_id.isna()
    ]
    assert len(farmranch_filler.facility_id.unique()) == 1, (
        "This function assumes that the we have fleet mix data for only one "
        "airport. Modify the function when this assumption is false."
    )
    farmranch_filler = (
        farmranch_filler.rename(columns={"facility_id": "filled_from_facility_id"})
        .assign(merge_key="x")
        .filter(
            items=[
                "merge_key",
                "aircraft_id",
                "aircraft_type_id",
                "aircraft_type",
                "closest_airframe_id_aedt",
                "closest_airframe_type_aedt",
                "fleetmix",
                "filled_from_facility_id",
            ]
        )
    )
    tfmsc_df_ops2019_farmranch_na_filled = (
        tfmsc_df_ops2019_farmranch_.loc[lambda df: df.aircraft_id.isna()]
        .assign(merge_key="x")
        .drop(
            columns=[
                "aircraft_id",
                "aircraft_type_id",
                "aircraft_type",
                "closest_airframe_id_aedt",
                "closest_airframe_type_aedt",
                "fleetmix",
            ]
        )
        .merge(farmranch_filler, on="merge_key", how="left")
        .drop(columns="merge_key")
    )

    tfmsc_df_ops2019_farmranch_1 = pd.concat(
        [
            tfmsc_df_ops2019_farmranch_na_filled,
            tfmsc_df_ops2019_farmranch_.loc[lambda df: ~df.aircraft_id.isna()],
        ]
    )
    return tfmsc_df_ops2019_farmranch_1


def fill_othpuair_arpts(tfmsc_df_ops2019_othpuair_):
    """
    Use airport with lowest operations to get fleet mix for other airports.
    Parameters
    ----------
    tfmsc_df_ops2019_othpuair_

    Returns
    -------

    """
    othpuair_filler = (
        tfmsc_df_ops2019_othpuair_.loc[lambda df: (~df.aircraft_id.isna())]
        .loc[lambda df: (df.annual_operations == min(df.annual_operations))]
        .assign(merge_key="x")
        .rename(columns={"facility_id": "filled_from_facility_id"})
        .filter(
            items=[
                "merge_key",
                "aircraft_id",
                "aircraft_type_id",
                "aircraft_type",
                "closest_airframe_id_aedt",
                "closest_airframe_type_aedt",
                "fleetmix",
                "filled_from_facility_id",
            ]
        )
    )
    tfmsc_df_ops2019_othpuair_na_filled = (
        tfmsc_df_ops2019_othpuair_.loc[lambda df: df.aircraft_id.isna()]
        .assign(merge_key="x")
        .drop(
            columns=[
                "aircraft_id",
                "aircraft_type_id",
                "aircraft_type",
                "closest_airframe_id_aedt",
                "closest_airframe_type_aedt",
                "fleetmix",
            ]
        )
        .merge(othpuair_filler, on="merge_key", how="left")
        .drop(columns="merge_key")
    )

    tfmsc_df_ops2019_othpuair_1 = pd.concat(
        [
            tfmsc_df_ops2019_othpuair_na_filled,
            tfmsc_df_ops2019_othpuair_.loc[lambda df: ~df.aircraft_id.isna()],
        ]
    )
    return tfmsc_df_ops2019_othpuair_1


def fill_othprair_arpts(tfmsc_df_ops2019_othprair_, tfmsc_df_ops2019_othpuair_):
    """
    Use PU airport with lowest operations to get fleet mix for other airports.

    Parameters
    ----------
    tfmsc_df_ops2019_othprair_
    tfmsc_df_ops2019_othpuair_

    Returns
    -------

    """
    othpuair_filler = (
        tfmsc_df_ops2019_othpuair_.loc[lambda df: (~df.aircraft_id.isna())]
        .loc[lambda df: (df.annual_operations == min(df.annual_operations))]
        .assign(merge_key="x")
        .rename(columns={"facility_id": "filled_from_facility_id"})
        .filter(
            items=[
                "merge_key",
                "aircraft_id",
                "aircraft_type_id",
                "aircraft_type",
                "closest_airframe_id_aedt",
                "closest_airframe_type_aedt",
                "fleetmix",
                "filled_from_facility_id",
            ]
        )
    )
    tfmsc_df_ops2019_othprair_na_filled = (
        tfmsc_df_ops2019_othprair_.loc[lambda df: df.aircraft_id.isna()]
        .assign(merge_key="x")
        .drop(
            columns=[
                "aircraft_id",
                "aircraft_type_id",
                "aircraft_type",
                "closest_airframe_id_aedt",
                "closest_airframe_type_aedt",
                "fleetmix",
            ]
        )
        .merge(othpuair_filler, on="merge_key", how="left")
        .drop(columns="merge_key")
    )

    tfmsc_df_ops2019_othprair_1 = pd.concat(
        [
            tfmsc_df_ops2019_othprair_na_filled,
            tfmsc_df_ops2019_othprair_.loc[lambda df: ~df.aircraft_id.isna()],
        ]
    )
    return tfmsc_df_ops2019_othprair_1


def get_aedt_engine_id_map():
    """Add engine ids."""
    conn = connect_to_sql_server(database_nm="FLEET")
    def_engine_db = pd.read_sql("SELECT * FROM [dbo].[FLT_DEFAULT_ENGINES]", conn)
    equip_db = pd.read_sql("SELECT * FROM [dbo].[FLT_EQUIPMENT]", conn)
    conn.close()
    path_tfmsc_aedt = Path.home().joinpath(
        PATH_INTERIM, "tfmsc_aedt_mapping", "tfmsc_aircrafts_v4.xlsx"
    )
    tfmsc_aedt_map = pd.read_excel(path_tfmsc_aedt, index_col=0)
    tfmsc_aedt_map_1 = tfmsc_aedt_map.loc[
        lambda df: ~df.closest_airframe_id_aedt.isna()
    ].drop(
        columns=[
            "justification",
            "useful information",
            "aircraft_id_list",
            "user_class",
            "facility_name",
        ]
    )
    equip_db_fil = (
        equip_db.rename(columns=get_snake_case_dict(equip_db))
        .sort_values(["airframe_id", "engine_id"])
        .groupby("airframe_id")
        .engine_id.first()
        .reset_index()
    )

    tfmsc_aedt_map_1_eng_na_filled = (
        tfmsc_aedt_map_1
        .merge(
            equip_db_fil,
            left_on="closest_airframe_id_aedt",
            right_on="airframe_id",
            how="left",
    ))
    assert tfmsc_aedt_map_1_eng_na_filled.engine_id.isna().sum() == 0
    return tfmsc_aedt_map_1_eng_na_filled


if __name__ == "__main__":
    path_ops2019_clean = Path.home().joinpath(
        PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx"
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
    path_tfmsc = Path.home().joinpath(PATH_RAW, "madhu_files", "FAA_2019TFMSC.csv")
    path_tfmsc_aedt = Path.home().joinpath(
        PATH_INTERIM, "tfmsc_aedt_mapping", "tfmsc_aircrafts_v4.xlsx"
    )
    tfmsc_df = pd.read_csv(path_tfmsc)
    tfmsc_aedt_map = pd.read_excel(path_tfmsc_aedt, index_col=0)
    tfmsc_aedt_map_1 = tfmsc_aedt_map.loc[
        lambda df: ~df.closest_airframe_id_aedt.isna()
    ].drop(
        columns=[
            "justification",
            "useful information",
            "aircraft_id_list",
            "user_class",
            "facility_name",
        ]
    )
    tfmsc_df_cln, merge_dat_loss = clean_tfmsc(tfmsc_df, tfmsc_aedt_map_1)
    tfmsc_df_ops2019 = ops2019.merge(
        tfmsc_df_cln, on="facility_id", how="left"
    ).sort_values(by=["facility_id", "aircraft_id"])

    merge_dat_loss_meta = merge_dat_loss.merge(
        ops2019, on="facility_id", how="left"
    ).sort_values(by=["facility_id"])
    path_dataloss_sum = Path.home().joinpath(
        PATH_INTERIM, "data_loss_tfmsc_aedt_merge.xlsx"
    )
    merge_dat_loss_meta.to_excel(path_dataloss_sum, index=False)

    tfmsc_df_ops2019_arpt = tfmsc_df_ops2019.loc[
        lambda df: df.facility_type != "HELIPORT"
    ]
    tfmsc_df_ops2019_heli = fill_heli_fleet(tfmsc_df_ops2019)
    assert np.allclose(
        tfmsc_df_ops2019_heli.groupby("facility_id").fleetmix.sum().values,
        1
    )
    tfmsc_df_ops2019_com = tfmsc_df_ops2019_arpt.loc[
        lambda df: df.facility_group == "Commercial"
    ]
    com_fac = set(
        ops2019.loc[lambda df: df.facility_group == "Commercial", "facility_id"].values
    )
    assert np.allclose(
        tfmsc_df_ops2019_com.groupby("facility_id").fleetmix.sum().values,
        1
    )
    assert (
        set(tfmsc_df_ops2019_com.facility_id.unique()) == com_fac
    ), "Some commercial airports did not get captured. Check data."
    tfmsc_df_ops2019_rel = tfmsc_df_ops2019_arpt.loc[
        lambda df: df.facility_group == "Reliever"
    ]
    rel_fac = set(
        ops2019.loc[lambda df: df.facility_group == "Reliever", "facility_id"].values
    )
    assert np.allclose(
        tfmsc_df_ops2019_rel.groupby("facility_id").fleetmix.sum().values,
        1
    )
    assert (
        set(tfmsc_df_ops2019_rel.facility_id.unique()) == rel_fac
    ), "Some reliever airports did not get captured. Check data."
    assert all(~tfmsc_df_ops2019_rel.isna()), "Check for na values."
    tfmsc_df_ops2019_tasp = tfmsc_df_ops2019_arpt.loc[
        lambda df: df.facility_group == "TASP"
    ]
    tfmsc_df_ops2019_tasp_filled = fill_tasp_mil_arpts(tfmsc_df_ops2019_tasp)
    tasp_fac = set(
        ops2019.loc[
            lambda df: (df.facility_group == "TASP") & (df.facility_type == "AIRPORT"),
            "facility_id",
        ].values
    )
    assert np.allclose(
        tfmsc_df_ops2019_tasp_filled.groupby("facility_id").fleetmix.sum().values,
        1
    )
    assert (
        set(tfmsc_df_ops2019_tasp_filled.facility_id.unique()) == tasp_fac
    ), "Some TASP airports did not get captured. Check data."
    tfmsc_df_ops2019_med = tfmsc_df_ops2019_arpt.loc[
        lambda df: df.facility_group == "Medical"
    ]
    assert len(tfmsc_df_ops2019_med) == 0, "All medical facilities are " "heliports."
    # Fill manually
    tfmsc_df_ops2019_mil = tfmsc_df_ops2019_arpt.loc[
        lambda df: df.facility_group == "Military"
    ]
    tfmsc_df_ops2019_mil_filled = fill_tasp_mil_arpts(tfmsc_df_ops2019_mil)
    assert np.allclose(
        tfmsc_df_ops2019_mil_filled.groupby("facility_id").fleetmix.sum().values,
        1
    )
    tfmsc_df_ops2019_farmranch = tfmsc_df_ops2019_arpt.loc[
        lambda df: df.facility_group == "Farm/Ranch"
    ]
    assert (
        sum(
            tfmsc_df_ops2019_farmranch.groupby(["facility_id"]).aircraft_id.count() >= 1
        )
        == 1
    ), (
        "1 airport with fleet mix data. fill_farm_arpts() is based on the "
        "premise that we have data for only 1 farm and ranch airport. Modify "
        "function if this assertion is false."
    )
    tfmsc_df_ops2019_farmranch_filled = fill_farm_arpts(tfmsc_df_ops2019_farmranch)
    assert np.allclose(
        tfmsc_df_ops2019_farmranch_filled.groupby("facility_id").fleetmix.sum().values,
        1
    )
    tfmsc_df_ops2019_othpuair = tfmsc_df_ops2019_arpt.loc[
        lambda df: df.facility_group == "Other_PU_Airports"
    ]
    tfmsc_df_ops2019_othpuair_filled = fill_othpuair_arpts(tfmsc_df_ops2019_othpuair)
    assert np.allclose(
        tfmsc_df_ops2019_othpuair_filled.groupby("facility_id").fleetmix.sum().values,
        1
    )
    tfmsc_df_ops2019_othprair = tfmsc_df_ops2019_arpt.loc[
        lambda df: df.facility_group == "Other_PR_Airports"
    ]
    assert all(tfmsc_df_ops2019_othprair.aircraft_id.isna().values), (
        "Except no airport having fleet mix. Use generic aircraft types and "
        "fleetmix."
    )
    tfmsc_df_ops2019_othprair_filled = fill_othprair_arpts(
        tfmsc_df_ops2019_othprair, tfmsc_df_ops2019_othpuair
    )
    assert np.allclose(
        tfmsc_df_ops2019_othprair_filled.groupby("facility_id").fleetmix.sum().values,
        1
    )
    tfmsc_df_ops2019_heli.facility_group.unique()
    assert set(["Other_PR_Heliports", "Other_PU_Heliports", "Medical"]) not in set(
        tfmsc_df_ops2019_arpt.facility_group.unique()
    ), 'There shouldnt be any airport in "Other_PR_Heliports", "Other_PU_Heliports", "Medical"'

    tfmsc_df_ops2019_1 = pd.concat(
        [
            tfmsc_df_ops2019_heli,
            tfmsc_df_ops2019_com,
            tfmsc_df_ops2019_rel,
            tfmsc_df_ops2019_tasp_filled,
            tfmsc_df_ops2019_mil_filled,
            tfmsc_df_ops2019_farmranch_filled,
            tfmsc_df_ops2019_othpuair_filled,
            tfmsc_df_ops2019_othprair_filled,
        ]
    )
    assert (
        len(tfmsc_df_ops2019_1.facility_id.unique()) == 2037
    ), "There should be 2037 facilities."
    assert all(
        np.ravel(
            ~tfmsc_df_ops2019_1[
                ["closest_airframe_id_aedt", "closest_airframe_type_aedt", "fleetmix"]
            ]
            .isna()
            .values
        )
    ), "Check for na values."
    assert np.allclose(
        tfmsc_df_ops2019_1.groupby("facility_id").fleetmix.sum().values,
        1)
    engine_map = get_aedt_engine_id_map()
    tfmsc_df_ops2019_2 = pd.merge(
        tfmsc_df_ops2019_1,
        engine_map,
        on="closest_airframe_id_aedt"
    )
    assert np.allclose(
        tfmsc_df_ops2019_2.groupby("facility_id").fleetmix.sum().values,
        1)
    assert (tfmsc_df_ops2019_2.eng_id.isna().sum() == 0), (
        "Check for missing engine ids."
    )
    tfmsc_df_ops2019_grp = tfmsc_df_ops2019_2.groupby("facility_group")

    path_fleetmix_clean = Path.home().joinpath(
        PATH_INTERIM, "fleetmix_axb_07_05_2021.xlsx"
    )
    writer = pd.ExcelWriter(path_fleetmix_clean, engine="xlsxwriter")
    for shnm, df in tfmsc_df_ops2019_grp:
        df.to_excel(writer, shnm.replace("/", "_"), index=False)
    writer.save()

