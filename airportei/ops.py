import os
import re
import numpy as np
import pandas as pd
import itertools
import pyodbc
from airportei.utilis import (PATH_RAW, get_snake_case_dict,
                              connect_to_sql_server)


def get_nfdc_ops(path_nfdc_may21_: str) -> dict[str, dict[str, list]]:
    """
    Process NFDC/ 5010 dataset.
    Parameters
    ----------
    path_nfdc_may21_: str
        Path to NFDC May 21, 2021 data.
    Returns
    -------
    dict:
        Processed NFDC data from May 2021 and a dict of unique categories for
        facility type, airport_status_code, ownership, use, medical_use,
        other_services, and military_joint_use.
    """
    nfdc_facilities_may21 = pd.read_csv(path_nfdc_may21_)
    nfdc_rename_dict = get_snake_case_dict(nfdc_facilities_may21)
    keep_cols_nfdc_facilities = [
        "location_id",
        "facility_name",
        "effective_date",
        "city",
        "county",
        "state",
        "type",
        "certification_type_date",
        "airport_status_code",
        "ownership",
        "use",
        "medical_use",
        "other_services",
        "military_joint_use",
        "operations_commercial",
        "operations_commuter",
        "operations_air_taxi",
        "operations_ga_local",
        "operations_ga_itin",
        "operations_military",
        "operations_date",
    ]
    nfdc_facilities_may21_1 = (
        nfdc_facilities_may21.rename(columns=nfdc_rename_dict)
        .loc[lambda df: df.airport_status_code == "O"]
        .filter(items=keep_cols_nfdc_facilities)
        .rename(columns={"location_id": "facility_id", "type": "facility_type"})
        .assign(
            facility_id=lambda df: df.facility_id.replace("'", ""),
            effective_yr=lambda df: (df.effective_date.str.split("/", expand=True)[2]),
            total_ops=lambda df: (
                df.operations_commercial.fillna(0)
                + df.operations_commuter.fillna(0)
                + df.operations_air_taxi.fillna(0)
                + df.operations_ga_local.fillna(0)
                + df.operations_ga_itin.fillna(0)
                + df.operations_military.fillna(0)
            ),
        )
    )

    oth_serv_2d_list = list(
        map(
            lambda ls: ls.split(","),
            list(
                filter(
                    lambda x: x == x, nfdc_facilities_may21_1.other_services.unique()
                )
            ),
        )
    )

    oth_serv_1d_list = list(set(itertools.chain(*oth_serv_2d_list)))

    imp_columns_val_dict = {
        "facility_type": nfdc_facilities_may21_1.facility_type.unique(),
        "airport_status_code": nfdc_facilities_may21_1.airport_status_code.unique(),
        "ownership": nfdc_facilities_may21_1.ownership.unique(),
        "use": nfdc_facilities_may21_1.use.unique(),
        "medical_use": nfdc_facilities_may21_1.medical_use.unique(),
        "other_services": oth_serv_1d_list,
        "military_joint_use": nfdc_facilities_may21_1.military_joint_use.unique(),
    }

    return {
        "nfdc_facilities_may21_1": nfdc_facilities_may21_1,
        "imp_columns_val_dict": imp_columns_val_dict,
    }


if __name__ == "__main__":

    path_nfdc_may21 = os.path.join(PATH_RAW, "nfdc_facilities.csv")
    path_tfmsc_19 = os.path.join(PATH_RAW, "madhu_files", "FAA_2019TFMSC.csv")
    nfdc_may21_dict = get_nfdc_ops(path_nfdc_may21)
    nfdc_df = nfdc_may21_dict["nfdc_facilities_may21_1"]

    tfmsc_19_df = pd.read_csv(path_tfmsc_19)
    tfmsc_19_df_1 = tfmsc_19_df.rename(columns=get_snake_case_dict(tfmsc_19_df)).rename(
        columns={"location_id": "facility_id", "airport": "facility_name"}
    )

    tfmsc_19_df_2 = (
        tfmsc_19_df_1.assign(
            aircraft_id=lambda df: df.aircraft_id.fillna(-999),
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
            engine_type=lambda df: np.select(
                [
                    df.physical_class.str.strip().str.lower() == "piston",
                    df.physical_class.str.strip().str.lower() == "jet",
                    df.physical_class.str.strip().str.lower() == "turbine",
                    df.physical_class.str.strip() == "-",
                ],
                ["Piston", "Turbine", "Turbine", None],
                -999,
            ),
        )
        .groupby(
            [
                "facility_id",
                "facility_name",
                "aircraft_id",
                "aircraft_type_id",
                "aircraft_type",
                "user_class",
            ]
        )
        .agg(
            total_ops_by_airframe=("total_ops", "sum"),
            engine_type=("engine_type", "first"),
        )
        .reset_index()
    )

    fill_engine_type_df = (
        tfmsc_19_df_2[
            ["aircraft_id", "aircraft_type", "aircraft_type_id", "engine_type"]
        ]
        .loc[lambda df: df.aircraft_id != -999]
        .drop_duplicates(["aircraft_id", "aircraft_type", "engine_type"])
        .loc[lambda df: ~df.engine_type.isnull()]
        .sort_values(["aircraft_id"])
        .assign(
            aircraft_id=lambda df: df.aircraft_id.str.strip().str.lower(),
            aircraft_type=lambda df: df.aircraft_type.str.strip().str.lower(),
            aircraft_id_and_type=lambda df: df.aircraft_id + " " + df.aircraft_type,
            aircraft_id_list=lambda df: df.aircraft_id_and_type.str.lower().str.split(
                " "
            ),
        )
    )

    conn = connect_to_sql_server(database_nm="FLEET")

    aedt_3d_airframe = pd.read_sql("SELECT * FROM [dbo].[FLT_AIRFRAMES]", conn)
    aedt_3d_airframe_1 = (
        aedt_3d_airframe.rename(columns=get_snake_case_dict(aedt_3d_airframe))
        .rename(columns={"model": "aircraft_type"})
        .assign(aircraft_type=lambda df: df.aircraft_type.str.strip().str.lower())
        .sort_values("aircraft_type")
        .reset_index(drop=True)
    )

    fill_engine_type_df_test = fill_engine_type_df.merge(
        aedt_3d_airframe_1, on="aircraft_type", how="left"
    )

    tfmsc_19_df_2_has_aircraft_id = tfmsc_19_df_2.loc[lambda df: df.aircraft_id != -999]
    tfmsc_19_df_2_has_aircraft_id_no_engine_type = tfmsc_19_df_2_has_aircraft_id.loc[
        lambda df: df.engine_type.isnull()
    ]

    tfmsc_19_df_2_no_aircraft_id = tfmsc_19_df_2.loc[lambda df: df.aircraft_id == -999]

    no_aircraft_id_facilities = tfmsc_19_df_2_no_aircraft_id.facility_id.unique()

    tfmsc_19_df_2_no_aircraft_id_facilities_valid_data = tfmsc_19_df_2.loc[
        lambda df: (df.facility_id.isin(no_aircraft_id_facilities))
        & (df.aircraft_id != -999)
    ]

    tfmsc_19_df_2_no_aircraft_id_fill = pd.merge(
        tfmsc_19_df_2_no_aircraft_id,
        tfmsc_19_df_2_no_aircraft_id_facilities_valid_data,
        on=["facility_id", "facility_name", "user_class"],
        suffixes=["_missing", "_fill"],
        how="left",
    )

    tfmsc_19_df_2_no_aircraft_id_fill_missing = tfmsc_19_df_2_no_aircraft_id_fill.loc[
        lambda df: df.aircraft_type_fill.isna()
    ]

    len(tfmsc_19_df_2_no_aircraft_id_facilities_valid_data.facility_id.unique())

    tfmsc_19_df_1_bad_names = tfmsc_19_df_1.loc[
        lambda df: df.aircraft_type_id.str.contains(r"[^-\w /,;()<`]")
    ]

    assert all(tfmsc_19_df_2.aircraft_type_id.str.contains(r"[-\w /,;()<`]")), (
        "Above code all werid naming conventions (#NAME?, - unknown, "
        "�) in aircraft type and aircraft type id."
    )
