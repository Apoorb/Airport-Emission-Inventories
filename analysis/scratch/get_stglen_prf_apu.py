
import pandas as pd
import numpy as np
import urllib
from pathlib import Path
from sqlalchemy import create_engine
from airportei.utilis import PATH_INTERIM, connect_to_sql_server, get_snake_case_dict


if __name__ == "__main__":
    path_imputed_ops = Path.home().joinpath(
        PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx"
    )
    ops2019 = pd.read_excel(path_imputed_ops)
    ops2019_com = ops2019.loc[lambda df: df.facility_group == "Commercial"]
    ops2019_com_dfw = ops2019_com.loc[lambda df: df.facility_id == "dfw"]
    dfw_ops_2019 = ops2019_com_dfw.annual_operations.values[0]

    path_fleetmix_clean = Path.home().joinpath(
        PATH_INTERIM, "fleetmix_axb_07_05_2021.xlsx"
    )
    fleetmix = pd.read_excel(path_fleetmix_clean, "Commercial")
    fleetmix_dfw = fleetmix.loc[lambda df: df.facility_id == "dfw",
        ["facility_id", "annual_operations", "fleetmix", "closest_airframe_id_aedt",
                                "eng_id"]]

    fleetmix_dfw_1 = (
        fleetmix_dfw
        .assign(
            ops_fleet=lambda df: df.annual_operations * df.fleetmix
        )
        .rename(columns={"eng_id": "engine_id", "closest_airframe_id_aedt": "airframe_id"})
    )
    fleetmix_dfw_1.fleetmix.sum()
    conn = connect_to_sql_server(database_nm="FLEET")
    eng_df = pd.read_sql("SELECT * FROM [dbo].[FLT_ENGINES]", conn)
    airfm_df = pd.read_sql("SELECT * FROM [dbo].[FLT_AIRFRAMES]", conn)

    eng_df_1 = (eng_df
     .rename(columns=get_snake_case_dict(eng_df))
     .filter(items=["engine_id", "engine_code"])
     )

    airfm_df_1 = (airfm_df
     .rename(columns=get_snake_case_dict(airfm_df))
     .filter(items=["airframe_id", "model"])
     .rename(columns={"model": "arfm_mod"})
     )


    fleetmix_dfw_1 = fleetmix_dfw_1.merge(eng_df_1, on="engine_id",
                                        how="left").merge(
        airfm_df_1, on="airframe_id", how="left")
    # Get Equipment
    equip_db = pd.read_sql("SELECT * FROM [dbo].[FLT_EQUIPMENT]", conn)
    equip_db_1 = (
        equip_db
        .rename(columns=get_snake_case_dict(equip_db))
    )
    fleetmix_dfw_eqp = (
        fleetmix_dfw_1
        .merge(
            equip_db_1,
            on=["airframe_id", "engine_id"],
            how="left"
        )
    )

    fleetmix_dfw_eqp.duplicated(["airframe_id", "engine_id"]).sum()
