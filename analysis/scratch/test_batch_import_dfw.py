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
    fleetmix_dfw = fleetmix.loc[
        lambda df: df.facility_id == "dfw",
        [
            "facility_id",
            "annual_operations",
            "fleetmix",
            "closest_airframe_id_aedt",
            "eng_id",
        ],
    ]

    fleetmix_dfw_1 = fleetmix_dfw.assign(
        ops_fleet=lambda df: df.annual_operations * df.fleetmix
    )
    conn = connect_to_sql_server(database_nm="FLEET")
    eng_df = pd.read_sql("SELECT * FROM [dbo].[FLT_ENGINES]", conn)
    airfm_df = pd.read_sql("SELECT * FROM [dbo].[FLT_AIRFRAMES]", conn)
    conn.close()

    eng_df_1 = (
        eng_df.rename(columns=get_snake_case_dict(eng_df))
        .filter(items=["engine_id", "engine_code"])
        .rename(columns={"engine_id": "eng_id"})
    )

    airfm_df_1 = (
        airfm_df.rename(columns=get_snake_case_dict(airfm_df))
        .filter(items=["airframe_id", "model"])
        .rename(columns={"model": "arfm_mod"})
    )

    fleetmix_dfw_1.columns

    fleetmix_dfw_1 = fleetmix_dfw.merge(eng_df_1, on="eng_id", how="left").merge(
        airfm_df_1,
        left_on="closest_airframe_id_aedt",
        right_on="airframe_id",
        how="left",
    )
    fleetmix_dfw_1.iloc[0]
    conn = connect_to_sql_server(database_nm="2019_IAH_3152021")
    air_ops = pd.read_sql("SELECT * FROM [dbo].[AIR_OPERATION]", conn)

    conn = connect_to_sql_server(database_nm="dfw_study")
    cur = conn.cursor()
    cur.execute("DELETE FROM [dfw_study].[dbo].[AIR_OPERATION]")
    conn.close()

    conn = connect_to_sql_server(database_nm="dfw_study")
    cur = conn.cursor()
    cur.execute(
        "ALTER TABLE [dbo].[AIR_OPERATION] NOCHECK CONSTRAINT FK__AIR_OPERATION__AIRCRAFT_ID"
    )
    conn.close()
    quoted = urllib.parse.quote_plus(
        "DRIVER={SQL Server};SERVER=HMP-HVT3G63-LW0\SQLEXPRESS_AEDT;DATABASE=dfw_study"
    )
    engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(quoted))

    air_ops.to_sql(
        "AIR_OPERATION",
        schema="dbo",
        con=engine,
        chunksize=200,
        method="multi",
        index=False,
        if_exists="append",
    )
