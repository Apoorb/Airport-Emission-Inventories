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
            "airframe_id",
            "engine_id",
            "engine_mod_id",
            "anp_airplane_id",
            "anp_helicopter_id"
        ],
    ]

    fleetmix_dfw_1 = fleetmix_dfw.assign(
        ops_fleet=lambda df: df.annual_operations * df.fleetmix
    )
    conn = connect_to_sql_server(database_nm="FLEET")

    airfrm = pd.read_sql("SELECT * FROM [dbo].[FLT_AIRFRAMES]", conn)
    engine = pd.read_sql("SELECT * FROM [dbo].[FLT_ENGINES]", conn)

    anp_arp_prof = pd.read_sql(
        "SELECT * FROM [dbo].[FLT_ANP_AIRPLANE_PROFILES] WHERE PROF_ID1 = "
        "'STANDARD'",
        conn)

    def_apu = pd.read_sql(
        "SELECT * FROM [FLEET].[dbo].[FLT_APU]",
        conn)
    apu_name = pd.read_sql(
        "SELECT * FROM [dbo].[FLT_APU_DEFAULTS]",
        conn)
    gse_ac_def = pd.read_sql(
        "SELECT * FROM [dbo].[FLT_GSE_AC_DEFAULTS]",
        conn)
    anp_arp_prof_1 = (
        anp_arp_prof
        .rename(columns=get_snake_case_dict(anp_arp_prof))
    )
    def_apu_1 = (
        def_apu
        .rename(columns=get_snake_case_dict(def_apu))
    )
    apu_name_1 = (
        apu_name
        .rename(columns=get_snake_case_dict(apu_name))
    )
    gse_ac_def_1 = (
        gse_ac_def
        .rename(columns=get_snake_case_dict(gse_ac_def))
        .filter(items=["airframe_id", "def_op_time_dep", "def_op_time_arr"])
    )
    def_apu_2 = (
        def_apu_1
        .merge(apu_name_1, on="apu_id")
        .filter(items=["apu_id", "apu_name", "airframe_id", "user_defined"])
    )


    fleetmix_dfw_2 = (
        fleetmix_dfw_1
        .merge(
            anp_arp_prof_1,
            left_on="anp_airplane_id",
            right_on="acft_id",
            how="left"
        )
    )

    fleetmix_dfw_3 = (
        fleetmix_dfw_2
        .merge(
            def_apu_2,
            on="airframe_id",
            how="left"
        )
    )

