import pandas as pd
import numpy as np
from pathlib import Path
from airportei.utilis import PATH_INTERIM, connect_to_sql_server, get_snake_case_dict
TESTING = True
if __name__ == "__main__":
    path_imputed_ops = Path.home().joinpath(
        PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx"
    )
    ops2019 = pd.read_excel(path_imputed_ops)
    ops2019_com = ops2019.loc[lambda df: df.facility_group == "Commercial"]
    ops2019_com_iah = ops2019_com.loc[lambda df: df.facility_id == "iah"]
    iah_ops_2019 = ops2019_com_iah.annual_operations.values[0]

    path_iah_fleetmix = Path.home().joinpath(
        PATH_INTERIM, "iah_airport_data", "iah_aedt_study_ops.csv"
    )
    iah_fleetmix = pd.read_csv(path_iah_fleetmix)

    iah_fleetmix_1 = (
        iah_fleetmix.rename(columns=get_snake_case_dict(iah_fleetmix))
        .drop(columns="operation_count")
        .assign(annual_operations=iah_ops_2019)
    )
    if TESTING:
        iah_fleetmix_1["annual_operations"] = iah_fleetmix["Operation Count"].sum() * 2
    else:
        "Do nothing."

    conn = connect_to_sql_server(database_nm="FLEET")
    eng_df = pd.read_sql("SELECT * FROM [dbo].[FLT_ENGINES]", conn)
    airfm_df = pd.read_sql("SELECT * FROM [dbo].[FLT_AIRFRAMES]", conn)
    equip_db = pd.read_sql("SELECT * FROM [dbo].[FLT_EQUIPMENT]", conn)

    eng_df_1 = (
        eng_df.rename(columns=get_snake_case_dict(eng_df))
        .rename(columns={"engine_code": "engine"})
        .filter(items=["engine_id", "engine"])
    )
    assert eng_df_1.duplicated("engine_id").sum() == 0, "Duplicated engine_id!"
    airfm_df_1 = (
        airfm_df.rename(columns=get_snake_case_dict(airfm_df))
        .filter(items=["airframe_id", "model"])
        .rename(columns={"model": "airframe"})
    )
    assert airfm_df_1.duplicated("airframe_id").sum() == 0, "Duplicated airframes!"

    equip_db_fil = (
        equip_db.rename(columns=get_snake_case_dict(equip_db))
        .sort_values(["airframe_id", "engine_id"])
        .groupby(["airframe_id", "engine_id"])
        .agg(
            engine_mod_id=("engine_mod_id", "first"),
            anp_airplane_id=("anp_airplane_id", "first"),
            anp_helicopter_id=("anp_helicopter_id", "first"),
            bada_id=("bada_id", "first"),
            bada4_id=("bada4_id", "first"),
        )
        .reset_index()
    )

    iah_fleetmix_2 = (
        iah_fleetmix_1.merge(airfm_df_1, on="airframe", how="left")
        .merge(eng_df_1, on="engine", how="left")
        .merge(equip_db_fil, on=["airframe_id", "engine_id"], how="left")
    )

    assert all(
        (
            (~iah_fleetmix_2.anp_airplane_id.isna())
            | (~iah_fleetmix_2.anp_helicopter_id.isna())
        )
    ), "Missing ANP ids. Can't assign profiles to non-anp crafts."

    iah_fleetmix_3 = (
        iah_fleetmix_2.rename(columns={"airframe_id": "closest_airframe_id_aedt"})
        .assign(facility_id="iah")
        .filter(
            items=[
                "facility_id",
                "annual_operations",
                "fleetmix",
                "closest_airframe_id_aedt",
                "engine_id",
                "anp_airplane_id",
                "anp_helicopter_id",
            ]
        )
    )

    path_iah_fleetmix_cln = Path.home().joinpath(
        PATH_INTERIM, "iah_airport_data", "iah_aedt_study_ops_cln.csv"
    )

    iah_fleetmix_3.to_csv(path_iah_fleetmix_cln, index=False)
