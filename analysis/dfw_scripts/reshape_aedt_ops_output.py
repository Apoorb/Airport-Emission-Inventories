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
    ops2019_com_dfw = ops2019_com.loc[lambda df: df.facility_id == "dfw"]
    dfw_ops_2019 = ops2019_com_dfw.annual_operations.values[0]

    path_dfw_fleetmix = Path.home().joinpath(
        PATH_INTERIM, "dfw_reported_data", "dfw_aedt_ops.csv"
    )
    dfw_fleetmix = pd.read_csv(path_dfw_fleetmix)
    dfw_fleetmix['Operation Count'].sum()

    dfw_fleetmix_1 = (
        dfw_fleetmix.rename(columns=get_snake_case_dict(dfw_fleetmix))
        .groupby(["airframe", "engine"]).operation_count.sum().reset_index()
        .assign(fleetmix=lambda df: df.operation_count /
                                    df.operation_count.sum())
        .drop(columns="operation_count")
        .assign(annual_operations=dfw_ops_2019)
    )
    assert dfw_fleetmix_1.fleetmix.sum() == 1

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

    dfw_fleetmix_2 = (
        dfw_fleetmix_1.merge(airfm_df_1, on="airframe", how="left")
        .merge(eng_df_1, on="engine", how="left")
        .merge(equip_db_fil, on=["airframe_id", "engine_id"], how="left")
    )

    assert all(
        (
            (~dfw_fleetmix_2.anp_airplane_id.isna())
            | (~dfw_fleetmix_2.anp_helicopter_id.isna())
        )
    ), "Missing ANP ids. Can't assign profiles to non-anp crafts."

    dfw_fleetmix_3 = (
        dfw_fleetmix_2.rename(columns={"airframe_id": "closest_airframe_id_aedt"})
        .assign(facility_id="dfw")
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

    path_dfw_fleetmix_cln = Path.home().joinpath(
        PATH_INTERIM, "dfw_reported_data", "dfw_aedt_study_ops_cln.xlsx"
    )

    dfw_fleetmix_3.to_excel(path_dfw_fleetmix_cln, index=False,
                            sheet_name="Commercial")
