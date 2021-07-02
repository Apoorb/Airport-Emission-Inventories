"""
Develop fleet mix from TFMSC data.
Created on: 7/20/2021
Created by: Apoorba Bibeka
"""
from pathlib import Path
import pandas as pd
from analysis.scratch.utilis import (PATH_RAW, PATH_INTERIM,
                                     get_snake_case_dict)
if __name__ == "__main__":
    path_ops2019_clean = Path.home().joinpath(
        PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx")
    ops2019 = pd.read_excel(path_ops2019_clean, usecols=[
        "facility_id", "facility_group"])
    path_tfmsc = Path.home().joinpath(
        PATH_RAW, "madhu_files", "FAA_2019TFMSC.csv")
    tfmsc_df = pd.read_csv(path_tfmsc)
    tfmsc_df_1 = (
        tfmsc_df.rename(columns=get_snake_case_dict(tfmsc_df))
        .rename(columns={"location_id": "facility_id"})
        .groupby([
            "year_id", "facility_id", "airport", 'aircraft_id',
            'aircraft_type_id', 'aircraft_type'])
        .agg(total_ops_by_craft=("total_ops", "sum"))
        .reset_index()
        .assign(
            facility_id=lambda df: df.facility_id.str.strip().str.lower(),
            total_ops_by_arpt=lambda df: df.groupby([
                "year_id", "facility_id",
                "airport"]).total_ops_by_craft.transform("sum"),
            fleetmix=lambda df: df.total_ops_by_craft / df.total_ops_by_arpt
        )
    )

    tfmsc_df_ops2019 = (
        ops2019
        .merge(
            tfmsc_df_1,
            on="facility_id",
            how="left"
        )
    )

    tfmsc_df_ops2019_com = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "Commercial"]
    )

    tfmsc_df_ops2019_rel = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "Reliever"]
    )

    tfmsc_df_ops2019_tasp = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "TASP"]
    )
