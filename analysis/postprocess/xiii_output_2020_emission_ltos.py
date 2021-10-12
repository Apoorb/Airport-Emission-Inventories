"""
Get 2020 emissions and LTOs.
"""
from itertools import chain
import numpy as np
import pandas as pd
from airportei.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED, get_snake_case_dict
from pathlib import Path


path_erg = Path.home().joinpath(PATH_RAW, "madhu_files", "ERG_2017.csv")
path_emis_fl = (
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - Documents\2020 Texas Statewide Airport EI\Tasks"
    r"\Task5_ Statewide_2020_AERR_EI\Data_Code\Airport_EIS_Formatted"
    r"\EIS_9_15_21\Airport_EIS_2020.txt"
)


df = pd.read_csv(path_emis_fl, sep="\t")
df.rename(columns=get_snake_case_dict(df), inplace=True)
df_fil = df.loc[lambda df: df["mode"].isin(["Aircraft", "GSE", "APU"])]
df_fil.rename(
    columns={
        "state_facility_identifier": "facility_id",
        "ltos": "lto",
        "airport": "airport_tti",
    },
    inplace=True,
)
df_fil.loc[df_fil.eis_pollutant_id == "PM2.5", "eis_pollutant_id"] = "PM25-PRI"
df_fil.loc[df_fil.eis_pollutant_id == "PM10", "eis_pollutant_id"] = "PM10-PRI"
df_fil_1 = (
    df_fil.loc[
        lambda df: df.eis_pollutant_id.isin(
            ["CO", "CO2", "VOC", "NOX", "SO2", "PM25-PRI", "PM10-PRI"]
        )
    ]
    .groupby(
        ["facility_id", "facility_group", "airport_tti", "mode", "eis_pollutant_id"]
    )
    .agg(lto=("lto", "sum"), uncntr_emis_tons=("uncontrolled_annual_emis_st", "sum"))
    .reset_index()
    .assign(uncntr_emis_per_lto=lambda df: df.uncntr_emis_tons / df.lto)
)

tti_df_17 = df_fil_1

path_out = Path.home().joinpath(PATH_PROCESSED, "report_tables", "emis_ltos_2020.xlsx")

with pd.ExcelWriter(path_out) as f:
    tti_df_17.to_excel(f, "uncntr")
