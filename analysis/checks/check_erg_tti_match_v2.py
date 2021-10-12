"""
Test emissions for commercial airports match between TTI and ERG.
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
    r"\EIS_9_15_21\Airport_EIS_2017.txt"
)

erg_df = pd.read_csv(path_erg)
erg_df_1 = erg_df.rename(columns=get_snake_case_dict(erg_df)).rename(
    columns={"state_facility_identifier": "facility_id"}
)
erg_df_1.loc[lambda df: df["mode"] == "GSE LTO", "mode"] = "GSE"
erg_ltos = (
    erg_df_1.loc[lambda df: df.eis_pollutant_id == "CO"]
    .groupby(["facility_id"])
    .agg(lto_fill=("lto", "sum"))
    .reset_index()
)

erg_df_2 = (
    erg_df_1.loc[
        lambda df: df.eis_pollutant_id.isin(
            ["CO", "CO2", "VOC", "NOX", "SO2", "PM25-PRI", "PM10-PRI"]
        )
    ]
    .rename(columns={"airport": "airport_erg"})
    .groupby(["facility_id", "airport_erg", "mode", "eis_pollutant_id"])
    .agg(lto=("lto", "sum"), uncntr_emis_tons=("uncontrolled_annual_emis_st", "sum"))
    .reset_index()
    .assign(
        facility_id=lambda df: df.facility_id.str.lower(),
        uncntr_emis_per_lto=lambda df: df.uncntr_emis_tons / df.lto,
    )
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

len(tti_df_17) / 6


tti_erg_df = tti_df_17.merge(
    erg_df_2,
    on=["facility_id", "mode", "eis_pollutant_id"],
    suffixes=["_tti", "_erg"],
    how="outer",
)
#
# tti_erg_arpt_df = tti_erg_df.loc[lambda df: df["mode"] == "Aircraft"]
#
# tti_erg_arpt_df_1 = tti_erg_arpt_df.assign(
#     tti_erg_diff=lambda df: df.uncntr_emis_per_lto_tti - df.uncntr_emis_per_lto_erg,
#     tti_erg_per_diff=lambda df: df.tti_erg_diff * 100 / df.uncntr_emis_per_lto_erg,
# )

path_comp_out = Path.home().joinpath(PATH_PROCESSED, "qc", "tti_vs_erg.xlsx")

with pd.ExcelWriter(path_comp_out) as writer:
    tti_erg_df.to_excel(writer, sheet_name="summary", index=False)

a = 1
# Check individual facility
###############################################################################
