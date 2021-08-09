"""
Test emissions for commercial airports match between TTI and ERG.
"""

import numpy as np
import pandas as pd
from airportei.utilis import PATH_RAW, PATH_INTERIM, get_snake_case_dict
from pathlib import Path


path_erg = Path.home().joinpath(PATH_RAW, "madhu_files", "ERG_2017.csv")
path_tti_com = (
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - Documents\2020 Texas Statewide Airport EI"
    r"\Tasks\Task5_ Statewide_2020_AERR_EI\aedt_ems_2019"
    r"\bakFile_metricResults\Commercial"
)

path_tti_rel = (
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - Documents\2020 Texas Statewide Airport EI"
    r"\Tasks\Task5_ Statewide_2020_AERR_EI\aedt_ems_2019"
    r"\bakFile_metricResults\Reliver"
)


tti_files = [file for file in Path(path_tti_com).glob("*.csv")] + [
    file for file in Path(path_tti_rel).glob("*.csv")
]

erg_df = pd.read_csv(path_erg)
erg_df_1 = erg_df.rename(columns=get_snake_case_dict(erg_df)).rename(
    columns={"state_facility_identifier": "facility_id"}
)


erg_df_1.loc[lambda df: df["mode"].isin(["GSE LTO", "APU"]), "lto"] = np.nan
erg_ltos = (
    erg_df_1.loc[lambda df: df.eis_pollutant_id == "CO"]
    .groupby(["facility_id"])
    .agg(lto_fill=("lto", "sum"))
    .reset_index()
)

erg_df_2 = (
    erg_df_1.groupby(["facility_id", "mode", "eis_pollutant_id"])
    .agg(lto=("lto", "sum"), emis_tons=("uncontrolled_annual_emis_st", "sum"))
    .reset_index()
    .assign(
        facility_id=lambda df: df.facility_id.str.lower(),
        emis_per_lto=lambda df: df.emis_tons / df.lto,
    )
)


list_df = []
list_df_detailed = []
for file in tti_files:
    facility_id = file.name.split("_")[0]
    fac_grp = file.parent.name
    df = pd.read_csv(file)
    df_1 = df.rename(columns=get_snake_case_dict(df)).assign(facility_id=facility_id)
    df_1["mode"].unique()
    df_1["mode"] = np.select(
        [
            df_1["mode"] == "Climb Below Mixing Height",
            df_1["mode"] == "Descend Below Mixing Height",
            df_1["mode"] == "GSE LTO",
            df_1["mode"] == "APU",
        ],
        ["Aircraft", "Aircraft", "GSE LTO", "APU"],
        np.nan,
    )
    df_1_fil = df_1.loc[df_1["mode"] != "nan"]

    df_1_fil.loc[df_1_fil["mode"].isin(["GSE LTO", "APU"]), "equipment_type"] = "nan"

    df_2_detailed = (
        df_1_fil.assign(
            co_st_=lambda df: df.co_st_ * df.num_ops,
            co2_st_=lambda df: df.co2_st_ * df.num_ops,
            voc_st_=lambda df: df.voc_st_ * df.num_ops,
            n_ox_st_=lambda df: df.n_ox_st_ * df.num_ops,
            s_ox_st_=lambda df: df.s_ox_st_ * df.num_ops,
            pm_2_5_st_=lambda df: df.pm_2_5_st_ * df.num_ops,
            pm_10_st_=lambda df: df.pm_10_st_ * df.num_ops,
        )
        .groupby(["facility_id", "mode", "equipment_type"])
        .agg(
            CO=("co_st_", "sum"),
            CO2=("co2_st_", "sum"),
            VOC=("voc_st_", "sum"),
            NOX=("n_ox_st_", "sum"),
            sox=("s_ox_st_", "sum"),
            pm25=("pm_2_5_st_", "sum"),
            pm10=("pm_10_st_", "sum"),
        )
        .reset_index()
    )

    df_2 = (
        df_1_fil.assign(
            co_st_=lambda df: df.co_st_ * df.num_ops,
            co2_st_=lambda df: df.co2_st_ * df.num_ops,
            voc_st_=lambda df: df.voc_st_ * df.num_ops,
            n_ox_st_=lambda df: df.n_ox_st_ * df.num_ops,
            s_ox_st_=lambda df: df.s_ox_st_ * df.num_ops,
            pm_2_5_st_=lambda df: df.pm_2_5_st_ * df.num_ops,
            pm_10_st_=lambda df: df.pm_10_st_ * df.num_ops,
        )
        .groupby(["facility_id", "mode"])
        .agg(
            CO=("co_st_", "sum"),
            CO2=("co2_st_", "sum"),
            VOC=("voc_st_", "sum"),
            NOX=("n_ox_st_", "sum"),
            sox=("s_ox_st_", "sum"),
            pm25=("pm_2_5_st_", "sum"),
            pm10=("pm_10_st_", "sum"),
        )
        .reset_index()
    )

    df_1_fil["id"] = df_1_fil.user_id.str.split(r"[AD]", expand=True)[1]
    df_ltos = df_1_fil.drop_duplicates(["mode", "id"])
    df_ltos_1 = (
        df_ltos.groupby(["facility_id", "mode"])
        .agg(lto=("num_ops", "sum"))
        .reset_index()
    )
    df_ltos_2 = df_ltos_1.loc[df_ltos_1["mode"] != "nan"]

    df_ltos_detailed_1 = (
        df_ltos.groupby(["facility_id", "mode", "equipment_type"])
        .agg(lto=("num_ops", "sum"))
        .reset_index()
    )
    df_ltos_detailed_2 = df_ltos_detailed_1.loc[df_ltos_detailed_1["mode"] != "nan"]

    df_3 = df_2.set_index(["facility_id", "mode"]).stack().reset_index()
    df_3.columns = ["facility_id", "mode", "eis_pollutant_id", "emis_tons"]
    df_4 = (df_3.merge(df_ltos_2, on=["facility_id", "mode"])).assign(
        fac_grp=fac_grp, emis_per_lto=lambda df: df.emis_tons / df.lto
    )

    df_3_detailed = (
        df_2_detailed.set_index(["facility_id", "mode", "equipment_type"])
        .stack()
        .reset_index()
    )
    df_3_detailed.columns = [
        "facility_id",
        "mode",
        "equipment_type",
        "eis_pollutant_id",
        "emis_tons",
    ]
    df_4_detailed = (
        df_3_detailed.merge(
            df_ltos_detailed_2, on=["facility_id", "mode", "equipment_type"]
        )
    ).assign(fac_grp=fac_grp)

    list_df.append(df_4)
    list_df_detailed.append(df_4_detailed)

tti_df = pd.concat(list_df)
tti_df_detailed = pd.concat(list_df_detailed)


erg_df_2.eis_pollutant_id.replace("PM10-PRI", "pm10", inplace=True)
erg_df_2.eis_pollutant_id.replace("PM25-PRI", "pm25", inplace=True)

tti_erg_df = tti_df.merge(
    erg_df_2, on=["facility_id", "mode", "eis_pollutant_id"], suffixes=["_tti", "_erg"]
)


tti_erg_arpt_df = tti_erg_df.loc[lambda df: df["mode"] == "Aircraft"]

tti_erg_arpt_df_1 = tti_erg_arpt_df.assign(
    tti_erg_diff=lambda df: df.emis_per_lto_tti - df.emis_per_lto_erg,
    tti_erg_per_diff=lambda df: df.tti_erg_diff * 100 / df.emis_per_lto_erg,
)

path_comp_out = Path.home().joinpath(PATH_INTERIM, "qc", "tti_vs_erg.xlsx")

with pd.ExcelWriter(path_comp_out) as writer:
    tti_erg_arpt_df_1.to_excel(writer, sheet_name="summary", index=False)
    tti_df_detailed.to_excel(writer, sheet_name="detailed", index=False)

# Check individual facility
###############################################################################
