"""
Test emissions for commercial airports match between TTI and ERG.
"""

import numpy as np
import pandas as pd
from airportei.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED, get_snake_case_dict
from pathlib import Path


path_erg = Path.home().joinpath(PATH_RAW, "madhu_files", "ERG_2017.csv")
path_tti_comm_reliev = (
    r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\processed"
    r"\emis_comm_reliev.xlsx"
)

path_tti_non_comm_reliev = (
    r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\processed"
    r"\emis_non_comm_reliev.xlsx"
)
path_proj_fac = Path.home().joinpath(PATH_INTERIM, "proj_fac_axb_07_11_2021.xlsx")

tti_files = [path_tti_comm_reliev, path_tti_non_comm_reliev]

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
    erg_df_1.loc[
        lambda df: df.eis_pollutant_id.isin(
            ["CO", "CO2", "VOC", "NOX", "PM25-PRI", "PM10-PRI"]
        )
    ]
    .rename(columns={"airport": "airport_erg"})
    .groupby(["facility_id", "airport_erg", "mode", "eis_pollutant_id"])
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
    df = pd.read_excel(file)
    df.rename(columns=get_snake_case_dict(df), inplace=True)
    df["mode"].unique()
    df["mode"] = np.select(
        [
            df["mode"] == "Climb Below Mixing Height",
            df["mode"] == "Descend Below Mixing Height",
            df["mode"] == "GSE LTO",
            df["mode"] == "APU",
        ],
        ["Aircraft", "Aircraft", "GSE LTO", "APU"],
        np.nan,
    )
    # df_fil = df.loc[~ df_fil["mode"].isin(["GSE LTO", "APU"])]
    df_fil = df
    df_1_detailed = (
        df_fil.assign(
            co_st_=lambda df: df.co_st_ * df.ltos,
            co2_st_=lambda df: df.co2_st_ * df.ltos,
            voc_st_=lambda df: df.voc_st_ * df.ltos,
            n_ox_st_=lambda df: df.n_ox_st_ * df.ltos,
            s_ox_st_=lambda df: df.s_ox_st_ * df.ltos,
            pm_2_5_st_=lambda df: df.pm_2_5_st_ * df.ltos,
            pm_10_st_=lambda df: df.pm_10_st_ * df.ltos,
        )
        .groupby(
            [
                "facility_group",
                "facility_type",
                "facility_id",
                "facility_name",
                "mode",
                "equipment_type",
            ]
        )
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
    len(df_fil.facility_id.unique())

    df_2 = (
        df_fil.assign(
            co_st_=lambda df: df.co_st_ * df.ltos,
            co2_st_=lambda df: df.co2_st_ * df.ltos,
            voc_st_=lambda df: df.voc_st_ * df.ltos,
            n_ox_st_=lambda df: df.n_ox_st_ * df.ltos,
            s_ox_st_=lambda df: df.s_ox_st_ * df.ltos,
            pm_2_5_st_=lambda df: df.pm_2_5_st_ * df.ltos,
            pm_10_st_=lambda df: df.pm_10_st_ * df.ltos,
        )
        .groupby(
            ["facility_group", "facility_type", "facility_id", "facility_name", "mode"]
        )
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
    df_ltos = df_fil.drop_duplicates(["facility_id", "mode", "equipment_type"])
    df_ltos_1 = (
        df_ltos.groupby(["facility_id", "mode"]).agg(lto=("ltos", "sum")).reset_index()
    )

    df_ltos_detailed_1 = (
        df_ltos.groupby(["facility_id", "mode", "equipment_type"])
        .agg(lto=("ltos", "sum"))
        .reset_index()
    )

    df_3 = (
        df_2.set_index(
            ["facility_group", "facility_type", "facility_id", "facility_name", "mode"]
        )
        .stack()
        .reset_index()
    )
    df_3.columns = [
        "facility_group",
        "facility_type",
        "facility_id",
        "facility_name",
        "mode",
        "eis_pollutant_id",
        "emis_tons",
    ]
    df_4 = (df_3.merge(df_ltos_1, on=["facility_id", "mode"])).assign(
        emis_per_lto=lambda df: df.emis_tons / df.lto
    )

    df_3_detailed = (
        df_1_detailed.set_index(
            [
                "facility_id",
                "facility_group",
                "facility_type",
                "facility_name",
                "mode",
                "equipment_type",
            ]
        )
        .stack()
        .reset_index()
    )
    df_3_detailed.columns = [
        "facility_id",
        "facility_group",
        "facility_type",
        "facility_name",
        "mode",
        "equipment_type",
        "eis_pollutant_id",
        "emis_tons",
    ]
    df_4_detailed = df_3_detailed.merge(
        df_ltos_detailed_1, on=["facility_id", "mode", "equipment_type"]
    )

    list_df.append(df_4)
    list_df_detailed.append(df_4_detailed)

tti_df = pd.concat(list_df)
tti_df_detailed = pd.concat(list_df_detailed)

x1 = pd.ExcelFile(path_proj_fac)
proj_fac = pd.concat(map(x1.parse, x1.sheet_names))
proj_fac_17 = proj_fac.loc[proj_fac.sysyear == 2017].filter(
    items=["facility_id", "proj_fac"]
)

tti_df_17 = (
    tti_df.merge(proj_fac_17, on="facility_id", how="left")
    .rename(
        columns={
            "lto": "lto_tmp",
            "emis_tons": "emis_tons_tmp",
            "emis_per_lto": "emis_per_lto_tmp",
        }
    )
    .assign(
        lto=lambda df: df.lto_tmp * df.proj_fac,
        emis_tons=lambda df: df.emis_tons_tmp.astype(float) * df.proj_fac,
        emis_per_lto=lambda df: df.emis_per_lto_tmp.astype(float) * df.proj_fac,
    )
)

assert len(tti_df_detailed.drop_duplicates(["facility_group", "facility_id"])) == 2032

erg_df_2.eis_pollutant_id.replace("PM10-PRI", "pm10", inplace=True)
erg_df_2.eis_pollutant_id.replace("PM25-PRI", "pm25", inplace=True)

tti_erg_df = tti_df_17.merge(
    erg_df_2,
    on=["facility_id", "mode", "eis_pollutant_id"],
    suffixes=["_tti", "_erg"],
    how="outer",
)


tti_erg_arpt_df = tti_erg_df.loc[lambda df: df["mode"] == "Aircraft"]

tti_erg_arpt_df_1 = tti_erg_arpt_df.assign(
    tti_erg_diff=lambda df: df.emis_per_lto_tti - df.emis_per_lto_erg,
    tti_erg_per_diff=lambda df: df.tti_erg_diff * 100 / df.emis_per_lto_erg,
)

path_comp_out = Path.home().joinpath(PATH_PROCESSED, "qc", "tti_vs_erg.xlsx")

with pd.ExcelWriter(path_comp_out) as writer:
    tti_erg_arpt_df_1.to_excel(writer, sheet_name="summary", index=False)
    tti_df_detailed.to_excel(writer, sheet_name="detailed", index=False)

a = 1
# Check individual facility
###############################################################################
