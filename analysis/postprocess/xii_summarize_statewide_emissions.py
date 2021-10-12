"""
Create statewide emission summaries.
Created by: Apoorba Bibeka
Created on: 8/27/2021
"""
from itertools import chain
import glob
from pathlib import Path
import pandas as pd
from airportei.utilis import PATH_INTERIM, PATH_PROCESSED


def get_emis_by_scc(emisdf20_fil_agg_,
                    val_col="UNCONTROLLED_ANNUAL_EMIS_ST"):
    if "County" in emisdf20_fil_agg_.columns:
        index_=["County", "SCC", "SCC_Description"]
    else:
        index_=["SCC", "SCC_Description"]

    emisdf20_fil_agg_pivot = pd.pivot(
        emisdf20_fil_agg_,
        index=index_,
        columns="EIS_Pollutant_ID",
        values=val_col,
    ).reset_index()

    old_nms = [
        "County",
        "SCC",
        "SCC_Description",
        "VOC",
        "NOX",
        "CO",
        "PM10",
        "PM2.5",
        "SO2",
        "Pb",
    ]
    new_nms = [
        "County",
        "SCC Code",
        "SCC Description",
        "VOC",
        "NOx",
        "CO",
        "PM10",
        "PM2.5",
        "SO2",
        "Pb",
    ]
    rename_map = {old_nm: new_nm for old_nm, new_nm in zip(old_nms, new_nms)}

    emisdf20_fil_agg_pivot_rename = (
        emisdf20_fil_agg_pivot.filter(items=old_nms)
        .rename(columns=rename_map)
    )
    sort_ord_scc_desc = [
        "Commercial Aviation",
        "Air Taxi: Piston",
        "Air Taxi: " "Turbine",
        "General Aviation: Piston",
        "General Aviation: Turbine",
        "Military",
        "APU",
        "GSE: Gasoline-fueled",
        "GSE: Diesel-fueled",
    ]
    emisdf20_fil_agg_pivot_rename["SCC Description"] = pd.Categorical(
        emisdf20_fil_agg_pivot_rename["SCC Description"],
        sort_ord_scc_desc,
        ordered=True
    )
    if "County" in emisdf20_fil_agg_pivot_rename.columns:
        emisdf20_fil_agg_pivot_rename.sort_values(["County", "SCC Description"], inplace=True)
    else:
        emisdf20_fil_agg_pivot_rename.sort_values(["SCC Description"], inplace=True)
    return emisdf20_fil_agg_pivot_rename


path_emis_fl = Path(
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute\HMP - TCEQ "
    r"Projects - Documents\2020 Texas Statewide Airport EI\Tasks\Task5_ "
    r"Statewide_2020_AERR_EI\Data_Code\Airport_EIS_Formatted\EIS_10_12_21"
    r"\Airport_EIS_2017_2020\Airport_EIS_2020.txt"
)

emisdf20 = pd.read_csv(path_emis_fl, sep="\t")

emisdf20.rename(columns={"SCC Description": "SCC_Description"}, inplace=True)
emisdf20_fil = emisdf20.loc[
    lambda df: (df["SCC_Description"] != "GSE: Electric")
    & (df.EIS_Pollutant_ID.isin(["VOC", "NOX", "CO", "PM10", "PM2.5", "SO2", "Pb"]))
]
emisdf20_fil_agg_st = (
    emisdf20_fil.groupby(["SCC", "SCC_Description", "EIS_Pollutant_ID"])
    .agg(
        UNCONTROLLED_ANNUAL_EMIS_ST=("UNCONTROLLED_ANNUAL_EMIS_ST", "sum"),
        CONTROLLED_ANNUAL_EMIS_ST=("CONTROLLED_ANNUAL_EMIS_ST", "sum"),
    )
    .reset_index()
)

emisdf20_fil["County"] = emisdf20_fil["County"].str.title()
emisdf20_fil_agg_cnty = (
    emisdf20_fil.groupby(["County", "SCC", "SCC_Description", "EIS_Pollutant_ID"])
    .agg(
        UNCONTROLLED_ANNUAL_EMIS_ST=("UNCONTROLLED_ANNUAL_EMIS_ST", "sum"),
        CONTROLLED_ANNUAL_EMIS_ST=("CONTROLLED_ANNUAL_EMIS_ST", "sum"),
    )
    .reset_index()
)

emisdf20_cntr_cnty = get_emis_by_scc(
    emisdf20_fil_agg_=emisdf20_fil_agg_cnty, val_col="CONTROLLED_ANNUAL_EMIS_ST"
)
emisdf20_uncntr_cnty = get_emis_by_scc(
    emisdf20_fil_agg_=emisdf20_fil_agg_cnty, val_col="UNCONTROLLED_ANNUAL_EMIS_ST"
)
emisdf20_cntr_st = get_emis_by_scc(
    emisdf20_fil_agg_=emisdf20_fil_agg_st, val_col="CONTROLLED_ANNUAL_EMIS_ST"
)
emisdf20_uncntr_st = get_emis_by_scc(
    emisdf20_fil_agg_=emisdf20_fil_agg_st, val_col="UNCONTROLLED_ANNUAL_EMIS_ST"
)


path_out = Path.home().joinpath(
    PATH_PROCESSED, "report_tables", "tx_emis_statewide_sum.xlsx"
)

with pd.ExcelWriter(path_out) as f:
    emisdf20_uncntr_st.to_excel(f, "uncntr_st_2020")
    emisdf20_cntr_st.to_excel(f, "cntr_st_2020")
    emisdf20_uncntr_cnty.to_excel(f, "uncntr_cnty_2020")
    emisdf20_cntr_cnty.to_excel(f, "cntr_cnty_2020")
