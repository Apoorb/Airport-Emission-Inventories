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


def get_emis_by_scc(emisdf20_fil_agg_, val_col="UNCONTROLLED_ANNUAL_EMIS_ST"):
    emisdf20_fil_agg_pivot_uncntr = pd.pivot(
        emisdf20_fil_agg_,
        index=["SCC", "SCC_Description"],
        columns="EIS_Pollutant_ID",
        values=val_col,
    ).reset_index()

    old_nms = [
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
        emisdf20_fil_agg_pivot_uncntr.filter(items=old_nms)
        .rename(columns=rename_map)
        .set_index(["SCC Code", "SCC Description"])
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
    emisdf20_fil_agg_pivot_rename.sort_index(
        axis=0,
        level=1,
        key=lambda x: pd.Categorical(x, sort_ord_scc_desc),
        inplace=True,
    )
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
emisdf20_fil_agg = (
    emisdf20_fil.groupby(["SCC", "SCC_Description", "EIS_Pollutant_ID"])
    .agg(
        UNCONTROLLED_ANNUAL_EMIS_ST=("UNCONTROLLED_ANNUAL_EMIS_ST", "sum"),
        CONTROLLED_ANNUAL_EMIS_ST=("CONTROLLED_ANNUAL_EMIS_ST", "sum"),
    )
    .reset_index()
)

emisdf20_uncntr = get_emis_by_scc(
    emisdf20_fil_agg_=emisdf20_fil_agg, val_col="UNCONTROLLED_ANNUAL_EMIS_ST"
)
emisdf20_cntr = get_emis_by_scc(
    emisdf20_fil_agg_=emisdf20_fil_agg, val_col="CONTROLLED_ANNUAL_EMIS_ST"
)

path_out = Path.home().joinpath(
    PATH_PROCESSED, "report_tables", "tx_emis_statewide_sum.xlsx"
)

with pd.ExcelWriter(path_out) as f:
    emisdf20_uncntr.to_excel(f, "uncntr")
    emisdf20_cntr.to_excel(f, "cntr")
