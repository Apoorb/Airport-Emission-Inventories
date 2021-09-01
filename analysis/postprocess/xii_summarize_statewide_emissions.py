"""
Create statewide emission summaries.
Created by: Apoorba Bibeka
Created on: 8/27/2021
"""
import glob
from pathlib import Path
import pandas as pd
from airportei.utilis import PATH_INTERIM, PATH_PROCESSED

path_emis_fl = (
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - Documents\2020 Texas Statewide Airport EI"
    r"\Tasks\Task5_ Statewide_2020_AERR_EI\Data_Code\Airport_EIS_Formatted"
)
path_emis_comm_reliev = Path.home().joinpath(path_emis_fl,
                                             "County_EIS_Summary_Comm_8_31_21")
path_emis_non_comm_reliev = Path.home().joinpath(
    path_emis_fl, "County_EIS_Summary_Noncomm_8_31_21"
)

emis_comm_reliev = pd.concat(
    map(pd.read_excel, path_emis_comm_reliev.glob("County_EIS_Info_Comm_*"))
)
emis_non_comm_reliev = pd.concat(
    map(pd.read_excel, path_emis_non_comm_reliev.glob("County_EIS_Info_non_Comm_*"))
)

ls_comm_reliev = []
for path in path_emis_comm_reliev.glob("County_EIS_Info_Comm_*"):
    county = path.name.split("County_EIS_Info_Comm_")[1].split("_EIS_Summary.xlsx")[0]
    df = pd.read_excel(path)
    df["county"] = county
    ls_comm_reliev.append(df)

emis_comm_reliev = pd.concat(ls_comm_reliev)

ls_non_comm_reliev = []
for path in path_emis_non_comm_reliev.glob("County_EIS_Info_non_Comm_*"):
    county = path.name.split("County_EIS_Info_non_Comm_")[1].split("_EIS_Summary.xlsx")[
        0
    ]
    df = pd.read_excel(path)
    df["county"] = county
    ls_non_comm_reliev.append(df)

emis_non_comm_reliev = pd.concat(ls_non_comm_reliev)

emisdf = pd.concat([emis_comm_reliev, emis_non_comm_reliev]).reset_index(drop=True)
emisdf20 = emisdf.loc[
    lambda df: (df.Year == 2020) & (df["SCC Description"] != "GSE: Electric")
]

old_nms = ["SCC code", "SCC Description", "VOC", "NOX", "CO", "PM10", "PM2.5", "SO2"]
new_nms = ["SCC Code", "SCC Description", "VOC", "NOx", "CO", "PM10", "PM2.5", "SO2"]
rename_map = {old_nm: new_nm for old_nm, new_nm in zip(old_nms, new_nms)}

emisdf20_fil = emisdf20.filter(items=old_nms).rename(columns=rename_map)

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
emisdf20_fil_agg = emisdf20_fil.groupby(["SCC Code", "SCC Description"]).sum()
emisdf20_fil_agg.sort_index(
    axis=0, level=1, key=lambda x: pd.Categorical(x, sort_ord_scc_desc), inplace=True
)
path_out = Path.home().joinpath(
    PATH_PROCESSED, "report_tables", "tx_emis_uncntr_sum.xlsx"
)
emisdf20_fil_agg.to_excel(path_out)
