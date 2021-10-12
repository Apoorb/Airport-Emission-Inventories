"""
Compare the pollutant list between ERG and TTI.
"""

import pandas as pd
from pathlib import Path
import numpy as np

path_tti_eidp = (
    r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\interim"
    r"\speciation\tti_pol_list.xlsx"
)
# C:\Users\a-bibeka\Texas A&M Transportation Institute\HMP - TCEQ Projects -
# Documents\2020 Texas Statewide Airport EI\Resources\ERG
path_erg17 = (
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - Documents\2020 Texas Statewide Airport EI"
    r"\Resources\ERG\2017 Airport Inventory Revised SCC.xlsx"
)
# C:\Users\a-bibeka\Texas A&M Transportation Institute\HMP - TCEQ Projects -
# Documents\2020 Texas Statewide Airport EI\Resources\EPA\2017NEI\2017Aircraft_main_25june2020.pdf
# Page 20
path_nei17 = (
    r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\processed"
    r"\qc\pol_list_qc\nei2017_pollist.xlsx"
)

path_aedt = (
    r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\interim"
    r"\speciation\speciation_fin.xlsx"
)

tti_eidp = pd.read_excel(path_tti_eidp)
tti_eidp = (
    tti_eidp.rename(
        columns={
            "Pollutant*": "polnm_tti",
            "Pollutant Code": "polcode",
            "Base Pollutant (Fraction of)": "basepol",
        }
    )
    .filter(items=["polnm_tti", "polcode", "basepol"])
    .assign(source_tti="tti_eidp")
)
tti_eidp["polcode"] = tti_eidp["polcode"].astype(str)

erg_17 = pd.read_excel(path_erg17, "2017 Statewide")
erg_17_fil = pd.DataFrame(
    erg_17.EIS_Pollutant_ID.drop_duplicates().reset_index(drop=True)
)
erg_17_fil.rename(columns={"EIS_Pollutant_ID": "polcode"}, inplace=True)
erg_17_fil["source"] = "ERG 17"
erg_17_fil.loc[lambda df: df.polcode == "PM10-PRI", "polcode"] = "PM10"
erg_17_fil.loc[lambda df: df.polcode == "PM25-PRI", "polcode"] = "PM25"

nei_17 = pd.read_excel(path_nei17)
nei_17.columns = ["polnm_nei", "polcode"]
nei_17["source"] = "NEI 17"
nei_erg_17 = nei_17.merge(
    erg_17_fil, on="polcode", suffixes=["_nei17", "_erg17"], how="outer"
)
nei_erg_17 = nei_erg_17.loc[
    lambda df: ~df.polcode.isin(
        ["CO", "CH4", "CO2", "NOX", "PM10", "PM25", "SO2", "VOC"]
    )
]
tti_nei_erg_17 = tti_eidp.merge(nei_erg_17, on="polcode", how="outer")


aedt_df = pd.read_excel(path_aedt)
aedt_df_fil = (
    aedt_df[["polcode", "pol_nm", "caa_hap", "iris_hap", "hc", "voc"]]
    .drop_duplicates()
    .rename(
        columns={
            "Pollutant Code": "polcode",
            "pol_nm": "polnm_aedt",
            "caa_hap": "caa_hap_aedt",
            "iris_hap": "iris_hap_aedt",
            "hc": "hc_aedt",
            "voc": "voc_aedt",
        }
    )
    .assign(source_aedt="aedt")
)


aedt_tti_nei_erg_17 = aedt_df_fil.merge(tti_nei_erg_17, on="polcode", how="outer")


aedt_tti_nei_erg_17 = aedt_tti_nei_erg_17.filter(
    items=[
        "source_nei17",
        "source_erg17",
        "source_aedt",
        "source_tti",
        "basepol",
        "polcode",
        "polnm_nei",
        "polnm_aedt",
        "polnm_tti",
        "caa_hap_aedt",
        "iris_hap_aedt",
        "hc_aedt",
        "voc_aedt",
    ]
)

aedt_tti_nei_erg_17.sort_values(
    ["source_nei17", "source_erg17", "source_aedt", "source_tti", "polcode"],
    inplace=True,
)

aedt_tti_nei_erg_17.to_excel(
    r"C:\Users\a-bibeka\PycharmProjects\airport_ei"
    r"\data\processed\qc\pol_list_qc\nei_erg17_aedt_tti_pollist_v1.xlsx",
    index=False,
)

a = 1
