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

# emis_comm_reliev = pd.concat(
#     map(pd.read_excel, path_emis_comm_reliev.glob("County_EIS_Info_Comm_*"))
# )
# emis_non_comm_reliev = pd.concat(
#     map(pd.read_excel, path_emis_non_comm_reliev.glob("County_EIS_Info_non_Comm_*"))
# )

ls_comm_reliev = []
for path in path_emis_comm_reliev.glob("EIS_Info_*2020.txt"):
    county = path.name.split("EIS_Info_")[1].split("_")[0]
    year = int(path.name.split("EIS_Info_")[1].split("_")[1].split(".txt")[0])
    df = pd.read_csv(path, sep="\t")
    df["county"] = county
    df["year"] = year
    ls_comm_reliev.append(df)

emis_comm_reliev = pd.concat(ls_comm_reliev)

ls_non_comm_reliev = []
for path in path_emis_non_comm_reliev.glob("EIS_Info_non_*2020.txt"):
    county = path.name.split("EIS_Info_non_")[1].split("_")[0]
    year = int(path.name.split("EIS_Info_non_")[1].split("_")[1].split(".txt")[0])
    df = pd.read_csv(path, sep="\t")
    df["county"] = county
    df["year"] = year
    ls_non_comm_reliev.append(df)

emis_non_comm_reliev = pd.concat(ls_non_comm_reliev)

emisdf = pd.concat([emis_comm_reliev, emis_non_comm_reliev]).reset_index(drop=True)
emisdf20 = emisdf.loc[
    lambda df: (df.year == 2020)
]

rename_map = {
    'State_Facility_Identifier': "State Facility Identifier",
    'Airport': 'Airport',
    'Facility_Group': 'Facility Group',
    'Facility_Type': 'Facility Type',
    'County': 'County',
    'year': 'Year',
    'FIP': "FIPS",
    'District': "District",
    'SCC': 'SCC',
    'SCC_Description': 'SCC Description',
    'Airframe': 'Airframe',
    'Engine': 'Engine',
    'Mode': 'Mode',
    'LTOS': "LTOs",
    'EIS_Pollutant_ID': "EIS Pollutant ID",
    'UNCONTROLLED_ANNUAL_EMIS_ST': "Uncontrolled Annual Emission ST",
    'CONTROLLED_ANNUAL_EMIS_ST': "Controlled Annual Emission ST",
    'UNCONTROLLED_DAILY_EMIS_ST': "Uncontrolled Daily Emission ST",
    'CONTROLLED_DAILY_EMIS_ST': "Controlled Daily Emission ST",
}

emisdf20_fil = emisdf20.filter(items=rename_map.keys()).rename(
    columns=rename_map)
path_out = Path.home().joinpath(
    PATH_PROCESSED, "report_tables", "task5_appendix_f"
)

for nm, grp in emisdf20_fil.groupby("County"):
    path_out_fi = Path.home().joinpath(path_out, f"{nm}_2020_emission.tsv")
    grp.to_csv(path_out_fi,  sep="\t", index=False)
