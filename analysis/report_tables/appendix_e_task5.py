"""
Create statewide emission summaries.
Created by: Apoorba Bibeka
Created on: 8/27/2021
"""
import glob
from pathlib import Path
import pandas as pd
from airportei.utilis import PATH_INTERIM, PATH_PROCESSED

path_emis_fl = Path(
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute\HMP - TCEQ "
    r"Projects - Documents\2020 Texas Statewide Airport EI\Tasks"
    r"\Task5_ Statewide_2020_AERR_EI\Data_Code\Airport_EIS_Formatted"
    r"\EIS_9_15_21\Airport_EIS_2020.txt"
)

emisdf20 = pd.read_csv(path_emis_fl, sep="\t")

rename_map = {
    "State_Facility_Identifier": "State Facility Identifier",
    "Airport": "Airport",
    "Facility_Group": "Facility Group",
    "Facility_Type": "Facility Type",
    "County": "County",
    "year": "Year",
    "FIP": "FIPS",
    "District": "District",
    "SCC": "SCC",
    "SCC_Description": "SCC Description",
    "Airframe": "Airframe",
    "Engine": "Engine",
    "Mode": "Mode",
    "LTOS": "LTOs",
    "EIS_Pollutant_ID": "EIS Pollutant ID",
    "UNCONTROLLED_ANNUAL_EMIS_ST": "Uncontrolled Annual Emission ST",
    "CONTROLLED_ANNUAL_EMIS_ST": "Controlled Annual Emission ST",
    "UNCONTROLLED_DAILY_EMIS_ST": "Uncontrolled Daily Emission ST",
    "CONTROLLED_DAILY_EMIS_ST": "Controlled Daily Emission ST",
}

emisdf20_fil = emisdf20.filter(items=rename_map.keys()).rename(columns=rename_map)
path_out = Path.home().joinpath(PATH_PROCESSED, "report_tables", "task5_appendix_f")

for nm, grp in emisdf20_fil.groupby("County"):
    path_out_fi = Path.home().joinpath(path_out, f"{nm}_2020_emission.tsv")
    grp.to_csv(path_out_fi, sep="\t", index=False)
