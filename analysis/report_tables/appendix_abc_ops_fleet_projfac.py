"""
Prepare operations for report appendix A.
"""
from pathlib import Path
import pandas as pd
from airportei.utilis import PATH_INTERIM, PATH_PROCESSED
import inflection

path_ops = Path.home().joinpath(PATH_PROCESSED, "ops2019_fin.xlsx")
ops = pd.read_excel(path_ops, index_col=0)
ops.columns

ops["summer_daily"] = ops["annual_operations"] / 365
columns = [
    "county_arpt",
    "city_arpt",
    "district_tx_boundar",
    "fips_tx_boundar",
    "facility_id",
    "facility_name",
    "facility_group",
    "facility_type",
    "annual_operations",
    "summer_daily",
    "tx_dot_group",
    "medical_use",
    "military_joint_use",
    "otherservices",
    "fuel_types",
    "ownership",
    "used",
]
rename_col = {
    col: inflection.titleize(col.replace("boundar", "boundary"))
    .replace("Tx Dot", "TxDOT")
    .replace("Otherservices", "Other Services")
    for col in columns
}
raise "Change IAH, HOU, and EFD ops based on the HAS values."
path_op_out = Path.home().joinpath(
    PATH_PROCESSED, "report_tables", "Appendix A 2019 Operations.xlsx"
)
ops.rename(columns=rename_col).to_excel(path_op_out, index=False)


path_out_flt1 = Path.home().joinpath(PATH_PROCESSED, "fleet_comm_reliev.xlsx")
path_out_flt2 = Path.home().joinpath(PATH_PROCESSED, "fleet_non_comm_reliev.xlsx")
flt1 = pd.read_excel(path_out_flt1)
flt2 = pd.read_excel(path_out_flt2)
flt = pd.concat([flt1, flt2])
cols = [
    "facility_id",
    "facility_name",
    "facility_group",
    "facility_type",
    "annual_operations",
    "airframe_id",
    "tfmsc_aircraft_id",
    "engine_id",
    "fleetmix",
    "engine_code",
    "Equipment Type",
    "ops",
    "ltos",
]
new_cols = {col: inflection.titleize(col).replace("Tfmsc", "TFMSC") for col in cols}
flt_fil = flt.filter(items=cols).rename(columns=new_cols)

path_flt_out = Path.home().joinpath(
    PATH_PROCESSED, "report_tables", "Appendix B 2019 Fleet Mix.xlsx"
)
flt_fil.to_excel(path_flt_out, index=False)

path_proj_fac = Path.home().joinpath(PATH_INTERIM, "proj_fac_axb_07_11_2021.xlsx")
x1 = pd.ExcelFile(path_proj_fac)
proj_fac = pd.concat(map(x1.parse, x1.sheet_names))

rename_cols = {
    "facility_id": "Facility",
    "facility_name": 'Facility Name',
    "facility_group": 'Facility Group',
    "facility_type": 'Facility Type',
    "sysyear": "Year",
    "proj_fac": "Projection Factor",
}

proj_fac_fil = proj_fac.filter(items=rename_cols.keys()).rename(columns=rename_cols)
path_projfac_out = Path.home().joinpath(
    PATH_PROCESSED, "report_tables", "Appendix C Forecasting Factors.xlsx"
)
proj_fac_fil.to_excel(path_projfac_out, index=False)