import pandas as pd
from pathlib import Path


path12345 = Path(
    r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\external"
    r"\2017neiJan_facility_process_byregions\point_12345.csv"
)
point12345 = pd.read_csv(path12345)

path678910 = Path(
    r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\external"
    r"\2017neiJan_facility_process_byregions\point_678910.csv"
)
point678910 = pd.read_csv(path678910)

pathunkn = Path(
    r"C:\Users\a-bibeka\Downloads\2017neiJan_facility_process_byregions"
    r"\point_unknown.csv"
)
pointunkn = pd.read_csv(pathunkn)
sccdict = {
    2275020000: "Commercial Aviation",
    2275060011: "Air taxis: Piston Driven",
    2275060012: "Air taxis: Turbine Driven",
    2275050011: "General Aviation: Piston Driven",
    2275050012: "General Aviation: Turbine Driven",
    2275001000: "Military",
    2275070000: "Auxiliary Power Units (APUs)",
    2268008005: "Ground Support Equipment (GSE): Compressed natural gas (CNG)-fueled",
    2270008005: "GSE: Diesel-fueled",
    2265008005: "GSE: Gasoline-fueled",
    2267008005: "GSE: Liquefied petroleum gas (LPG)-fueled",
}

set_inter = set(sccdict.keys()).intersection(set(point12345.scc.unique()))
set(sccdict.keys()) - set_inter
set(sccdict.keys()).intersection(set(point678910.scc.unique()))


min(point12345.scc.unique())
max(point12345.scc.unique())

min(point678910.scc.unique())
max(point678910.scc.unique())

point12345_fil = point12345.loc[
    lambda df: (df.state == "TX") & (df.scc.isin(sccdict.keys()))
]


point678910_fil = point678910.loc[
    lambda df: (df.state == "TX") & (df.scc.isin(sccdict.keys()))
]


point678910_fil_dfw = point678910_fil.loc[
    lambda df: df.agency_facility_id.str.lower() == "dfw"
]
point678910_fil_iah = point678910_fil.loc[
    lambda df: df.agency_facility_id.str.lower() == "iah"
]
point678910_fil.calc_method_code.unique()
point678910_fil_test = point678910_fil.loc[point678910_fil.calc_method_code == 13]


fac_map = point678910_fil.drop_duplicates(["eis_facility_id", "agency_facility_id"])


pollutant_code_df = point678910_fil[
    [
        "pollutant_code",
        "pollutant_desc",
        "pollutant_type",
        "calc_method_code",
        "calculation_method",
        "emission_comment",
        "source_data_set",
        "data_tagged",
        "data_set",
    ]
].drop_duplicates(["pollutant_code"])
