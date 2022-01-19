import numpy as np
import pandas as pd
from pathlib import Path

path_common = Path(
    r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\processed\epa_ltos_review"
)
path_iah_out = Path.joinpath(path_common, "iah_epa_aedt_out.xlsx")
path_dfw_out = Path.joinpath(path_common, "dfw_epa_aedt_out.xlsx")
path_epa_ltos = Path(
    r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data" r"\external\LTO for State.xlsx"
)
path_unkn_erg_17 = Path.joinpath(path_common, "erg_2017_unknown_factors.xlsx")
unkn_erg_17 = (
    pd.read_excel(path_unkn_erg_17)
    .loc[lambda df: df.Unit == "ST"]
    .drop(columns=["Aircraft Type", "SCC"])
)
unkn_erg_17_1 = (
    unkn_erg_17.set_index(["Code", "Unit"])
    .stack()
    .reset_index()
    .rename(columns={"level_2": "pollutants", 0: "emis_st_per_op"})
)

epa_ltos = pd.read_excel(path_epa_ltos)
epa_ltos_iah_dfw = epa_ltos.loc[epa_ltos.FacilitySiteIdentifier.isin(["DFW", "IAH"])]
epa_ltos_iah_dfw[
    "FacilitySiteIdentifier"
] = epa_ltos_iah_dfw.FacilitySiteIdentifier.str.lower()
epa_ltos_iah_dfw_fil = (
    epa_ltos_iah_dfw.groupby(["FacilitySiteIdentifier", "AircraftEngineTypeCode"])
    .EPA_LTO.sum()
    .reset_index()
)
epa_ltos_iah_dfw_fil_unknowns = epa_ltos_iah_dfw_fil.loc[
    epa_ltos_iah_dfw_fil.AircraftEngineTypeCode > 999900
].rename(
    columns={"AircraftEngineTypeCode": "Code", "FacilitySiteIdentifier": "facility_id"}
)
epa_ltos_iah_dfw_fil_unknowns_1 = epa_ltos_iah_dfw_fil_unknowns.merge(
    unkn_erg_17_1, on="Code", how="left"
)
epa_ltos_iah_dfw_fil_unknowns_1["emis_st"] = (
    epa_ltos_iah_dfw_fil_unknowns_1.emis_st_per_op
    * epa_ltos_iah_dfw_fil_unknowns_1.EPA_LTO
)
epa_ltos_iah_dfw_fil_unknowns_2 = (
    epa_ltos_iah_dfw_fil_unknowns_1.groupby(["facility_id", "pollutants"])
    .agg(emis_st=("emis_st", sum), ltos=("EPA_LTO", sum))
    .reset_index()
)
epa_ltos_iah_dfw_fil_unknowns_2["type"] = "unknown"

iah_df_known = pd.read_excel(path_iah_out)
iah_df_known["facility_id"] = "iah"

dfw_df_known = pd.read_excel(path_dfw_out)
dfw_df_known["facility_id"] = "dfw"

df_known = pd.concat([iah_df_known, dfw_df_known])
df_known["Mode"] = np.select(
    [
        df_known["Mode"] == "Climb Below Mixing Height",
        df_known["Mode"] == "Descend Below Mixing Height",
        df_known["Mode"] == "GSE LTO",
        df_known["Mode"] == "APU",
    ],
    ["Climb Below Mixing Height", "Descend Below Mixing Height", "GSE LTO", "APU"],
    np.nan,
)
df_known = df_known.loc[df_known["Mode"] != "nan"]
df_known = df_known.rename(
    columns={
        "Equipment Type": "equip_type",
        "CO (ST)": "CO",
        "NOx (ST)": "NOx",
        "PM 10 (ST)": "PM10",
        "PM 2.5 (ST)": "PM25",
        "SOx (ST)": "SOx",
        "VOC (ST)": "VOC",
        "Num Ops": "op",
    }
)
df_known_1 = df_known.filter(
    items=[
        "facility_id",
        "equip_type",
        "Mode",
        "CO",
        "NOx",
        "PM10",
        "PM25",
        "SOx",
        "VOC",
        "op",
    ]
)

df_known_2 = (
    df_known_1.set_index(["facility_id", "equip_type", "Mode", "op"])
    .stack()
    .reset_index()
    .rename(columns={"level_4": "pollutants", 0: "emis_st_per_op"})
)
df_known_2["emis_st"] = df_known_2.emis_st_per_op * df_known_2.op
df_known_3 = (
    df_known_2.groupby(["facility_id", "pollutants"])
    .agg(emis_st=("emis_st", sum))
    .reset_index()
)
df_known_3["type"] = "known"
df_known_3.groupby(["facility_id", "pollutants"]).emis_st.sum()

df_ltos = (
    df_known_2.loc[lambda df: ~df.Mode.isin(["GSE LTO", "APU"])]
    .drop_duplicates(["facility_id", "equip_type"])
    .assign(ltos=lambda df: df.op)
)
df_ltos_agg = df_ltos.groupby(["facility_id"]).agg(ltos=("ltos", "sum")).reset_index()


df_known_4 = df_known_3.merge(df_ltos_agg, on="facility_id")

df_tot = pd.concat([epa_ltos_iah_dfw_fil_unknowns_2, df_known_4])
df_tot = (
    df_tot.groupby(["facility_id", "pollutants"])
    .agg(emis_st=("emis_st", sum), ltos=("ltos", sum))
    .reset_index()
)

epa_ltos_iah_dfw.groupby("FacilitySiteIdentifier").EPA_LTO.sum()
erg_epa_emis_iah_dfw = df_tot
erg_epa_emis_iah_dfw_1 = erg_epa_emis_iah_dfw.rename(columns={"emis_st": "erg_epa_emis_tons"}).drop(columns="ltos")

path_tti_emis = Path(r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\processed\report_tables\emis_ltos_2020_v2.xlsx")
df_tti = pd.read_excel(path_tti_emis, "cntr_uncntr", index_col=0)
df_tti_emis_iah_dfw = df_tti.loc[df_tti.facility_id.isin(["iah", "dfw"])]
df_tti_emis_iah_dfw_1 = df_tti_emis_iah_dfw.groupby(["facility_id", "eis_pollutant_id"]).cntr_enis_tons.sum().reset_index()

pol_tti = ['CO', 'NOX', 'PM10-PRI', 'PM25-PRI', 'SO2', 'VOC']
pol_erg = ['CO', 'NOx', 'PM10', 'PM25', 'SOx', 'VOC']
pol_map = {i:j for i,j in zip(pol_tti, pol_erg)}

df_tti_emis_iah_dfw_1["pollutants"] = df_tti_emis_iah_dfw_1.eis_pollutant_id.map(pol_map)
df_tti_emis_iah_dfw_2 = df_tti_emis_iah_dfw_1.loc[lambda df: ~ df.pollutants.isna()].drop(columns="eis_pollutant_id").rename(columns={"cntr_enis_tons":"tti_emis_tons"})

df_tti_erg_emis = erg_epa_emis_iah_dfw_1.merge(df_tti_emis_iah_dfw_2, on=["facility_id", 'pollutants'])

df_tti_erg_emis["diff1"] = (df_tti_erg_emis.tti_emis_tons - df_tti_erg_emis.erg_epa_emis_tons)
df_tti_erg_emis["per_diff"] = (df_tti_erg_emis.diff1) / df_tti_erg_emis.erg_epa_emis_tons
path_out_emis = Path.joinpath(path_common, "erg_epa_tti_emis_iah_dfw.xlsx")
df_tti_erg_emis.to_excel(path_out_emis)
