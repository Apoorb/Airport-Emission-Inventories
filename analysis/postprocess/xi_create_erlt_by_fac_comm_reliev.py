"""
Get fleetmix and emission rates for the commercial and reliever airports.
"""
import numpy as np
import pandas as pd
from pathlib import Path
from analysis.preprocess.vii_prepare_ops_for_asif import get_flt_db_tabs
from airportei.utilis import PATH_PROCESSED, PATH_INTERIM


path_ops2019_clean = Path.home().joinpath(
    PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx"
)
ops_2019 = pd.read_excel(path_ops2019_clean, index_col=0)
ops_2019_fil = ops_2019.loc[
    lambda df: (df.facility_group.isin(["Commercial", "Reliever"]))
]

path_tti_com = (
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - Documents\2020 Texas Statewide Airport EI"
    r"\Tasks\Task5_ Statewide_2020_AERR_EI\aedt_ems_2019"
    r"\bakFile_metricResults\Commercial"
)
path_tti_rel = (
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - Documents\2020 Texas Statewide Airport EI"
    r"\Tasks\Task5_ Statewide_2020_AERR_EI\aedt_ems_2019"
    r"\bakFile_metricResults\Reliever"
)
path_out_emis = Path.home().joinpath(PATH_PROCESSED, "emis_comm_reliev.xlsx")
path_out_flt = Path.home().joinpath(PATH_PROCESSED, "fleet_comm_reliev.xlsx")

tti_files = [file for file in Path(path_tti_com).glob("*.csv")] + [
    file for file in Path(path_tti_rel).glob("*.csv")
]

pathasifinput = Path(
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - Documents\2020 Texas Statewide Airport EI"
    r"\Tasks\Task5_ Statewide_2020_AERR_EI\aedt_ems_2019\asif_inputs"
)

asifinputdict = {file.name.split("_")[0]: file for file in pathasifinput.glob("*.xlsx")}

flt_list = list()
emis_list = list()
for file in tti_files:
    df = pd.read_csv(file)
    facility_id = file.name.split("_")[0]
    ops_2019_fil_fac = ops_2019_fil.loc[lambda df: df.facility_id == facility_id]
    if facility_id not in ["hou", "iah", "efd"]:
        asif_fi = asifinputdict[facility_id]
        x1 = pd.ExcelFile(asif_fi)
        asif_in = x1.parse(
            "ltos",
            usecols=[
                "airframe_id",
                "arfm_mod",
                "aircraft_id",
                "engine_id",
                "engine_code",
                "op_type",
                "ids",
            ],
        )
        asif_in["User ID"] = asif_in["op_type"] + asif_in["ids"].astype(str)
        df = df.merge(asif_in, on="User ID", how="left")
        df["source"] = "TFMSC"
        df["annual_operations"] = ops_2019_fil_fac["annual_operations"].values[0]

    else:
        flt_tabs = get_flt_db_tabs()
        df_arfm_eng_lookup = pd.DataFrame(
            df.loc[df.Mode == "Climb Below Mixing Height"].drop_duplicates(
                ["Equipment Type"], keep="first"
            )["Equipment Type"]
        )
        df_arfm_eng_lookup[["arfm_mod", "engine_code"]] = df_arfm_eng_lookup[
            "Equipment Type"
        ].str.rsplit("/", expand=True, n=1)
        df_arfm_eng_lookup = df_arfm_eng_lookup.merge(
            flt_tabs["eng"].assign(engine_code=lambda df: df.engine_code),
            on="engine_code",
        )
        df_arfm_eng_lookup = df_arfm_eng_lookup.merge(
            flt_tabs["airfm"], on="arfm_mod", how="left"
        )
        df_arfm_eng_lookup.loc[lambda df: df.arfm_mod == "A109", ["airframe_id"]] = 5238
        df_arfm_eng_lookup.loc[
            lambda df: df.arfm_mod == "EC130", ["airframe_id"]
        ] = 5177
        # df_arfm_eng_lookup.loc[lambda df: df.arfm_mod == "EC130",
        # "airframe_id"] = 5177
        assert (
            df_arfm_eng_lookup.airframe_id.isna().sum() == 0
        ), "Remove GSE equipments."
        df = df.merge(df_arfm_eng_lookup, on="Equipment Type", how="left")
        df["source"] = "HAS"
        df_ops_has = df.loc[
            df.Mode.isin(["Climb Below Mixing Height", "Descend Below Mixing Height"])
        ]
        df["annual_operations"] = df_ops_has["Num Ops"].sum() / 2
    # Cessna Float is an amphibious airplane. Remove it from here.
    df = df.loc[lambda df: df.arfm_mod != "Cessna 182 Float"]
    df["facility_id"] = ops_2019_fil_fac["facility_id"].values[0]
    df["facility_name"] = ops_2019_fil_fac["facility_name"].values[0]
    df["facility_group"] = ops_2019_fil_fac["facility_group"].values[0]
    df["facility_type"] = ops_2019_fil_fac["facility_type"].values[0]
    df["filled_from_facility_id"] = np.nan
    df["filled_from_facility_ops_per_diff"] = np.nan
    df["Mode"] = np.select(
        [
            df["Mode"] == "Climb Below Mixing Height",
            df["Mode"] == "Descend Below Mixing Height",
            df["Mode"] == "GSE LTO",
            df["Mode"] == "APU",
        ],
        ["Climb Below Mixing Height", "Descend Below Mixing Height", "GSE LTO", "APU"],
        np.nan,
    )
    df.rename(columns={"aircraft_id": "tfmsc_aircraft_id"}, inplace=True)
    df.rename(columns={"Num Ops": "ltos"}, inplace=True)
    df_fil = df.loc[df["Mode"] != "nan"]
    df_ltos = (
        df_fil.loc[lambda df: ~df.Mode.isin(["GSE LTO", "APU"])]
        .drop_duplicates("Equipment Type")
        .assign(ops=lambda df: df.ltos * 2)
    )
    df_ltos_agg = (
        df_ltos.groupby(["facility_id", "Mode"])
        .agg(tot_ops=("ops", "sum"), annual_operations=("annual_operations", "first"))
        .reset_index()
    )

    # Corrects for rounding error. Also, need to reassign Cessna 182 Float
    # ops (as it was removed) to other equipment.
    fac_opscor = df_ltos_agg.annual_operations / df_ltos_agg.tot_ops

    df_fil_1 = df_fil
    df_lto_2 = df_ltos
    df_fil_1["ltos"] = df_fil_1["ltos"] * fac_opscor.values[0]
    df_lto_2["ltos"] = df_lto_2["ltos"] * fac_opscor.values[0]
    df_lto_2["ops"] = df_lto_2["ops"] * fac_opscor.values[0]

    if facility_id not in ["hou", "iah", "efd"]:
        assert np.isclose(
            df_lto_2.ops.sum(), ops_2019_fil_fac["annual_operations"].values[0]
        ), "Operations don't match."

    emis = df_fil_1
    flt = df_lto_2.sort_values("ops", ascending=False).reset_index(drop=True)
    flt["fleetmix"] = flt["ops"] / flt["annual_operations"]
    assert all(
        ~flt.duplicated(["facility_id", "Equipment Type"]).values
    ), "Dupicates found. Check data."
    emis = emis.merge(
        flt[["facility_id", "Equipment Type", "fleetmix", "ops"]],
        on=["facility_id", "Equipment Type"],
        how="left",
    )

    # FixMe: Find a way to assign the fleetmix to the emis dataframe.

    flt_keep_cols = [
        "facility_id",
        "facility_name",
        "facility_group",
        "facility_type",
        "annual_operations",
        "airframe_id",
        "tfmsc_aircraft_id",
        "engine_id",
        "fleetmix",
        "anp_airplane_id",
        "anp_helicopter_id",
        "engine_code",
        "Equipment Type",
        "ops",
        "ltos",
        "filled_from_facility_id",
        "filled_from_facility_ops_per_diff",
        "source",
    ]

    flt_fil = flt.filter(items=flt_keep_cols)

    keep_cols = [
        "facility_id",
        "facility_name",
        "facility_group",
        "facility_type",
        "annual_operations",
        "airframe_id",
        "tfmsc_aircraft_id",
        "engine_id",
        "fleetmix",
        "anp_airplane_id",
        "anp_helicopter_id",
        "engine_code",
        "Equipment Type",
        "ops",
        "ltos",
        "filled_from_facility_id",
        "filled_from_facility_ops_per_diff",
        "source",
        "Event ID",
        "Departure Airport",
        "Arrival Airport",
        "Mode",
        "Fuel (ST)",
        "Distance (mi)",
        "Duration ",
        "CO (ST)",
        "THC (ST)",
        "TOG (ST)",
        "VOC (ST)",
        "NMHC (ST)",
        "NOx (ST)",
        "nvPM Mass (ST)",
        "nvPM Number",
        "PMSO (ST)",
        "PMFO (ST)",
        "CO2 (ST)",
        "H2O (ST)",
        "SOx (ST)",
        "PM 2.5 (ST)",
        "PM 10 (ST)",
        "Operation ID",
        "User ID",
        "Operation Time",
    ]
    emis.rename(columns={"aircraft_id": "tfmsc_aircraft_id"}, inplace=True)

    emis_fil = emis.filter(items=keep_cols)

    flt_list.append(flt_fil)
    emis_list.append(emis_fil)

flt_fin = pd.concat(flt_list)
emis_fin = pd.concat(emis_list)

flt_fin.to_excel(path_out_flt, index=False)
emis_fin.to_excel(path_out_emis, index=False)
