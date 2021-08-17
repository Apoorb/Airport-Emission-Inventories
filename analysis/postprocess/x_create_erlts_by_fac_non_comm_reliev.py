"""
Create the emission rate tables by facilities from the emission rates for TASP,
military,
other public, other private, medical, farm and ranch airports. Assigning
arbitrary ids f1 to f12 to keep track of different facility categories.
f1) med_hel
f2) fandr_arpt f3) fandr_heli
f4) mil_arpt f5) mil_heli
f6) opra_arpt f7) opra_heli
f8) opua_arpt f9) opua_heli
f10) tasp_arpt f11) tasp_heli
Created by: Apoorb
Created on: 8/7/2021
"""

import numpy as np
import pandas as pd
from pathlib import Path
from analysis.preprocess.vii_prepare_ops_for_asif import get_flt_db_tabs
from airportei.utilis import PATH_PROCESSED, PATH_INTERIM


def filter_erlt(aedt_erlt_):
    """
    Filter operation mode emission output to only keep relevant rows.
    Parameters
    ----------
    aedt_erlt_: AEDT ERLT: ERLT for different facility types and facility
    group combinations.
    Returns
    -------
    dict: Filtered AEDT ERLT
    """
    aedt_erlt_1_ = {}
    for key, df in aedt_erlt_.items():
        df_1 = df.copy()
        df_1["Mode"] = np.select(
            [
                df_1["Mode"] == "Climb Below Mixing Height",
                df_1["Mode"] == "Descend Below Mixing Height",
                df_1["Mode"] == "GSE LTO",
                df_1["Mode"] == "APU",
            ],
            [
                "Climb Below Mixing Height",
                "Descend Below Mixing Height",
                "GSE LTO",
                "APU",
            ],
            np.nan,
        )
        df_1_fil = df_1.loc[df_1["Mode"] != "nan"]
        aedt_erlt_1_[key] = df_1_fil
    return aedt_erlt_1_


def create_mil_flt(opsdict_, aedt_erlt_1_, flt_tabs_, analyfac="mil_heli"):
    """
    Create fleetmix for military heliports. We are using the following three
    helicopters: CH-47F Chinook USAF, SIkorsky UH-60 Black Hawk US Army, and
    Boeing AH-64 Apache. Based on tables [2020AERR_YR2019_Mil_Heliport].[dbo].[
    # AIR_OPERATION_AIRCRAFT] and [2020AERR_YR2019_Mil_Heliport].[
    # dbo].[FLT_EQUIPMENT], these helicopter have the following airframe ids:
    4891, 4935, and 5245.
    Parameters
    ----------
    opsdict_: Operations for different facility types and facility group
    combinations.
    aedt_erlt_1_: AEDT ERLT: ERLT for different facility types and facility
    group combinations.
    flt_tabs_: FLEET database tables.
    analyfac: Analysis facility group and type tag.
    Returns
    -------
    pd.DataFrame: Military heliport fleetmix.
    """
    df = pd.DataFrame(
        aedt_erlt_1_[analyfac].drop_duplicates(["Equipment Type"], keep="first")[
            "Equipment Type"
        ]
    )
    df[["anp_helicopter_id", "engine_code"]] = df["Equipment Type"].str.split(
        "/", expand=True
    )
    df_1 = df.merge(
        flt_tabs_["eng"].assign(engine_code=lambda df: df.engine_code), on="engine_code"
    )
    # Based on tables [2020AERR_YR2019_Mil_Heliport].[dbo].[
    # AIR_OPERATION_AIRCRAFT] and [2020AERR_YR2019_Mil_Heliport].[
    # dbo].[FLT_EQUIPMENT]
    df_1["airframe_id"] = np.select(
        [
            df_1.anp_helicopter_id == "CH47D",
            df_1.anp_helicopter_id == "S70",
            df_1.anp_helicopter_id == "B212",
        ],
        [4891, 4935, 5245],
    )
    df_1["aircraft_id"] = None

    df_2 = df_1.merge(flt_tabs_["airfm"], on="airframe_id")
    df_2["fleetmix"] = 0.33

    flt_heli_list = []
    for idx, row in opsdict_[analyfac].iterrows():
        df_2_cpy = df_2.copy()
        df_2_cpy["facility_id"] = row["facility_id"]
        df_2_cpy["facility_name"] = row["facility_name"]
        df_2_cpy["facility_group"] = row["facility_group"]
        df_2_cpy["facility_type"] = row["facility_type"]
        df_2_cpy["annual_operations"] = row["annual_operations"]
        flt_heli_list.append(df_2_cpy)
    flt_heli_ = pd.concat(flt_heli_list)
    flt_heli_["source"] = "Madhu used 3 generic military helicopters."
    return flt_heli_


def getheliemis(opsdict_, aedt_erlt_1_, flt_tabs_, analyfac="med_heli"):
    """
    Create emission rates for f1 medical heliports, f3 farm and ranch
    heliports, f5 military heliports, f7 other private heliports, f9 other
    public heliports, and f11 TASP heliports.
    Parameters
    ----------
    opsdict_: Operations for different facility types and facility group
    combinations.
    aedt_erlt_1_: AEDT ERLT: ERLT for different facility types and facility
    group combinations.
    flt_tabs_: FLEET database tables.
    analyfac: Analysis facility group and type tag.
    Returns
    -------
    (pd.DataFrame, pd.DataFrame): Heliport fleet and emission rates.
    """
    if analyfac == "mil_heli":
        flt_heli = create_mil_flt(
            opsdict_=opsdict_,
            aedt_erlt_1_=aedt_erlt_1_,
            flt_tabs_=flt_tabs_,
            analyfac=analyfac,
        )
    else:
        flt_heli = (
            x1flt.parse(
                opsdict_[analyfac].facility_group.values[0].replace("/", "_"),
                usecols=[
                    "facility_id",
                    "facility_name",
                    "facility_group",
                    "facility_type",
                    "airframe_id",
                    "aircraft_id",
                    "anp_airplane_id",
                    "anp_helicopter_id",
                    "engine_id",
                    "fleetmix",
                    "annual_operations",
                    "ops_per_diff",
                ],
            )
            .loc[
                lambda df: df.facility_type.isin(
                    opsdict_[analyfac].facility_type.unique()
                )
            ]
            .merge(eng_df_1, on="engine_id", how="left")
        )
        flt_heli["source"] = (
            "Apoorb used 3 generic helicopters based on: "
            "https://www.helis.com/database/org/Texas-Helicopters/."
        )
    flt_heli["Equipment Type"] = flt_heli.anp_helicopter_id + "/" + flt_heli.engine_code
    flt_heli["ops"] = flt_heli.annual_operations * flt_heli.fleetmix
    flt_heli["ltos"] = flt_heli.annual_operations * flt_heli.fleetmix / 2
    flt_heli.rename(columns={"aircraft_id": "tfmsc_aircraft_id"}, inplace=True)
    flt_heli["filled_from_facility_id"] = np.nan
    flt_heli["filled_from_facility_ops_per_diff"] = np.nan
    assert flt_heli.engine_code.isna().sum() == 0
    emis_heli = flt_heli.merge(
        aedt_erlt_1_[analyfac].drop(columns="Num Ops"), on="Equipment Type", how="left"
    )
    assert emis_heli["CO (ST)"].isna().sum() == 0
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

    emis_keep_cols = [
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
    emis_heli_fil = emis_heli.filter(items=emis_keep_cols)
    flt_heli_fil = flt_heli.filter(items=flt_keep_cols)
    return flt_heli_fil, emis_heli_fil


def create_fandr_flt(
    opsdict_, aedt_erlt_1_, flt_tabs_, fil_source_, analyfac="fandr_arpt"
):
    """
    Create farm and ranch fleetmix.
    """
    df = pd.DataFrame(
        aedt_erlt_1_[analyfac].drop_duplicates(["Equipment Type"], keep="first")[
            "Equipment Type"
        ]
    )
    df = df[
        df["Equipment Type"]
        != '"Diesel - F750 Dukes Transportation Services DART 3000 to 6000 '
        'gallon - Fuel Truck"'
    ]
    df[["arfm_mod", "engine_code"]] = df["Equipment Type"].str.rsplit(
        "/", expand=True, n=1
    )
    df_1 = df.merge(
        flt_tabs_["eng"].assign(engine_code=lambda df: df.engine_code), on="engine_code"
    )

    df_2 = df_1.merge(flt_tabs_["airfm"], on="arfm_mod", how="left")
    assert df_2.airframe_id.isna().sum() == 0, "Remove GSE equipments."
    df_2["aircraft_id"] = None
    # Assign each row equal weightage.
    df_2["fleetmix"] = 1 / len(df_2)

    flt_arpt_list = []
    for idx, row in opsdict_[analyfac].iterrows():
        df_2_cpy = df_2.copy()
        df_2_cpy["facility_id"] = row["facility_id"]
        df_2_cpy["facility_name"] = row["facility_name"]
        df_2_cpy["facility_group"] = row["facility_group"]
        df_2_cpy["facility_type"] = row["facility_type"]
        df_2_cpy["annual_operations"] = row["annual_operations"]
        flt_arpt_list.append(df_2_cpy)
    flt_arpt_ = pd.concat(flt_arpt_list)
    flt_arpt_ = flt_arpt_.assign(airpl_heli_lab=lambda df: df.arfm_mod)
    flt_arpt_["filled_from_facility_id"] = np.nan
    flt_arpt_["filled_from_facility_ops_per_diff"] = np.nan
    flt_arpt_["source"] = fil_source_
    return flt_arpt_


def getarptemis(
    opsdict_, aedt_erlt_1_, flt_tabs_, analyfac, arfm_eng_omits_, fil_source_
):
    """
    Create emission rates for f2 farm and ranch airports, f4 military
    airports, f6 other private airports, f8 other
    public airports, and f10 TASP airports.
    Parameters
    ----------
    opsdict_: Operations for different facility types and facility group
    combinations.
    aedt_erlt_1_: AEDT ERLT: ERLT for different facility types and facility
    group combinations.
    flt_tabs_: FLEET database tables.
    analyfac: Analysis facility group and type tag.
    arfm_eng_omits_: Airframe_id + "_" + engine_id combination to be omitted.
    These combinations do not have a profile so were not modeled in AEDT.
    Returns
    -------
    (pd.DataFrame, pd.DataFrame): Airport fleet and emission rates.
    """
    if analyfac == "fandr_arpt":
        flt_arpt = create_fandr_flt(
            opsdict_=opsdict_,
            aedt_erlt_1_=aedt_erlt_1_,
            flt_tabs_=flt_tabs_,
            analyfac=analyfac,
            fil_source_=fil_source_,
        )
    else:
        flt_arpt = (
            x1flt.parse(
                opsdict_[analyfac].facility_group.values[0].replace("/", "_"),
                usecols=[
                    "facility_id",
                    "facility_name",
                    "facility_group",
                    "facility_type",
                    "airframe_id",
                    "aircraft_id",
                    "anp_airplane_id",
                    "anp_helicopter_id",
                    "engine_id",
                    "fleetmix",
                    "annual_operations",
                    "filled_from_facility_id",
                    "ops_per_diff",
                ],
            )
            .rename(columns={"ops_per_diff": "filled_from_facility_ops_per_diff"})
            .loc[
                lambda df: df.facility_type.isin(
                    opsdict_[analyfac].facility_type.unique()
                )
            ]
            # Removing airframe+engine ids with missing profiles.
            .loc[
                lambda df: ~(
                    df.airframe_id.astype(str) + "_" + df.engine_id.astype(str)
                ).isin(arfm_eng_omits_)
            ]
            .merge(flt_tabs_["eng"], on="engine_id", how="left")
            .merge(flt_tabs_["airfm"], on="airframe_id", how="left")
            .assign(
                # Helicopter uses anp_helicopter_id and airplanes uses
                # arfm_mod as the prefix in equipment type.
                airpl_heli_lab=lambda df: np.select(
                    [df.anp_helicopter_id.isna(), ~df.anp_helicopter_id.isna()],
                    [df.arfm_mod, df.anp_helicopter_id],
                )
            )
        )
        flt_arpt.filled_from_facility_id.isna().sum()
        flt_arpt["source"] = np.select(
            [
                flt_arpt.filled_from_facility_id.isna(),
                ~flt_arpt.filled_from_facility_id.isna(),
            ],
            ["TFMSC", fil_source_],
            None,
        )
    # Adjust fleetmix and operations after removing missing airframes+engines.
    flt_arpt = flt_arpt.assign(
        fleetmix_adj=lambda df: df.fleetmix / df.fleetmix.sum(),
        ops_fleet_adj=lambda df: df.fleetmix_adj * df.annual_operations,
    )
    flt_arpt["Equipment Type"] = (
        flt_arpt.airpl_heli_lab.str.lower() + "/" + flt_arpt.engine_code.str.lower()
    )
    flt_arpt.rename(columns={"aircraft_id": "tfmsc_aircraft_id"}, inplace=True)
    # Reassign number of operations based on the fleetmix data. We are using
    # AEDT output only as ERLT.
    flt_arpt["ops"] = flt_arpt.annual_operations * flt_arpt.fleetmix
    flt_arpt["ltos"] = flt_arpt.annual_operations * flt_arpt.fleetmix / 2

    assert flt_arpt.airpl_heli_lab.isna().sum() == 0
    assert flt_arpt.engine_code.isna().sum() == 0

    aedt_erlt_1_[analyfac] = aedt_erlt_1_[analyfac].drop(columns="Num Ops")
    aedt_erlt_1_[analyfac]["Equipment Type"] = aedt_erlt_1_[analyfac][
        "Equipment Type"
    ].str.lower()

    emis_arpt = flt_arpt.merge(aedt_erlt_1_[analyfac], on="Equipment Type", how="left")
    # test = emis_arpt[emis_arpt["CO (ST)"].isna()].drop_duplicates(
    #     ["airframe_id", "engine_id"]
    # )
    # with pd.option_context("display.max_rows", 20, "display.max_columns", 20):
    #     print(test)
    assert emis_arpt["CO (ST)"].isna().sum() == 0

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

    emis_keep_cols = [
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
    flt_arpt_fil = flt_arpt.filter(items=flt_keep_cols)
    emis_arpt_fil = emis_arpt.filter(items=emis_keep_cols)
    return flt_arpt_fil, emis_arpt_fil


if __name__ == "__main__":
    # 1. Set paths.
    path_ops2019_clean = Path.home().joinpath(
        PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx"
    )
    path_tti_fi = (
        r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
        r"\HMP - TCEQ Projects - Documents\2020 Texas Statewide Airport EI"
        r"\Tasks\Task5_ Statewide_2020_AERR_EI\aedt_ems_2019"
        r"\bakFile_metricResults"
    )
    path_fleetmix_clean = Path.home().joinpath(
        PATH_INTERIM, "fleetmix_axb_07_05_2021.xlsx"
    )
    x1flt = pd.ExcelFile(path_fleetmix_clean)
    path_mil_arpt = Path.home().joinpath(path_tti_fi, "Mil_Airport_2019_opmode_st.csv")
    path_mil_heli = Path.home().joinpath(path_tti_fi, "Mil_Heliport_2019_opmode_st.csv")
    path_med_heli = Path.home().joinpath(path_tti_fi, "Medical_2019_opmode_st.csv")
    path_fandr = Path.home().joinpath(path_tti_fi, "FarmRanch_2019_opmode_st.csv")
    path_opra_arpt = Path.home().joinpath(
        path_tti_fi, "OPRA_Airport_2019_opmode_st.csv"
    )
    path_opua_arpt = Path.home().joinpath(
        path_tti_fi, "OPUA_Airport_2019_opmode_st.csv"
    )
    path_tasp = Path.home().joinpath(path_tti_fi, "TASP_Airport_2019_opmode_st.csv")
    path_out_emis = Path.home().joinpath(PATH_PROCESSED, "emis_non_comm_reliev.xlsx")
    path_out_flt = Path.home().joinpath(PATH_PROCESSED, "fleet_non_comm_reliev.xlsx")
    med_heli_tmp = pd.read_csv(path_med_heli)

    # 2. Get the AEDT ERLTs
    # Assigning arbitrary ids f1 to f12 to keep track of these categories.
    aedt_erlt = dict(
        med_heli=med_heli_tmp,  # f1
        fandr_arpt=pd.read_csv(path_fandr),  # f2
        fandr_heli=med_heli_tmp,  # f3
        mil_arpt=pd.read_csv(path_mil_arpt),  # f4
        mil_heli=pd.read_csv(path_mil_heli),  # f5
        opra_arpt=pd.read_csv(path_opra_arpt),  # f6
        opra_heli=med_heli_tmp,  # f7
        opua_arpt=pd.read_csv(path_opua_arpt),  # f8
        opua_heli=med_heli_tmp,  # f9
        tasp_arpt=pd.read_csv(path_tasp),  # f10
        tasp_heli=med_heli_tmp,  # f11
    )
    aedt_erlt_1 = filter_erlt(aedt_erlt)

    # 3. Get ops.
    ops_2019 = pd.read_excel(path_ops2019_clean, index_col=0)
    ops_2019_fil = ops_2019.loc[
        lambda df: (~df.facility_type.isin(["GLIDERPORT"]))
        & (~df.facility_group.isin(["Commercial", "Reliever"]))
    ]
    ops_2019_fil[["facility_group", "facility_type"]].drop_duplicates(
        ["facility_group", "facility_type"]
    ).sort_values(["facility_group", "facility_type"])
    opsdict = dict(
        med_heli=ops_2019_fil.loc[
            lambda df: (df.facility_group == "Medical")
            & (df.facility_type == "HELIPORT")
        ],  # f1
        fandr_arpt=ops_2019_fil.loc[
            lambda df: (df.facility_group == "Farm/Ranch")
            & (df.facility_type.isin(["AIRPORT", "ULTRALIGHT"]))
        ],  # f2
        fandr_heli=ops_2019_fil.loc[
            lambda df: (df.facility_group == "Farm/Ranch")
            & (df.facility_type == "HELIPORT")
        ],  # f3
        mil_arpt=ops_2019_fil.loc[
            lambda df: (df.facility_group == "Military")
            & (df.facility_type == "AIRPORT")
        ],  # f4
        mil_heli=ops_2019_fil.loc[
            lambda df: (df.facility_group == "Military")
            & (df.facility_type == "HELIPORT")
        ],  # f5
        opra_arpt=ops_2019_fil.loc[
            lambda df: (df.facility_group == "Other_PR_Airports")
            & (df.facility_type.isin(["AIRPORT", "ULTRALIGHT"]))
        ],  # f6
        opra_heli=ops_2019_fil.loc[
            lambda df: (df.facility_group == "Other_PR_Heliports")
            & (df.facility_type == "HELIPORT")
        ],  # f7
        opua_arpt=ops_2019_fil.loc[
            lambda df: (df.facility_group == "Other_PU_Airports")
            & (df.facility_type.isin(["SEAPLANE BASE", "AIRPORT"]))
        ],  # f8
        opua_heli=ops_2019_fil.loc[
            lambda df: (df.facility_group == "Other_PU_Heliports")
            & (df.facility_type == "HELIPORT")
        ],  # f9
        tasp_arpt=ops_2019_fil.loc[
            lambda df: (df.facility_group == "TASP")
            & (df.facility_type.isin(["AIRPORT"]))
        ],  # f10
        tasp_heli=ops_2019_fil.loc[
            lambda df: (df.facility_group == "TASP")
            & (df.facility_type.isin(["HELIPORT"]))
        ],  # f11
    )

    assert set() == set(ops_2019_fil.facility_id) - set(
        pd.concat(opsdict.values())["facility_id"]
    ), "Missed some facilities."

    # 4. Get engine and airframe databases.
    flt_tabs = get_flt_db_tabs()
    airfm_df_1 = flt_tabs["airfm"]
    eng_df_1 = flt_tabs["eng"]

    emis_rt = {}
    flt = {}
    # f1
    flt["med_heli"], emis_rt["med_heli"] = getheliemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="med_heli",
    )
    # f3
    flt["fandr_heli"], emis_rt["fandr_heli"] = getheliemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="fandr_heli",
    )
    # f5
    flt["mil_heli"], emis_rt["mil_heli"] = getheliemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="mil_heli",
    )
    # f7
    flt["opra_heli"], emis_rt["opra_heli"] = getheliemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="opra_heli",
    )
    # f9
    flt["opua_heli"], emis_rt["opua_heli"] = getheliemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="opua_heli",
    )
    # f11
    flt["tasp_heli"], emis_rt["tasp_heli"] = getheliemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="tasp_heli",
    )

    # Get airport emissions.
    # Removing airframe+engine ids with missing profiles.
    arfm_eng_omits_fandr = ["4784_1205", "4779_1205"]
    arfm_eng_omits_tasp = [
        "4894_1224",
        "4896_1270",
        "4911_1564",
        "4922_1678",
        "5175_1743",
    ]
    arfm_eng_omits_mil = [
        "4894_1224",
        "4896_1270",
        "4911_1564",
        "4922_1678",
        "5359_1562",
    ]
    arfm_eng_omits_opua = ["4894_1224", "4922_1678"]
    # f2
    flt["fandr_arpt"], emis_rt["fandr_arpt"] = getarptemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1.copy(),
        flt_tabs_=flt_tabs,
        analyfac="fandr_arpt",
        arfm_eng_omits_=arfm_eng_omits_fandr,
        fil_source_=("Madhu used generic fleetmix for all farm and " "ranch airports."),
    )
    # f4
    flt["mil_arpt"], emis_rt["mil_arpt"] = getarptemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1.copy(),
        flt_tabs_=flt_tabs,
        analyfac="mil_arpt",
        arfm_eng_omits_=arfm_eng_omits_mil,
        fil_source_=(
            "Apoorb used military airports in the same district to fill "
            "the military airport fleet mix data"
        ),
    )
    # f6
    flt["opra_arpt"], emis_rt["opra_arpt"] = getarptemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="opra_arpt",
        arfm_eng_omits_=[None],
        fil_source_=(
            "Apoorb used other public airports with the lowest "
            "operations to get fleet mix for other airports. No other "
            "private airport had data. Apoorb has assumed that the other "
            "public airport with lowest operation is represntative of "
            "other private airports."
        ),
    )
    # f8
    flt["opua_arpt"], emis_rt["opua_arpt"] = getarptemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1.copy(),
        flt_tabs_=flt_tabs,
        analyfac="opua_arpt",
        arfm_eng_omits_=arfm_eng_omits_opua,
        fil_source_=(
            "Apoorb used other public airports with the lowest "
            "operations to get fleet mix for other airports. No other "
            "private airport had data. Apoorb has assumed that the other "
            "public airport with lowest operation is representative of "
            "other public airports that were likely minor and thus did "
            "not get included in TFMSC."
        ),
    )
    # f10
    flt["tasp_arpt"], emis_rt["tasp_arpt"] = getarptemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="tasp_arpt",
        arfm_eng_omits_=arfm_eng_omits_tasp,
        fil_source_=(
            "Used military airports in the same district to fill "
            "the military airport fleet mix data"
        ),
    )

    flt_fin = pd.concat(flt.values())
    emis_rt_fin = pd.concat(emis_rt.values())

    for key, value in emis_rt.items():
        assert set(emis_rt[key].facility_id.unique()) == set(
            opsdict[key].facility_id.unique()
        )

    assert set() == set(ops_2019_fil.facility_id) - set(emis_rt_fin["facility_id"]), (
        "Missed some " "facilities."
    )

    emis_rt_fin.to_excel(path_out_emis, index=False)
    flt_fin.to_excel(path_out_flt, index=False)
