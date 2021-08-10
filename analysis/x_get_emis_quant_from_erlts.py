"""
Get the emission quantities from the emission rates for TASP, military,
other public, other private, medical, farm and ranch airports.
Created by: Apoorb
Created on: 8/7/2021
"""

import numpy as np
import pandas as pd
from pathlib import Path
from analysis.vii_prepare_ops_for_asif import get_flt_db_tabs
from airportei.utilis import (
    PATH_RAW,
    PATH_INTERIM,
    get_snake_case_dict,
    connect_to_sql_server,
)


def filter_erlt(aedt_erlt_):
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


# Create emission quantities for f1 medical heliports, f3 farm and ranch
# heliports, f5 military heliports, f7 other private heliports, f9 other
# public heliports, and f11 TASP heliports.
def getheliemis(opsdict_, aedt_erlt_1_, flt_tabs_, analyfac="med_heli"):
    if analyfac == "mil_heli":
        df = pd.DataFrame(
            aedt_erlt_1_[analyfac].drop_duplicates(["Equipment Type"], keep="first")[
                "Equipment Type"
            ]
        )
        df[["anp_helicopter_id", "engine_code"]] = df["Equipment Type"].str.split(
            "/", expand=True
        )
        df_1 = df.merge(
            flt_tabs_["eng"].assign(engine_code=lambda df: df.engine_code),
            on="engine_code",
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
        for idx, row in opsdict_[analyfac][
            ["facility_id", "annual_operations"]
        ].iterrows():
            df_2_cpy = df_2.copy()
            df_2_cpy["facility_id"] = row["facility_id"]
            df_2_cpy["annual_operations"] = row["annual_operations"]
            flt_heli_list.append(df_2_cpy)
        flt_heli = pd.concat(flt_heli_list)
    else:
        flt_heli = (
            x1flt.parse(
                opsdict_[analyfac].facility_group.values[0].replace("/", "_"),
                usecols=[
                    "facility_id",
                    "facility_type",
                    "aircraft_id",
                    "anp_airplane_id",
                    "anp_helicopter_id",
                    "engine_id",
                    "fleetmix",
                    "annual_operations",
                ],
            )
            .loc[
                lambda df: df.facility_type.isin(
                    opsdict_[analyfac].facility_type.unique()
                )
            ]
            .merge(eng_df_1, on="engine_id", how="left")
        )
    flt_heli["Equipment Type"] = flt_heli.anp_helicopter_id + "/" + flt_heli.engine_code
    flt_heli["Num Ops"] = flt_heli.annual_operations * flt_heli.fleetmix / 2
    assert flt_heli.engine_code.isna().sum() == 0
    emis_heli = flt_heli.merge(
        aedt_erlt_1_[analyfac].drop(columns="Num Ops"), on="Equipment Type", how="left"
    )
    assert emis_heli["CO (ST)"].isna().sum() == 0
    return emis_heli


def getarptemis(opsdict_, aedt_erlt_1_, flt_tabs_, analyfac, arfm_eng_omits_):

    if analyfac == "fandr_arpt":
        df = pd.DataFrame(
            aedt_erlt_1_[analyfac].drop_duplicates(["Equipment Type"], keep="first")[
                "Equipment Type"
            ]
        )
        df = df[
            df["Equipment Type"]
            != '"Diesel - F750 Dukes Transportation Services DART 3000 to 6000 gallon - Fuel Truck"'
        ]
        df[["arfm_mod", "engine_code"]] = df["Equipment Type"].str.rsplit(
            "/", expand=True, n=1
        )
        df_1 = df.merge(
            flt_tabs_["eng"].assign(engine_code=lambda df: df.engine_code),
            on="engine_code",
        )

        df_2 = df_1.merge(flt_tabs_["airfm"], on="arfm_mod", how="left")
        df_2["aircraft_id"] = None
        df_2["fleetmix"] = 1 / len(df_2)

        flt_arpt_list = []
        for idx, row in opsdict_[analyfac][
            ["facility_id", "annual_operations"]
        ].iterrows():
            df_2_cpy = df_2.copy()
            df_2_cpy["facility_id"] = row["facility_id"]
            df_2_cpy["annual_operations"] = row["annual_operations"]
            flt_arpt_list.append(df_2_cpy)
        flt_arpt = pd.concat(flt_arpt_list)
        flt_arpt = flt_arpt.assign(airpl_heli_lab=lambda df: df.arfm_mod)
    else:
        flt_arpt = (
            x1flt.parse(
                opsdict_[analyfac].facility_group.values[0].replace("/", "_"),
                usecols=[
                    "facility_id",
                    "facility_type",
                    "airframe_id",
                    "aircraft_id",
                    "anp_airplane_id",
                    "anp_helicopter_id",
                    "engine_id",
                    "fleetmix",
                    "annual_operations",
                ],
            )
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
                airpl_heli_lab=lambda df: np.select(
                    [df.anp_helicopter_id.isna(), ~df.anp_helicopter_id.isna()],
                    [df.arfm_mod, df.anp_helicopter_id],
                )
            )
        )
    flt_arpt = flt_arpt.assign(
        fleetmix_adj=lambda df: df.fleetmix / df.fleetmix.sum(),
        ops_fleet_adj=lambda df: df.fleetmix_adj * df.annual_operations,
    )
    flt_arpt["Equipment Type"] = (
        flt_arpt.airpl_heli_lab.str.lower() + "/" + flt_arpt.engine_code.str.lower()
    )
    flt_arpt["Num Ops"] = flt_arpt.annual_operations * flt_arpt.fleetmix / 2

    assert flt_arpt.airpl_heli_lab.isna().sum() == 0
    assert flt_arpt.engine_code.isna().sum() == 0

    aedt_erlt_1_[analyfac] = aedt_erlt_1_[analyfac].drop(columns="Num Ops")
    aedt_erlt_1_[analyfac]["Equipment Type"] = aedt_erlt_1_[analyfac][
        "Equipment Type"
    ].str.lower()

    emis_arpt = flt_arpt.merge(aedt_erlt_1_[analyfac], on="Equipment Type", how="left")
    test = emis_arpt[emis_arpt["CO (ST)"].isna()].drop_duplicates(
        ["airframe_id", "engine_id"]
    )
    with pd.option_context("display.max_rows", 20, "display.max_columns", 20):
        print(test)
    assert emis_arpt["CO (ST)"].isna().sum() == 0
    return emis_arpt


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

    emis_quant = {}
    # f1
    emis_quant["med_heli"] = getheliemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="med_heli",
    )
    # f3
    emis_quant["fandr_heli"] = getheliemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="fandr_heli",
    )
    # f5
    emis_quant["mil_heli"] = getheliemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="mil_heli",
    )
    # f7
    emis_quant["opra_heli"] = getheliemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="opra_heli",
    )
    # f9
    emis_quant["opua_heli"] = getheliemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="opua_heli",
    )
    # f11
    emis_quant["tasp_heli"] = getheliemis(
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
    emis_quant["fandr_arpt"] = getarptemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1.copy(),
        flt_tabs_=flt_tabs,
        analyfac="fandr_arpt",
        arfm_eng_omits_=arfm_eng_omits_fandr,
    )
    # f4
    emis_quant["mil_arpt"] = getarptemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1.copy(),
        flt_tabs_=flt_tabs,
        analyfac="mil_arpt",
        arfm_eng_omits_=arfm_eng_omits_mil,
    )
    # f6
    emis_quant["opra_arpt"] = getarptemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="opra_arpt",
        arfm_eng_omits_=[None],
    )
    # f8
    emis_quant["opua_arpt"] = getarptemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1.copy(),
        flt_tabs_=flt_tabs,
        analyfac="opua_arpt",
        arfm_eng_omits_=arfm_eng_omits_opua,
    )
    # f10
    emis_quant["tasp_arpt"] = getarptemis(
        opsdict_=opsdict,
        aedt_erlt_1_=aedt_erlt_1,
        flt_tabs_=flt_tabs,
        analyfac="tasp_arpt",
        arfm_eng_omits_=arfm_eng_omits_tasp,
    )

    for key, value in emis_quant.items():
        print(key)
        assert set(emis_quant[key].facility_id.unique()) == set(
            opsdict[key].facility_id.unique()
        )

    assert set() == set(ops_2019_fil.facility_id) - set(
        pd.concat(emis_quant.values())["facility_id"]
    ), ("Missed some " "facilities.")
