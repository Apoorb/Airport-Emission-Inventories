"""
Develop ASIF input file based on ERG/EPA 2020 NEI LTO data.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import pathlib
from airportei.utilis import PATH_INTERIM, connect_to_sql_server, get_snake_case_dict


def clean_fleetmix(path_fltmix_: pathlib.WindowsPath, fac_id="iah") -> pd.DataFrame:
    """
    Get Ops from annual operations and fleet mix.
    """
    fltmix_fgrp = pd.read_excel(path_fltmix_)
    fltmix_fgrp_fid = fltmix_fgrp.loc[lambda df: df.facility_id == fac_id]
    if fac_id in ["dfw", "iah"]:
        fltmix_fgrp_fid["aircraft_id"] = np.nan
    fltmix_fgrp_fid_fin = (
        fltmix_fgrp_fid.groupby(
            ["facility_id", "closest_airframe_id_aedt", "engine_id"]
        )
        .agg(
            annual_operations=("annual_operations", "first"),
            anp_airplane_id=("anp_airplane_id", "first"),
            anp_helicopter_id=("anp_helicopter_id", "first"),
            aircraft_id=("aircraft_id", "first"),
            fleetmix=("fleetmix", "sum"),
        )
        .reset_index()
        .assign(
            ops_fleet=lambda df: df.annual_operations * df.fleetmix,
            anp_airplane_id=lambda df: df.anp_airplane_id.fillna(df.anp_helicopter_id),
        )
    )
    return fltmix_fgrp_fid_fin


if __name__ == "__main__":
    # Read EPA LTOs data
    path_epa_ltos = Path(
        r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data"
        r"\external\LTO for State.xlsx"
    )
    # Read EPA Airframe Engine lookup data
    path_aedt_epa_lookup = Path(
        r"C:\Users\a-bibeka\PycharmProjects"
        r"\airport_ei\data\external"
        r"\2020_engine_type_codes.csv"
    )

    epa_ltos = pd.read_excel(path_epa_ltos)
    # > 999900 are generic---will use ERG 2017 Unknown emission factors for it.
    # They are handled separately.
    aedt_epa_lookup = pd.read_csv(path_aedt_epa_lookup).loc[
        lambda df: df.Code <= 999900
    ]
    # Filter to keep only IAH and DFW
    epa_ltos_iah_dfw = epa_ltos.loc[
        epa_ltos.FacilitySiteIdentifier.isin(["DFW", "IAH"])
    ]
    epa_ltos_iah_dfw_fil = (
        epa_ltos_iah_dfw.groupby(["FacilitySiteIdentifier", "AircraftEngineTypeCode"])
        .EPA_LTO.sum()
        .reset_index()
    )
    # Operations are twice of LTOs
    epa_ltos_iah_dfw_fil["annual_operations"] = (
        epa_ltos_iah_dfw_fil.groupby("FacilitySiteIdentifier").EPA_LTO.transform(sum)
        * 2
    )
    # Get fleetmix based on annual operations.
    epa_ltos_iah_dfw_fil["fleetmix"] = (
        epa_ltos_iah_dfw_fil.EPA_LTO * 2 / epa_ltos_iah_dfw_fil.annual_operations
    )

    # > 999900 are generic---will use ERG 2017 Unknown emission factors for it.
    # They are handled separately.
    epa_ltos_iah_dfw_fil_1 = epa_ltos_iah_dfw_fil.loc[
        epa_ltos_iah_dfw_fil.AircraftEngineTypeCode <= 999900
    ]
    epa_ltos_iah_dfw_fil_2 = epa_ltos_iah_dfw_fil.loc[
        epa_ltos_iah_dfw_fil.AircraftEngineTypeCode > 999900
    ]

    # See the Ops with Unknown Airframe+EngineIDs
    epa_ltos_iah_dfw_fil.groupby("FacilitySiteIdentifier").EPA_LTO.agg(sum) * 2
    # See the Ops without Unknown Airframe+EngineIDs
    epa_ltos_iah_dfw_fil_1.groupby("FacilitySiteIdentifier").EPA_LTO.agg(sum) * 2

    # Get Mapping tables from AEDT
    conn = connect_to_sql_server(database_nm="FLEET")
    equip = pd.read_sql("SELECT * FROM [dbo].[FLT_EQUIPMENT]", conn)
    eng_df = pd.read_sql(
        "SELECT engine_id, MODEL AS Engine FROM [dbo].[FLT_ENGINES]", conn
    )
    # Need anp_airplane_id and anp_helicopter_id to get profiles.
    equip_db_fil = (
        equip.rename(columns=get_snake_case_dict(equip))
        .rename(
            columns={
                "airframe_id": "closest_airframe_id_aedt",
                "engine_id": "engine_id",
            }
        )
        .sort_values(["closest_airframe_id_aedt", "engine_id"])
        .groupby(["closest_airframe_id_aedt", "engine_id"])
        .agg(
            anp_airplane_id=("anp_airplane_id", "first"),
            anp_helicopter_id=("anp_helicopter_id", "first"),
        )
        .reset_index()
        .merge(eng_df, on=["engine_id"], how="left")
    )
    aedt_epa_lookup_1 = aedt_epa_lookup.merge(
        equip_db_fil,
        left_on=["EDMS Aircraft Code", "Engine"],
        right_on=["closest_airframe_id_aedt", "Engine"],
        how="left",
    )

    test = aedt_epa_lookup_1.loc[lambda df: df["EDMS ID"] != df["engine_id"]]
    # No need to worry about the merge where Airframe+Engine had one-to-one
    # mapping between EPA and AEDT
    # There are 2784 out of 3579 rows that need no treatment.
    aedt_epa_lookup_2 = aedt_epa_lookup_1.loc[
        lambda df: ~df.Code.duplicated(keep=False)
    ]
    sum(aedt_epa_lookup_2["EDMS ID"] == aedt_epa_lookup_2["engine_id"])
    # Fix the merge where Airframe+Engine had one-to-many mapping between EPA
    # and AEDT. Use EDMS ID to resolve conflict.
    # There are 756 out of 3579 rows that need this treatment.
    fix_merge = aedt_epa_lookup_1.loc[lambda df: df.Code.duplicated(keep=False)]
    fix_merge.Code.drop_duplicates(keep="first")
    fix_merge_1 = fix_merge.loc[lambda df: df["EDMS ID"] == df["engine_id"]]
    missing_Codes = set(fix_merge.Code).difference(set(fix_merge_1.Code))
    # Fix the merge where Airframe+Engine had one-to-many mapping between EPA
    # and AEDT. And no EDMS ID == Engine_ID. Just arbitrary pick the lower
    # Engine ID code.
    # There only 39 out of 3579 rows that need this treatment.
    fix_merge_2 = fix_merge.loc[lambda df: df.Code.isin(missing_Codes)]
    idx = fix_merge_2.groupby("Code").engine_id.transform(min) == fix_merge_2.engine_id
    fix_merge_3 = fix_merge_2[idx]
    aedt_epa_lookup_3 = pd.concat([aedt_epa_lookup_2, fix_merge_1, fix_merge_3])

    # Add AEDT airframe and engine information to the EPA data.
    epa_ltos_iah_dfw_fil_1_1 = epa_ltos_iah_dfw_fil_1.merge(
        aedt_epa_lookup_3, left_on="AircraftEngineTypeCode", right_on="Code", how="left"
    )
    epa_ltos_iah_dfw_fil_1_1["FacilitySiteIdentifier"] = epa_ltos_iah_dfw_fil_1_1[
        "FacilitySiteIdentifier"
    ].str.lower()
    epa_ltos_iah_dfw_fil_1_1 = epa_ltos_iah_dfw_fil_1_1[
        [
            "FacilitySiteIdentifier",
            "closest_airframe_id_aedt",
            "engine_id",
            "AircraftEngineTypeCode",
            "annual_operations",
            "fleetmix",
            "anp_airplane_id",
            "anp_helicopter_id",
        ]
    ].rename(columns={"FacilitySiteIdentifier": "facility_id"})

    # Output finalized IAH and DFW datasets.
    epa_ltos_iah_fin = epa_ltos_iah_dfw_fil_1_1.loc[lambda df: df.facility_id == "iah"]
    epa_ltos_dfw_fin = epa_ltos_iah_dfw_fil_1_1.loc[lambda df: df.facility_id == "dfw"]
    path_out = Path(
        r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\processed\epa_ltos_review"
    )
    path_out_mapping = Path.joinpath(
        path_out, "mapping_epa_to_aedt_conflict_resolve_min.xlsx"
    )
    path_out_iah = Path.joinpath(path_out, "iah_input_fi_epa_raw.xlsx")
    path_out_dfw = Path.joinpath(path_out, "dfw_input_fi_epa_raw.xlsx")
    xlwr = pd.ExcelWriter(path_out_mapping)
    aedt_epa_lookup_3.to_excel(xlwr, "fin_epa_aedt_conflt_resolve_min")
    aedt_epa_lookup_2.to_excel(xlwr, "No Conflict Rows")
    fix_merge.to_excel(xlwr, "ConfltRowsEDMS_ID_in_EngID")
    fix_merge_2.to_excel(xlwr, "ConfltRowsEDMS_ID_notin_EngID")
    xlwr.save()

    epa_ltos_iah_fin.to_excel(path_out_iah)
    epa_ltos_dfw_fin.to_excel(path_out_dfw)
    conn.close()

    fac_ids = ["dfw", "iah"]
    path_fleetmixes = [path_out_dfw, path_out_iah]
    ops_grps = ["Commercial"]

    for path_fleetmix_clean, fac_id in zip(path_fleetmixes, fac_ids):
        if fac_id == "dfw":
            # Airport database with track information.
            arpt_db = f"{fac_id}_dummy"
        elif fac_id == "iah":
            arpt_db = "2020_IAH_Final"
        else:
            """"""
        # Get Annual Ops from the facility.
        annual_ops = epa_ltos_iah_dfw_fil_1_1.loc[
            lambda df: df.facility_id == fac_id
        ].annual_operations.values[0]

        # Get ['facility_id', 'closest_airframe_id_aedt', 'engine_id',
        #        'annual_operations', 'anp_airplane_id', 'anp_helicopter_id',
        #        'aircraft_id', 'fleetmix', 'ops_fleet']
        ops = clean_fleetmix(path_fleetmix_clean, fac_id=fac_id)
        from analysis.preprocess.vi_prepare_ops_for_asif import (
            get_flt_db_tabs,
            add_eng_arfm_equip_cols,
            add_apu_nms,
            add_profiles,
            split_ops_arrdep,
            get_arptdummy_trks_layout,
        )

        # Get profiles, APU, engine, airframe, and equipment data.
        flt_db_dict = get_flt_db_tabs()
        ops_airfm_eng = add_eng_arfm_equip_cols(
            ops_=ops, airfm=flt_db_dict["airfm"], eng=flt_db_dict["eng"]
        )
        ops_airfm_eng_apu = add_apu_nms(
            ops_airfm_eng_=ops_airfm_eng, apu_nm=flt_db_dict["apu_nm"]
        )
        ops_airfm_eng_apu_dict = add_profiles(
            ops_airfm_eng_apu_=ops_airfm_eng_apu,
            anp_arp_heli_prof=flt_db_dict["anp_arp_heli_prof"],
        )
        missing_deleted_rows = ops_airfm_eng_apu_dict["missing_rows"]
        with pd.option_context("display.max_rows", 20, "display.max_columns", 20):
            print(missing_deleted_rows)
        # input("Are we okay with the deleted rows?")
        # Split ops to arrival and departure.
        ltos_airfm_eng_apu_prf = split_ops_arrdep(
            ops_airfm_eng_apu_prf_=ops_airfm_eng_apu_dict["data"],
            annual_ops_=annual_ops,
        )
        # Get Track info
        trk_layout_dict = get_arptdummy_trks_layout(arptdummy_db=arpt_db)
        # Write ASIF file
        asif_input_fi = Path.home().joinpath(
            path_out, "{}_input_fi_epa.xlsx".format(fac_id)
        )
        xlwr = pd.ExcelWriter(asif_input_fi)
        trk_layout_dict["layout"].to_excel(xlwr, "layout")
        trk_layout_dict["track"].to_excel(xlwr, "track")
        ltos_airfm_eng_apu_prf.to_excel(xlwr, "ltos")
        xlwr.save()
