"""
Get unique airframe ids and engine ids for military, TASP, and OPUA airports.
Assign 1 to the operations.
"""
from datetime import datetime
import pandas as pd
import numpy as np
import pathlib
from pathlib import Path
from airportei.utilis import PATH_INTERIM, connect_to_sql_server, get_snake_case_dict
from analysis.vii_prepare_ops_for_asif import (
    get_flt_db_tabs,
    add_eng_arfm_equip_cols,
    add_apu_nms,
    add_profiles,
    split_ops_arrdep,
    get_arptdummy_trks_layout,
)


def clean_fleetmix_1(
    path_fltmix_: pathlib.WindowsPath,
    ass_fac_id,
    ops_grp="Military",
    facility_types=["AIRPORT"],
) -> pd.DataFrame:
    fltmix_fgrp = pd.read_excel(path_fltmix_, sheet_name=ops_grp)
    test = fltmix_fgrp.loc[lambda df: df.closest_airframe_id_aedt == 4959]
    fltmix_fgrp_fin = (
        fltmix_fgrp.loc[lambda df: df.facility_type.isin(facility_types)]
        .assign(facility_id=ass_fac_id)
        .groupby(["facility_id", "closest_airframe_id_aedt", "engine_id"])
        .agg(
            annual_operations=("annual_operations", "first"),
            anp_airplane_id=("anp_airplane_id", "first"),
            anp_helicopter_id=("anp_helicopter_id", "first"),
        )
        .reset_index()
        .assign(
            anp_airplane_id=lambda df: df.anp_airplane_id.fillna(df.anp_helicopter_id),
            ops_fleet=2,
            fleetmix=np.NaN,
        )
    )
    return fltmix_fgrp_fin


if __name__ == "__main__":
    ass_fac_ids = ["bif", "hpy", "t23"]
    ops_grps = ["Military", "Other_PU_Airports", "TASP"]
    facility_types_ls = [["AIRPORT"], ["AIRPORT", "SEAPLANE BASE"], ["AIRPORT"]]
    for ass_fac_id, ops_grp, facility_types in zip(
        ass_fac_ids, ops_grps, facility_types_ls
    ):
        path_fleetmix_clean = Path.home().joinpath(
            PATH_INTERIM, "fleetmix_axb_07_05_2021.xlsx"
        )
        ops = clean_fleetmix_1(
            path_fltmix_=path_fleetmix_clean,
            ops_grp=ops_grp,
            facility_types=facility_types,
            ass_fac_id=ass_fac_id,
        )
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
        ops_airfm_eng_apu_dict["data"]["ops_fleet_adj"] = 2
        annual_ops = ops_airfm_eng_apu_dict["data"]["ops_fleet_adj"].sum() / 2
        ltos_airfm_eng_apu_prf = split_ops_arrdep(
            ops_airfm_eng_apu_prf_=ops_airfm_eng_apu_dict["data"],
            annual_ops_=annual_ops,
        )

        arpt_db = f"{ass_fac_id}_dummy"
        trk_layout_dict = get_arptdummy_trks_layout(arptdummy_db=arpt_db)
        asif_input_fi = Path.home().joinpath(
            PATH_INTERIM, "asif_xmls", "{}_input_fi.xlsx".format(ass_fac_id)
        )
        xlwr = pd.ExcelWriter(asif_input_fi)
        trk_layout_dict["layout"].to_excel(xlwr, "layout")
        trk_layout_dict["track"].to_excel(xlwr, "track")
        ltos_airfm_eng_apu_prf.to_excel(xlwr, "ltos")
        xlwr.save()
