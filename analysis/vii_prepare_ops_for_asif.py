"""
Get operations by airframe and engine code for the analysis airport along
with auxiliary inputs like the APU name, profile, layout, and track.
"""
from datetime import datetime

import pandas as pd
import numpy as np
import pathlib
from pathlib import Path
from airportei.utilis import PATH_INTERIM, connect_to_sql_server, get_snake_case_dict

TESTING = False


def clean_profiles(
    anp_arp_prof_: pd.DataFrame, anp_heli_prof_: pd.DataFrame
) -> pd.DataFrame:
    # Get anp profiles.
    # Get anp profile for airplanes.
    anp_arp_prof_1 = anp_arp_prof_.rename(
        columns=get_snake_case_dict(anp_arp_prof_)
    ).loc[lambda df: df.op_type.isin(["A", "D"])]
    anp_arp_prof_stnd = anp_arp_prof_1.loc[lambda df: df.prof_id1 == "STANDARD"]

    acft_id_stnd = anp_arp_prof_stnd.acft_id.unique()
    acft_id_all = anp_arp_prof_1.acft_id.unique()
    missing_ids = set(acft_id_all) - set(acft_id_stnd)
    anp_arp_prof_non_stnd = anp_arp_prof_1.loc[lambda df: df.acft_id.isin(missing_ids)]
    assert anp_arp_prof_non_stnd.prof_id1.unique() == [
        "NOISEMAP"
    ], "All non-standard profiles are NOISEMAP profiles."

    anp_arp_prof_fin = (
        pd.concat([anp_arp_prof_stnd, anp_arp_prof_non_stnd])
        .assign(
            stage_len_cats=lambda df: pd.Categorical(
                df.prof_id2, [1, 2, 3, 4, 5, 6, 7, 8, "M"], ordered=True
            )
        )
        .sort_values(
            ["acft_id", "op_type", "stage_len_cats"], ascending=[True, True, False]
        )
        .reset_index(drop=True)
    )

    anp_arp_prof_fin = (
        pd.concat([anp_arp_prof_stnd, anp_arp_prof_non_stnd])
        .assign(
            stage_len_cats=lambda df: pd.Categorical(
                df.prof_id2, [1, 2, 3, 4, 5, 6, 7, 8, "M"], ordered=True
            )
        )
        .sort_values(
            ["acft_id", "op_type", "stage_len_cats"], ascending=[True, True, False]
        )
        .reset_index(drop=True)
        .groupby(["acft_id", "op_type"])
        .apply(lambda df: df[df.prof_id2 == df.prof_id2.max()])
        .rename(columns={"prof_id1": "profile", "prof_id2": "stage_len"})
        .filter(items=["acft_id", "op_type", "profile", "stage_len", "weight"])
        .reset_index(drop=True)
    )

    # Get anp profile for helicopters.
    anp_heli_prof_1 = anp_heli_prof_.rename(
        columns=get_snake_case_dict(anp_heli_prof_)
    ).loc[lambda df: df.op_type.isin(["A", "D"])]
    anp_heli_prof_stnd = anp_heli_prof_1.loc[lambda df: df.prof_id1 == "STANDARD"]
    assert len(anp_heli_prof_stnd) == len(
        anp_heli_prof_1
    ), "Some helicopters have non standard profile."
    anp_heli_prof_fin = (
        anp_heli_prof_stnd.rename(columns={"helo_id": "acft_id"})
        .assign(
            stage_len_cats=lambda df: pd.Categorical(
                df.prof_id2, [1, 2, 3, 4, 5, 6, 7, 8, "M"], ordered=True
            )
        )
        .sort_values(
            ["acft_id", "op_type", "stage_len_cats"], ascending=[True, True, False]
        )
        .reset_index(drop=True)
        .groupby(["acft_id", "op_type"])
        .apply(lambda df: df[df.prof_id2 == df.prof_id2.max()])
        .rename(columns={"prof_id1": "profile", "prof_id2": "stage_len"})
        .filter(items=["acft_id", "op_type", "profile", "stage_len", "weight"])
        .reset_index(drop=True)
    )
    anp_arp_heli_prof_fin = pd.concat([anp_arp_prof_fin, anp_heli_prof_fin])
    return anp_arp_heli_prof_fin


def get_flt_db_tabs(flt_db="FLEET") -> dict[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    conn = connect_to_sql_server(database_nm=flt_db)
    eng_df = pd.read_sql("SELECT * FROM [dbo].[FLT_ENGINES]", conn)
    airfm_df = pd.read_sql("SELECT * FROM [dbo].[FLT_AIRFRAMES]", conn)
    anp_arp_prof = pd.read_sql("SELECT * FROM [dbo].[FLT_ANP_AIRPLANE_PROFILES]", conn)
    anp_heli_prof = pd.read_sql(
        "SELECT * FROM [dbo].[FLT_ANP_HELICOPTER_PROFILES]", conn
    )
    def_apu = pd.read_sql("SELECT * FROM [FLEET].[dbo].[FLT_APU]", conn)
    apu_name = pd.read_sql("SELECT * FROM [dbo].[FLT_APU_DEFAULTS]", conn)
    conn.close()
    eng_df_1 = eng_df.rename(columns=get_snake_case_dict(eng_df)).filter(
        items=["engine_id", "engine_code"]
    )
    assert eng_df_1.duplicated("engine_id").sum() == 0, "Duplicated engine_id!"
    airfm_df_1 = (
        airfm_df.rename(columns=get_snake_case_dict(airfm_df))
        .filter(items=["airframe_id", "model"])
        .rename(columns={"model": "arfm_mod"})
    )
    assert airfm_df_1.duplicated("airframe_id").sum() == 0, "Duplicated airframes!"
    anp_arp_heli_prof = clean_profiles(
        anp_arp_prof_=anp_arp_prof, anp_heli_prof_=anp_heli_prof
    )

    def_apu_1 = def_apu.rename(columns=get_snake_case_dict(def_apu))
    apu_name_1 = apu_name.rename(columns=get_snake_case_dict(apu_name))
    def_apu_2 = def_apu_1.merge(apu_name_1, on="apu_id").filter(
        items=["apu_id", "apu_name", "airframe_id", "user_defined"]
    )
    assert def_apu_2.duplicated("airframe_id").sum() == 0, "Duplicated airframes!"
    return dict(
        airfm=airfm_df_1,
        eng=eng_df_1,
        anp_arp_heli_prof=anp_arp_heli_prof,
        apu_nm=def_apu_2,
    )


def get_arptdummy_trks_layout(
    arptdummy_db="get_iah_tracks"
) -> dict[pd.DataFrame, pd.DataFrame]:
    conn = connect_to_sql_server(database_nm=arptdummy_db)
    tracks = pd.read_sql("SELECT * FROM [dbo].[APT_TRACK]", conn)
    subtracks = pd.read_sql("SELECT * FROM [dbo].[APT_SUBTRACK]", conn)
    segments = pd.read_sql("SELECT * FROM [dbo].[APT_SEGMENT]", conn)
    runway = pd.read_sql("SELECT * FROM [dbo].[APT_RWY_END]", conn)
    arpt_layout = pd.read_sql("SELECT * FROM [dbo].[AIRPORT_LAYOUT]", conn)
    arpt_code = pd.read_sql("SELECT * FROM [dbo].[APT_CODE]", conn)
    arpt_main = pd.read_sql("SELECT * FROM [dbo].[APT_MAIN]", conn)

    lam_date = lambda df: (
        (df.eff_date < datetime.strptime("1-1-2019", "%m-%d-%Y"))
        & (df.exp_date > datetime.strptime("1-1-2019", "%m-%d-%Y"))
    )
    tracks_1 = (
        tracks.rename(columns=get_snake_case_dict(tracks))
        .loc[lam_date]
        .filter(
            items=["aircraft_type", "track_id", "rwy_end_id", "track_name", "op_type"]
        )
        .groupby(["op_type", "aircraft_type"])
        .apply(lambda df: df[df.track_id == df.track_id.min()])
        .reset_index(drop=True)
    )
    subtracks_1 = subtracks.rename(columns=get_snake_case_dict(subtracks)).filter(
        items=["subtrack_id", "track_id", "subtrack_num"]
    )
    segments_1 = (
        segments.rename(columns=get_snake_case_dict(segments))
        .filter(
            items=[
                "segment_id",
                "subtrack_id",
                "segment_num",
                "segment_type",
                "param_1",
                "param_2",
            ]
        )
        .rename(columns={"param_1": "dist_or_rad", "param_2": "turn_angle"})
    )
    runway_1 = runway.rename(columns=get_snake_case_dict(runway)).filter(
        items=["rwy_end_id", "rwy_end_name"]
    )

    tracks_fin = (
        tracks_1.merge(runway_1, on="rwy_end_id", how="left")
        .merge(subtracks_1, on="track_id", how="left")
        .merge(segments_1, on="subtrack_id", how="left")
    )
    arpt_code_1 = (
        arpt_code.rename(columns=get_snake_case_dict(arpt_code))
        .loc[lambda df: df.preferred.str.lower() == "y"]
        .drop(columns=["rec_id", "eff_date", "exp_date"])
    )
    arpt_main_1 = arpt_main.rename(columns=get_snake_case_dict(arpt_main)).drop(
        columns=["rec_id", "eff_date", "exp_date"]
    )

    arpt_layout_1 = arpt_layout.rename(columns=get_snake_case_dict(arpt_layout))
    arpt_layout_fin = (
        arpt_layout_1.loc[lam_date]
        .merge(arpt_code_1, left_on="airport_id", right_on="apt_id")
        .merge(arpt_main_1, on="apt_id")
    )
    assert len(arpt_layout_fin) == 1, "Handle multiple layouts."

    return dict(track=tracks_fin, layout=arpt_layout_fin)


def ops_prep(
    path_imputed_ops_: pathlib.WindowsPath,
    path_fltmix_: pathlib.WindowsPath,
    ops_grp="Commercial",
    fac_id="iah",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    ops2019 = pd.read_excel(path_imputed_ops)
    ops2019_fgrp = ops2019.loc[lambda df: df.facility_group == ops_grp]
    ops2019_fgrp_fid = ops2019_fgrp.loc[lambda df: df.facility_id == fac_id]
    annual_ops_2019 = ops2019_fgrp_fid.annual_operations.values[0]
    ops_ = clean_fleetmix(path_fltmix_=path_fltmix_, ops_grp=ops_grp, fac_id=fac_id)
    if not TESTING:
        assert np.isclose(
            annual_ops_2019, ops_.ops_fleet.sum()
        ), "Total ops do not match between fleetmix and operations data."
    return ops_, annual_ops_2019


def clean_fleetmix(
    path_fltmix_: pathlib.WindowsPath, ops_grp="Commercial", fac_id="iah"
) -> pd.DataFrame:
    fltmix_fgrp = pd.read_excel(path_fltmix_, sheet_name=ops_grp)
    fltmix_fgrp_fid = fltmix_fgrp.loc[lambda df: df.facility_id == fac_id]
    fltmix_fgrp_fid_fin = (
        fltmix_fgrp_fid.groupby(
            ["facility_id", "closest_airframe_id_aedt", "engine_id"]
        )
        .agg(
            annual_operations=("annual_operations", "first"),
            anp_airplane_id=("anp_airplane_id", "first"),
            anp_helicopter_id=("anp_helicopter_id", "first"),
            fleetmix=("fleetmix", "sum"),
        )
        .reset_index()
        .assign(
            ops_fleet=lambda df: df.annual_operations * df.fleetmix,
            anp_airplane_id=lambda df: df.anp_airplane_id.fillna(df.anp_helicopter_id),
        )
    )
    assert np.isclose(
        fltmix_fgrp_fid_fin.fleetmix.sum(), 1
    ), "Fleetmix doesn't add up to 1."
    return fltmix_fgrp_fid_fin


def add_eng_arfm_equip_cols(
    ops_: pd.DataFrame, airfm: pd.DataFrame, eng: pd.DataFrame
) -> pd.DataFrame:
    ops_airfm_eng_ = ops_.merge(eng, on="engine_id", how="left").merge(
        airfm, left_on="closest_airframe_id_aedt", right_on="airframe_id", how="left"
    )
    return ops_airfm_eng_


def add_apu_nms(ops_airfm_eng_: pd.DataFrame, apu_nm: pd.DataFrame) -> pd.DataFrame:
    ops_airfm_eng_apu_ = ops_airfm_eng_.merge(apu_nm, on=["airframe_id"], how="left")
    return ops_airfm_eng_apu_


def add_profiles(
    ops_airfm_eng_apu_: pd.DataFrame, anp_arp_heli_prof: pd.DataFrame
) -> dict[pd.DataFrame, pd.DataFrame]:
    ops_airfm_eng_apu_["anp_airplane_id"] = ops_airfm_eng_apu_[
        "anp_airplane_id"
    ].astype(str)
    anp_arp_heli_prof["acft_id"] = anp_arp_heli_prof["acft_id"].astype(str)
    acft_id_anp = anp_arp_heli_prof.acft_id.unique()
    acft_id_flt = ops_airfm_eng_apu_.anp_airplane_id.unique()
    missing_profiles = set(acft_id_flt) - set(acft_id_anp)
    reassign_ops = ops_airfm_eng_apu_.loc[
        lambda df: df.anp_airplane_id.isin(missing_profiles), "ops_fleet"
    ].sum()
    if reassign_ops > 1000:
        raise ValueError("Check the issue with missing profiles.")
    missing_profiles_df = ops_airfm_eng_apu_.loc[
        lambda df: df.anp_airplane_id.isin(missing_profiles)
    ]
    ops_airfm_eng_apu_fil = ops_airfm_eng_apu_.loc[
        lambda df: ~df.anp_airplane_id.isin(missing_profiles)
    ].assign(
        ops_fleet_adj=lambda df: df.fleetmix * reassign_ops + df.ops_fleet,
        fleetmix_adj=lambda df: df.ops_fleet_adj / df.ops_fleet_adj.sum(),
    )
    ops_airfm_eng_apu_prf_ = (
        ops_airfm_eng_apu_fil.merge(
            anp_arp_heli_prof, left_on="anp_airplane_id", right_on="acft_id", how="left"
        )
        .sort_values(["acft_id", "op_type"])
        .reset_index(drop=True)
    )
    assert (
        ops_airfm_eng_apu_prf_.stage_len.isna().sum() == 0
    ), "All profiles are present"
    return dict(data=ops_airfm_eng_apu_prf_, missing_rows=missing_profiles_df)


def split_ops_arrdep(
    ops_airfm_eng_apu_prf_: pd.DataFrame, annual_ops_: float
) -> pd.DataFrame:
    ops_airfm_eng_apu_prf_["op_counts"] = ops_airfm_eng_apu_prf_.groupby(
        ["airframe_id", "engine_id"]
    ).op_type.transform("count")
    assert all(
        (
            ops_airfm_eng_apu_prf_.groupby(["airframe_id", "engine_id"]).op_type.count()
            == 2
        ).values
    ), "Some airframes + engines have an arrival or departure missing"

    ops_airfm_eng_apu_prf_["ltos"] = (
        ops_airfm_eng_apu_prf_.ops_fleet_adj / ops_airfm_eng_apu_prf_.op_counts
    ).round(0)
    ops_airfm_eng_apu_prf_ = (
        ops_airfm_eng_apu_prf_.sort_values(
            ["ltos", "arfm_mod", "engine_code", "op_type"],
            ascending=[False, True, True, True],
        )
        .reset_index(drop=True)
        .assign(ids=lambda df: (np.floor(df.index / 2) + 1).astype(int))
    )

    assert np.isclose(
        ops_airfm_eng_apu_prf_.ltos.sum(), annual_ops_, rtol=0.1
    ), "All operations are not correctly reassigned."
    return ops_airfm_eng_apu_prf_


def get_asif_input_tab():
    ...


if __name__ == "__main__":
    path_imputed_ops = Path.home().joinpath(
        PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx"
    )
    path_dfw_fleetmix_cln = Path.home().joinpath(
        PATH_INTERIM, "dfw_reported_data", "dfw_aedt_study_ops_cln.xlsx"
    )

    path_fleetmix_clean = Path.home().joinpath(
        PATH_INTERIM, "fleetmix_axb_07_05_2021.xlsx"
    )
    if TESTING:
        path_iah_fleetmix_cln = Path.home().joinpath(
            PATH_INTERIM, "harris_airport_data", "iah_aedt_study_ops_cln.xlsx"
        )

    fac_ids = ["aus", "abi", "act", "ama", "elp", "dfw", "bpt"]
    path_fleetmixes = [path_fleetmix_clean] * 5 + [path_dfw_fleetmix_cln]

    path_fleetmixes = [path_fleetmix_clean]
    fac_ids = ["bro"]

    for path_fleetmix_clean, fac_id in zip(path_fleetmixes, fac_ids):
        arpt_db = f"{fac_id}_dummy"
        ops, annual_ops = ops_prep(
            path_imputed_ops_=path_imputed_ops,
            path_fltmix_=path_fleetmix_clean,
            ops_grp="Commercial",
            fac_id=fac_id,
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
        # input("Are we okay with the deleted rows?")
        ltos_airfm_eng_apu_prf = split_ops_arrdep(
            ops_airfm_eng_apu_prf_=ops_airfm_eng_apu_dict["data"],
            annual_ops_=annual_ops,
        )
        trk_layout_dict = get_arptdummy_trks_layout(arptdummy_db=arpt_db)

        asif_input_fi = Path.home().joinpath(
            PATH_INTERIM, "asif_xmls", "{}_input_fi.xlsx".format(fac_id)
        )
        xlwr = pd.ExcelWriter(asif_input_fi)
        trk_layout_dict["layout"].to_excel(xlwr, "layout")
        trk_layout_dict["track"].to_excel(xlwr, "track")
        ltos_airfm_eng_apu_prf.to_excel(xlwr, "ltos")
        xlwr.save()
