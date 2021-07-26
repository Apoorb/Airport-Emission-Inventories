import pandas as pd
import numpy as np
from pathlib import Path
from airportei.utilis import PATH_INTERIM, connect_to_sql_server, get_snake_case_dict
TESTING = True
if __name__ == "__main__":
    analysis_arpt = "KIAH"
    # 1. Get operations and Fleetmix for IAH
    path_imputed_ops = Path.home().joinpath(
        PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx"
    )
    ops2019 = pd.read_excel(path_imputed_ops)
    ops2019_com = ops2019.loc[lambda df: df.facility_group == "Commercial"]
    ops2019_com_iah = ops2019_com.loc[lambda df: df.facility_id == "iah"]
    iah_ops_2019 = ops2019_com_iah.annual_operations.values[0]

    path_fleetmix_clean = Path.home().joinpath(
        PATH_INTERIM, "fleetmix_axb_07_05_2021.xlsx"
    )
    path_iah_fleetmix_cln = Path.home().joinpath(
        PATH_INTERIM, "iah_airport_data", "iah_aedt_study_ops_cln.csv"
    )

    fleetmix_iah = pd.read_csv(path_iah_fleetmix_cln)

    fleetmix_iah_1 = (
        fleetmix_iah.groupby(["facility_id", "closest_airframe_id_aedt", "engine_id"])
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
    assert np.isclose(fleetmix_iah_1.fleetmix.sum(), 1), "Fleetmix doesn't add up to 1."

    # 2. Add Engine and airframe to the fleetmix.
    conn = connect_to_sql_server(database_nm="FLEET")
    eng_df = pd.read_sql("SELECT * FROM [dbo].[FLT_ENGINES]", conn)
    airfm_df = pd.read_sql("SELECT * FROM [dbo].[FLT_AIRFRAMES]", conn)

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
    fleetmix_iah_eng_arfm = fleetmix_iah_1.merge(
        eng_df_1, on="engine_id", how="left"
    ).merge(
        airfm_df_1,
        left_on="closest_airframe_id_aedt",
        right_on="airframe_id",
        how="left",
    )

    # 3. Get anp profiles.
    # 3.1 Get anp profile for airplanes.
    anp_arp_prof = pd.read_sql("SELECT * FROM [dbo].[FLT_ANP_AIRPLANE_PROFILES]", conn)
    anp_arp_prof_1 = anp_arp_prof.rename(columns=get_snake_case_dict(anp_arp_prof)).loc[
        lambda df: df.op_type.isin(["A", "D"])
    ]
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
        .sort_values(["acft_id", "op_type", "profile_id"])
        .drop_duplicates(["acft_id", "op_type"])
        .rename(columns={"prof_id1": "profile", "prof_id2": "stage_len"})
        .filter(items=["acft_id", "op_type", "profile", "stage_len", "weight"])
        .reset_index(drop=True)
    )

    # 3.2 Get anp profile for helicopters.
    anp_heli_prof = pd.read_sql(
        "SELECT * FROM [dbo].[FLT_ANP_HELICOPTER_PROFILES]", conn
    )
    anp_heli_prof_1 = anp_heli_prof.rename(
        columns=get_snake_case_dict(anp_heli_prof)
    ).loc[lambda df: df.op_type.isin(["A", "D"])]
    anp_heli_prof_stnd = anp_heli_prof_1.loc[lambda df: df.prof_id1 == "STANDARD"]
    assert len(anp_heli_prof_stnd) == len(
        anp_heli_prof_1
    ), "Some helicopters have non standard profile."
    anp_heli_prof_fin = (
        anp_heli_prof_stnd.rename(columns={"helo_id": "acft_id"})
        .sort_values(["acft_id", "op_type", "profile_id"])
        .drop_duplicates(["acft_id", "op_type"])
        .rename(columns={"prof_id1": "profile", "prof_id2": "stage_len"})
        .filter(items=["acft_id", "op_type", "profile", "stage_len", "weight"])
        .reset_index(drop=True)
    )

    anp_arp_heli_prof_fin = pd.concat([anp_arp_prof_fin, anp_heli_prof_fin])

    # 4.1 Get APU data.
    def_apu = pd.read_sql("SELECT * FROM [FLEET].[dbo].[FLT_APU]", conn)
    apu_name = pd.read_sql("SELECT * FROM [dbo].[FLT_APU_DEFAULTS]", conn)
    def_apu_1 = def_apu.rename(columns=get_snake_case_dict(def_apu))
    apu_name_1 = apu_name.rename(columns=get_snake_case_dict(apu_name))
    def_apu_2 = def_apu_1.merge(apu_name_1, on="apu_id").filter(
        items=["apu_id", "apu_name", "airframe_id", "user_defined"]
    )
    assert def_apu_2.duplicated("airframe_id").sum() == 0, "Duplicated airframes!"
    conn.close()

    # 5 Add APU name to fleetmix.
    fleetmix_iah_eng_arfm_apu = fleetmix_iah_eng_arfm.merge(
        def_apu_2, on=["airframe_id"], how="left"
    )

    # 6. Handle missing profiles by removing the airplanes
    # without profiles.
    acft_id_anp = anp_arp_heli_prof_fin.acft_id.unique()
    acft_id_flt = fleetmix_iah_eng_arfm_apu.anp_airplane_id.unique()
    missing_profiles = set(acft_id_flt) - set(acft_id_anp)
    reassign_ops = fleetmix_iah_eng_arfm_apu.loc[
        lambda df: df.anp_airplane_id.isin(missing_profiles), "ops_fleet"
    ].sum()

    fleetmix_iah_eng_arfm_apu_fil = fleetmix_iah_eng_arfm_apu.loc[
        lambda df: ~df.anp_airplane_id.isin(missing_profiles)
    ].assign(
        ops_fleet_adj=lambda df: df.fleetmix * reassign_ops + df.ops_fleet,
        fleetmix_adj=lambda df: df.ops_fleet_adj / df.ops_fleet_adj.sum(),
    )

    if not TESTING:
        assert np.isclose(
            fleetmix_iah_eng_arfm_apu_fil.ops_fleet_adj.sum(), iah_ops_2019
        ), "All operations are not correctly reassigned."

    # 7. Assign profiles.
    fleetmix_iah_eng_arfm_apu_prf = (
        fleetmix_iah_eng_arfm_apu_fil.merge(
            anp_arp_heli_prof_fin,
            left_on="anp_airplane_id",
            right_on="acft_id",
            how="left",
        )
        .sort_values(["acft_id", "op_type"])
        .reset_index(drop=True)
    )

    assert (
        fleetmix_iah_eng_arfm_apu_prf.stage_len.isna().sum() == 0
    ), "All profiles are present"

    # 8. Divide operations into arrival and departure.
    fleetmix_iah_eng_arfm_apu_prf["op_counts"] = fleetmix_iah_eng_arfm_apu_prf.groupby(
        ["airframe_id", "engine_id"]
    ).op_type.transform("count")

    assert all(
        (
            fleetmix_iah_eng_arfm_apu_prf.groupby(
                ["airframe_id", "engine_id"]
            ).op_type.count()
            == 2
        ).values
    ), "Some airframes + engines have an arrival or departure missing"

    fleetmix_iah_eng_arfm_apu_prf["ltos"] = (
        fleetmix_iah_eng_arfm_apu_prf.ops_fleet_adj
        / fleetmix_iah_eng_arfm_apu_prf.op_counts
    ).round(0)

    if not TESTING:
        assert np.isclose(
            fleetmix_iah_eng_arfm_apu_prf.ltos.sum(), iah_ops_2019
        ), "All operations are not correctly reassigned."

    # 9. Get track info.
    conn_iah = connect_to_sql_server(database_nm="get_iah_tracks")
    tracks_iah = pd.read_sql("SELECT * FROM [dbo].[APT_TRACK]", conn_iah)
    subtracks_iah = pd.read_sql("SELECT * FROM [dbo].[APT_SUBTRACK]", conn_iah)
    segments_iah = pd.read_sql("SELECT * FROM [dbo].[APT_SEGMENT]", conn_iah)
    runway_iah = pd.read_sql("SELECT * FROM [dbo].[APT_RWY_END]", conn_iah)
    tracks_iah_1 = (
        tracks_iah.rename(columns=get_snake_case_dict(tracks_iah))
        .filter(
            items=["aircraft_type", "track_id", "rwy_end_id", "track_name", "op_type"]
        )
        .groupby(["op_type", "aircraft_type"])
        .apply(lambda df: df[df.track_id == df.track_id.min()])
        .reset_index(drop=True)
    )
    subtracks_iah_1 = subtracks_iah.rename(
        columns=get_snake_case_dict(subtracks_iah)
    ).filter(items=["subtrack_id", "track_id", "subtrack_num"])
    segments_iah_1 = (
        segments_iah.rename(columns=get_snake_case_dict(segments_iah))
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
    runway_iah_1 = runway_iah.rename(columns=get_snake_case_dict(runway_iah)).filter(
        items=["rwy_end_id", "rwy_end_name"]
    )

    tracks_iah_2 = (
        tracks_iah_1.merge(runway_iah_1, on="rwy_end_id", how="left")
        .merge(subtracks_iah_1, on="track_id", how="left")
        .merge(segments_iah_1, on="subtrack_id", how="left")
    )

    arpt_layout = pd.read_sql("SELECT * FROM [dbo].[AIRPORT_LAYOUT]", conn_iah)
    arpt_layout_1 = arpt_layout.rename(columns=get_snake_case_dict(arpt_layout))

    assert len(arpt_layout_1) == 1, "Handle multiple layouts."

    iah_asif_input_fi = Path.home().joinpath(PATH_INTERIM, "iah_asif_input.xlsx")
    xlwr = pd.ExcelWriter(iah_asif_input_fi)
    arpt_layout_1.to_excel(xlwr, "layout")
    tracks_iah_2.to_excel(xlwr, "track")
    fleetmix_iah_eng_arfm_apu_prf.to_excel(xlwr, "fleet")
    xlwr.save()
