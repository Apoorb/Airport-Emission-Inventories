import pandas as pd
import numpy as np
from pathlib import Path
from airportei.utilis import PATH_PROCESSED, PATH_INTERIM, PATH_RAW, get_snake_case_dict

# FixME: Clean up the global variables---more to pytest fixtures.
path_ops2019_clean = Path.home().joinpath(
    PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx"
)
ops_2019 = pd.read_excel(path_ops2019_clean, index_col=0)
ops_2019_fil = ops_2019.loc[lambda df: (~df.facility_type.isin(["GLIDERPORT"]))]
# expected_fac_ids = ops_2019_fil.facility_id.unique()

path_out_emis_comm_rel = Path.home().joinpath(PATH_PROCESSED, "emis_comm_reliev.xlsx")
path_out_flt_comm_rel = Path.home().joinpath(PATH_PROCESSED, "fleet_comm_reliev.xlsx")
path_out_emis_non_comm_rel = Path.home().joinpath(
    PATH_PROCESSED, "emis_non_comm_reliev.xlsx"
)
path_out_flt_non_comm_rel = Path.home().joinpath(
    PATH_PROCESSED, "fleet_non_comm_reliev.xlsx"
)
path_tfmsc_fleetmix = Path.home().joinpath(PATH_RAW, "madhu_files", "FAA_2019TFMSC.csv")

emis_comm_rel = pd.read_excel(path_out_emis_comm_rel)
flt_comm_rel = pd.read_excel(path_out_flt_comm_rel)
emis_non_comm_rel = pd.read_excel(path_out_emis_non_comm_rel)
flt_non_comm_rel = pd.read_excel(path_out_flt_non_comm_rel)
flt_df = pd.concat([flt_comm_rel, flt_non_comm_rel])
input_fleetmix = pd.read_csv(path_tfmsc_fleetmix)
emis_df = pd.concat([emis_comm_rel, emis_non_comm_rel])


def test_all_fac_id_present(expected_fac_ids=2032):
    """
    There are 2,032 operational airport facilities in Texas. 5010 data shows
    2,037 airports. We have not considered the 5 GLIDERPORT in this
    inventory. Check with Madhu about the justification.
    """
    comm_rel_fac = emis_comm_rel.facility_id.unique()
    non_comm_rel_fac = emis_non_comm_rel.facility_id.unique()
    fac_output = set(comm_rel_fac).union(non_comm_rel_fac)
    assert fac_output == set(expected_fac_ids)


def test_all_fleetmix_in_flt_eq_1():
    """
    The sum of fleetmix should add-up to 1.
    """
    assert np.allclose(flt_df.groupby(["facility_id"]).fleetmix.sum().values, 1)


def test_sum_ltos_time_2_eq_ops():
    """
    LTOs = Number of operations / 2. Check if the sum of ltos * 2 by facility
    is equal to the input operations.
    """
    flt_df_ltos = flt_df.groupby("facility_id").ltos.sum() * 2
    input_ops = ops_2019_fil[["facility_id", "annual_operations"]].set_index(
        "facility_id"
    )
    test_df = pd.merge(flt_df_ltos, input_ops, left_index=True, right_index=True)
    assert np.allclose(test_df.annual_operations, test_df.ltos)


def test_emis_fleet_eq_flt_fleet():
    """
    Test that emission data fleetmix, ltos, and operations are equal to the
    fleetmix
    data.
    """
    emis_df["tfmsc_aircraft_id"] = emis_df["tfmsc_aircraft_id"].fillna(-99)
    emis_df_fleetmix = (
        emis_df.loc[lambda df: ~df.fleetmix.isna()]
        .groupby(["facility_id", "airframe_id", "engine_id", "tfmsc_aircraft_id"])
        .agg(fleetmix=("fleetmix", "mean"), ops=("ops", "mean"), ltos=("ltos", "mean"))
        .reset_index()
    )
    flt_df["tfmsc_aircraft_id"] = flt_df["tfmsc_aircraft_id"].fillna(-99)

    test_df = emis_df_fleetmix.merge(
        flt_df,
        on=["facility_id", "airframe_id", "engine_id", "tfmsc_aircraft_id"],
        suffixes=["_emis", "_flt"],
        how="outer",
    )

    t1 = test_df[~np.isclose(test_df["ops_emis"], test_df["ops_flt"])]
    assert (np.allclose(test_df["fleetmix_emis"], test_df["fleetmix_flt"])) & (
        np.allclose(test_df["ops_emis"], test_df["ops_flt"])
        & (np.allclose(test_df["ltos_emis"], test_df["ltos_flt"]))
    )


def test_emis_unique_cats_eq_4():
    """
    Test that there are only 4 unique categories
    """
    emis_df["tfmsc_aircraft_id"] = emis_df["tfmsc_aircraft_id"].fillna(-99)
    emis_df_fil = emis_df.loc[lambda df: ~df.Mode.isin(["GSE LTO", "APU"])]
    mask = (
        emis_df_fil.groupby(
            ["facility_id", "airframe_id", "engine_id", "tfmsc_aircraft_id"]
        ).facility_id.transform("count")
        != 4
    )
    assert all(
        emis_df_fil.groupby(
            ["facility_id", "airframe_id", "engine_id", "tfmsc_aircraft_id"]
        ).facility_id.count()
        == 4
    )


def test_fleetmix_approx_eq_tfmsc():
    """
    Test that the fleetmix is approximately equal to tfmsc data for the
    airports in TFMSC.
    """
    final_aircraft_ids = flt_df.tfmsc_aircraft_id.unique()
    input_fleetmix_1 = (
        input_fleetmix.rename(columns=get_snake_case_dict(input_fleetmix))
        .rename(
            columns={"location_id": "facility_id", "aircraft_id": "aircraft_id_temp"}
        )
        .groupby(["facility_id", "aircraft_id_temp"])
        .agg(aircraft_ops=("total_ops", "sum"))
        .reset_index()
        .assign(
            facility_id=lambda df: df.facility_id.str.lower().str.strip(),
            aircraft_id=lambda df: df.aircraft_id_temp.str.lower().str.strip(),
        )
        .loc[lambda df: df.aircraft_id.isin(final_aircraft_ids)]
        .assign(
            annual_ops=lambda df: df.groupby("facility_id").aircraft_ops.transform(sum),
            fleetmix=lambda df: df.aircraft_ops / df.annual_ops,
        )
    )
    flt_df_1 = flt_df.rename(columns={"tfmsc_aircraft_id": "aircraft_id"}).loc[
        :, ["facility_id", "aircraft_id", "fleetmix"]
    ]
    test_df = flt_df_1.merge(
        input_fleetmix_1,
        on=["facility_id", "aircraft_id"],
        suffixes=["_postp", "_input"],
    )
    test_df["abs_diff_flt_mx"] = np.abs(test_df.fleetmix_input - test_df.fleetmix_postp)
    test_df["per_diff_flt_mx"] = test_df.abs_diff_flt_mx * 100 / test_df.fleetmix_input
    test_df_fil = test_df.loc[test_df.per_diff_flt_mx > 5]
    np.quantile(test_df_fil.fleetmix_input * 100, 0.95)


def test_farm_and_ranch_fleetmix_eq_0_2():
    ...


def test_heliport_fleetmix_eq_0_3333():
    ...
