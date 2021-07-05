"""
Develop fleet mix from TFMSC data.
Created on: 7/20/2021
Created by: Apoorba Bibeka
"""
from pathlib import Path
import pandas as pd
from airportei.utilis import (PATH_RAW, PATH_INTERIM,
                              get_snake_case_dict)


def clean_tfmsc(tfmsc_):
    """
    Remove special characters from the TFMSC data.
    Parameters
    ----------
    tfmsc_

    Returns
    -------

    """
    tfmsc_1_ = (
        tfmsc_
        .rename(columns=get_snake_case_dict(tfmsc_df))
        .rename(columns={"location_id": "facility_id"})
        .loc[lambda df: ~ (
            df.aircraft_type_id.isna() | df.aircraft_type.str.contains("unknown")
            | df.aircraft_type.str.contains("#VALUE!")
        )]
        .groupby([
        "year_id", "facility_id", "airport", 'aircraft_id',
        'aircraft_type_id', 'aircraft_type'])
        .agg(
            total_ops_by_craft=("total_ops", "sum"))
        .reset_index()
        .assign(
            facility_id=lambda df: df.facility_id.str.strip().str.lower(),
            total_ops_by_arpt=lambda df: df.groupby([
                "year_id", "facility_id",
                "airport"]).total_ops_by_craft.transform("sum"),
            fleetmix=lambda df: df.total_ops_by_craft / df.total_ops_by_arpt
        )
    )

    tfmsc_2_ = (
        tfmsc_1_.assign(
            aircraft_type_id=lambda df: (
                df.aircraft_type_id.replace(
                    ["Myst�re", "A�rospatiale", "Br�guet", "#NAME?"],
                    ["Mystere", "Aerospatiale", "Breguet", -999],
                    regex=True,
                )
            ),
            aircraft_type=lambda df: (
                df.aircraft_type.replace(
                    ["Myst�re", "A�rospatiale", "Br�guet", "- unknown"],
                    ["Mystere", "Aerospatiale", "Breguet", -999],
                    regex=True,
                )
            ),
        )
    )
    return tfmsc_2_


def fill_tasp_mil_arpts(tfmsc_df_ops2019_tasp_):
    """
    Use TASP and Military airports in the same district to fill the TASP and
    Military airport fleet mix data.
    Parameters
    ----------
    tfmsc_df_ops2019_tasp_

    Returns
    -------

    """
    tfmsc_df_ops2019_cat_na = (
        tfmsc_df_ops2019_tasp_.loc[lambda df: df.fleetmix.isna()]
        .sort_values(["district_tx_boundar", "facility_id", "aircraft_id"])
    )

    tfmsc_df_ops2019_cat_non_na = (
        tfmsc_df_ops2019_tasp_.loc[lambda df: ~ df.fleetmix.isna()]
        .sort_values(["district_tx_boundar", "facility_id", "aircraft_id"])
    )

    if all(tfmsc_df_ops2019_cat_non_na.facility_group == "Military"):
        bexar_ops = (
            tfmsc_df_ops2019_cat_non_na
                .loc[lambda df: df.county_arpt == "bexar"]
                .assign(
                county_arpt="travis",
                district_tx_boundar="Austin"
            )
        )
        travis_ops = bexar_ops
        tfmsc_df_ops2019_cat_non_na = pd.concat(
            [tfmsc_df_ops2019_cat_non_na, travis_ops]
        )
    # Check if we can use the airports from the same district to fill the
    # TASP data.
    assert (set(tfmsc_df_ops2019_cat_na.district_tx_boundar.unique()) - set(
        tfmsc_df_ops2019_cat_non_na.district_tx_boundar.unique())) == set(), (
        "We cannot use districts to fill fleetmix."
    )
    list_fill_df = []
    for indx, row in tfmsc_df_ops2019_cat_na.iterrows():
        df_fil_district = tfmsc_df_ops2019_cat_non_na.loc[
            lambda df: df.district_tx_boundar == row.district_tx_boundar]

        df_fil_district_fil_ops = (
            df_fil_district
            .assign(
                ops_diff=lambda df: abs(df.annual_operations -
                                    row.annual_operations),
                ops_per_diff=lambda df: df.ops_diff * 100/ row.annual_operations
            )
            .loc[lambda df: df.ops_diff == min(df.ops_diff)]
            .loc[lambda df: df.facility_id == df.facility_id.iloc[0]]
            .rename(columns={"facility_id":"filled_from_facility_id"})
            .filter(items=["district_tx_boundar", 'aircraft_id',
                           'aircraft_type_id', "aircraft_type", "fleetmix",
                           "ops_diff", "ops_per_diff", "filled_from_facility_id"])
        )
        fill_row = row.to_frame().T

        fill_row.drop(columns=['aircraft_id', 'aircraft_type_id',
                               "aircraft_type", "fleetmix"], inplace=True)

        fill_df = fill_row.merge(df_fil_district_fil_ops,
                               on="district_tx_boundar")
        list_fill_df.append(fill_df)

    cat_fill_dfs = pd.concat(list_fill_df)

    tfmsc_df_ops2019_tasp_1 = pd.concat([tfmsc_df_ops2019_cat_non_na,
                                            cat_fill_dfs])
    return tfmsc_df_ops2019_tasp_1


def fill_farm_arpts(tfmsc_df_ops2019_farmranch_):
    """
    Use the one farm and ranch airport with fleet mix to fill fleetmix for
    other airports.
    Parameters
    ----------
    tfmsc_df_ops2019_farmranch_

    Returns
    -------

    """
    farmranch_filler = tfmsc_df_ops2019_farmranch_.loc[lambda df:
    ~df.aircraft_type_id.isna()]
    assert len(farmranch_filler.facility_id.unique()) == 1, (
        "This function assumes that the we have fleet mix data for only one "
        "airport. Modify the function when this assumption is false.")
    farmranch_filler = (
        farmranch_filler
        .rename(columns={"facility_id": "filled_from_facility_id"})
        .assign(merge_key="x")
        .filter(items=[
        'merge_key', 'aircraft_id', 'aircraft_type_id', 'aircraft_type',
            'fleetmix', 'filled_from_facility_id'])
    )
    tfmsc_df_ops2019_farmranch_na_filled = (
        tfmsc_df_ops2019_farmranch_
        .loc[lambda df: df.aircraft_id.isna()]
        .assign(merge_key="x")
        .drop(columns=['aircraft_id', 'aircraft_type_id', 'aircraft_type',
            'fleetmix'])
        .merge(farmranch_filler, on="merge_key", how="left")
    )

    tfmsc_df_ops2019_farmranch_1 = pd.concat(
        [
            tfmsc_df_ops2019_farmranch_na_filled,
            tfmsc_df_ops2019_farmranch_.loc[lambda df : ~ df.aircraft_id.isna()]
        ]
    )
    return tfmsc_df_ops2019_farmranch_1


def fill_othpuair_arpts(tfmsc_df_ops2019_othpuair_):
    """
    Use airport with lowest operations to get fleet mix for other airports.
    Parameters
    ----------
    tfmsc_df_ops2019_othpuair_

    Returns
    -------

    """
    othpuair_filler = (
        tfmsc_df_ops2019_othpuair_
        .loc[lambda df: (~ df.aircraft_id.isna())]
        .loc[lambda df: (df.annual_operations == min(df.annual_operations))]
        .assign(merge_key="x")
        .rename(columns={"facility_id": "filled_from_facility_id"})
        .filter(items=[
            'merge_key', 'aircraft_id', 'aircraft_type_id', 'aircraft_type',
            'fleetmix', 'filled_from_facility_id'])
    )
    tfmsc_df_ops2019_othpuair_na_filled = (
        tfmsc_df_ops2019_othpuair_
            .loc[lambda df: df.aircraft_id.isna()]
            .assign(merge_key="x")
            .drop(columns=['aircraft_id', 'aircraft_type_id', 'aircraft_type',
                           'fleetmix'])
            .merge(othpuair_filler, on="merge_key", how="left")
    )

    tfmsc_df_ops2019_othpuair_1 = pd.concat(
        [
            tfmsc_df_ops2019_othpuair_na_filled,
            tfmsc_df_ops2019_othpuair_.loc[lambda df: ~ df.aircraft_id.isna()]
        ]
    )
    return tfmsc_df_ops2019_othpuair_1


def fill_othprair_arpts(tfmsc_df_ops2019_othprair_, tfmsc_df_ops2019_othpuair_):
    """
    Use PU airport with lowest operations to get fleet mix for other airports.

    Parameters
    ----------
    tfmsc_df_ops2019_othprair_
    tfmsc_df_ops2019_othpuair_

    Returns
    -------

    """
    othpuair_filler = (
        tfmsc_df_ops2019_othpuair_
        .loc[lambda df: (~ df.aircraft_id.isna())]
        .loc[lambda df: (df.annual_operations == min(df.annual_operations))]
        .assign(merge_key="x")
        .rename(columns={"facility_id": "filled_from_facility_id"})
        .filter(items=[
            'merge_key', 'aircraft_id', 'aircraft_type_id', 'aircraft_type',
            'fleetmix', 'filled_from_facility_id'])
    )
    tfmsc_df_ops2019_othprair_na_filled = (
        tfmsc_df_ops2019_othprair_
            .loc[lambda df: df.aircraft_id.isna()]
            .assign(merge_key="x")
            .drop(columns=['aircraft_id', 'aircraft_type_id', 'aircraft_type',
                           'fleetmix'])
            .merge(othpuair_filler, on="merge_key", how="left")
    )

    tfmsc_df_ops2019_othprair_1 = pd.concat(
        [
            tfmsc_df_ops2019_othprair_na_filled,
            tfmsc_df_ops2019_othprair_.loc[lambda df: ~ df.aircraft_id.isna()]
        ]
    )
    return tfmsc_df_ops2019_othprair_1


if __name__ == "__main__":
    path_ops2019_clean = Path.home().joinpath(
        PATH_INTERIM, "ops2019_meta_imputed_cor_counties.xlsx")


    ops2019 = pd.read_excel(path_ops2019_clean, usecols=[
        "facility_id", "facility_name", "facility_group", "county_arpt",
        "district_tx_boundar",
        "fips_tx_boundar", "medical_use", "military_joint_use",
        "otherservices", "fuel_types",	"ownership", "used", "annual_operations"])
    path_tfmsc = Path.home().joinpath(
        PATH_RAW, "madhu_files", "FAA_2019TFMSC.csv")
    tfmsc_df = pd.read_csv(path_tfmsc)
    tfmsc_df_cln = clean_tfmsc(tfmsc_df)

    tfmsc_df_ops2019 = (
        ops2019
        .merge(
            tfmsc_df_cln,
            on="facility_id",
            how="left"
        )
        .sort_values(by=["facility_id", "aircraft_id"])
    )
    tfmsc_df_ops2019_com = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "Commercial"]
    )
    assert all(~ tfmsc_df_ops2019_com.isna())
    tfmsc_df_ops2019_rel = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "Reliever"]
    )
    assert all(~ tfmsc_df_ops2019_rel.isna())
    tfmsc_df_ops2019_tasp = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "TASP"]
    )
    tfmsc_df_ops2019_tasp_filled = fill_tasp_mil_arpts(tfmsc_df_ops2019_tasp)

    tfmsc_df_ops2019_med = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "Medical"]
    )
    assert all(tfmsc_df_ops2019_med.aircraft_id.isna()), (
        "Use generic helicopter types and fleetmix.")
    # Fill manually
    tfmsc_df_ops2019_mil = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "Military"]
    )
    tfmsc_df_ops2019_mil_filled = fill_tasp_mil_arpts(tfmsc_df_ops2019_mil)
    tfmsc_df_ops2019_othprheli = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "Other_PR_Heliports"]
    )
    assert all(tfmsc_df_ops2019_othprheli.aircraft_id.isna()), (
        "Use generic helicopter types and fleetmix.")
    tfmsc_df_ops2019_othpuheli = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "Other_PU_Heliports"]
    )
    assert all(tfmsc_df_ops2019_othpuheli.aircraft_id.isna()), (
        "Use generic helicopter types and fleetmix.")
    tfmsc_df_ops2019_farmranch = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "Farm/Ranch"]
    )
    assert sum(tfmsc_df_ops2019_farmranch.groupby([
        "facility_id"]).aircraft_id.count()>=1)==1, ("1 airport with fleet "
                                                     "mix data.")
    tfmsc_df_ops2019_farmranch_filled = fill_farm_arpts(tfmsc_df_ops2019_farmranch)
    tfmsc_df_ops2019_othpuair = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "Other_PU_Airports"]
    )
    tfmsc_df_ops2019_othpuair_filled = fill_othpuair_arpts(
        tfmsc_df_ops2019_othpuair)

    tfmsc_df_ops2019_othprair = (
        tfmsc_df_ops2019.loc[lambda df: df.facility_group == "Other_PR_Airports"]
    )
    assert all(tfmsc_df_ops2019_othprair.aircraft_id.isna()), (
        "Use generic aircraft types and fleetmix.")

    tfmsc_df_ops2019_othprair_filled = fill_othprair_arpts(
        tfmsc_df_ops2019_othprair, tfmsc_df_ops2019_othpuair)


    output_dfs = {
        "Commercial": tfmsc_df_ops2019_com,
        "Reliever": tfmsc_df_ops2019_rel,
        "TASP": tfmsc_df_ops2019_tasp_filled,
        "Medical": tfmsc_df_ops2019_med,
        "Military": tfmsc_df_ops2019_mil_filled,
        "Other_PR_Heliports": tfmsc_df_ops2019_othprheli,
        "Other_PU_Heliports": tfmsc_df_ops2019_othpuheli,
        "Farm_Ranch": tfmsc_df_ops2019_farmranch_filled,
        "Other_PR_Airports": tfmsc_df_ops2019_othprair_filled,
        "Other_PU_Airports": tfmsc_df_ops2019_othpuair_filled
    }

    path_fleetmix_clean = Path.home().joinpath(
        PATH_INTERIM, "fleetmix_axb_07_05_2021.xlsx")

    writer = pd.ExcelWriter(path_fleetmix_clean, engine="xlsxwriter")
    for shnm, df in output_dfs.items():
        df.to_excel(writer, shnm, index=False)
    writer.save()






