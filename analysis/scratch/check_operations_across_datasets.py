import os
import pandas as pd
from analysis.scratch.utilis import PATH_RAW, get_snake_case_dict


if __name__ == "__main__":
    path_nfdc_may21 = os.path.join(PATH_RAW, "nfdc_facilities.csv")
    path_airport = os.path.join(PATH_RAW, "Airports.csv")
    path_tfmsc2020_mv = os.path.join(PATH_RAW, "madhu_files", "FAA_2020TFMSC.csv")
    path_taf = os.path.join(PATH_RAW, "madhu_files", "TX_TAF1990_2045.csv")
    path_airnav = os.path.join(PATH_RAW, "madhu_files", "AirNav.csv")
    nfdc_facilities_may21 = pd.read_csv(path_nfdc_may21)
    nfdc_rename_dict = get_snake_case_dict(nfdc_facilities_may21)
    airport = pd.read_csv(path_airport)
    airnav = pd.read_csv(path_airnav)
    nfdc_facilities_may21_1 = (
        nfdc_facilities_may21.rename(columns=nfdc_rename_dict)
        .filter(
            items=[
                "location_id",
                "facility_name",
                "effective_date",
                "city",
                "county",
                "state",
                "type",
                "airport_status_code",
                "ownership",
                "use",
                "medical_use",
                "other_services",
                "fuel_types",
                "military_joint_use",
                "single_engine_ga",
                "multi_engine_ga",
                "jet_engine_ga",
                "helicopters_ga",
                "gliders_operational",
                "military_operational",
                "ultralights",
                "operations_commercial",
                "operations_commuter",
                "operations_air_taxi",
                "operations_ga_local",
                "operations_ga_itin",
                "operations_military",
                "operations_date",
            ]
        )
        .rename(columns={"location_id": "facility_id", "type": "facility_type"})
        .assign(
            total_ops=lambda df: (
                df.operations_commercial.fillna(0)
                + df.operations_commuter.fillna(0)
                + df.operations_air_taxi.fillna(0)
                + df.operations_ga_local.fillna(0)
                + df.operations_ga_itin.fillna(0)
                + df.operations_military.fillna(0)
            )
        )
    )

    nfdc_facilities_may21_1_non_zero = nfdc_facilities_may21_1.loc[
        lambda df: df.total_ops > 0
    ].assign(facility_id=lambda df: df.facility_id.str.replace("'", ""))
    nfdc_facilities_may21_1_commercial = nfdc_facilities_may21_1.loc[
        lambda df: ~df.operations_commercial.isna()
    ]

    airport_tx = airport.rename(columns=get_snake_case_dict(airport)).loc[
        lambda df: df.state == "TX"
    ]

    tfmsc2020 = pd.read_csv(path_tfmsc2020_mv)
    taf = pd.read_csv(path_taf)
    tfmsc2020_1 = tfmsc2020.rename(columns=get_snake_case_dict(tfmsc2020)).rename(
        columns={"location_id": "facility_id", "airport": "airport"}
    )

    tfmsc2020_1_facilites = tfmsc2020_1.filter(
        items=["facility_id", "airport"]
    ).drop_duplicates("facility_id")

    taf_1 = taf.rename(columns=get_snake_case_dict(taf)).rename(
        columns={"loc_id": "facility_id", "aport_name": "airport"}
    )

    taf_1_facilites = taf_1.filter(items=["facility_id", "airport"]).drop_duplicates(
        "facility_id"
    )

    airnav_1 = (
        airnav.rename(columns=get_snake_case_dict(airnav))
        .assign(
            annualized_aircraft_operations=lambda df: df.annualized_aircraft_operations.astype(
                float
            )
        )
        .loc[lambda df: (df.annualized_aircraft_operations > 0)]
        .filter(items=["facility_id", "annualized_aircraft_operations"])
        .drop_duplicates(["facility_id"])
    )

    tfmsc_taf_merge = pd.merge(
        tfmsc2020_1_facilites,
        taf_1_facilites,
        on=["facility_id"],
        how="outer",
        suffixes=["_tfmsc2020", "_taf"],
    )

    nfdc_tfmsc_taf = pd.merge(
        nfdc_facilities_may21_1_non_zero, tfmsc_taf_merge, on="facility_id", how="outer"
    )

    nfdc_tfmsc_taf_airnav = pd.merge(
        nfdc_tfmsc_taf, airnav_1, on="facility_id", how="outer"
    )
