import os
import pandas as pd
from airportei.utilis import (PATH_RAW, get_snake_case_dict)


if __name__ == "__main__":
    path_nfdc_may21 = os.path.join(PATH_RAW, "nfdc_facilities.csv")
    path_airport = os.path.join(PATH_RAW, "Airports.csv")
    nfdc_facilities_may21 = pd.read_csv(path_nfdc_may21)
    nfdc_rename_dict = get_snake_case_dict(nfdc_facilities_may21)
    airport = pd.read_csv(path_airport)
    nfdc_facilities_may21_1 = (
        nfdc_facilities_may21
        .rename(columns=nfdc_rename_dict)
        .filter(items=[
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
        ])
        .rename(
            columns={
                "location_id": "facility_id",
                "type": "facility_type"
            }
        )
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

    airport_tx = (
        airport
        .rename(columns=get_snake_case_dict(airport))
        .loc[lambda df: df.state == "TX"]
    )

    nfdc_facilities_may21_1_fil = (
        nfdc_facilities_may21_1
        .filter(items=["facility_id", "facility_name", "city", "county"])
        .assign(
            facility_id=lambda df: (
                df.facility_id.str.strip().str.lower().str.replace("'", "")
            ),
            facility_name=lambda df: (
                df.facility_name.str.strip().str.lower().str.replace("'", "")
            ),
        )
    )

    airport_tx_fil = (
        airport_tx
        .rename(columns={"ident": "facility_id",
                         "name": "facility_name",
                         "operstatus": "operstatus_arpt",
                         "latitude": "lat_arpt",
                         "longitude": "long_arpt",
                         "servcity": "city"
                         })
        .assign(
            facility_id=lambda df: (
                df.facility_id.str.strip().str.lower().str.replace("'", "")
            ),
            facility_name=lambda df: (
                df.facility_name.str.strip().str.lower().str.replace("'", "")
            ),
        )
        .filter(items=[
            "facility_id", "facility_name", "operstatus_arpt", "city",
            "lat_arpt",
            "long_arpt"])
    )

    nfdc_arpt_mrg = (
        pd.merge(
            nfdc_facilities_may21_1_fil,
            airport_tx_fil,
            on=["facility_id"],
            suffixes=["_nfdc", "_arpt"],
            how="outer"
        )
    )

    miss_nfdc = nfdc_arpt_mrg.loc[lambda df: df.city_nfdc.isna()]

    miss_arpt = nfdc_arpt_mrg.loc[lambda df: df.city_arpt.isna()]

    mask = (nfdc_arpt_mrg.city_nfdc.isna()) | (nfdc_arpt_mrg.city_arpt.isna())
    assert all(nfdc_arpt_mrg.loc[~mask, "city_nfdc"]
               == nfdc_arpt_mrg.loc[~mask, "city_arpt"])

    operation_date = (
        nfdc_facilities_may21_1.operations_date.value_counts().sort_index())