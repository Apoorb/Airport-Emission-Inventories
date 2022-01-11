import geopandas as gpd
import pandas as pd
from pathlib import Path
import os
from shapely.geometry import Point
from airportei.utilis import PATH_RAW, PATH_INTERIM, get_snake_case_dict

if __name__ == "__main__":

    path_fin_out = Path(
        r"C:\Users\a-bibeka\Texas A&M Transportation "
        r"Institute\HMP - TCEQ Projects - 2020 Texas "
        r"Statewide Airport "
        r"EI\Tasks\Task8_Reports\Final_Deliverables_15Oct2021"
    )
    path_ops = Path.joinpath(path_fin_out, "Appendix B 2019 Operations.xlsx")
    path_epa_arpt = Path(
        r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\external\Airports for State.xlsx"
    )
    ops = pd.read_excel(path_ops, "operations")
    epa_arpt = pd.read_excel(path_epa_arpt)
    epa_arpt = epa_arpt.rename(
        columns={"FacilitySiteIdentifier": "facility_id"}
    ).assign(
        facility_id=lambda df: (
            df.facility_id.str.strip().str.lower().str.replace("'", "")
        ),
        facility_name=lambda df: (
            df.Airport.str.strip().str.lower().str.replace("'", "")
        ),
    )
    epa_arpt.loc[epa_arpt.facility_id.str.contains("0e"), "facility_id"] = "3"

    path_airport_shp = os.path.join(PATH_RAW, "Airports", "Airports.shp")
    airport = gpd.read_file(path_airport_shp)

    airport_tx = airport.rename(columns=get_snake_case_dict(airport)).loc[
        lambda df: df.state == "TX"
    ]

    airport_tx_fil_counties = (
        airport_tx.rename(columns={"ident": "facility_id"})
        .assign(
            facility_id=lambda df: (
                df.facility_id.str.strip().str.lower().str.replace("'", "")
            )
        )
        .assign(
            facility_id=lambda df: df.facility_id.replace(
                ["1e4", "1e2", "1e7", "2e5", "2e7", "3e0"],
                [str(int(i)) for i in [1e4, 1e2, 1e7, 2e5, 2e7, 3e0]],
            )
        )
        .filter(items=["facility_id", "geometry"])
    )

    ops_fil = ops.rename(columns=get_snake_case_dict(ops)).assign(
        facility_id=lambda df: (
            df.facility_id.str.strip().str.lower().str.replace("'", "")
        ),
        facility_name=lambda df: (
            df.facility_name.str.strip().str.lower().str.replace("'", "")
        ),
    )

    ops_arpt_mrg = pd.merge(
        ops_fil, airport_tx_fil_counties, on=["facility_id"], how="left"
    )
    ops_arpt_mrg.geometry.isna().sum()
    ops_arpt_mrg.loc[
        lambda df: df.facility_id == "0ta7", "geometry"
    ] = gpd.points_from_xy([-103.900948], [30.149786], [0])
    ops_arpt_mrg.loc[
        lambda df: df.facility_id == "ta27", "geometry"
    ] = gpd.points_from_xy([-97.950298], [30.837960], [0])
    ops_arpt_mrg.loc[
        lambda df: df.facility_id == "xs10", "geometry"
    ] = gpd.points_from_xy([-98.350527], [29.553092], [0])
    ops_arpt_mrg_epa = ops_arpt_mrg.merge(
        epa_arpt.drop(columns=["Latitude", "Longitude"]), on="facility_id", how="left"
    )
    ops_arpt_mrg_epa.EISFacilitySiteIdentifier.isna().sum()
    facility_ids_match = ops_arpt_mrg_epa.loc[
        ~ops_arpt_mrg_epa.EISFacilitySiteIdentifier.isna(), "facility_id"
    ].values
    facility_ids_no_match = ops_arpt_mrg_epa.loc[
        ops_arpt_mrg_epa.EISFacilitySiteIdentifier.isna(), "facility_id"
    ].values

    ops_arpt_mrg_no_match = ops_arpt_mrg.loc[
        ops_arpt_mrg.facility_id.isin(facility_ids_no_match)
    ]
    ops_fil_no_match_epa_merge = pd.merge(
        ops_arpt_mrg_no_match,
        epa_arpt,
        on=["facility_name"],
        how="left",
        suffixes=["_tti", "_epa"],
    )

    name_match_epa = ops_fil_no_match_epa_merge.loc[
        lambda df: ~df.EISFacilitySiteIdentifier.isna()
    ]

    facility_id_name_match_epa = ops_fil_no_match_epa_merge.loc[
        lambda df: ~df.EISFacilitySiteIdentifier.isna(), "facility_id_epa"
    ].values
    facility_id_name_match_tti = ops_fil_no_match_epa_merge.loc[
        lambda df: ~df.EISFacilitySiteIdentifier.isna(), "facility_id_tti"
    ].values
    facility_name_no_match = ops_fil_no_match_epa_merge.EISFacilitySiteIdentifier.isna()

    epa_arpt_gdf = gpd.GeoDataFrame(
        epa_arpt,
        geometry=gpd.points_from_xy(y=epa_arpt.Latitude, x=epa_arpt.Longitude),
        crs="EPSG:4326",
    )

    epa_arpt_remaining_gdf = epa_arpt_gdf.loc[
        ~epa_arpt_gdf.facility_id.isin(
            list(facility_ids_match) + list(facility_id_name_match_epa)
        )
    ]

    ops_arpt_mrg_gdf = gpd.GeoDataFrame(ops_arpt_mrg, geometry="geometry")
    ops_arpt_mrg_remaining_gdf = ops_arpt_mrg_gdf.loc[
        ~ops_arpt_mrg_gdf.facility_id.isin(
            list(facility_ids_match) + list(facility_id_name_match_tti)
        )
    ]

    ops_arpt_mrg_remaining_gdf.set_index("facility_id", inplace=True)
    epa_arpt_remaining_gdf.set_index("facility_id", inplace=True)

    ops_arpt_mrg_remaining_gdf = ops_arpt_mrg_remaining_gdf.to_crs("EPSG:3081")
    epa_arpt_remaining_gdf = epa_arpt_remaining_gdf.to_crs("EPSG:3081")

    distance_matrix = ops_arpt_mrg_remaining_gdf.geometry.apply(
        lambda g: epa_arpt_remaining_gdf.distance(g)
    )
    metre_to_mi = 0.000621371
    distance_matrix_mi = distance_matrix * metre_to_mi
    distance_matrix_mi.index.name = "facility_id_tti"
    distance_matrix_mi_long = distance_matrix_mi.stack().reset_index()
    distance_matrix_mi_long.columns = ["facility_id_tti", "facility_id_epa", "dist_mi"]
    distance_matrix_mi_long.sort_values(["facility_id_tti", "dist_mi"], inplace=True)
    distance_matrix_mi_long_closest = distance_matrix_mi_long.loc[
        distance_matrix_mi_long.groupby("facility_id_tti").dist_mi.idxmin().values
    ]
    distance_matrix_mi_long_closest = distance_matrix_mi_long.groupby(
        "facility_id_tti"
    ).head(3)
    distance_matrix_mi_long_closest["rank"] = list([1, 2, 3]) * int(
        len(distance_matrix_mi_long_closest) / 3
    )

    tti_cols = {
        col: f"{col}_tti"
        for col in [
            "county",
            "city",
            "district",
            "fips",
            "facility_id",
            "facility_name",
            "geometry",
        ]
    }
    ops_arpt_mrg_1 = ops_arpt_mrg.filter(items=tti_cols.keys()).rename(columns=tti_cols)

    distance_matrix_mi_long_closest_1 = distance_matrix_mi_long_closest.merge(
        epa_arpt, left_on="facility_id_epa", right_on="facility_id", how="left"
    )

    distance_matrix_mi_long_closest_2 = distance_matrix_mi_long_closest_1.merge(
        ops_arpt_mrg_1,
        left_on="facility_id_tti",
        right_on="facility_id_tti",
        how="left",
    )

    path_out_name_match = Path.joinpath(path_epa_arpt.parent, "name_match.csv")
    path_out_dist_matrix = Path.joinpath(
        path_epa_arpt.parent, "unmatched_fac_dist_matrix.csv"
    )

    name_match_epa = name_match_epa.rename(columns=tti_cols)
    name_match_epa = name_match_epa.filter(
        items=[
            "county_tti",
            "city_tti",
            "district_tti",
            "fips_tti",
            "facility_id_tti",
            "facility_name_tti",
            "geometry_tti",
            "AirportKey",
            "StateAndCountyFIPSCode",
            "TribalCode",
            "Airport",
            "City",
            "State",
            "ZIP",
            "Latitude",
            "Longitude",
            "facility_id_epa",
            "EISFacilitySiteIdentifier",
            "OpStatus",
            "RevisionNotes",
        ]
    )

    distance_matrix_mi_long_closest_2 = distance_matrix_mi_long_closest_2.filter(
        items=[
            "facility_id_tti",
            "facility_id_epa",
            "dist_mi",
            "rank",
            "county_tti",
            "city_tti",
            "district_tti",
            "fips_tti",
            "facility_name_tti",
            "geometry_tti",
            "AirportKey",
            "StateAndCountyFIPSCode",
            "TribalCode",
            "Airport",
            "City",
            "State",
            "ZIP",
            "Latitude",
            "Longitude",
            "facility_id",
            "EISFacilitySiteIdentifier",
            "OpStatus",
            "RevisionNotes",
            "geometry",
        ]
    )
    name_match_epa.to_csv(path_out_name_match, index=False)
    distance_matrix_mi_long_closest_2.to_csv(path_out_dist_matrix, index=False)

    # Good to know
    ops_arpt_mrg_remaining_gdf.crs
    epa_arpt_remaining_gdf.crs.axis_info[0].unit_name
