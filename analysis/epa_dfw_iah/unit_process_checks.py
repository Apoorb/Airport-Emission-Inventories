import pandas as pd
import numpy as np
from pathlib import Path
import pathlib
from airportei.utilis import PATH_INTERIM, connect_to_sql_server, get_snake_case_dict


path_aedt_epa_lookup = Path(
    r"C:\Users\a-bibeka\PycharmProjects"
    r"\airport_ei\data\external"
    r"\2020_engine_type_codes.csv"
)

path_tti_ltos = Path(
    r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data\processed\epa_ltos_review\Revised LTO for State.xlsx"
)

arfm_eng_scc_map = pd.read_csv(path_aedt_epa_lookup)
lto_review = pd.read_excel(path_tti_ltos)
lto_review_tti = lto_review.loc[lto_review.Revised_LTO > 0]
lto_review_tti.dtypes
arfm_eng_scc_map.dtypes


lto_review_tti_test = lto_review_tti.merge(
    arfm_eng_scc_map,
    left_on=["SourceClassificationCode", "AircraftEngineTypeCode"],
    right_on=["SCC", "Code"],
    how="left",
)

lto_review_tti_test_na = lto_review_tti_test.loc[lambda df: df.SCC.isna()]

lto_review_tti_test_na_1 = lto_review_tti_test_na[
    [
        "Airport",
        "SourceClassificationCode",
        "ProcessDescription",
        "AircraftEngineTypeCode",
    ]
].merge(
    arfm_eng_scc_map, left_on=["AircraftEngineTypeCode"], right_on=["Code"], how="left"
)

lto_review_tti_test_na_1.columns

test = (
    lto_review_tti_test_na_1.groupby(
        ["Airport", "FAA Aircraft Type", "ProcessDescription", "SCC Description"]
    )
    .AircraftEngineTypeCode.count()
    .sort_values(ascending=False)
    .reset_index()
)


lto_review_tti_test_na.Revised_LTO.sum() / lto_review_tti_test.Revised_LTO.sum()
