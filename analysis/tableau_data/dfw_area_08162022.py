"""
Get emissions for the following 16 counties: Collin, Dallas, Denton, Ellis, Erath, Hood, Hunt, Johnson, Kaufman, Navarro, Palo Pinto, Parker, Rockwall, Somervell, Tarrant, and Wise 
Created by: AxB
Created on: 08/16/2022
"""
import time
import re
from pathlib import Path
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import dask.dataframe as dd

from analysis.tableau_data.i_tableau_input import read_yr_raw_emis


def agg_yr_fac_ltos(emis_df_):
    """
    Aggregate the raw data by year, facility, and scc.
    """
    emis_df_ltos = (
        emis_df_.loc[(emis_df_.PolID == "CO2") & (emis_df_.Mode == "Aircraft"),]
        .groupby(
            ["Year", "County", "FIPS", "Facility", "Airport", "SCC", "SccDesc", "PolID"]
        )
        .LTOs.sum()
        .reset_index()
    )
    return emis_df_ltos


# Set paths
#######################################################################################
path_com = Path(
    r"E:\OneDrive - Texas A&M Transportation Institute"
    r"\Documents\Projects\Airports\Data Analysis\Data"
    r"\uncntr_cntr_emis_raw_data"
)
path_cnty_scc_spec = Path.home().joinpath(path_com.parent, "emis_cnty_scc_spec.csv")
path_cnty_scc_ltos = Path.home().joinpath(path_com.parent, "ltos_fac.csv")

path_emis_raw_data = [
    list(path_com.glob("*EIS_2019.txt"))[0],
    list(path_com.glob("*EIS_2020.txt"))[0],
]
path_out_raw_concat = Path.home().joinpath(
    path_com.parent, "Raw_08162022", "emis_df_19_20.parquet"
)
# Convert data Format (run once)
#######################################################################################
# emis_df = pd.concat(map(read_yr_raw_emis, path_emis_raw_data))
# emis_df.to_parquet(
#     path_out_raw_concat,
#     index=False,
#     partition_cols=["Year", "FIPS", "SCC"],
#     engine="fastparquet",
# )

# Read Data
#######################################################################################+
filt_counties = {
    48085: "Collin",
    48113: "Dallas",
    48121: "Denton",
    48139: "Ellis",
    48143: "Erath",
    48221: "Hood",
    48231: "Hunt",
    48251: "Johnson",
    48257: "Kaufman",
    48349: "Navarro",
    48363: "Palo Pinto",
    48367: "Parker",
    48397: "Rockwall",
    48425: "Somervell",
    48439: "Tarrant",
    48497: "Wise",
}
filt_counties_ls = list(filt_counties)

emis_cnty_scc_fil_spec = pd.read_csv(path_cnty_scc_spec)
emis_19_20_pq = pq.ParquetDataset(
    path_out_raw_concat, filters=[("Year", "in", [2019, 2020])]
)
emis_df_19_20 = (
    emis_19_20_pq.read().to_pandas().loc[lambda df: (df.FIPS.isin(filt_counties_ls))]
)
emis_19_20_pq.validate_schemas()
emis_df_19_20.dtypes
emis_df_19_20 = emis_df_19_20.assign(
    FIPS=lambda df: df.FIPS.astype(int),
    SCC=lambda df: df.SCC.astype(float),
    Year=lambda df: df.Year.astype(int),
)

emis_df_19_20_filt = emis_df_19_20.loc[
    lambda df: (df.PolID == "CO") & (df.Mode == "Aircraft")
]
emis_df_19_20_filt = emis_df_19_20_filt.reset_index(drop=True)

# Process Data
#######################################################################################
ltos = (
    emis_df_19_20_filt.groupby(
        ["Year", "FIPS", "Facility", "Airport", "SCC", "SccDesc"]
    )
    .LTOs.sum()
    .reset_index()
    .assign(Operations=lambda df: df.LTOs * 2)
)
ltos["County"] = ltos.FIPS.map(filt_counties)
pol_ids = emis_cnty_scc_fil_spec[["PolID", "PolNm"]].drop_duplicates()
emis = (
    emis_df_19_20.groupby(
        ["Year", "FIPS", "Facility", "Airport", "SCC", "SccDesc", "PolID"]
    )
    .CntrAnnEmisST.sum()
    .reset_index()
)
emis["County"] = emis.FIPS.map(filt_counties)
emis_filt = emis.merge(pol_ids, on="PolID")
emis_filt = emis_filt.rename(
    columns={"CntrAnnEmisST": "Cntrolled Annual Emission in Short Ton"}
)
emis_filt = emis_filt.filter(
    items=[
        "Year",
        "FIPS",
        "County",
        "Facility",
        "Airport",
        "SCC",
        "SccDesc",
        "PolID",
        "PolNm",
        "Cntrolled Annual Emission in Short Ton",
    ]
)
ltos = ltos.filter(
    items=[
        "Year",
        "FIPS",
        "County",
        "Facility",
        "Airport",
        "SCC",
        "SccDesc",
        "LTOs",
        "Operations",
    ]
)

path_out_ltos = Path.home().joinpath(path_out_raw_concat.parent, "ltos_dfw_19_20.xlsx")
path_out_emis = Path.home().joinpath(path_out_raw_concat.parent, "emis_dfw_19_20.xlsx")
ltos.to_excel(path_out_ltos, index=False)
emis_filt.to_excel(path_out_emis, index=False)

# Testing
########################################################################################
assert set(filt_counties_ls) - set(emis_cnty_scc_fil_spec.FIPS.unique()) == set()
ltos_old = pd.read_csv(path_cnty_scc_ltos)
emis_dfw_19_20 = emis_cnty_scc_fil_spec.loc[
    lambda df: (df.FIPS.isin(filt_counties_ls)) & (df.Year.isin([2019, 2020]))
].filter(
    items=[
        "Year",
        "County",
        "FIPS",
        "SCC",
        "SccDesc",
        "PolID",
        "UncntrAnnEmisST",
        "CntrAnnEmisST",
        "PolNm",
    ]
)
emis_filt_test = (
    emis_filt.groupby(["Year", "FIPS", "SCC", "SccDesc", "PolID", "PolNm"])
    .CntrAnnEmisST.sum()
    .reset_index()
)
emis_filt_test["Year"] = emis_filt_test.Year.astype(int)
emis_filt_test["FIPS"] = emis_filt_test.FIPS.astype(int)
emis_filt_test["SCC"] = emis_filt_test.SCC.astype("float")

emis_test = emis_filt_test.merge(
    emis_dfw_19_20,
    on=["Year", "FIPS", "SCC", "SccDesc", "PolID", "PolNm"],
    suffixes=["_new", "_old"],
)
ltos.Facility.unique()
ltos_1 = ltos.groupby(["Year", "FIPS", "Facility"]).LTOs.sum().reset_index()
lto_test = ltos_1.merge(ltos_old, on=["Facility", "Year"], suffixes=["_new", "_old"])
assert np.allclose(emis_test.CntrAnnEmisST_new, emis_test.CntrAnnEmisST_old)
assert np.allclose(lto_test.LTOs_new, lto_test.LTOs_old)
