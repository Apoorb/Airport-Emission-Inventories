"""
Get a condensed data for Tableau Dashboard.
"""
import time
import re
from pathlib import Path
import pandas as pd
import numpy as np
import inflection


def read_yr_raw_emis(path_emis_, verbose=True):
    """
    Read and aggregate the raw data for each. Aggregation is by county and
    SCC.
    """
    t = time.process_time()
    year = int(re.search(r"Airport_EIS_(20\d\d).txt", path_emis_.name).group(1))
    df = pd.read_csv(path_emis_, sep="\t")
    rename_map = {
        "State_Facility_Identifier": "Facility",
        "Airport": "Airport",
        "Facility_Group": "FacGroup",
        "Facility_Type": "FacType",
        "County": "County",
        "FIP": "FIPS",
        "District": "District",
        "SCC": "SCC",
        "SCC_Description": "SccDesc",
        "Airframe": "Airframe",
        "Engine": "Engine",
        "Airframe_Engine_Code": "ArfmEngCd",
        "Mode": "Mode",
        "LTOS": "LTOs",
        "EIS_Pollutant_ID": "PolID",
        "UNCONTROLLED_ANNUAL_EMIS_ST": "UncntrAnnEmisST",
        "CONTROLLED_ANNUAL_EMIS_ST": "CntrAnnEmisST",
        "UNCONTROLLED_DAILY_EMIS_ST": "UncntrDailyEmisST",
        "CONTROLLED_DAILY_EMIS_ST": "CntrDailyEmisST",
    }
    df_1 = df.rename(columns=rename_map)
    df_1["Year"] = year
    elapsed_time = time.process_time() - t
    if verbose:
        print(f"Finished reading year {year} in {elapsed_time} sec.")
    return df_1


def agg_yr_fac(emis_df_):
    """
    Aggregate the raw data by year, facility, and scc.
    """

    emis_df_agg = (
        emis_df_.groupby(
            [
                "Year",
                "County",
                "FIPS",
                "Facility",
                "Airport",
                "FacGroup",
                "FacType",
                "SCC",
                "SccDesc",
                "PolID",
            ]
        )
        .agg(
            UncntrAnnEmisST=("UncntrAnnEmisST", "sum"),
            CntrAnnEmisST=("CntrAnnEmisST", "sum"),
        )
        .reset_index()
    )
    # Cannot directly get it from above. Speciated pollutants mess up the
    # aggregation.
    emis_df_ltos = (
        emis_df_.loc[
            (emis_df_.PolID == "CO2") & (emis_df_.Mode == "Aircraft"),
            ["Facility", "SCC", "Year", "ArfmEngCd", "LTOs"],
        ]
        .groupby(["Facility", "Year"])
        .LTOs.sum()
        .reset_index()
    )
    return emis_df_agg, emis_df_ltos


def agg_yr_cnty(emis_df_):
    """
    Aggregate by year, county, and SCC.
    """
    emis_df_agg = (
        emis_df_.groupby(["Year", "County", "FIPS", "SCC", "SccDesc", "PolID"])
        .agg(
            UncntrAnnEmisST=("UncntrAnnEmisST", "sum"),
            CntrAnnEmisST=("CntrAnnEmisST", "sum"),
        )
        .reset_index()
    )
    return emis_df_agg


if __name__ == "__main__":
    path_com = Path(
        r"C:\Users\a-bibeka\OneDrive - Texas A&M Transportation Institute"
        r"\Documents\Projects\Airports\Data Analysis\Data"
        r"\uncntr_cntr_emis_raw_data"
    )
    path_nfdc = Path.home().joinpath(path_com.parent, "FAA_NFDC_Facilities.csv")
    path_out_fac_scc = Path.home().joinpath(path_com.parent, "emis_fac_scc.csv")
    path_out_cnty_scc = Path.home().joinpath(path_com.parent, "emis_cnty_scc.csv")
    path_out_fac_scc_spec = Path.home().joinpath(
        path_com.parent, "emis_fac_scc_spec.csv"
    )
    path_out_cnty_scc_spec = Path.home().joinpath(
        path_com.parent, "emis_cnty_scc_spec.csv"
    )
    path_out_ltos_fac = Path.home().joinpath(path_com.parent, "ltos_fac.csv")
    path_spec = Path.home().joinpath(path_com.parent, "speciation_fin.xlsx")
    path_emis_raw_data = list(path_com.glob("*.txt"))

    # Read data
    debug_df = read_yr_raw_emis(path_emis_raw_data[9])
    emis_fac_scc, lto_fac_scc = agg_yr_fac(debug_df)
    # debug_lto = emis_fac_scc.loc[emis_fac_scc.LTOs_x != emis_fac_scc.LTOs_y]
    emis_cnty_scc = agg_yr_cnty(debug_df)
    dfw = lto_fac_scc.loc[lambda df: df.Facility == "dfw"]

    emis_df = pd.concat(map(read_yr_raw_emis, path_emis_raw_data))
    emis_fac_scc, lto_fac_scc = agg_yr_fac(emis_df)
    emis_cnty_scc = agg_yr_cnty(emis_df)
    emis_fac_scc.to_csv(path_out_fac_scc, index=False)
    lto_fac_scc.to_csv(path_out_ltos_fac, index=False)
    emis_cnty_scc.to_csv(path_out_cnty_scc, index=False)

    # Post Process: Run after above script has been executed.
    emis_fac_scc = pd.read_csv(path_out_fac_scc)
    emis_cnty_scc = pd.read_csv(path_out_cnty_scc)

    filter_pols = ["CO", "CO2", "NH3", "NOX", "PM10", "PM2.5", "Pb", "SO2", "VOC"]
    emis_fac_scc_fil = emis_fac_scc.loc[lambda df: df.PolID.isin(filter_pols)]
    emis_cnty_scc_fil = emis_cnty_scc.loc[lambda df: df.PolID.isin(filter_pols)]
    emis_fac_scc_fil["County"] = emis_fac_scc_fil.County.str.title()
    emis_cnty_scc_fil["County"] = emis_cnty_scc_fil.County.str.title()
    spec_df = pd.read_excel(path_spec, "Sheet1")
    spec_df_1 = spec_df.loc[:, ["polcode", "pol_nm"]].rename(
        columns={"polcode": "PolID", "pol_nm": "PolNm"}
    )
    spec_df_1.loc[spec_df_1.PolID == "108383 & 106423", "PolID"] = "108383+106423"

    nfdc = pd.read_csv(path_nfdc, usecols=["LocationID", "ARPLatitude", "ARPLongitude"])
    nfdc["Facility"] = nfdc["LocationID"].str.lower()
    set(emis_fac_scc_fil["Facility"]) - set(nfdc["Facility"])

    lat_df = nfdc.copy()
    lat_df[["d_lat", "m_lat", "sdir_lat"]] = nfdc.ARPLatitude.str.split(
        "-", expand=True
    )
    lat_df["s_lat"] = lat_df.sdir_lat.str[:-1]
    lat_df["dir_lat"] = lat_df.sdir_lat.str[-1]
    lat_df["sign_lat"] = np.where(lat_df.dir_lat == "N", 1, -1)
    lat_df.sign_lat.isna().sum()
    lat_df = lat_df.astype(
        {"d_lat": float, "m_lat": float, "s_lat": float, "sign_lat": float}
    )
    lat_df["latitude"] = lat_df.eval("sign_lat* (d_lat + m_lat / 60 + s_lat / 3600)")
    lat_df_1 = lat_df.filter(items=["Facility", "latitude"])

    long_df = nfdc.copy()
    long_df[["d_lon", "m_lon", "sdir_lon"]] = nfdc.ARPLongitude.str.split(
        "-", expand=True
    )
    long_df["s_lon"] = long_df.sdir_lon.str[:-1]
    long_df["dir_lon"] = long_df.sdir_lon.str[-1]
    long_df["sign_lon"] = np.where(long_df.dir_lon == "E", 1, -1)
    long_df.sign_lon.isna().sum()
    long_df = long_df.astype(
        {"d_lon": float, "m_lon": float, "s_lon": float, "sign_lon": float}
    )
    long_df["longitude"] = long_df.eval("sign_lon* (d_lon + m_lon / 60 + s_lon / 3600)")
    long_df_1 = long_df.filter(items=["Facility", "longitude"])

    nfdc_1 = (
        nfdc.merge(lat_df_1, on="Facility")
        .merge(long_df_1, on="Facility")
        .filter(items=["Facility", "latitude", "longitude"])
    )

    emis_fac_scc_fil_spec = emis_fac_scc_fil.merge(spec_df_1, on="PolID", how="left")
    emis_fac_scc_fil_spec["PolNm"] = emis_fac_scc_fil_spec.PolNm.fillna(
        emis_fac_scc_fil_spec["PolID"]
    )
    emis_fac_scc_fil_spec_1 = emis_fac_scc_fil_spec.merge(
        nfdc_1, on="Facility", how="left"
    )
    assert emis_fac_scc_fil_spec_1.latitude.isna().sum() == 0
    emis_fac_scc_fil_spec_1.to_csv(path_out_fac_scc_spec, index=False)

    emis_cnty_scc_fil_spec = emis_cnty_scc_fil.merge(spec_df_1, on="PolID", how="left")
    emis_cnty_scc_fil_spec["PolNm"] = emis_cnty_scc_fil_spec.PolNm.fillna(
        emis_cnty_scc_fil_spec["PolID"]
    )
    emis_cnty_scc_fil_spec.to_csv(path_out_cnty_scc_spec, index=False)
