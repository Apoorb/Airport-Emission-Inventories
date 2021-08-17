"""
Generate table for available speciation factors.
"""
from pathlib import Path
import pandas as pd
import pyodbc
from airportei.utilis import connect_to_sql_server, get_snake_case_dict

if __name__ == "__main__":
    path_tti_list = (
        r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data"
        r"\interim\speciation\tti_pol_list.xlsx"
    )
    conn = connect_to_sql_server("FLEET")
    path_speciation_out1 = (
        r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data"
        r"\interim\speciation\speciation_fin.xlsx"
    )
    # Get TOG speciation from the AEDT database.
    sql_text = """
        SELECT A.[PROF_ID]
            ,A.[POL_ID]
            ,[SPECIATEID]
            ,B.[NAME] AS [Prof_nm]
            ,C.[NAME] AS [Pol_nm]
            ,[MASSFRAC]
            ,[CAS_NUM]
            ,[CAA_HAP]
            ,[IRIS_HAP]
            ,[HC]
            ,[VOC]
        FROM [FLEET].[dbo].[STN_MASSFRAC] AS A
        JOIN [FLEET].[dbo].[STN_SPECPROF] AS B ON A.[PROF_ID] = B.[PROF_ID]
        JOIN [FLEET].[dbo].[STN_SPEC_HC] AS C ON A.[POL_ID] = C.[POL_ID]
        WHERE B.[NAME] IN ('Aircraft: Piston', 'Aircraft: Turbine and APU', 'GSE: Diesel', 'GSE: Gasoline, LPG and CNG');
    """
    aedtspeciation = pd.read_sql(sql_text, conn)
    aedtspeciation.rename(
        columns={"CAS_NUM": "polcode", "Prof_nm": "prof_nm_aedt"}, inplace=True
    )
    aedtspeciation.rename(columns=get_snake_case_dict(aedtspeciation), inplace=True)

    ttipollist = pd.read_excel(path_tti_list)
    ttipollist.columns = ["pol_nm", "polcode", "basepolfracof"]
    ttipollist["polcode"] = ttipollist["polcode"].astype(str)
    ttipolspeciate = ttipollist.merge(
        aedtspeciation, on=["polcode"], suffixes=["_tti", "_aedt"], how="left"
    )
    ttipolspeciatefil = ttipolspeciate.filter(
        items=["polcode", "pol_nm_tti", "prof_nm_aedt", "massfrac", "speciateid"]
    )
    ttipolspeciatefil.to_excel(path_speciation_out1, index=False)




    # Get speciation from speciate database.
    path_speicate_db = (
        r"C:\Users\a-bibeka\PycharmProjects\airport_ei\data"
        r"\interim\speciation\speciate_5.1_0\SPECIATE 5.1 New Structure 7-13-2020.accdb"
    )
    conn = pyodbc.connect(
        r"""Driver={0};DBQ={1}""".format(
            "{Microsoft Access Driver (*.mdb, *.accdb)}", path_speicate_db
        )
    )
    cur = conn.cursor()
    for row in cur.tables():
        print(row.table_name)
    species = pd.read_sql("SELECT * FROM SPECIES", conn)
    species.rename(columns=get_snake_case_dict(species), inplace=True)
    species_prop = pd.read_sql("SELECT * FROM SPECIES_PROPERTIES", conn)
    species_prop.rename(columns=get_snake_case_dict(species_prop), inplace=True)
    profiles = pd.read_sql("SELECT * FROM PROFILES", conn)
    profiles.rename(columns=get_snake_case_dict(profiles), inplace=True)
    profiles_fil = profiles.loc[
        lambda df: df.profile_name.str.contains("aircraft", case=False)
        & (df.region.isin(["United States", "California"]))
    ]

    profiles_fil_spec = profiles_fil.merge(
        species, on="profile_code", how="left"
    ).merge(species_prop, on="species_id", how="left")

    profiles_fil_spec_fil = profiles_fil_spec.filter(
        items=[
            "profile_code",
            "profile_name",
            "profile_type",
            "master_pollutant",
            "qscore",
            "qscore_desc",
            "quality",
            "control",
            "test_year",
            "species_id",
            "spec_nm",
            "cas_no_hyphen",
            "weight_percent",
        ]
    )

