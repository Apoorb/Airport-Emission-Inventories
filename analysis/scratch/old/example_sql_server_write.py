from sqlalchemy import create_engine
import urllib
import pandas as pd
from airportei.utilis import connect_to_sql_server


conn = connect_to_sql_server(database_nm="2019_IAH_3152021")
air_ops = pd.read_sql("SELECT * FROM [dbo].[AIR_OPERATION]", conn)

conn = connect_to_sql_server(database_nm="dfw_study")
cur = conn.cursor()
cur.execute("DELETE FROM [dfw_study].[dbo].[AIR_OPERATION]")
conn.close()

conn = connect_to_sql_server(database_nm="dfw_study")
cur = conn.cursor()
cur.execute(
    "ALTER TABLE [dbo].[AIR_OPERATION] NOCHECK CONSTRAINT "
    "FK__AIR_OPERATION__AIRCRAFT_ID"
)
conn.close()
quoted = urllib.parse.quote_plus(
    "DRIVER={SQL Server};SERVER=HMP-HVT3G63-LW0\SQLEXPRESS_AEDT;DATABASE"
    "=dfw_study"
)
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(quoted))

air_ops.to_sql(
    "AIR_OPERATION",
    schema="dbo",
    con=engine,
    chunksize=200,
    method="multi",
    index=False,
    if_exists="append",
)
