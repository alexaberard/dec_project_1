import os
from dec_project_1.connectors.open_sky_api_client import OpenSkyAPIClient
from dec_project_1.connectors.postgresql import PostgreSqlClient
from dec_project_1.assets.opensky import extract_arrivals, extract_departures, load, extract_max_date
import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, DateTime 


def main():
    load_dotenv()
    username = os.environ.get("username")
    password = os.environ.get("password")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    
    opensky_client = OpenSkyAPIClient(
        username, 
        password
    )

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD
    )
    metadata = MetaData()

    table = Table(
        "flightdata", metadata, 
        Column("icao24", String, primary_key=True),
        Column("flightDataType", String),
        Column("firstSeen", String),
        Column("estDepartureAirport", String),
        Column("lastSeen", String),
        Column("estArrivalAirport", String),
        Column("callsign", String),
        Column("estDepartureAirportHorizDistance", String),
        Column("estDepartureAirportVertDistance", String),
        Column("estArrivalAirportHorizDistance", String),
        Column("estArrivalAirportVertDistance", String),
        Column("departureAirportCandidatesCount", String),
        Column("arrivalAirportCandidatesCount", String),
        Column("extractDateTime", DateTime)
    )
    
    airport = "ESSA" # ICAO: Arlanda Stockholm -- IATA names are not used.
    default_begin_timestamp = datetime.datetime(2023,1,1,0,0)
    end_timestamp = datetime.datetime.now().replace(microsecond = 0)

    begin_timestamp = extract_max_date(postgresql_client = postgresql_client, table = table, default_begin_timestamp = default_begin_timestamp)
    begin_timestamp = pd.to_datetime(begin_timestamp)
   
   #testing 
    begin_timestamp = datetime.datetime(2023,8,25,22,0)
    
    #extract the data
    df_arrivals = extract_arrivals(opensky_client = opensky_client, airport = airport, begin_timestamp = begin_timestamp, end_timestamp = end_timestamp)
    if df_arrivals:
        print(df_arrivals.head(10))
   #load(df=df_arrivals,postgresql_client=postgresql_client, table=table, metadata=metadata)

   #df_departures = extract_departures(opensky_client = opensky_client, airport = airport, begin_timestamp = begin_timestamp, end_timestamp = end_timestamp)
   ##print(df_departures.head(10))
   #load(df=df_departures,postgresql_client=postgresql_client, table=table, metadata=metadata)


if __name__ == "__main__":
    main()