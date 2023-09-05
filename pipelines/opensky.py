import os
from connectors.open_sky_api_client import OpenSkyAPIClient
from connectors.postgresql import PostgreSqlClient
from assets.opensky import  load, extract_max_date, extract_by_direction
import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, DateTime 
import pytz

def main():
    load_dotenv()
    username = os.environ.get("USERNAME_API_OPENSKY")
    password = os.environ.get("PASSWORD_API_OPENSKY")
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
        Column("icao24", String),
        Column("flightDataType", String),
        Column("firstSeen", DateTime, nullable=True),
        Column("estDepartureAirport", String),
        Column("lastSeen", DateTime, nullable=True),
        Column("estArrivalAirport", String),
        Column("callsign", String),
        Column("estDepartureAirportHorizDistance", String),
        Column("estDepartureAirportVertDistance", String),
        Column("estArrivalAirportHorizDistance", String),
        Column("estArrivalAirportVertDistance", String),
        Column("departureAirportCandidatesCount", String),
        Column("arrivalAirportCandidatesCount", String),
        Column("extractDateTime", DateTime),
        Column("hashed", String, primary_key=True)
    )
    
    airport = "ESSA" # ICAO: Arlanda Stockholm -- IATA names are not used.
    default_begin_timestamp = datetime.datetime(2023,9,4,0,0, tzinfo=pytz.utc)
    end_timestamp = datetime.datetime.now(pytz.utc).replace(microsecond = 0)
    arrival = "arrival"
    departure = "departure"

    begin_timestamp = extract_max_date(postgresql_client = postgresql_client, table = table, default_begin_timestamp = default_begin_timestamp)
   
   #testing 
    # begin_timestamp = datetime.datetime(2023,9,4,0,0, tzinfo=pytz.utc) #TODO: uncommented
    
    #extract the data
    df_arrivals = extract_by_direction(opensky_client = opensky_client, direction = arrival, airport = airport, begin_timestamp = begin_timestamp, end_timestamp = end_timestamp)
    if df_arrivals is None:
        print(f"The dataframe is None for direction: {arrival}")
        print(f"Begin time is: {begin_timestamp} and end time is: {end_timestamp}")
    elif isinstance(df_arrivals, pd.DataFrame):
        print("Printing dataframes first 10 rows")
        
        print(df_arrivals.head(10).sort_values(by='firstSeen', ascending=True))
        print(df_arrivals.shape[0])
    else:
        print("Variables is of unknown type")
    try:
        load(df=df_arrivals,postgresql_client=postgresql_client, table=table, metadata=metadata)
    except:
        print("Nothing to load. Exiting.")

   #df_departures = extract_departures(opensky_client = opensky_client, airport = airport, begin_timestamp = begin_timestamp, end_timestamp = end_timestamp)
   ##print(df_departures.head(10))
   #load(df=df_departures,postgresql_client=postgresql_client, table=table, metadata=metadata)

if __name__ == "__main__":
    main()