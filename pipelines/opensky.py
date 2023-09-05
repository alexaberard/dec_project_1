import os
from dec_project_1.connectors.open_sky_api_client import OpenSkyAPIClient
from dec_project_1.connectors.postgresql import PostgreSqlClient
from dec_project_1.assets.opensky import  load, extract_max_date, extract_by_direction
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
    
    #airport = "EPWA" #"ESSA" # ICAO: Arlanda Stockholm -- IATA names are not used.
    airports = ["EPKK","EPWA"] # EPWA = Warzaw, EPKK = Krakow
    default_begin_timestamp = datetime.datetime(2023,8,1,0,0, tzinfo=pytz.utc)
    end_timestamp = datetime.datetime.now(pytz.utc).replace(microsecond = 0)
    arrival = "arrival"
    departure = "departure"

    begin_timestamp = extract_max_date(postgresql_client = postgresql_client, table = table, default_begin_timestamp = default_begin_timestamp)
    
    #extract the data
    print(f"Begin time is: {begin_timestamp} and end time is: {end_timestamp}")

    for airport in airports:
        print(f"Extracting and loading for airport: {airport}")
        df_arrivals = extract_by_direction(opensky_client = opensky_client, direction = arrival, airport = airport, begin_timestamp = begin_timestamp, end_timestamp = end_timestamp)
        df_departures = extract_by_direction(opensky_client = opensky_client, direction = departure, airport = airport, begin_timestamp = begin_timestamp, end_timestamp = end_timestamp)
        # end extract

        #load flightdata to database
        if isinstance(df_arrivals, pd.DataFrame):
            print(f"Loading {arrival} data to database")
            load(df=df_arrivals,postgresql_client=postgresql_client, table=table, metadata=metadata)
        else:
            print(f"No data to extract for direction: {arrival}")

        if isinstance(df_departures, pd.DataFrame):
            print(f"Loading {departure} data to database")
            load(df=df_departures,postgresql_client=postgresql_client, table=table, metadata=metadata)
        else:
            print(f"No data to extract for direction: {departure}")
        
        # end load to database

if __name__ == "__main__":
    main()