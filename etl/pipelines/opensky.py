import os
from etl.connectors.open_sky_api_client import OpenSkyAPIClient
from etl.connectors.postgresql import PostgreSqlClient
from etl.assets.opensky import  load, extract_max_date, extract_by_direction
from etl.assets.transform import transform, load_source_table_to_dwh
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy.engine import URL, Engine
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, DateTime, create_engine
from jinja2 import Environment, FileSystemLoader, Template
import pytz

def main():
    load_dotenv()
    username = os.environ.get("USERNAME_API_OPENSKY")
    password = os.environ.get("PASSWORD_API_OPENSKY")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DATABASE_DWH_NAME = os.environ.get("DATABASE_DWH_NAME")
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
    
    airports = ["EPKK","EPWA"] # EPWA = Warzaw, EPKK = Krakow
    default_begin_timestamp = datetime.now(pytz.utc).replace(microsecond=0) - timedelta(days=30)
    end_timestamp = datetime.now(pytz.utc).replace(microsecond = 0)
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
            print(f"No data to load for direction: {arrival}")

        if isinstance(df_departures, pd.DataFrame):
            print(f"Loading {departure} data to database")
            load(df=df_departures,postgresql_client=postgresql_client, table=table, metadata=metadata)
        else:
            print(f"No data to load for direction: {departure}")
        
    # end load to database
    
    # start load to dwh
    source_connection_url = URL.create(
        drivername = "postgresql+pg8000", 
        username = DB_USERNAME,
        password = DB_PASSWORD,
        host = SERVER_NAME, 
        # port = SERVER_PORT,
        database = DATABASE_NAME, 
    )
    
    source_engine = create_engine(source_connection_url)    
    
    dwh_connection_url = URL.create(
        drivername = "postgresql+pg8000", 
        username = DB_USERNAME,
        password = DB_PASSWORD,
        host = SERVER_NAME, 
        # port = SERVER_PORT,
        database = DATABASE_DWH_NAME, 
    )
    dwh_engine = create_engine(dwh_connection_url)
    
    
    source_file_system_loader = FileSystemLoader("etl/sql/load")
    source_environment = Environment(loader=source_file_system_loader)
    
    for sql_load_file in source_environment.list_templates():
        load_source_table_to_dwh(
            sql_load_file=sql_load_file,
            source_environment=source_environment,
            source_engine=source_engine,
            dwh_engine=dwh_engine
        )    
        
    # end load to dwh
    
    # start of transform

    transform_file_system_loader = FileSystemLoader("etl/sql/transform")
    transform_environment = Environment(loader=transform_file_system_loader)
    
    for sql_transform_file in transform_environment.list_templates():
        sql_transform_template = transform_environment.get_template(sql_transform_file)
        # staging_table_name = sql_transform_template.make_module().config.get("table_name")
        staging_table_name = os.path.splitext(sql_transform_file)[0]
        transform(
            engine=dwh_engine,
            sql_template=sql_transform_template,
            table_name=staging_table_name,
        )  
        print(f"Table {staging_table_name} loaded to datawarehouse")
    # end of transform
        

if __name__ == "__main__":
    main()