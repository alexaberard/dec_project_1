from dec_project_1.connectors.open_sky_api_client import OpenSkyAPIClient
from dec_project_1.connectors.postgresql import PostgreSqlClient
import pandas as pd
from sqlalchemy import Table, MetaData, text, create_engine
from datetime import datetime


def extract_arrivals(opensky_client: OpenSkyAPIClient, airport:str, begin_timestamp: datetime, end_timestamp: datetime):
  flightdata_type = "arrival"
  current_datetime = pd.Timestamp.now()
  flight_data = opensky_client.get_arrivals_by_airport(airport,begin_timestamp,end_timestamp)
  if flight_data:
    df_flight_data = pd.json_normalize(flight_data)
    df_flight_data.insert(1,'flightDataType',flightdata_type)
    df_flight_data['extractDateTime'] = current_datetime

    return df_flight_data
  else:
    return None

def extract_departures(opensky_client: OpenSkyAPIClient, airport:str, begin_timestamp: datetime, end_timestamp: datetime):
  flightdata_type = "departure"
  current_datetime = pd.Timestamp.now()

  flight_data = opensky_client.get_departures_by_airport(airport,begin_timestamp,end_timestamp)
  if flight_data:
    df_flight_data = pd.json_normalize(flight_data)
    df_flight_data.insert(1,'flightDataType',flightdata_type)
    df_flight_data['extractDateTime'] = current_datetime

    return df_flight_data
  else:
    return None

def load(df: pd.DataFrame, postgresql_client: PostgreSqlClient, table, metadata):
  postgresql_client.write_to_table(data=df.to_dict(orient="records"), table=table, metadata=metadata)

def extract_max_date(postgresql_client: PostgreSqlClient, table, default_begin_timestamp):

  table_exists = postgresql_client.table_exists(table)
  if table_exists:
    max_date_query = f"""SELECT MAX("extractDateTime") as max_date FROM {table}"""

    #run_sql returns a list of dictionaries but this query will only have one row and column and can be extracted 
    #by adding [0]['max_date'] to the dict. 
    extract_date_time = postgresql_client.run_sql(max_date_query)[0]['max_date']
    formatted_date = datetime.strftime(extract_date_time,'%Y-%m-%d %H:%M:%S')
    
  else:
    formatted_date = default_begin_timestamp
  
  return formatted_date
