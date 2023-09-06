from etl.connectors.open_sky_api_client import OpenSkyAPIClient
from etl.connectors.postgresql import PostgreSqlClient
import pandas as pd
from sqlalchemy import Table, MetaData, text, create_engine
from datetime import datetime, timedelta
import json
import pytz

def is_json_compatible() -> bool:
    try:
        json.dumps(data, cls=json.JSONEncoder)
        return True
    except (TypeError, OverflowError):
        return False

def extract_by_direction(opensky_client: OpenSkyAPIClient, direction: str, airport:str, begin_timestamp: datetime, end_timestamp: datetime) -> pd.DataFrame:
  """Extract data from opensky api for selected range of dates. API is called in smaller chunks of 7 days intervals

  Args:
      opensky_client (OpenSkyAPIClient): API class
      direction (str): arrivals/departures
      airport (str): name of the airport (4 letter abbreviation code)
      begin_timestamp (datetime): start date
      end_timestamp (datetime): end date

  Returns:
      pd.DataFrame: accumulated dataframe
  """
  print(f"Extracting flightdata for direction: {direction}")
  # create variables with data that is going to be inserted into the df
  flightdata_type = direction
  current_datetime = pd.Timestamp.now(pytz.utc).replace(microsecond = 0)

  current_date = begin_timestamp

  # Initialize an empty DataFrame to accumulate data
  accumulated_data = pd.DataFrame()

  #extract data from opensky. Looping since we only get data for 7 day period at a time.
  while current_date <= end_timestamp:
    # Calculate the end date for the 7-day interval
    interval_end_date = current_date + timedelta(days=6) # 6 days added to cover 7 days

    # Ensure the interval end date does not exceed the end_date
    interval_end_date = min(interval_end_date, end_timestamp)
    
    # Extract data for the current interval (current_date to interval_end_date)
    
    #flight_data = opensky_client.get_direction_by_airport(direction,airport,current_date,interval_end_date)

    try:
       # Extract data for the current interval (current_date to interval_end_date)
       flight_data = opensky_client.get_direction_by_airport(direction,airport,current_date,interval_end_date)
       
       # Create dataframe - content is the received json
       df_flight_data = pd.json_normalize(flight_data)
       
       #add column to dataframe with direction info.
       df_flight_data.insert(1,'flightDataType',flightdata_type)
       
      #transform unix time to normal timestamp
       df_flight_data['firstSeen'] = pd.to_datetime(df_flight_data['firstSeen'], unit='s', errors='coerce')  # Assuming seconds
       df_flight_data['lastSeen'] = pd.to_datetime(df_flight_data['lastSeen'], unit='s', errors='coerce')  # Assuming seconds

      #replace NaT-values with None. NaT values are created by the function pd.to_datetime 'errors='coerce''
      #to handle values that are not correct
       df_flight_data = df_flight_data.replace({pd.NaT: None})
      
      #hash all columns and store the hash in a new column called hashed - this is they key in the database table
       df_flight_data['hashed'] = pd.util.hash_pandas_object(df_flight_data, index=False).astype(str)

      # adding extracted time after the hash to not be included in the hash key
       df_flight_data['extractDateTime'] = current_datetime

      # add received data to accumulated dataframe.
       accumulated_data = pd.concat([accumulated_data,df_flight_data])
    except:
       print("Could not extract data")
       return None
  
    # Move the current_date to the next interval
    current_date = interval_end_date + timedelta(days=1)  # Move 1 day ahead for the next interval
  #end while loop  
  return accumulated_data

def load(df: pd.DataFrame, postgresql_client: PostgreSqlClient, table, metadata):
  """Load data into database

  Args:
      df (pd.DataFrame): dataset
      postgresql_client (PostgreSqlClient): postgres class
      table (_type_): 
      metadata (_type_): 
  """
  #postgresql_client.write_to_table(data=df.to_dict(orient="records"), table=table, metadata=metadata)
  postgresql_client.upsert_in_chunks(data=df.to_dict(orient="records"), table=table, metadata=metadata)

def extract_max_date(postgresql_client: PostgreSqlClient, table, default_begin_timestamp):
  table_exists = postgresql_client.table_exists(table)
  if table_exists:

    max_date_query = f"""SELECT MAX("extractDateTime") as max_date FROM {table}"""
    #run_sql returns a list of dictionaries but this query will only have one row and column and can be extracted 
    #by adding [0]['max_date'] to the dict. 
    extract_date_time = postgresql_client.run_sql(max_date_query)[0]['max_date']

    if extract_date_time is not None: #If field exists but is null then handle this too.
      formatted_date = datetime.strftime(extract_date_time,'%Y-%m-%d %H:%M:%S')
      formatted_date = pd.to_datetime(formatted_date, utc=True)
    else:
      formatted_date = default_begin_timestamp

    return formatted_date

  else:
    return default_begin_timestamp

