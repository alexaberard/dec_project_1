import requests
import datetime
import pandas as pd
from dotenv import load_dotenv
import os

def convert_to_unix_timestamp(timestamp):
    return int(timestamp.timestamp())

def get_arrivals_by_airport(airport,begin_timestamp, end_timestamp, username, password):
    base_url = "https://opensky-network.org/api/flights/arrival"

    begin_unix = convert_to_unix_timestamp(begin_timestamp)
    print(begin_unix)
    end_unix = convert_to_unix_timestamp(end_timestamp)
    print(end_unix)

    params = {
        "airport": airport,
        "begin": begin_unix,
        "end": end_unix
    }

    auth = (username, password)

    response = requests.get(base_url,params=params, auth=auth)

    if response.status_code == 200:
        return response.json()
    else:
        print("Error:",response.text)
        return None
    

def main():
    load_dotenv()
    username = os.environ.get("username")
    password = os.environ.get("password")
    airport = "ESSA"
    begin_timestamp = datetime.datetime(2023,8,20,22,0)
    end_timestamp = datetime.datetime(2023,8,20,23,59)

    arrivals = get_arrivals_by_airport(airport,begin_timestamp,end_timestamp, username, password)

    if arrivals:
        print("Arrivals:")
        for arrival in arrivals:
            print(arrival)
    else:
        print("No data available")

if __name__ == "__main__":
    main()