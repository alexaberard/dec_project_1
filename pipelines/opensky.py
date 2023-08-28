import os
from dec_project_1.connectors.open_sky_api_client import OpenSkyAPIClient
import datetime
from dotenv import load_dotenv


def main():
    load_dotenv()
    username = os.environ.get("username")
    password = os.environ.get("password")
    airport = "ESSA" # ICAO: Arlanda Stockholm -- IATA names are not used.
    begin_timestamp = datetime.datetime(2023,8,20,22,0)
    end_timestamp = datetime.datetime(2023,8,20,23,59)

    api = OpenSkyAPIClient(username, password)

    arrivals = api.get_arrivals_by_airport(airport,begin_timestamp,end_timestamp)

    if arrivals:
        print("Arrivals:")
        for arrival in arrivals:
            print(arrival)
    else:
        print("No data available")

if __name__ == "__main__":
    main()