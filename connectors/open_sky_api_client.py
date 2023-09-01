import requests
import datetime
import sys

class OpenSkyAPIClient:
    def __init__(self, username, password):
        self.base_url = "https://opensky-network.org/api/"
        self.username = username
        self.password = password
    
    def convert_to_unix_timestamp(self, timestamp):
        return int(timestamp.timestamp())

    def get_arrivals_by_airport(self, airport,begin_timestamp, end_timestamp):
        endpoint_url = self.base_url + "flights/arrival"

        begin_unix = self.convert_to_unix_timestamp(begin_timestamp)
        end_unix = self.convert_to_unix_timestamp(end_timestamp)

        if begin_unix >= end_unix:
            print("The end parameter must be greater than begin.")
            return None
            
        if end_unix - begin_unix > 604800:
            print("The time interval must be smaller than 7 days.")
            return None

        params = {
            "airport": airport,
            "begin": begin_unix,
            "end": end_unix
        }

        auth = (self.username, self.password)

        response = requests.get(endpoint_url,params=params, auth=auth)

        if response.status_code == 200 and response.text:
            return response.json()
        elif response.status_code == 404:
            print("Response is empty")
            return None
        else:
            print("Error:",response.status_code)
            return None

    def get_departures_by_airport(self, airport,begin_timestamp, end_timestamp):
        endpoint_url = self.base_url + "flights/departure"

        begin_unix = self.convert_to_unix_timestamp(begin_timestamp)
        end_unix = self.convert_to_unix_timestamp(end_timestamp)

        params = {
            "airport": airport,
            "begin": begin_unix,
            "end": end_unix
        }

        auth = (self.username, self.password)

        response = requests.get(endpoint_url,params=params, auth=auth)

        if response.status_code == 200 and response.text:
            return response.json()
        elif response.status_code == 404:
            print("Response is empty")
            return None
        else:
            print("Error:",response.status_code)
            return None
