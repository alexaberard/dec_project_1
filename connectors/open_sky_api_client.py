import requests
import datetime

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

        params = {
            "airport": airport,
            "begin": begin_unix,
            "end": end_unix
        }

        auth = (self.username, self.password)

        response = requests.get(endpoint_url,params=params, auth=auth)

        if response.status_code == 200:
            return response.json()
        else:
            print("Error:",response.text)
            return None
