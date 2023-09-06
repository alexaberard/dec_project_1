import os
import pytest
from dotenv import load_dotenv
from etl.connectors.open_sky_api_client import OpenSkyAPIClient
from datetime import datetime, timezone, timedelta

@pytest.fixture
def setup_extract():
    load_dotenv()
    
def test_open_sky_api_client_get_data_by_airport(setup_extract):
    username = os.environ.get("USERNAME_API_OPENSKY")
    password = os.environ.get("PASSWORD_API_OPENSKY")
    
    open_sky_api_client = OpenSkyAPIClient(username, password)
    
    direction = "arrival"
    airport = "EPWA"
    begin_timestamp = datetime(year=2023, month=9, day=2, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    end_timestamp = datetime(year=2023, month=9, day=5, hour=0, minute=0, second=0, tzinfo=timezone.utc)            
    
    data = open_sky_api_client.get_direction_by_airport(
        direction=direction,
        airport=airport,
        begin_timestamp=begin_timestamp,
        end_timestamp=end_timestamp            
    )
    
    assert len(data) == 116
    assert type(data) == list
    