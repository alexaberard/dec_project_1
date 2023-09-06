import os
import pytest
import pandas as pd
from dotenv import load_dotenv
from etl.connectors.open_sky_api_client import OpenSkyAPIClient
from etl.assets.opensky import extract_by_direction
from datetime import datetime, timezone, timedelta

@pytest.fixture
def setup_extract():
    load_dotenv()
    
def test_opensky_extract_by_direction(setup_extract):
    username = os.environ.get("USERNAME_API_OPENSKY")
    password = os.environ.get("PASSWORD_API_OPENSKY")
    
    open_sky_api_client = OpenSkyAPIClient(username, password)
    
    direction = "arrival"
    airport = "EPWA"
    begin_timestamp = datetime(year=2023, month=9, day=2, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    end_timestamp = datetime(year=2023, month=9, day=5, hour=0, minute=0, second=0, tzinfo=timezone.utc)            
    
    data = extract_by_direction(
        opensky_client=open_sky_api_client,
        direction=direction,
        airport=airport,
        begin_timestamp=begin_timestamp,
        end_timestamp=end_timestamp
    )
    
    assert data.shape[0] == 116
    assert type(data) == pd.DataFrame