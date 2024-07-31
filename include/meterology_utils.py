from geopy.geocoders import Nominatim
from geopy.adapters import AdapterHTTPError
import requests
from include.global_variables import global_variables as gv


from geopy.geocoders import Nominatim
from geopy.adapters import AdapterHTTPError
import requests
import logging
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_lat_long_for_cityname(city: str):
    """Converts a string of a city name provided into
    lat/long coordinates."""

    geolocator = Nominatim(user_agent="MyApp")

    try:
        location = geolocator.geocode(city)
        lat = location.latitude
        long = location.longitude

        # log the coordinates retrieved
        logger.info(f"Coordinates for {city}: {lat}/{long}")

    # if the coordinates cannot be retrieved log a warning
    except (AttributeError, KeyError, ValueError, AdapterHTTPError) as err:
        logger.warning(
            f"""Coordinates for {city}: could not be retrieved.
            Error: {err}"""
        )
        lat = "NA"
        long = "NA"

    city_coordinates = {"city": city, "lat": lat, "long": long}

    return city_coordinates


def get_zipcode_for_lat_long(lat: float, long: float):
    """Converts lat/long coordinates into a zipcode."""

    geolocator = Nominatim(user_agent="MyApp")

    try:
        location = geolocator.reverse((lat, long), exactly_one=True)
        zipcode = location.raw['address']['postcode']

        # log the zipcode retrieved
        logger.info(f"Zipcode for {lat}/{long}: {zipcode}")

    # if the zipcode cannot be retrieved log a warning
    except (AttributeError, KeyError, ValueError, AdapterHTTPError) as err:
        logger.warning(
            f"""Zipcode for {lat}/{long}: could not be retrieved.
            Error: {err}"""
        )
        zipcode = "NA"

    return {"lat": lat, "long": long, "zipcode": zipcode}


def get_current_weather_from_city_coordinates(coordinates, timestamp):
    """Queries an open weather API for the current weather at the
    coordinates provided."""

    lat = coordinates["lat"]
    long = coordinates["long"]
    city = coordinates["city"]

    r = requests.get(
        f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={long}&current_weather=true"
    )

    # if the API call is successful log the current temp
    if r.status_code == 200:
        current_weather = r.json()["current_weather"]

        logger.info(
            "The current temperature in {0} is {1}°C".format(
                city, current_weather["temperature"]
            )
        )

    # if the API call is not successful, log a warning
    else:
        current_weather = {
            "temperature": "NULL",
            "windspeed": "NULL",
            "winddirection": "NULL",
            "weathercode": "NULL",
            "time": f"{timestamp}",
        }

        logger.warning(
            f"""
                Could not retrieve current temperature for {city} at
                {lat}/{long} from https://api.open-meteo.com.
                Request returned {r.status_code}.
            """
        )

    return {
        "city": city,
        "lat": lat,
        "long": long,
        "current_weather": current_weather,
        "API_response": r.status_code,
    }



logger = logging.getLogger(__name__)

def get_weather_for_timestamp(city: str, timestamp: datetime):
    """Queries the Open-Meteo API for weather data at a specific timestamp for the given city."""
    coordinates = get_lat_long_for_cityname(city)
    lat = coordinates["lat"]
    long = coordinates["long"]

    date = timestamp.strftime('%Y-%m-%d')
    hour = timestamp.hour

    r = requests.get(
        f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={long}&start_date={date}&end_date={date}&hourly=temperature_2m,rain&timezone=GMT"
    )

    response_json = r.json()
    logger.info(f"API Response: {response_json}")

    if r.status_code == 200 and "hourly" in response_json:
        hourly_data = response_json["hourly"]
        weather = {
            "temperature": hourly_data["temperature_2m"][hour],
            "rain": hourly_data["rain"][hour]
        }
        logger.info(f"Retrieved weather data for {city} at {timestamp}")
    else:
        weather = {
            "temperature": None,
            "rain": None
        }
        logger.warning(
            f"Could not retrieve weather for {city} at {timestamp}. "
            f"Request returned {r.status_code}."
        )

    return weather

def get_weather_for_timerange(city: str, start_timestamp: datetime, end_timestamp: datetime):
    """Queries the Open-Meteo API for hourly weather data for the given city and date range."""
    coordinates = get_lat_long_for_cityname(city)
    lat = coordinates["lat"]
    long = coordinates["long"]

    start_date = start_timestamp.strftime('%Y-%m-%d')
    end_date = end_timestamp.strftime('%Y-%m-%d')

    r = requests.get(
        f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={long}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,rain&timezone=GMT"
    )

    response_json = r.json()
    logger.info(f"API Response: {response_json}")

    if r.status_code == 200 and "hourly" in response_json:
        hourly_weather = response_json["hourly"]
        logger.info(f"Retrieved hourly weather data for {city} from {start_date} to {end_date}")
    else:
        hourly_weather = {
            "time": [],
            "temperature_2m": [],
            "rain": []
        }
        logger.warning(
            f"Could not retrieve hourly weather for {city} from {start_date} to {end_date}. "
            f"Request returned {r.status_code}."
        )

    return hourly_weather

# Assuming get_lat_long_for_cityname function is defined elsewhere

def process_bike_data_with_weather(parquet_file: str, city: str):
    # Read the bike data
    df = pd.read_parquet(parquet_file)
    
    # Convert timestamp to datetime if it's not already, remove timezone info, and set precision to nanoseconds
    df['record_timestamp'] = pd.to_datetime(df['record_timestamp'], utc=True).dt.tz_localize(None).astype('datetime64[ns]')
    
    # Sort the dataframe by timestamp
    df = df.sort_values('record_timestamp')
    
    # Get the start and end timestamps
    start_timestamp = df['record_timestamp'].min()
    end_timestamp = df['record_timestamp'].max()
    
    # Get hourly weather data for the entire date range
    weather_data = get_weather_for_timerange(city, start_timestamp, end_timestamp)
    
    # Convert weather data to a DataFrame
    weather_df = pd.DataFrame(weather_data)
    weather_df['time'] = pd.to_datetime(weather_df['time'], utc=True).dt.tz_localize(None).astype('datetime64[ns]')
    
    # Merge bike data with weather data
    result = pd.merge_asof(df, weather_df, left_on='record_timestamp', right_on='time', 
                           direction='nearest', tolerance=pd.Timedelta('30min'))
    
    # Drop the 'time' column as it's redundant with 'record_timestamp'
    result = result.drop(columns=['time'])
    
    return result

# Usage
city = "Paris"
parquet_file = 'numbikesavailable.parquet'
result_df = process_bike_data_with_weather(parquet_file, city)

# Display the first few rows and info of the result
print(result_df.head(60))
print(result_df.info())