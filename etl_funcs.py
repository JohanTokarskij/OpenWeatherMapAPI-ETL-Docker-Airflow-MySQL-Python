import datetime
import os
import requests
from dotenv import load_dotenv
from helper_funcs import get_coordinates
from pymysql_funcs import create_location_table


lat = 59.3099
lon = 18.0215


def extract_owm_data(latitude, longitude):
    """
    Fetches weather data from the OpenWeatherMap (OWM) API for a specified latitude and longitude.

    Args:
        latitude (float): The latitude of the location.
        longitude (float): The longitude of the location.

    Returns:
        dict: A dictionary containing the raw weather data from OWM.

    Raises:
        ConnectionError: If there's a network problem or the API is unreachable.
        HTTPError: If the API returns a non-200 HTTP status code.
    """
    try:
        load_dotenv()
        API_KEY = os.getenv('API_KEY')

        response = requests.get(f'https://api.openweathermap.org/data/3.0/onecall?lat={latitude}&lon={longitude}&appid={API_KEY}&units=metric&exclude=current,minutely,daily,alerts')

        if response.status_code != 200:
            raise Exception(f'Error fetching data: HTTP status code {response.status_code}')
        return response.json()
    
    except requests.RequestException as e:
        raise Exception(f'Error during API request: {e}')

def transform_owm_data(data, latitude, longitude):
    """
    Transforms raw weather data from OpenWeatherMap API into a structured format.

    Args:
        data (dict): The raw weather data from OWM API.
        latitude (float): The latitude of the location.
        longitude (float): The longitude of the location.

    Returns:
        list: A list of weather data, where each sublist contains:
              - 'fetched': Timestamp of data retrieval ('YYYY-MM-DD hh:mm').
              - 'latitude': Latitude of the location.
              - 'longitude': Longitude of the location.
              - 'date': Date of the weather data ('YYYY-MM-DD').
              - 'hour': Hour of the weather data (00:00-23:00).
              - 'temperature': Temperature (in °C).
              - 'humidity': Humidity percentage.
              - 'condition': Weather condition/precipitation.
              - 'precipitation_mm': Precipitation amount (in mm).

    Raises:
        ValueError: If the input data is not in the expected format or missing required information.
    """
    try:
        transformed_data = []
    
        rounded_start_time = datetime.datetime.now() + datetime.timedelta(minutes=60 - datetime.datetime.now().minute)
        format_string = "%Y-%m-%d %H:%M"
        formatted_rounded_start_time = rounded_start_time.strftime(format_string)

        for _ in range(24):
            for observation in data['hourly']:
                dt_object = datetime.datetime.fromtimestamp(observation['dt'])
                formatted_dt_object = dt_object.strftime('%Y-%m-%d %H:%M')

                if formatted_rounded_start_time == formatted_dt_object:
                    fetched = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
                    date, hour = formatted_rounded_start_time.split(' ')
                    temperature = float(observation['temp'])
                    humidity = observation['humidity']
                    weather_condition = observation['weather'][0]['main'].lower()
                    precipitation_mm = 0 

                    if weather_condition in observation and isinstance(observation[weather_condition], dict):
                        precipitation_mm = observation[weather_condition].get('1h', 0)

                    transformed_data.append([latitude, longitude, fetched, date, hour, temperature, humidity, weather_condition, precipitation_mm])

                    rounded_start_time += datetime.timedelta(hours=1)
                    formatted_rounded_start_time = rounded_start_time.strftime(
                        format_string)
                    break
        return transformed_data
    
    except KeyError as ke:
        raise ValueError(f"Missing or invalid key in input data: {ke}")
    except TypeError as te:
        raise ValueError(f"Invalid data type in input: {te}")
    except Exception as e:
        raise Exception(f"An error occurred during data transformation: {e}")

def load_data_to_sql(location, transformed_data):
    print(location)
    print(transformed_data)
    


def etl(location, latitude, longitude):
    data = extract_owm_data(latitude, longitude)
    if data:
        transformed_data = transform_owm_data(data, latitude, longitude)
        load_data_to_sql(location, transformed_data)
        create_location_table(location, latitude, longitude )


dynamic_location, dynamic_latitude, dymanic_logitude  = get_coordinates()
etl(dynamic_location, dynamic_latitude, dymanic_logitude)