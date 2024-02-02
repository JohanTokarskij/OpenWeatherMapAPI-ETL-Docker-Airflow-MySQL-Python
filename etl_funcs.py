import datetime
import os
import requests
from dotenv import load_dotenv
import pymysql
from pymysql_funcs import establish_mysql_connection, create_location_table, insert_transformed_data, get_coordinates_from_db


def extract_owm_data(latitude, longitude):
    """
    Fetches weather data from the OpenWeatherMap (OWM) API for a specified latitude and longitude.

    Args:
        latitude (float): The latitude of the location.
        longitude (float): The longitude of the location.

    Returns:
        dict or None: A dictionary containing the raw weather data from OWM if the request is successful,
                      None otherwise.

    
    Note:
        The function prints specific error messages for HTTP errors or other exceptions,
        returning None for any failure.
    """

    try:
        load_dotenv()
        API_KEY = os.getenv('API_KEY')

        response = requests.get(f'https://api.openweathermap.org/data/3.0/onecall?lat={latitude}&lon={longitude}&appid={API_KEY}&units=metric&exclude=current,minutely,daily,alerts')

        if response.status_code == 200:
            return response.json()
        else:
            print(f'Failed to fetch data: HTTP status code {response.status_code}')
            if response.status_code == 401:
                print('Check if the API key is correct.')
            elif response.status_code == 404:
                print('Requested resource not found.')
            
            return None
    
    except Exception as e:
        print(f'Unexpected error occurred: {e}')
        return None


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

    Note:
        Errors are managed internally, printing messages for issues (e.g., missing keys, invalid types) without halting execution.
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
                    date_str, hour = formatted_rounded_start_time.split(' ')
                    date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                    temperature = round(float(observation['temp']), 1)
                    humidity = int(observation['humidity'])
                    weather_condition = observation['weather'][0]['main'].lower()
                    precipitation_mm = round(float(0), 1)

                    if weather_condition in observation and isinstance(observation[weather_condition], dict):
                        precipitation_mm = round(observation[weather_condition].get('1h', 0), 1)

                    transformed_data.append([latitude, longitude, fetched, date, hour, temperature, humidity, weather_condition, precipitation_mm])

                    rounded_start_time += datetime.timedelta(hours=1)
                    formatted_rounded_start_time = rounded_start_time.strftime(
                        format_string)
                    break
        return transformed_data
    
    except KeyError as ke:
        print(f'Missing or invalid key in input data: {ke}')
    except TypeError as te:
        print(f'Invalid data type in input: {te}')
    except Exception as e:
        print(f'An error occurred during data transformation: {e}')


def load_data_to_sql(location, transformed_data, latitude, longitude, connection):
    try:
        if connection:
            create_location_table(location, latitude, longitude, connection)
            insert_transformed_data(location, transformed_data, connection)  
    except pymysql.Error as e:
        print(f'Database error occurred: {e}')
    except Exception as e:
        print(f'An unexpected error occurred: {e}')


def etl(location, latitude, longitude, connection=None, manage_connection=True):
    """
    Performs ETL process for a specific location.

    Args:
        location (str): Location name.
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        connection: Database connection object. If None, the function will create and manage its own connection.
        manage_connection (bool): Indicates whether this function should manage the database connection (open/close) or leave it to the caller.

    Returns:
        bool: True if ETL process completes successfully, False otherwise.
    """
    if manage_connection:
        connection = establish_mysql_connection('weather_db')
    
    try:
        data = extract_owm_data(latitude, longitude)
        if data:
            transformed_data = transform_owm_data(data, latitude, longitude)
            load_data_to_sql(location, transformed_data, latitude, longitude, connection)
            return True
        else:
            return False
    except Exception as e:
        print(f'ETL process error for location {location}: {e}')
        return False
    finally:
        if manage_connection and connection:
            connection.close()


def airflow_etl():
    connection = establish_mysql_connection('weather_db')
    if connection:
        try:
            for observation in get_coordinates_from_db():
                db_location, db_latitude, db_longitude = observation
                etl(db_location, db_latitude, db_longitude, connection, manage_connection=False)
        except Exception as e:
            print(f'Error during ETL process: {e}')
        finally:
            connection.close()
    else:
        print('Failed to establish database connection.')




