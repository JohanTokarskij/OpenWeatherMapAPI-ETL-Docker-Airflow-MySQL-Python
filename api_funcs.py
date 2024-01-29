import datetime
import os
import requests
from dotenv import load_dotenv
from geopy.geocoders import Nominatim

lon = 18.0215
lat = 59.3099

def get_coordinates():
    geolocator = Nominatim(user_agent="GeocodingApp")
    location = geolocator.geocode(input("City or Street Name: "))

    latitude = None
    longitude = None

    if location:
        latitude = round(location.latitude, 6)
        longitude = round(location.longitude, 6)
        print(f'Coordinates for {location} are: \n Latitude: {latitude}, Longitude: {longitude}\n')
        print(location)
        return latitude, longitude, location[0]
    else:
        print('Geocoding failed. Check your input or try a different location.')


def request_owm_data(latitude, longitude, location):
    """
    Fetches data from the SMHI API and extracts temperature and precipitation values for the next 24 hours for a given longitude and latitude.

    Returns:
        A list of weather data in the form of lists, where each sublist contains the following elements in order:
            0. 'fetched': Timestamp when the data was fetched ('YYYY-MM-DD hh:mm').
            1. 'location': General location or place name.
            2. 'longitude': Longitude of the location (in decimal degrees).
            3. 'latitude': Latitude of the location (in decimal degrees).
            4. 'date': Date when the weather data is for ('YYYY-MM-DD').
            5. 'hour': Hour of the day for the weather data (00:00-23:00).
            6. 'temperature': Temperature at the specified date and time (in °C).
            7. 'humidity': Humidity percentage at the specified date and time.
            8. 'condition': Weather condition/precipitation.
            9. 'precipitation_mm': Precipitation amount (in millimeters or equivalent units).

    Raises:
        Exception: If an incorrect status code is returned from the API call."""
    try:
        load_dotenv()
        API_KEY = os.getenv('API_KEY')

        response = requests.get(f'https://api.openweathermap.org/data/3.0/onecall?lat={latitude}&lon={longitude}&appid={API_KEY}&units=metric&exclude=current,minutely,daily,alerts')

        data = []

        if response.status_code != 200:
            raise Exception(f'Error fetching data: HTTP status code {response.status_code}')

        rounded_start_time = datetime.datetime.now() + datetime.timedelta(minutes=60 - datetime.datetime.now().minute)
        format_string = "%Y-%m-%d %H:%M"
        formatted_rounded_start_time = rounded_start_time.strftime(format_string)

        for _ in range(24):
            for observation in response.json()['hourly']:
                dt_object = datetime.datetime.fromtimestamp(observation['dt'])
                formatted_dt_object = dt_object.strftime('%Y-%m-%d %H:%M')

                if formatted_rounded_start_time == formatted_dt_object:
                    fetched = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
                    date, hour = formatted_rounded_start_time.split(' ')
                    temperature = float(observation['temp'])
                    humidity = observation['humidity']
                    condition = observation['weather'][0]['main'].lower()
                    precipitation_mm = 0 

                    if condition in observation and isinstance(observation[condition], dict):
                        precipitation_mm = observation[condition].get('1h', 0)

                    data.append([fetched, location, latitude, longitude, date, hour, temperature, humidity, condition, precipitation_mm])

                    rounded_start_time += datetime.timedelta(hours=1)
                    formatted_rounded_start_time = rounded_start_time.strftime(
                        format_string)
                    break
        return data
    except requests.RequestException as e:
        raise Exception(f'Error during API request: {e}')

    except Exception as e:
        raise Exception(f'An error occurred: {e}')


#latitude, longitude, location = get_coordinates()
request_owm_data(lat, lon, 'test')