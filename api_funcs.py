import requests
import datetime

lon = 18.0215
lat = 59.3099

precipitation_categories = {0: 'No precipitation',
                            1: 'Snow',
                            2: 'Snow and rain',
                            3: 'Rain',
                            4: 'Drizzle',
                            5: 'Freezing rain',
                            6: 'Freezing drizzle'}

def request_smhi_api():
    """
    Fetches data from the SMHI API and extracts temperature and precipitation values for the next 24 hours for a given longitude and latitude.

    Returns:
        A list of weather data in the form of lists, each sublist having the following order:
            0. 'fetched': Timestamp when the data was fetched ('YYYY-MM-DD hh:mm').
            1. 'longitude': Longitude (in decimal degrees).
            2. 'latitude': Latitude (in decimal degrees).
            3. 'date': Date ('YYYY-MM-DD').
            4. 'hour': Hour (0-23).
            5. 'temperature': Temperature (in °C).
            6. 'precipitation',
            7. 'provider': Data provider ('SMHI').

    Raises:
        Exception: If an incorrect status code is returned from the API call.
    """

    try:
        response = requests.get(f'https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/{lon}/lat/{lat}/data.json')

        if response.status_code != 200:
            raise Exception(f'Error fetching data: HTTP status code {response.status_code}')

        data = []
        rounded_start_time = datetime.datetime.now() + datetime.timedelta(minutes = 60 - datetime.datetime.now().minute)
        format_string = "%Y-%m-%d %H:%M"
        formatted_rounded_start_time = rounded_start_time.strftime(format_string)

        for _ in range(24):
            for observation in response.json()['timeSeries']:
                if formatted_rounded_start_time == ' '.join(observation['validTime'].split('T'))[:-4]:
                    fetched = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
                    date = formatted_rounded_start_time.split(' ')[0]
                    hour = f'{int(formatted_rounded_start_time.split(" ")[1].split(":")[0]):02d}:00'
                    temperature = [param['values'][0] for param in observation['parameters'] if param['name'] == 't'][0]
                    precipitation_category = [param['values'][0] for param in observation['parameters'] if param['name'] == 'pcat'][0]
                    precipitation = precipitation_categories[precipitation_category]
                    provider = 'SMHI'
                    data.append([fetched, lon, lat, date, hour, temperature, precipitation, provider])

                    rounded_start_time += datetime.timedelta(hours=1)
                    formatted_rounded_start_time = rounded_start_time.strftime(format_string)
                    break
        return data
    except requests.RequestException as e:
        raise Exception(f'Error during API request: {e}')

    except Exception as e:
        raise Exception(f'An error occurred: {e}')

        
print(request_smhi_api())




