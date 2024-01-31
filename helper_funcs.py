from time import sleep
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError


def clear_screen(sleep_time=1):
    sleep(sleep_time)
    os.system('cls' if os.name == 'nt' else 'clear')


def wait_for_keypress(sleep_time=0):
    while True:
        input('\nPress "Enter" to continue...')
        sleep(sleep_time)
        break


def get_coordinates():
    geolocator = Nominatim(user_agent='GeocodingApp')
    while True:
        user_input = input(
            'Enter city name(type "exit" to exit): ')
        if user_input.lower() == 'exit':
            print('\nAction cancelled.')
            clear_screen()
            return None

        try:
            location = geolocator.geocode(user_input)

            latitude = None
            longitude = None

            if location:
                latitude, longitude = location.latitude, location.longitude
                return latitude, longitude, location[0].split(',')[0]
            else:
                print('Geocoding failed. Check your input or try a different location.')
        except GeocoderTimedOut:
            print("Geocoder service timed out. Please try again.")
        except GeocoderServiceError:
            print("Geocoder service error. Please try again later.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")