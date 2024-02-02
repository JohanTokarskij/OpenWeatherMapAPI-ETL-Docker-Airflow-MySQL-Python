import questionary
from etl_funcs import etl
from helper_funcs import clear_screen, get_coordinates, wait_for_keypress
from pymysql_funcs import get_coordinates_from_db, remove_location_from_db


def main_manu():
     while True:
        clear_screen(0)
        print('\n' + '*' * 40)
        print('MAIN MENU'.center(40))
        print('*' * 40 + '\n')

        choice = questionary.select(
            'Select an option:',
            choices=[
                'Add New Location & Schedule Airflow',
                'View Registered Locations in the Database',
                'Remove Location(s) from the Database',
                'Exit',
            ], qmark='').ask()
        
        if choice == 'Add New Location & Schedule Airflow':
            coordinates = get_coordinates()
            if coordinates:
                location, latitude, longitude = coordinates
                etl_result = etl(location, latitude, longitude)
                if not etl_result:
                    print('Something went wrong, new data was not added.')

                wait_for_keypress()
        
        elif choice == 'View Registered Locations in the Database':
            try:
                locations_from_db = get_coordinates_from_db()
                if not locations_from_db:
                    print('No locations found in the database.')
                    wait_for_keypress()
                    continue

                locations_from_db_formated = [f'- {loc[0]} (Latitude: {loc[1]}, Longitude: {loc[2]})' for loc in locations_from_db]

                for location in locations_from_db_formated:
                    print(location)

            except Exception as e:
                print(f"Failed to fetch locations. Error: {e}")
            wait_for_keypress()

        elif choice == 'Remove Location(s) from the Database':
            try:
                locations_from_db = get_coordinates_from_db()
                if not locations_from_db:
                    print('No locations found in the database.')
                    wait_for_keypress()
                    continue

                choices = [f'{loc[0]} (Latitude: {loc[1]}, Longitude: {loc[2]})' for loc in locations_from_db]

                selected_locations = questionary.checkbox(
                'Select location(s) to remove:',
                choices=choices).ask()

                
                if selected_locations:
                    selected_locations_to_remove_list = [selected_location.split(' ')[0] for selected_location in selected_locations]
                    remove_location_from_db(selected_locations_to_remove_list)
            
            except Exception as e:
                print(f"Error during location removal: {e}")
            wait_for_keypress()
        
        elif choice == "Exit":
            print('\nExiting the application.')
            clear_screen()
            break

        elif choice is None:
            clear_screen()
            break