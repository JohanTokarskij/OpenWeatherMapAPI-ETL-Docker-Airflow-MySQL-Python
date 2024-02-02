import questionary
from etl_funcs import etl
from helper_funcs import clear_screen, get_coordinates, wait_for_keypress


def main_manu():
     while True:
        clear_screen(0)
        print('\n' + '*' * 40)
        print('MAIN MENU'.center(40))
        print('*' * 40 + '\n')

        choice = questionary.select(
            'Select an option:',
            choices=[
                '1. Add a new location to database and Airflow scheduling',
                '2. Exit'
            ]).ask()
        
        if choice == '1. Add a new location to database and Airflow scheduling':
            coordinates = get_coordinates()
            if coordinates:
                location, latitude, longitude = coordinates
                etl_result = etl(location, latitude, longitude)
                if etl_result:
                    print(f'Data for {location} has been added to database and Airflow Schedule!')
                else:
                    print('Something went wrong, new data was not added.')

                wait_for_keypress()
        
        elif choice == "2. Exit":
            print('\nExiting the application.')
            clear_screen()
            break

        elif choice is None:
            clear_screen()
            break