import os
import logging
import pymysql

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()


def establish_mysql_connection(database=None):
    try:
        # Diffenrent hosts for Docker and local enviroment
        host = os.getenv('MYSQL_HOST', 'localhost')

        connection = pymysql.connect(
            user='root',
            password='password',
            host=host,
            port=3306,
            database=database
        )
        return connection
    except pymysql.Error as e:
        print(f'\nError connecting to MySQL: {e}')
        return None


def create_location_table(location, latitude, longitude, connection):
    try:
        formated_table_name = location.replace(' ', '_')
        with connection.cursor() as cursor:
            check_query = """SELECT * FROM _location_id_table WHERE location = %s AND latitude = %s AND longitude = %s"""
            cursor.execute(check_query, (formated_table_name, latitude, longitude))
            result = cursor.fetchone()

            if result is None:
                insert_query = """INSERT INTO _location_id_table (location, latitude, longitude) VALUES (
                                %s, %s, %s)"""
                cursor.execute(
                    insert_query, (formated_table_name, latitude, longitude))

                create_table_query = f"""CREATE TABLE IF NOT EXISTS {formated_table_name} (
                                    entry_id INT AUTO_INCREMENT PRIMARY KEY,
                                    location_id INT,
                                    fetched VARCHAR(100),
                                    date DATE,
                                    hour VARCHAR(10),
                                    temperature DECIMAL(3, 1),
                                    humidity INT,
                                    weather_condition VARCHAR(100),
                                    precipitation_mm DECIMAL(3, 1),
                                    FOREIGN KEY(location_id) REFERENCES _location_id_table(location_id),
                                    UNIQUE (date, hour, temperature, humidity, weather_condition, precipitation_mm))
                                    """
                cursor.execute(create_table_query)
                connection.commit()
                return 'New table created'
            else:
                return 'Table already exists'
    except pymysql.Error as e:
        print(f'\nError in create_location_table: {e}')


def insert_transformed_data(location, transformed_data, connection):
    changes_made = False 
    message = 'No new data to insert; data is up-to-date'
    try:
        formated_table_name = location.replace(' ', '_')
        with connection.cursor() as cursor:
            get_location_id_query = """SELECT location_id
                                    FROM _location_id_table
                                    WHERE location = %s"""
            cursor.execute(get_location_id_query, (formated_table_name,))
            location_id = cursor.fetchone()[0]
        
            for observation in transformed_data:
                _, _, fetched, date, hour, temperature, humidity, weather_condition, precipitation_mm = observation

                check_query = f"""SELECT date, hour, temperature, humidity, weather_condition, precipitation_mm FROM {formated_table_name}
                                WHERE date = %s AND hour = %s """
                cursor.execute(check_query, (date, hour))
                
                
                result = cursor.fetchone()
                if result is None:
                    insert_query = f"""INSERT INTO {formated_table_name}
                                        (location_id, fetched, date, hour, temperature, humidity, weather_condition, precipitation_mm)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                    try:
                        cursor.execute(insert_query, (location_id, fetched, date, hour, temperature, humidity, weather_condition, precipitation_mm))
                        changes_made = True
                        message = 'New data inserted'
                    except pymysql.err.IntegrityError:
                        continue
                else:
                    (date_from_result, hour_from_result, temperature_from_result, humidity_from_result, weather_condition_from_result, precipitation_mm_from_result) = (result[0], result[1], float(result[2]), int(result[3]), result[4], float(result[5]))
                    if [date, hour, temperature, humidity, weather_condition, precipitation_mm] != [date_from_result, hour_from_result, temperature_from_result, humidity_from_result, weather_condition_from_result, precipitation_mm_from_result]:
                        update_query = f"""UPDATE {formated_table_name}
                                        SET fetched = %s, temperature = %s, humidity = %s, weather_condition = %s, precipitation_mm = %s
                                        WHERE date = %s AND hour = %s"""
                        changes_made = True
                        message = 'Existing data updated'

                        cursor.execute(update_query, (fetched, temperature, humidity, weather_condition, precipitation_mm, date, hour))
                        logger.info(f"""\nOutdated data from {formated_table_name}: {[date_from_result, hour_from_result, temperature_from_result, humidity_from_result, weather_condition_from_result, precipitation_mm_from_result]}\nNew data from requests for {formated_table_name}: {observation[3:]}""")

            if changes_made:
                connection.commit()
    except pymysql.Error as e:
        print(f'\nError in insert_transformed_data: {e}')
        message = f'Database error occurred: {e}'
    except Exception as e:
        print(f'\nUnexpected error: {e}')
        message = f'An unexpected error occurred: {e}'
    return message


def get_coordinates_from_db():
    try:
        with establish_mysql_connection('weather_db') as connection:
            with connection.cursor() as cursor:
                search_query = """SELECT location, latitude, longitude
                                FROM _location_id_table"""
                cursor.execute(search_query)
                result = [list(row) for row in cursor.fetchall()]
                return result
    except pymysql.Error as e:
        print(f'\nError in get_coordinates_from_db: {e}')


def remove_location_from_db(location_list):
    try:
        with establish_mysql_connection('weather_db') as connection:
            with connection.cursor() as cursor:
                for location in location_list:
                    delete_location_table = f"""DROP TABLE IF EXISTS {location}"""
                    cursor.execute(delete_location_table)
                    delete_location_id_query = """DELETE FROM _location_id_table
                                                WHERE location = %s"""
                    cursor.execute(delete_location_id_query, (location,))

                    print(f'{location} has been removed from the database')

                connection.commit()
    except pymysql.Error as e:
        print(f'\nError in get_coordinates_from_db: {e}')
