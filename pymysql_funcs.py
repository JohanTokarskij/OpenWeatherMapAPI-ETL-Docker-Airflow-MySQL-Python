import pymysql
from time import sleep
from helper_funcs import clear_screen, wait_for_keypress


def establish_mysql_connection(database=None):
    try:
        connection = pymysql.connect(
            user='root',
            password='password',
            host='localhost',
            port=3306,
            database=database
        )
        return connection
    except pymysql.Error as e:
        print(f'\nError connecting to MySQL: {e}')
        return None


def create_location_table(location, latitude, longitude, connection):
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
                            FOREIGN KEY(location_id) REFERENCES _location_id_table(location_id))"""
        cursor.execute(create_table_query)
        connection.commit()


def insert_transformed_data(location, transformed_data, connection):
    formated_table_name = location.replace(' ', '_')
    print(formated_table_name)
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
               cursor.execute(insert_query, (location_id, fetched, date, hour, temperature, humidity, weather_condition, precipitation_mm))
            else:
                (date_from_result, hour_from_result, temperature_from_result, humidity_from_result, weather_condition_from_result, precipitation_mm_from_result) = (result[0], result[1], float(result[2]), int(result[3]), result[4], float(result[5]))
                if [date, hour, temperature, humidity, weather_condition, precipitation_mm] != [date_from_result, hour_from_result, temperature_from_result, humidity_from_result, weather_condition_from_result, precipitation_mm_from_result]:
                    update_query = f"""UPDATE {formated_table_name}
                                    SET fetched = %s, temperature = %s, humidity = %s, weather_condition = %s, precipitation_mm = %s
                                    WHERE date = %s AND hour = %s"""

                    cursor.execute(update_query, (fetched, temperature, humidity, weather_condition, precipitation_mm, date, hour))
                    print('\nOutdated row from database:')
                    print([date_from_result, hour_from_result, temperature_from_result, humidity_from_result, weather_condition_from_result, precipitation_mm_from_result])
                    print(f'\nNew data from requests: \n{observation[3:]}')
                    print([date, hour, temperature, humidity, weather_condition, precipitation_mm] == [date_from_result, hour_from_result, temperature_from_result, humidity_from_result, weather_condition_from_result, precipitation_mm_from_result])

        connection.commit()