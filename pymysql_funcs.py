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
            database=database,
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except pymysql.Error as e:
        print(f'\nError connecting to MySQL: {e}')
        return None

def create_location_table(location, latitude, longitude):
    formated_table_name = location.replace(" ", "_")
    with establish_mysql_connection('weather_db') as connect:
        with connect.cursor() as cursor:
            insert_query = """INSERT INTO _location_id_table (location, latitude, longitude) VALUES(
                           %s, %s, %s)"""
            cursor.execute(insert_query, (formated_table_name, latitude, longitude))
            create_table_query = f"""CREATE TABLE IF NOT EXISTS {formated_table_name} (
                                entry_id INT AUTO_INCREMENT PRIMARY KEY,
                                location_id INT,
                                fetched VARCHAR(100),
                                date DATE,
                                hour VARCHAR(10),
                                temperature INT(3),
                                humidity INT(3),
                                weather_condition VARCHAR(100),
                                precipitation_mm FLOAT,
                                FOREIGN KEY(location_id) REFERENCES _location_id_table(location_id)
                            )"""
            cursor.execute(create_table_query)
            connect.commit()
