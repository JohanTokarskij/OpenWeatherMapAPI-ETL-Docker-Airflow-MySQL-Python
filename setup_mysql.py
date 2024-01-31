import pymysql
from pymysql_funcs import establish_mysql_connection


def initiate_databases_and_tables():
    try:    
        connect = establish_mysql_connection()
        with connect.cursor() as cursor:
            # Creating a database for Airflow
            cursor.execute('CREATE DATABASE IF NOT EXISTS airflow')

            cursor.execute('CREATE DATABASE IF NOT EXISTS weather_db')
            
            connect.select_db('weather_db')

            cursor.execute("""CREATE TABLE IF NOT EXISTS location_table(
                                location_id INT AUTO_INCREMENT PRIMARY KEY,
                                location_name VARCHAR(100),
                                latitude FLOAT,
                                longitude FLOAT
                            )""")
            
            cursor.execute("""CREATE TABLE IF NOT EXISTS weather_data(
                                entry_id INT AUTO_INCREMENT PRIMARY KEY,
                                location_id INT,
                                fetched VARCHAR(100),
                                date DATE,
                                hour VARCHAR(10),
                                temperature INT(3),
                                humidity INT(3),
                                weather_condition VARCHAR(100),
                                precipitation_mm FLOAT,
                                FOREIGN KEY(location_id) REFERENCES location_table(location_id)
                            )""")
        connect.commit()
        print('Done.')
    except pymysql.Error as e:
        print(f'An error has occured: {e}')


if __name__ == '__main__':
    initiate_databases_and_tables()