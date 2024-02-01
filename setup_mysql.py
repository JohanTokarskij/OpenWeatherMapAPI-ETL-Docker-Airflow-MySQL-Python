import pymysql
from pymysql_funcs import establish_mysql_connection


def initiate_databases_and_tables():
    try:    
        with establish_mysql_connection() as connect:
            with connect.cursor() as cursor:
                # Creating a database for Airflow
                cursor.execute('CREATE DATABASE IF NOT EXISTS airflow')

                cursor.execute('CREATE DATABASE IF NOT EXISTS weather_db')
                
                connect.select_db('weather_db')

                cursor.execute("""CREATE TABLE IF NOT EXISTS _location_id_table(
                                    location_id INT AUTO_INCREMENT PRIMARY KEY,
                                    location VARCHAR(100),
                                    latitude DECIMAL(10, 7),
                                    longitude DECIMAL(10, 7)
                                )""")
                
            connect.commit()
            print('Done.')
    except pymysql.Error as e:
        print(f'An error has occured: {e}')

if __name__ == '__main__':
    initiate_databases_and_tables()