import pymysql
import time
from pymysql_funcs import establish_mysql_connection


def initiate_databases_and_tables(retries=5):
    for attempt in range(retries):
        try:
            print(f'Initializing databases and tables, Attempt: {attempt + 1}')
            with establish_mysql_connection() as connect:
                with connect.cursor() as cursor:
                    # Creating a database and admin account for Airflow
                    cursor.execute('CREATE DATABASE IF NOT EXISTS airflow')
                    cursor.execute('CREATE USER "admin" IDENTIFIED BY "password"')
                    cursor.execute('GRANT ALL PRIVILEGES ON airflow_db.* TO "admin"')

                    cursor.execute('CREATE DATABASE IF NOT EXISTS weather_db')
                    
                    connect.select_db('weather_db')

                    cursor.execute("""CREATE TABLE IF NOT EXISTS _location_id_table(
                                        location_id INT AUTO_INCREMENT PRIMARY KEY,
                                        location VARCHAR(100),
                                        latitude DECIMAL(10, 7),
                                        longitude DECIMAL(10, 7)
                                    )""")
                    
                connect.commit()
                print('Initialization of MySQL is complete.')
                return
        except pymysql.Error as e:
            print(f'Attempt {attempt + 1}: An error occurred: {e}')
            time.sleep(3)

    print(f'Failed to set up databases after {retries} attempts. Please check the MySQL connection and try again.')

if __name__ == '__main__':
    initiate_databases_and_tables()