import pymysql
import subprocess


def initiate_databases_and_tables():
    try:    
        with pymysql.connect(user='root',
                             password='password',
                             host='localhost',
                             port=3306,
                             cursorclass=pymysql.cursors.DictCursor) as connect:
            with connect.cursor() as cursor:
                # Creating a database for Airflow
                cursor.execute('CREATE DATABASE IF NOT EXISTS airflow')

                cursor.execute('CREATE DATABASE IF NOT EXISTS weather_db')
                connect.select_db('weather_db')
                cursor.execute("""CREATE TABLE IF NOT EXISTS weather_data(
                               fetched VARCHAR(100),
                               location VARCHAR(255),
                               date DATE,
                               hour VARCHAR(10),
                               temperature INT(3),
                               precipitation VARCHAR(100),
                               provider VARCHAR(100)
                                )""")
            connect.commit()
        print('Done.')
    except pymysql.Error as e:
        print(f'An error has occured: {e}')


if __name__ == '__main__':
    initiate_databases_and_tables()