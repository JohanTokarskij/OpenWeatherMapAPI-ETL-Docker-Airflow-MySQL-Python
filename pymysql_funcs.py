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
        print('Connection to MySQL is successful')
        return connection
    except pymysql.Error as e:
        print(f'\nError connecting to MySQL: {e}')
        return None