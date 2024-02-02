from datetime import datetime, timedelta
from etl_funcs import airflow_etl
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args= {
    'ownder': 'Johan Tokarskij',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1, 0, 0),
    'retries':2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'update_weather_table_dag',
    default_args=default_args,
    description="""This DAG fetches new data every 15 minutes from OpenWeatherMap for each of the table in "weather_db" 
    and updates each table with the new data. """,
    schedule_interval=timedelta(minutes=15),
    catchup=False
)

update_weather_tables_task = PythonOperator(
    task_id='update_weather_table',
    python_callable=airflow_etl,
    dag=dag
)

update_weather_tables_task