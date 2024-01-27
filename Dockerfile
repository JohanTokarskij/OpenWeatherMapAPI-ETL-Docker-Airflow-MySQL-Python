FROM apache/airflow:latest

WORKDIR /opt/airflow

COPY requirements.txt .

RUN pip install -r requirements.txt
