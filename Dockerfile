FROM apache/airflow:latest

#WORKDIR /opt/airflow

COPY requirements.txt ./project/requirements.txt

RUN pip install -r ./project/requirements.txt
