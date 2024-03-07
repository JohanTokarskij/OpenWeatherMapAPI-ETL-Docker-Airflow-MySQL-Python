FROM apache/airflow:latest

#WORKDIR /opt/airflow

COPY requirements.txt ./project/requirements.txt

RUN pip install --no-cache-dir -r  ./project/requirements.txt
