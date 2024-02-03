# Weather Data ETL Pipeline with Docker, Apache Airflow, MySQL and Python

## Overview

This project automates the extraction, transformation, and loading (ETL) process of weather data from the OpenWeatherMap API into a MySQL database using Python, leveraging Apache Airflow for the scheduling of data fetches and Docker for orchestration. Designed to retrieve weather updates for user-configured locations at regular intervals, it processes the data into a structured format and updates a MySQL database every 15 minutes with updated weather forecast for 24 hours ahead. This project integrates a user-friendly interface through the `questionary` Python module, abstracting a technical side of the implementation. By incorporating modern technologies and practices such as Docker, Apache Airflow, and programmatic MySQL access, the project stands as a robust example of practical ETL processes in the field of data engineering.

## Features
- Automated weather data fetch from OpenWeatherMap API.
- Data transformation and normalization for database storage.
- Scheduled data updates using Apache Airflow.
- MySQL database integration for data persistence.
- Interactive menu for weather data locations management.
- (more to come?)


## Prerequisites

- Docker
- Docker Compose
- Python 3.8+

## Initial Setup (First-time Run):

### To set up the project environment for the first time, follow these steps:

1. Clone the repository: `git clone https://github.com/JohanTokarskij/OpenWeatherMapAPI-ETL-Docker-Airflow-MySQL-Python`
2. Navigate to the project directory: `cd [project directory]`
3. To ensure proper isolation of project dependencies, it is required to create and activate a virtual environment before running this app. Follow these steps:
- Create a new virtual environment: `python -m venv venv`
- Activate the virtual environment: `venv\Scripts\activate` on Windows or `source venv/bin/activate` on Linux/macOS
4. Install the required dependencies: `pip install -r requirements.txt`
5. Run `docker-compose up -d` in terminal to download and start the containers.
6. Run `python setup_mysql.py` to set up MySQL.
7. Run `python setup_airflow.py` to set up Apache Airflow.

### To schedule Airflow logging:
1. Navigate to `http://localhost:8080/`.
2. Login with `admin:password`.
3. Start the `update_weather_tables.py` DAG to enable (Airflow is confidured to fetch new data for each table every 15 minutes. Adjust param `schedule_interval` in `update_weather_tables.py` for other needs).

### Configuration of environment variables:
You need to have an API key to be able to use OpenWeatherMaps. Register at https://openweathermap.org/ to obtain it. Add the API key to a file named `.env` in the project's root folder. Use the following format: `API_KEY='your-api-key'`

## Usage

Run the application by executing the 'app.py' script: `python app.py`

Follow the on-screen prompts to interact with the application. You can:
   - **Add new locations & Schedule Airflow**: Provide the name of the location you want to track.
   - **View registered locations in the Database**: See a list of all locations currently being monitored.
   - **Remove location(s) from the Database**: Delete locations you no longer wish to track.

### Accessing the Data

All fetched and transformed weather data is stored in the `weather_db` database. Each location's data is stored in a distinct table named after the location. The `_location_id_table` is a key table in the database, uniquely mapping each location to its ID, name, and geographical coordinates (latitude and longitude).  

The database is accessible via MySQL:

- **Host**: `localhost`
- **Port**: `3306`
- **User**: `root`
- **Password**: `password`

## Note

This project serves as a practical demonstration of ETL (Extract, Transform, Load) processes, Python scripting, Airflow scheduling, Docker orchestration and programmatically accessing a database. Developed primarily for educational purposes, it illustrates key data engineering techniques through the automated fetching, processing, and storage of weather data into a MySQL database.
