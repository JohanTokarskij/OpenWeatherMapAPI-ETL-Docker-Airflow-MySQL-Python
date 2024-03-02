import subprocess
import time

def run_command_with_retry(service_name, command, retries=5):
    for attempt in range(retries):
        try:
            print(f'Starting the service {service_name}, Attempt: {attempt + 1}')
            subprocess.run(f'docker-compose up -d {service_name}', shell=True, check=True)

            compose_command = f'docker-compose exec -T {service_name} /bin/bash -c \"{command}\"'
            subprocess.run(compose_command, shell=True, check=True)
            print(f'Initialization of {service_name} is complete')
            return
        except subprocess.CalledProcessError as e:
            print(f'Attempt {attempt + 1}: An error occurred: {e}')
            time.sleep(3)

    print(f"Failed to execute command on {service_name} after {retries} attempts.")

def initialize_airflow():    
    # Airflow webserver initialization command
    airflow_commands = "airflow db migrate && airflow users create --username admin --password password --firstname Admin --lastname User --role Admin --email admin@example.com"

    # Initialization of Airflow webserver
    run_command_with_retry('airflow-webserver', airflow_commands)
    
    # Initialization of Airflow webscheduler
    run_command_with_retry('airflow-scheduler', "airflow db migrate")

if __name__ == '__main__':
    initialize_airflow()