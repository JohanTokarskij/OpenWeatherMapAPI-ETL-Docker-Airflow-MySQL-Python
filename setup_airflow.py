import subprocess


def run_command_in_compose(service_name, command):
    try:
        compose_command = f'docker-compose exec -T {service_name} /bin/bash -c \"{command}\"'
        subprocess.run(compose_command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f'An error occurred: {e}')

def initialize_airflow():
    # Start the Airflow services
    try:
        subprocess.run(f'docker-compose up -d airflow-webserver airflow-scheduler', shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f'An error occurred: {e}')

    # Run the Airflow database upgrade
    run_command_in_compose('airflow-webserver', 'airflow db migrate')

    # Create an admin user
    run_command_in_compose('airflow-webserver', 'airflow users create --username admin --password password --firstname Admin --lastname User --role Admin --email admin@example.com')

    # Upgrade the database for the scheduler
    run_command_in_compose('airflow-scheduler', 'airflow db migrate')

    print('Airflow is set up and ready.')

if __name__ == '__main__':
    initialize_airflow()