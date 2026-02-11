from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

NETWORK = "flight_delay_net"

with DAG(
    dag_id="get_data",
    default_args=default_args,
    description="Get flight and schedule data",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
    tags=['example'],
) as dag:

    get_flight_data = DockerOperator(
        task_id='get_flight_data',
        image='get_flight_data:latest',
        api_version='auto',
        auto_remove=True,
        network_mode=NETWORK,
        docker_url='unix://var/run/docker.sock',
        environment={
            "KAFKA_BROKER": "kafka:9092",
            "OPENSKY_CLIENT_ID": "{{ var.value.OPENSKY_CLIENT_ID }}",
            "OPENSKY_CLIENT_SECRET": "{{ var.value.OPENSKY_CLIENT_SECRET }}",
        },
    )

    get_schedule_data = DockerOperator(
        task_id='get_schedule_data',
        image='get_schedule_data:latest',
        api_version='auto',
        auto_remove=True,
        network_mode=NETWORK,
        docker_url='unix://var/run/docker.sock',
        environment={
            "KAFKA_BROKER": "kafka:9092",
            "FLIGHTAWARE_API": "{{ var.value.FLIGHTAWARE_API }}",
        },
    )



    get_flight_data >> get_schedule_data
