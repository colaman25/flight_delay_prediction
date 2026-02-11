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
    dag_id="train_models",
    default_args=default_args,
    description="Get flight and schedule data",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
    tags=['example'],
) as dag:

    train_model = DockerOperator(
        task_id='train_models',
        image='train_models:latest',
        api_version='auto',
        auto_remove=True,
        network_mode=NETWORK,
        docker_url='unix://var/run/docker.sock',
        environment={
            "MINIO_ACCESS_KEY": "{{ var.value.MINIO_ACCESS_KEY }}",
            "MINIO_SECRET_KEY": "{{ var.value.MINIO_SECRET_KEY }}",
        },
    )


    train_model