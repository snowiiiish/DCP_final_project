from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from job1_producer import run_ingestion_loop

kafka_servers = ['kafka:29092']
kafka_topic= 'raw_events'

default_args = {
    'owner': 'dcp_team',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'job1_ingestion_dag',
    default_args=default_args,
    description='Fetches Carbon Intensity data every 30 seconds',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2025, 12, 1),
    catchup=False,
    max_active_runs=1,
    tags=['final','project']
) as dag:

    def trigger_producer():
        run_ingestion_loop(
            kafka_topic=kafka_topic,
            kafka_servers=kafka_servers,
            run_duration_minutes=14.5
        )

    task_ingest = PythonOperator(
        task_id='start_ingestion_loop',
        python_callable=trigger_producer,
        execution_timeout=timedelta(minutes=20),
    )
