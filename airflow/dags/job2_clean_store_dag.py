from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from job2_cleaner import run_cleaning_job

kafka_servers = ['kafka:29092']

default_args = {
    'owner': 'dcp_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'job2_clean_store_dag',
    default_args=default_args,
    description='Reads Kafka, Cleans, Stores to SQLite',
    schedule_interval='@hourly',
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['final','project']
) as dag:

    def trigger_cleaner():
        run_cleaning_job(kafka_servers=kafka_servers)

    task_clean = PythonOperator(
        task_id='clean_and_store',
        python_callable=trigger_cleaner,
    )
