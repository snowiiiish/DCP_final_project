from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from job3_analytics import run_analytics_job

default_args = {
    'owner': 'dcp_team',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'job3_daily_summary_dag',
    default_args=default_args,
    description='Computes daily analytics from SQLite',
    schedule_interval='@daily',
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['final','project']
) as dag:

    task_analyze = PythonOperator(
        task_id='calculate_daily_metrics',
        python_callable=run_analytics_job,
    )