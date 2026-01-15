from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from etl_pipeline.main import run_pipeline  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'health_etl_dag',
    default_args=default_args,
    description='Automatisierte ETL für Gesundheits-DWH',
    schedule_interval='0 2 * * *',  # Täglich 2 Uhr
    start_date=datetime(2026, 1, 15),
    catchup=False,
) as dag:

    run_etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=run_pipeline
    )

    run_etl_task
