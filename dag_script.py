# Import necessary libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extraction import data_extraction
from transformation import data_transformation
from loading import data_loading

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email': ['classrep.dbs.cucg@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2025, 4, 17),
}

# Define the DAG
with DAG(
    dag_id='finance_data_pipeline_week1',
    default_args=default_args,
    description='Automated ETL pipeline for Yahoo Finance stock data',
    schedule_interval='0 6,12,18 * * *',  # Run at 6am, 12pm, and 6pm daily
    catchup=False,
    max_active_runs=1,
    tags=['finance', 'etl', 'pipeline']
) as dag:

    # Task 1: Extract data from Yahoo Finance
    extract_task = PythonOperator(
        task_id='extraction',
        python_callable=data_extraction
    )

    # Task 2: Transform the newly extracted data
    transform_task = PythonOperator(
        task_id='transformation',
        python_callable=data_transformation
    )

    # Task 3: Load the transformed data into PostgreSQL
    load_task = PythonOperator(
        task_id='loading',
        python_callable=data_loading
    )

    # Set task dependencies: extract >> transform >> load
    extract_task >> transform_task >> load_task
