from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Import tasks from individual files
from tasks.fetch_and_upload_data import fetch_and_upload_crypto_data
from tasks.trigger_databricks_notebook import trigger_databricks_job

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'crypto_data_pipeline',
    default_args = default_args,
    description = 'A DAG to fetch crypto data, store on GCS, and process in Databricks',
    schedule_interval = '@daily',
) as dag:

     # Start task to print timestamp
    start = PythonOperator(
        task_id = 'start',
        python_callable = print(f"Start time: {datetime.now()}"),
    )

    # Define tasks using PythonOperators
    fetch_and_upload_data_task = PythonOperator(
        task_id = 'fetch_crypto_data',
        python_callable = fetch_and_upload_crypto_data,
    )

    databricks_task = PythonOperator(
        task_id = 'trigger_databricks_job',
        python_callable = trigger_databricks_job,
    )

    # end task to print timestamp
    end = PythonOperator(
        task_id = 'end',
        python_callable = print(f"End time: {datetime.now()}"),
    )

    # Task dependencies
    start >> fetch_and_upload_data_task >> databricks_task >> end
