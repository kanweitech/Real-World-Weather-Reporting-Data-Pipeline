from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import logging
import sys
import os

# Add utilities folder to python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utilities')))

from helper_functions import main

with DAG(
    dag_id='data_fetch_daily',
    start_date=pendulum.datetime(2026, 8, 21, tz="UTC"),
    schedule='@daily',
    catchup=False
) as dag:
    fetch_weather_task = PythonOperator(
        task_id='ingest_weather_data',
        python_callable=main,
        retries=2,
        retry_delay=pendulum.duration(minutes=2),
    )