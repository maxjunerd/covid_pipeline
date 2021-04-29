import pandas as pd
import requests
import datetime
import pendulum 

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

bangkok_tz = pendulum.timezone('Asia/Bangkok')

default_args = {
    'owner': 'max',
    'depends_on_past': False,
    'email': ['email@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=60)
}

with DAG(
    'covid_pipeline',
    default_args=default_args,
    description='Thailand COVID Data Pipeline',
    schedule_interval=datetime.timedelta(days=1),
    start_date=datetime.datetime(2021, 4, 29, tzinfo=bangkok_tz),
    tags=['pipeline', 'covid'],
) as dag:

    