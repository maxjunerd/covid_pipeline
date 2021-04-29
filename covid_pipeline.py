import pandas as pd
import requests
import datetime
import pendulum 
from google.cloud import storage

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

bangkok_tz = pendulum.timezone("Asia/Bangkok")
covid_pipeline = Variable.get("covid_pipeline", deserialize_json = True)
bucket = covid_pipeline["bucket"]

default_args = {
    "owner": "max",
    "depends_on_past": False,
    "email": ["email@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=60)
}

# Get COVID data from Thai Department of Disease Control and save to Google Storage
def get_data(bucket):
    response = requests.get("https://covid19.th-stat.com/api/open/timeline")
    df = response.json()
    print(bucket)
    print(df)


with DAG(
    "covid_pipeline",
    default_args=default_args,
    description="Thailand COVID Data Pipeline",
    schedule_interval=datetime.timedelta(days=1),
    start_date=datetime.datetime(2021, 4, 29, tzinfo=bangkok_tz),
    tags=["pipeline", "covid"],
) as dag:

    get_data_task = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
        op_kwargs={"bucket": bucket}
    )
    