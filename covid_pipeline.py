import pandas as pd
import requests
from datetime import date, datetime, timedelta
import pendulum 
from google.cloud import storage

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

bangkok_tz = pendulum.timezone('Asia/Bangkok')
covid_pipeline = Variable.get('covid_pipeline', deserialize_json = True)
bucket = covid_pipeline['bucket']
table = covid_pipeline['table']
file_path = covid_pipeline['file_path']
data_date = datetime.now(bangkok_tz) - timedelta(days=1)

default_args = {
    'owner': 'max',
    'depends_on_past': False,
    'email': ['email@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=60)
}

# Get COVID data from Thai Department of Disease Control and save to Google Storage
def get_data(bucket, data_date, **kwargs):
    response = requests.get('https://covid19.th-stat.com/api/open/timeline')
    df = response.json()
    df = pd.DataFrame(df)
    covid_data = pd.json_normalize(df['Data'])
    covid_data.to_csv(bucket + data_date.strftime('%d-%m-%Y') + '.csv', index=False)
    print(bucket)
    print(df)

def check_and_ingest(data_date, **kwargs):
    df = pd.read_csv('/home/airflow/gcs/data/covid/' + data_date.strftime('%d-%m-%Y') + '.csv')

with DAG(
    'covid_pipeline',
    default_args=default_args,
    description='Thailand COVID Data Pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 4, 29, tzinfo=bangkok_tz),
    tags=['pipeline', 'covid'],
) as dag:

    get_data_task = PythonOperator(
        task_id='get_data',
        provide_context=True,
        python_callable=get_data,
        op_kwargs={
            'bucket': bucket,
            'data_date': data_date
            }
    )

    get_data_task
    