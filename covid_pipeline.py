import pandas as pd
import requests
from datetime import datetime, timedelta
import pendulum 

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

bangkok_tz = pendulum.timezone('Asia/Bangkok')
covid_pipeline = Variable.get('covid_pipeline', deserialize_json = True)
bucket = covid_pipeline['bucket']
table = covid_pipeline['table']
file_path = covid_pipeline['file_path']
data_date = datetime.now(bangkok_tz) - timedelta(days=1)
file_name_and_path = file_path + data_date.strftime('%d-%m-%Y') + '.csv'

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

# Delete all rows from the table and ingest new data 
bs_to_bq_bash = '''
bq query --use_legacy_sql=false delete 'from {{ params.table }} where true' && 
bq load --source_format=CSV --autodetect {{ params.table }} {{ params.file_name_and_path }}
'''

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

    ingest_from_gs_to_bq = BashOperator(
        task_id='ingest_from_gs_to_bq',
        bash_command=bs_to_bq_bash,
        params={
            'file_name_and_path': file_name_and_path,
            'table': table}
    )

    get_data_task >> ingest_from_gs_to_bq
    