from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import os

SOURCE_URL = os.getenv("SOURCE_URL", "https://data.insideairbnb.com/spain/catalonia/barcelona/2026-09-17/data/listings.csv.gz")
DATASET_SLUG = os.getenv("DATASET_SLUG", "inside_airbnb_barcelona")
PRIMARY_FILE = os.getenv("PRIMARY_FILE", "listings.csv.gz")
S3_CONN_ID = "s3_conn"

def upload_to_raw(**context):
    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    ds = context['ds']
    s3_key = f"{DATASET_SLUG}/ingested_on={ds}/{PRIMARY_FILE}"
    
    if hook.check_for_key(key=s3_key, bucket_name="raw"):
        print(f"Файл уже существует: s3://raw/{s3_key}. Пропускаем.")
        return
    
    response = requests.get(SOURCE_URL, timeout=300)
    response.raise_for_status()
    file_bytes = response.content
    
    hook.load_bytes(
        bytes_data=file_bytes,
        key=s3_key,
        bucket_name="raw",
        replace=False
    )
    print(f"Файл загружен: s3://raw/{s3_key}")

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ingest_raw',
    default_args=default_args,
    description='Загрузка сырых данных Inside Airbnb в Data Lake',
    schedule_interval='@daily',
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['lab2', 'ingestion'],
) as dag:

    wait_for_source = HttpSensor(
        task_id='wait_for_primary_source',
        http_conn_id='source_http_conn',
        endpoint='',
        poke_interval=60,
        timeout=600,
        mode='poke',
    )

    load_to_raw = PythonOperator(
        task_id='load_to_raw',
        python_callable=upload_to_raw,
        provide_context=True,
    )

    wait_for_source >> load_to_raw
