from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests


def upload_to_raw(source_url, dataset_slug, file_name, s3_conn_id, **context):
    """Скачивает файл по HTTP и загружает в S3 (SeaweedFS)."""
    hook = S3Hook(aws_conn_id=s3_conn_id)

    # Бакет raw должен существовать (создаётся инфраструктурой).
    if not hook.check_for_bucket(bucket_name="raw"):
        raise ValueError(
            "Bucket 'raw' does not exist. "
            "Создайте его вручную или через docker-compose инициализацию."
        )

    ds = context['ds']
    s3_key = f"{dataset_slug}/ingested_on={ds}/{file_name}"

    if hook.check_for_key(key=s3_key, bucket_name="raw"):
        print(f"Файл уже существует: s3://raw/{s3_key}. Пропускаем.")
        return

    print(f"Скачиваю: {source_url}")
    response = requests.get(source_url, timeout=300)
    response.raise_for_status()
    file_bytes = response.content
    print(f"Скачано {len(file_bytes)} байт")

    hook.load_bytes(
        bytes_data=file_bytes,
        key=s3_key,
        bucket_name="raw",
        replace=False,
    )
    print(f"Файл загружен: s3://raw/{s3_key}")


default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingest_raw',
    default_args=default_args,
    description='Загрузка сырых данных Inside Airbnb в Data Lake',
    schedule_interval=None,
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
        op_kwargs={
            'source_url': 'https://data.insideairbnb.com/spain/catalonia/barcelona/2026-09-17/data/listings.csv.gz',
            'dataset_slug': 'inside_airbnb_barcelona',
            'file_name': 'listings.csv.gz',
            's3_conn_id': 's3_conn',
        },
    )

    wait_for_source >> load_to_raw
