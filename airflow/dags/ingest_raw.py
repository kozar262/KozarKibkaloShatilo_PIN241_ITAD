from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import os

SOURCE_URL = os.getenv(
    "SOURCE_URL",
    "https://data.insideairbnb.com/spain/catalonia/barcelona/2026-09-17/data/listings.csv.gz"
)
DATASET_SLUG = os.getenv("DATASET_SLUG", "inside_airbnb_barcelona")
PRIMARY_FILE = os.getenv("PRIMARY_FILE", "listings.csv.gz")
S3_CONN_ID = "s3_conn"


def check_source(**context):
    """Проверяет доступность источника через HEAD-запрос."""
    response = requests.head(SOURCE_URL, timeout=30, allow_redirects=True)
    if response.status_code != 200:
        raise ValueError(f"Источник недоступен: HTTP {response.status_code}")
    print(f"Источник доступен: {SOURCE_URL}")


def upload_to_raw(**context):
    """Скачивает файл и загружает в S3 (SeaweedFS)."""
    hook = S3Hook(aws_conn_id=S3_CONN_ID)

    # Проверяем/создаём бакет raw
    if not hook.check_for_bucket(bucket_name="raw"):
        hook.create_bucket(bucket_name="raw")
        print("Создан бакет raw")

    ds = context['ds']
    s3_key = f"{DATASET_SLUG}/ingested_on={ds}/{PRIMARY_FILE}"

    if hook.check_for_key(key=s3_key, bucket_name="raw"):
        print(f"Файл уже существует: s3://raw/{s3_key}. Пропускаем.")
        return

    print(f"Скачиваю: {SOURCE_URL}")
    response = requests.get(SOURCE_URL, timeout=300)
    response.raise_for_status()
    file_bytes = response.content
    print(f"Скачано {len(file_bytes)} байт")

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

    check_source_task = PythonOperator(
        task_id='wait_for_primary_source',
        python_callable=check_source,
    )

    load_to_raw = PythonOperator(
        task_id='load_to_raw',
        python_callable=upload_to_raw,
    )

    check_source_task >> load_to_raw
