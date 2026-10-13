# Доказательства ЛР2

## 1. Граф DAG ingest_raw
![DAG Graph](dag_graph.png)

Обе задачи (`wait_for_primary_source` и `load_to_raw`) завершены успешно.

## 2. Бакет raw в SeaweedFS
![SeaweedFS Bucket](seaweedfs_buckets.png))

Файл `listings.csv.gz` загружен с партиционированием по `ingested_on`.

## 3. Проверка через DuckDB
![DuckDB Output](read_raw_output.png))

Файл успешно прочитан из SeaweedFS через `scripts/read_raw.py`.
