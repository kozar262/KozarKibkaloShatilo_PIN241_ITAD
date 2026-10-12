import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

S3_PATH = f"s3://raw/{os.getenv('DATASET_SLUG')}/ingested_on=2026-10-12/{os.getenv('PRIMARY_FILE')}"

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("""
    SET s3_endpoint='localhost:8333';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='admin';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

try:
    df = con.execute(f"SELECT * FROM read_csv_auto('{S3_PATH}') LIMIT 10").fetchdf()
    print("✅ Файл успешно прочитан из SeaweedFS:")
    print(df)
except Exception as e:
    print(f"❌ Ошибка: {e}")
