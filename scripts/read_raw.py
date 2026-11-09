import argparse
import duckdb
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser(description="Чтение raw-файла из SeaweedFS через DuckDB")
parser.add_argument("--path", required=True, help="S3-путь к файлу, например s3://raw/.../listings.csv.gz")
args = parser.parse_args()

S3_PATH = args.path

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
