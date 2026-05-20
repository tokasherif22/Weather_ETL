from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, json, os, pandas as pd
import psycopg2

CITY = "London"
LAT, LON = 51.5, -0.12
DATA_DIR = "/opt/airflow/data"
RAW_PATH = f"{DATA_DIR}/raw_{CITY}.json"
PARQUET_PATH = f"{DATA_DIR}/processed_{CITY}"

def fetch_data():
    """Pull last 7 days of weather from Open-Meteo (free, no API key)."""
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}&longitude={LON}"
        f"&daily=temperature_2m_max,temperature_2m_min,"
        f"precipitation_sum,windspeed_10m_max"
        f"&past_days=7&timezone=Europe/London"
    )
    os.makedirs(DATA_DIR, exist_ok=True)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    with open(RAW_PATH, "w") as f:
        json.dump(resp.json(), f)
    print(f"✓ Fetched data → {RAW_PATH}")



def transform_data():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, explode, arrays_zip, lit,
        to_date, current_timestamp
    )

    spark = (SparkSession.builder
             .appName("WeatherETL")
             .master("local[*]")
             .config("spark.driver.memory", "512m")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    raw = spark.read.option("multiline", "true").json(RAW_PATH)

    daily = raw.select("daily.*")

    df = daily.select(
        explode(
            arrays_zip(
                col("time"),
                col("temperature_2m_max"),
                col("temperature_2m_min"),
                col("precipitation_sum"),
                col("windspeed_10m_max")
            )
        ).alias("z")
    ).select(
        lit(CITY).alias("city"),
        to_date(col("z.time")).alias("date"),
        col("z.temperature_2m_max").alias("temperature_max"),
        col("z.temperature_2m_min").alias("temperature_min"),
        col("z.precipitation_sum").alias("precipitation"),
        col("z.windspeed_10m_max").alias("windspeed_max"),
        current_timestamp().alias("ingested_at")
    ).dropna(subset=["date"])

    df.write.mode("overwrite").parquet(PARQUET_PATH)
    print(f"✓ Wrote {df.count()} rows to {PARQUET_PATH}")
    spark.stop()

def load_data():
    df = pd.read_parquet(PARQUET_PATH)
    conn = psycopg2.connect(
        host="postgres", dbname="weather_db",
        user="airflow", password="airflow"
    )
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO weather_data
              (city, date, temperature_max, temperature_min,
               precipitation, windspeed_max)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (city, date) DO NOTHING
        """, (row.city, row.date, row.temperature_max,
                  row.temperature_min, row.precipitation,
                  row.windspeed_max))
    conn.commit()
    cur.close(); conn.close()
    print(f"✓ Loaded {len(df)} rows into weather_data")

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "weather"],
) as dag:

    t1 = PythonOperator(task_id="fetch_data",    python_callable=fetch_data)
    t2 = PythonOperator(task_id="transform_data", python_callable=transform_data)
    t3 = PythonOperator(task_id="load_data",      python_callable=load_data)

    t1 >> t2 >> t3  