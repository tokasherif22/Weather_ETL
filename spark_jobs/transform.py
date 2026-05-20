from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, arrays_zip, lit,
    to_date, current_timestamp
)
import sys

def transform(input_path: str, output_path: str, city: str):
    spark = (SparkSession.builder
             .appName("WeatherETL")
             .master("local[*]") 
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # Read raw JSON (Open-Meteo returns nested arrays)
    raw = spark.read.option("multiline", "true").json(input_path)

    # Explode parallel arrays into rows
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
        lit(city).alias("city"),
        to_date(col("z.time")).alias("date"),
        col("z.temperature_2m_max").alias("temperature_max"),
        col("z.temperature_2m_min").alias("temperature_min"),
        col("z.precipitation_sum").alias("precipitation"),
        col("z.windspeed_10m_max").alias("windspeed_max"),
        current_timestamp().alias("ingested_at")
    ).dropna(subset=["date"])

    # Write as Parquet — efficient & typed
    df.write.mode("overwrite").parquet(output_path)
    print(f"✓ Wrote {df.count()} rows to {output_path}")
    spark.stop()

if __name__ == "__main__":
    # Called by Airflow: python transform.py <in> <out> <city>
    transform(sys.argv[1], sys.argv[2], sys.argv[3])