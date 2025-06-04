import argparse
import os
import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
SPARK_APP_NAME = os.environ.get("SPARK_APP_NAME")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
BUCKET_NAME = os.environ.get("INPUT_BUCKET")

ASTRA_KEYSPACE = os.environ.get("ASTRA_KEYSPACE")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")

#Get arguments
parser = argparse.ArgumentParser()
year = parser.add_argument('--year', type=int, required=True)
args = parser.parse_args()

# -------------------- CREATE SparkSession --------------------
spark = (
    SparkSession.builder
    .appName(SPARK_APP_NAME)
    .master(SPARK_MASTER)

    # Cassandra configs
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
    # Configure Spark for Cassandra optimization
    .config("spark.cassandra.output.batch.size.rows", "200")
    .config("spark.cassandra.output.batch.size.bytes", "1048576")  # 5MB
    .config("spark.cassandra.output.concurrent.writes", "50")
    .config("spark.cassandra.output.batch.grouping.buffer.size", "200")

    .config("spark.sql.files.maxPartitionBytes", "134217728")
    .config("spark.cassandra.connection.timeout_ms", "30000")
    .config("spark.cassandra.read.timeout_ms", "30000")

    .getOrCreate()
)

# -------------------- Aggregate raw data --------------------

df_daily = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="daily", keyspace="weather") \
    .load() \
    .filter(col("year") == args.year) \

df_monthly = df_daily.groupBy("wsid", "year", "month") \
    .agg(
        sum("n_records").alias("n_records"),
        avg("temperature_avg").alias("temperature_avg"),
        min("temperature_min").alias("temperature_min"),
        max("temperature_max").alias("temperature_max"),

        avg("dewpoint_avg").alias("dewpoint_avg"),
        min("dewpoint_min").alias("dewpoint_min"),
        max("dewpoint_max").alias("dewpoint_max"),

        avg("pressure_avg").alias("pressure_avg"),
        min("pressure_min").alias("pressure_min"),
        max("pressure_max").alias("pressure_max"),

        avg("wind_direction").alias("wind_direction"),
        avg("wind_speed_avg").alias("wind_speed_avg"),
        min("wind_speed_min").alias("wind_speed_min"),
        max("wind_speed_max").alias("wind_speed_max"),

        round(avg("sky_condition")).alias("sky_condition"),

        avg("one_hour_precip").alias("one_hour_precipitation_avg"),
        min("one_hour_precip").alias("one_hour_precipitation_min"),
        max("one_hour_precip").alias("one_hour_precipitation_max"),

        avg("six_hour_precip").alias("six_hour_precipitation_avg"),
        min("six_hour_precip").alias("six_hour_precipitation_min"),
        max("six_hour_precip").alias("six_hour_precipitation_max")
    )

df_monthly.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="monthly", keyspace=ASTRA_KEYSPACE) \
    .mode("append") \
    .save()