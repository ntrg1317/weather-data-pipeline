import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Configuration
SPARK_APP_NAME = os.environ.get("SPARK_APP_NAME")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
BUCKET_NAME = os.environ.get("INPUT_BUCKET")

KEYSPACE = os.environ.get("KEYSPACE")

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
    .config("spark.cassandra.connection.timeoutMS", "30000")
    .config("spark.cassandra.read.timeoutMS", "30000")

    .getOrCreate()
)

# -------------------- Aggregate raw data --------------------

df_monthly= spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="monthly", keyspace=KEYSPACE) \
    .load() \
    .filter(col("year") == args.year) \

df_yearly = df_monthly.groupBy("wsid", "year") \
    .agg(
        sum("n_records").alias("n_records"),
        round(avg("temperature_avg"), 0).alias("temperature_avg"),
        min("temperature_min").alias("temperature_min"),
        max("temperature_max").alias("temperature_max"),

        round(avg("dewpoint_avg"), 0).alias("dewpoint_avg"),
        min("dewpoint_min").alias("dewpoint_min"),
        max("dewpoint_max").alias("dewpoint_max"),

        round(avg("pressure_avg"), 0).alias("pressure_avg"),
        min("pressure_min").alias("pressure_min"),
        max("pressure_max").alias("pressure_max"),

        round(avg("wind_direction"), 0).alias("wind_direction"),
        round(avg("wind_speed_avg"), 0).alias("wind_speed_avg"),
        min("wind_speed_min").alias("wind_speed_min"),
        max("wind_speed_max").alias("wind_speed_max"),

        round(avg("sky_condition"), 0).alias("sky_condition"),

        sum("precipitation").alias("precipitation"),
        round(avg("one_hour_precipitation_avg"), 0).alias("one_hour_precipitation_avg"),
        min("one_hour_precipitation_min").alias("one_hour_precipitation_min"),
        max("one_hour_precipitation_max").alias("one_hour_precipitation_max"),

        round(avg("six_hour_precipitation_avg"), 0).alias("six_hour_precipitation_avg"),
        min("six_hour_precipitation_min").alias("six_hour_precipitation_min"),
        max("six_hour_precipitation_max").alias("six_hour_precipitation_max")
    )

df_yearly.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="yearly", keyspace=KEYSPACE) \
    .mode("append") \
    .save()


