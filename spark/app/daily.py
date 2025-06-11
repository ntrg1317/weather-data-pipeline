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
parser.add_argument('--year', type=int, required=True)
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

df_hourly = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="hourly", keyspace=KEYSPACE) \
    .load() \
    .filter(col("year") == args.year) \
    .select("wsid", "year", "month", "day", "hour",  # only necessary columns
            "temperature", "dewpoint", "pressure",
            "wind_direction", "wind_speed",
            "sky_condition", "one_hour_precip",
            "six_hour_precip")

df_daily = df_hourly \
    .groupBy("wsid", "year", "month", "day") \
    .agg(
        count("hour").alias("n_records"),

        round(avg("temperature"), 0).alias("temperature_avg"),
        min("temperature").alias("temperature_min"),
        max("temperature").alias("temperature_max"),

        round(avg("dewpoint"), 0).alias("dewpoint_avg"),
        min("dewpoint").alias("dewpoint_min"),
        max("dewpoint").alias("dewpoint_max"),

        round(avg("pressure"), 0).alias("pressure_avg"),
        min("pressure").alias("pressure_min"),
        max("pressure").alias("pressure_max"),

        round(avg("wind_direction"), 0).alias("wind_direction"),
        round(avg("wind_speed"), 0).alias("wind_speed_avg"),
        min("wind_speed").alias("wind_speed_min"),
        max("wind_speed").alias("wind_speed_max"),

        round(avg("sky_condition"), 0).alias("sky_condition"),

        sum("one_hour_precip").alias("precipitation"),
        round(avg("one_hour_precip"), 0).alias("one_hour_precipitation_avg"),
        min("one_hour_precip").alias("one_hour_precipitation_min"),
        max("one_hour_precip").alias("one_hour_precipitation_max"),

        round(avg("six_hour_precip"), 0).alias("six_hour_precipitation_avg"),
        min("six_hour_precip").alias("six_hour_precipitation_min"),
        max("six_hour_precip").alias("six_hour_precipitation_max")
    ) \
    .withColumn("timestamp", make_date(col("year"), col("month"), col("day")))

df_daily.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="daily", keyspace=KEYSPACE) \
    .mode("append") \
    .save()