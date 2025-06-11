import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Configuration
SPARK_APP_NAME = os.environ.get("SPARK_APP_NAME")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
BUCKET_NAME = os.environ.get("INPUT_BUCKET")

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

df_station = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="station", keyspace="station") \
    .load()

df_country = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="country", keyspace="station") \
    .load()

station_by_country = df_country.join(df_station, df_country.fips == df_station.country, "left")

df_station['year'] = df_station['begin_date'].dt.year
df_station['month'] = df_station['begin_date'].dt.month

station_by_start_date = df_station.select("year", "month", "wsid", "name", "latitude", "longitude", "elevation", "begin_date")


df_station['year'] = df_station['end_date'].dt.year
df_station['month'] = df_station['end_date'].dt.month

station_by_end_date = df_station.select("year", "month", "wsid", "name", "latitude", "longitude", "elevation", "end_date")