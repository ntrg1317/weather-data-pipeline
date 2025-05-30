import argparse
import os
import time
import logging

from pyspark.sql import SparkSession
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

    # MinIO configs
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    # Cassandra configs
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")

    # Cassandra performance tuning (optional)
    .config("spark.cassandra.output.batch.size.rows", "1000")
    .config("spark.cassandra.output.batch.size.bytes", "5242880")
    .config("spark.cassandra.output.concurrent.writes", "100")
    .config("spark.cassandra.output.batch.grouping.buffer.size", "1000")

    .getOrCreate()
)

# Configure Spark for Cassandra optimization
spark.conf.set("spark.cassandra.output.batch.size.rows", "1000")
spark.conf.set("spark.cassandra.output.batch.size.bytes", "5242880")  # 5MB
spark.conf.set("spark.cassandra.output.concurrent.writes", "100")
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", "1000")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")

# -------------------- Schema Definition --------------------
schema = StructType([
    StructField("wsid", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("hour", IntegerType(), False),
    StructField("temperature", DoubleType(), True),
    StructField("dewpoint", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("wind_direction", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("sky_condition", IntegerType(), True),
    StructField("one_hour_precip", DoubleType(), True),
    StructField("six_hour_precip", DoubleType(), True),
])

# -------------------- Read and Filter Data --------------------
df = spark.read \
    .format("csv") \
    .option("recursiveFileLookup", "true") \
    .option("compression", "bzip2") \
    .option("delimiter", ",") \
    .option("multiline", "false") \
    .option("escape", '"') \
    .schema(schema) \
    .load(f"s3a://weather-hourly-raw/{args.year}/*.csv.bz2")

df.show()

df = df.na.drop(subset=["year", "month", "day", "hour"])
df = df.coalesce(100)

try:
    start_time = time.time()

    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", ASTRA_KEYSPACE) \
        .option("table", "hourly") \
        .mode("append") \
        .save()

    end_time = time.time()
    logging.info(f"Write completed in {end_time - start_time:.2f} seconds")

    # Verify write
    verification_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", ASTRA_KEYSPACE) \
        .option("table", "hourly") \
        .load()
    verification_count = verification_df.count()
    logging.info(f"Verification: {verification_count} records in Cassandra")

except Exception as e:
    logging.error(f"Error writing to Cassandra: {str(e)}")
    raise

finally:
    spark.stop()
