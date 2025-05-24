import logging
import os
from pyspark.sql.types import *

from util.sparkMinIOConn import MinIOConnector
from util.sparkCassandraConn import SparkCassandraConnector


SPARK_APP_NAME=os.environ.get("SPARK_APP_NAME")
SPARK_MASTER=os.environ.get("SPARK_MASTER", "local[*]")
BUCKET_NAME=os.environ.get("INPUT_BUCKET")

MINIO_CONFIG = {
    "endpoint": os.environ.get("MINIO_ENDPOINT"),
    "access_key": os.environ.get("MINIO_ACCESS_KEY"),
    "secret_key": os.environ.get("MINIO_SECRET_KEY"),
}


minio_conn = MinIOConnector(
    app_name = "IngestRawData",
    master = SPARK_MASTER,
    minio_config = MINIO_CONFIG,
)

spark = minio_conn.get_session()

schema = StructType([
    StructField("station", StringType(), False),
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

df = spark \
    .read \
    .format("csv") \
    .option("compression", "gzip") \
    .option("delimiter", ",") \
    .schema(schema=schema) \
    .load("s3a://weather-hourly-raw/*/*.csv.gz")

df.show()

spark.stop()


## Insert to Cassandra
spark_conn = SparkCassandraConnector(
    app_name = "AstraDB_Spark_Integration",
    master = SPARK_MASTER,
    secure_connect_bundle_file_path = config["ASTRA"]["SECURE_CONNECT_BUNDLE"],
    secure_connect_bundle_file = "secure-connect-weather-cluster.zip",
    username = config["ASTRA"]["ASTRA_CLIENT_ID"],
    password = config["ASTRA"]["ASTRA_CLIENT_SECRET"]
)
