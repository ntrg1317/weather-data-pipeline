import logging
import os
from pyspark.sql.types import *

from util.sparkMinIOConn import MinIOConnector
from util.sparkCassandraConn import SparkCassandraConnector


SPARK_APP_NAME=os.environ.get("SPARK_APP_NAME")
SPARK_MASTER=os.environ.get("SPARK_MASTER", "local[*]")
BUCKET_NAME=os.environ.get("INPUT_BUCKET")

SECURE_CONNECT_BUNDLE_PATH=os.environ.get("SECURE_CONNECT_BUNDLE_PATH")
SECURE_CONNECT_BUNDLE=os.environ.get("SECURE_CONNECT_BUNDLE")
ASTRA_CLIENT_ID=os.environ.get("ASTRA_CLIENT_ID")
ASTRA_CLIENT_SECRET=os.environ.get("ASTRA_CLIENT_SECRET")
ASTRA_KEYSPACE=os.environ.get("ASTRA_KEYSPACE")

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

df = spark \
    .read \
    .format("csv") \
    .option("compression", "gzip") \
    .option("delimiter", ",") \
    .schema(schema=schema) \
    .load("s3a://weather-hourly-raw/2014/*.csv.gz")

before_count = df.count()
logging.info(f"Number of rows before filtering nulls: {before_count}")

df = df.na.drop(subset=["year", "month", "day", "hour"])

after_count = df.count()
logging.info(f"Number of rows after filtering nulls: {after_count}")

df.show()

# Insert to Cassandra
spark_conn = SparkCassandraConnector(
    app_name = "AstraDB_Spark_Integration",
    master = SPARK_MASTER,
    secure_connect_bundle_file_path = 'spark/data/secure-connect-weather-cluster.zip',
    secure_connect_bundle_file = 'secure-connect-weather-cluster.zip',
    username = ASTRA_CLIENT_ID,
    password = ASTRA_CLIENT_SECRET
)

spark_conn.write_to_cassandra(df, ASTRA_KEYSPACE, "hourly", "overwrite")

# PostgreSQL connection info
# POSTGRES_URL = f"jdbc:postgresql://postgres:{os.environ.get('POSTGRES_PORT', 5432)}/{os.environ.get('POSTGRES_DB')}"
# POSTGRES_PROPERTIES = {
#     "user": os.environ.get("POSTGRES_USER"),
#     "password": os.environ.get("POSTGRES_PASSWORD"),
#     "driver": "org.postgresql.Driver"
# }
#
# # Write DataFrame to PostgreSQL
# df.write \
#     .jdbc(
#         url=POSTGRES_URL,
#         table="isd.hourly",  # PostgreSQL table name
#         mode="overwrite",        # or "append", "ignore", "error"
#         properties=POSTGRES_PROPERTIES
#     )

spark.stop()
spark_conn.close()