import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


SPARK_APP_NAME=os.environ.get("SPARK_APP_NAME")
SPARK_MASTER=os.environ.get("SPARK_MASTER", "local[*]")
SPARK_PACKAGES=os.environ.get("SPARK_PACKAGES")
MINIO_ENDPOINT=os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY=os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.environ.get("MINIO_SECRET_KEY")
BUCKET_NAME=os.environ.get("INPUT_BUCKET")

spark = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .master(SPARK_MASTER) \
    .config("spark.jars.packages", SPARK_PACKAGES) \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

year = sys.argv[1]
logging.info(f"Start processing data of {year}")

weather_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", StringType(), True),
])

# Đọc dữ liệu từ MinIO S3
input_path = f"s3a://{BUCKET_NAME}/{year}/"
logging.info(f"Đọc dữ liệu từ: {input_path}")

# Đọc tất cả các file CSV nén gzip trong thư mục năm
# df = spark.read \
#     .option("header", "true") \
#     .option("mode", "PERMISSIVE") \
#     .option("dateFormat", "yyyy-MM-dd HH:mm:ss") \
#     .schema(weather_schema) \
#     .load(input_path)


# Hiển thị số lượng bản ghi
# row_count = df.count()
# logging.info(f"Đã đọc {row_count} bản ghi cho năm {year}")

data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
df = spark.createDataFrame(data, ["name", "id"])

df.show()

spark.stop()
