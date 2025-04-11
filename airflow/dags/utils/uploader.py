# External Modules
import concurrent.futures
import logging
import os
import urllib3

# import boto3

from .decorator import logger

# MinIO Modules
from minio import Minio

#MinIO Conn
config = {
  "dest_bucket":    "processed", # This will be auto created
  "minio_endpoint": "localhost:9000",
  "minio_username": "minioadmin",
  "minio_password": "minioadmin",
}

http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
urllib3.disable_warnings()

MINIO_CLI = Minio(
    endpoint=config["minio_endpoint"],
    secure=True,
    access_key=os.environ['MINIO_ACCESS_KEY'],
    secret_key=os.environ['MINIO_SECRET_KEY'],
    http_client=http_client,
)

BUCKET_NAME = "weather-hourly-raw"

# Airflow directory
airflow_dir = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

def list_files(folder_name):
    try:
        for f in os.listdir(folder_name):
            if os.path.isfile(os.path.join(folder_name, f)):
                logging.info(f"Found {f}")
                yield os.path.join(folder_name, f)
    except Exception as e:
        logging.error("ERROR: %s", e)

def _upload_file(filepath):
    year, local_name = filepath.split('/')[3:]
    filename = f"{year}/{local_name}"

    try:
        logging.info(f"Uploading %s", filename)
        MINIO_CLI.fput_object(BUCKET_NAME, filename, filepath)
        logging.info(f"Uploaded %s into %s/%s", filename, BUCKET_NAME, filepath)
    except Exception as e:
        logging.error("ERROR: %s", e)

    return filename

@logger
def upload_multiple(list_of_files):
    try:
        if not MINIO_CLI.bucket_exists(BUCKET_NAME):
            MINIO_CLI.make_bucket(BUCKET_NAME)
        logging.info("Starting download list of objects with multi-threading")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(_upload_file, list_of_files)
    except Exception as e:
        logging.error("ERROR: %s", e)