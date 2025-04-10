# External Modules
import concurrent.futures
import logging
import os
from .decorator import logger

# MinIO Modules
from minio import Minio

#MinIO Conn
MINIO_CLI = Minio(
    endpoint=os.environ['MINIO_HOST'],
    access_key=os.environ['MINIO_ACCESS_kEY'],
    secret_key=os.environ['MINIO_SECRET_KEY'],
    secure=False
)

BUCKET_NAME = "weather-hourly-raw"

# Airflow directory
airflow_dir = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

def list_files(folder_name):
    try:
        for f in os.listdir(folder_name):
            if os.path.isfile(os.path.join(folder_name, f)):
                yield os.path.join(folder_name, f)
    except Exception as e:
        logging.error("ERROR: %s", e)

def _upload_file(filepath):
    filename = os.path.basename(filepath)

    try:
        logging.info(f"Uploading %s", filename)
        MINIO_CLI.fput_object(BUCKET_NAME, filename, filepath)
        logging.info(f"Uploaded %s into %s/%s", filename, BUCKET_NAME, filepath)
    except Exception as e:
        logging.error("ERROR: %s", e)

@logger
def upload_multiple(list_of_files):
    if not MINIO_CLI.bucket_exists(BUCKET_NAME):
        MINIO_CLI.make_bucket(BUCKET_NAME)

    logging.info("Starting download list of objects with multi-threading")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(_upload_file, list_of_files)