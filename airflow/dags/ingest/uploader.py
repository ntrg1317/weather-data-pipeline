# External Modules
import concurrent.futures
import logging
import os
import urllib3

# import boto3

from .decorator import logger

# MinIO Modules
from minio import Minio

http_client = urllib3.PoolManager(maxsize=500, cert_reqs='CERT_NONE')
urllib3.disable_warnings()

# Use environment variables with fallback to config values
MINIO_CLI = Minio(
    endpoint="minio:9000",
    secure=False,
    access_key="root",
    secret_key="trangnt1317",
    http_client=http_client,
)

BUCKET_NAME = "weather-hourly-raw"

# Airflow directory
airflow_dir = os.environ.get("AIRFLOW_HOME", "/opt/airflow")


def check_connection():
    """
    Check if MinIO connection is working properly
    :return: True if connect to MinIO successfully, otherwise False
    :rtype: bool
    """
    try:
        # List buckets to verify connection
        buckets = MINIO_CLI.list_buckets()
        logging.info(f"MinIO connection successful. Found {len(buckets)} buckets.")
        return True
    except Exception as e:
        logging.error(f"MinIO connection failed: {e}")
        return False


def list_files(folder_name):
    """
    List of files in folder data/raw/{year}
    :param folder_name:
    :type folder_name:
    :return:
    :rtype:
    """
    try:
        for f in os.listdir(folder_name):
            if os.path.isfile(os.path.join(folder_name, f)):
                # logging.info(f"Found {f}")
                yield os.path.join(folder_name, f)
    except Exception as e:
        logging.error("ERROR: %s", e)


def _upload_file(filepath):
    """
    Upload a file to MinIO bucket
    :param filepath:
    :type filepath:
    :return:
    :rtype:
    """
    year, local_name = filepath.strip('/').split('/')[-2:]
    filename = f"{year}/{local_name}"

    try:
        MINIO_CLI.fput_object(BUCKET_NAME, filename, filepath)
    except Exception as e:
        logging.error("ERROR: %s", e)

    return filename


@logger
def upload_multiple(list_of_files):
    """
    Upload multiple file into a MinIO bucket using multi-threading
    :param list_of_files:
    :type list_of_files:
    :return:
    :rtype:
    """
    if not MINIO_CLI.bucket_exists(BUCKET_NAME):
        MINIO_CLI.make_bucket(BUCKET_NAME)
        logging.info(f"Created bucket: {BUCKET_NAME}")

    logging.info("Starting upload of objects with multi-threading")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(_upload_file, list_of_files))
        logging.info(f"Uploaded {len(results)} files")