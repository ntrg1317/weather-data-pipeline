# External Modules
import concurrent.futures
import logging
import os
from .decorator import logger

# S3 Modules
import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.handlers import disable_signing

S3 = boto3.resource('s3', config=Config(max_pool_connections=50))
S3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

PUBLIC_BUCKET = S3.Bucket('noaa-isd-pds')

# Airflow directory
airflow_dir = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

def list_object(folder_name, bucket=PUBLIC_BUCKET):
    try:
        logging.info()
        for idx, obj in enumerate(bucket.objects.filter(Prefix=folder_name)):
            yield obj
        if 'idx' in locals():
            logging.info(f"Found %s objects in %s", idx + 1, folder_name)
        else:
            logging.info(f"No object found in %s", folder_name)

    except ClientError as e:
        logging.error("ERROR: %s", e)

def _download_file(obj_key, bucket=PUBLIC_BUCKET):
    year, local_name = obj_key.split('/')[2:]
    dir = f"{airflow_dir}/raw/{year}"
    filename = f"{dir}/{local_name}"
    try:
        logging.info(f"Downloading {filename}")
        bucket.download_file(obj_key, filename)
    except ClientError as e:
        logging.error("ERROR: %s", e)

@logger
def download_multiple(list_of_obj):
    logging.info("Starting download list of objects with multi-threading")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(_download_file, list_of_obj)