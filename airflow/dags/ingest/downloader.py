# External Modules
import concurrent.futures
from datetime import datetime, timedelta
import logging
import os
from .decorator import logger

# S3 Modules
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.handlers import disable_signing

S3 = boto3.resource('s3', config=Config(max_pool_connections=200))
S3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

PUBLIC_BUCKET = S3.Bucket('noaa-isd-pds')

# Airflow directory
airflow_dir = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

def list_object(folder_name, bucket=PUBLIC_BUCKET):
    """
    Lists objects in a specific folder within an S3 bucket.

    This function filters objects in the provided bucket based on the given
    folder name prefix. It logs the number of objects found or logs a
    message if no objects are found. Any client error during the process
    is caught and logged.

    :param folder_name: The prefix (folder name) to filter objects in the bucket.
    :type folder_name: Str
    :param bucket: The S3 bucket instance where objects are stored. Defaults
                   to PUBLIC_BUCKET if not provided.
    :type bucket: Bucket
    :return: Yields each object filtered by the prefix in the folder.
    :rtype: Iterator
    """
    try:
        for idx, obj in enumerate(bucket.objects.filter(Prefix=folder_name)):
            yield obj
        if 'idx' in locals():
            logging.info(f"Found %s objects in %s", idx + 1, folder_name)
        else:
            logging.info(f"No object found in %s", folder_name)
    except ClientError as e:
        logging.error("ERROR: %s", e)


def get_daily_list(folder_objects):
    """
    Filters and yields objects from the input list `folder_objects` that have been
    modified within the last 24 hours. This function also logs the count of such
    objects compared to the total number of objects processed.

    The `folder_objects` parameter should be a list where each object has an
    attribute `last_modified` of datetime type. The function utilizes this attribute
    to filter objects based on their modification time.

    :param folder_objects: List of objects with `last_modified` datetime attribute
    :type folder_objects: list
    :return: Generator yielding objects modified within the last 24 hours
    :rtype: Generator
    """
    modified_objects = 0
    for index, obj in enumerate(folder_objects):
        # Check if the object's last modified date is more than 24 hours ago
        yesterday = datetime.now() - timedelta(hours=24)
        if yesterday < obj.last_modified.replace(tzinfo=None):
            modified_objects += 1
            yield obj
    if 'index' in locals():
        logging.info("Found %s / %s objects modified within the last 24 hours since today %s", modified_objects, index + 1, datetime.now().strftime("%Y-%m-%d %H:%M"))
    else:
        logging.info("List of objects empty. No keys for daily data filtered.")



def _download_file(obj_key, bucket=PUBLIC_BUCKET):
    """
    Downloads a file from the specified bucket using the given object key. The
    function constructs the file path based on the object key and saves the file
    to a local directory. If an error occurs during the download, it logs the
    error.

    :param obj_key: The object key of the file to be downloaded from the bucket.
    :type obj_key: Str
    :param bucket: The S3 bucket from which the file will be downloaded. Defaults
                   to PUBLIC_BUCKET.
    :type bucket: Any
    :return: The local file path where the downloaded file is saved.
    :rtype: Str
    """
    year, local_name = obj_key.split('/')[2:]
    dir = f"{airflow_dir}/data/raw/{year}"
    filename = f"{dir}/{local_name}"
    try:
        # logging.info(f"Downloading %s", filename)
        bucket.download_file(obj_key, filename)
    except ClientError as e:
        logging.error("ERROR: %s", e)

    return filename

@logger
def download_multiple(list_of_obj):
    """
    Download multiple objects concurrently using multi-threading.

    This function uses a thread pool executor to download a list of
    objects concurrently. It maps a helper function `_download_file`
    to each object in the provided list and logs the progress.

    :param list_of_obj: List of objects to be downloaded.
    :type list_of_obj: List
    :return: None
    :rtype: None
    """
    logging.info("Starting download list of objects with multi-threading")
    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        executor.map(_download_file, list_of_obj, chunksize=100)