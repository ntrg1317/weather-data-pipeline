#Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

#External modules
from datetime import timedelta, datetime
import logging
import os

#Utils modules
from ingest import downloader, uploader

YEAR = datetime.now().strftime("%Y")
RAW_FILES_DIRECTORY = f"{downloader.airflow_dir}/data/raw"


def download():
    """
    Download data file from S3 Bucket using the given YEAR
    :param data_interval_start:
    :type data_interval_start:
    :return:
    :rtype:
    """
    list_of_objs = downloader.list_object(f"data/{YEAR}")
    object_keys = (obj.key for obj in downloader.get_daily_list(list_of_objs))


    if not os.path.exists(f"{RAW_FILES_DIRECTORY}/{YEAR}"):
        os.makedirs(f"{RAW_FILES_DIRECTORY}/{YEAR}")

    downloader.download_multiple(object_keys)
    logging.info(f"All files for YEAR %s retrieved from %s and saved to %s",
                 YEAR, f"data/{YEAR}", f"data/raw/{YEAR}")


def upload():
    list_of_files = uploader.list_files(f"{RAW_FILES_DIRECTORY}/{YEAR}")

    if uploader.check_connection():
        uploader.upload_multiple((file for file in list_of_files))
        logging.info(f"All files for YEAR %s from %s was uploaded to MinIO bucket %s",
                     YEAR, f"data/raw/{YEAR}", f"{uploader.BUCKET_NAME}")

default_args = {
    'owner': 'ntrg',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

daily_workflow = DAG(
    'DailyData',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
)

with daily_workflow:

    start = BashOperator(
        task_id='Start',
        bash_command='echo "Start downloading historical data"',
    )

    task1 = PythonOperator(
        task_id='Download',
        python_callable=download,
    )

    task2 = PythonOperator(
        task_id='Upload',
        python_callable=upload,
    )

    task3 = BashOperator(
        task_id='Cleanup',
        bash_command=f"rm -rf {RAW_FILES_DIRECTORY}/{YEAR}"
    )

    end = BashOperator(
        task_id='End',
        bash_command='echo "Upload raw data to MinIO successfully!"',
    )

start >> task1 >> task2 >> task3 >> end