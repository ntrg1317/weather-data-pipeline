#Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#External modules
from datetime import datetime, timedelta
import logging
import os
import psycopg2

#Utils modules
from utils import downloader, uploader

RAW_FILES_DIRECTORY = f"{downloader.airflow_dir}/data/raw"
CONN = psycopg2.connect(**{
    "host": "postgres",
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD'],
    "database": os.environ['POSTGRES_DB']
})



def download(data_interval_start):
    """
    Download data file from S3 Bucket using the given year
    :param data_interval_start:
    :type data_interval_start:
    :return:
    :rtype:
    """
    year = data_interval_start.strftime('%Y')
    list_of_objs = downloader.list_object(f"data/{year}")

    if not os.path.exists(f"{RAW_FILES_DIRECTORY}/{year}"):
        os.makedirs(f"{RAW_FILES_DIRECTORY}/{year}")

    downloader.download_multiple((obj.key for obj in list_of_objs))
    logging.info(f"All files for year %s retrieved from %s and saved to %s",
                 year, f"data/{year}", f"data/raw/{year}")


def upload(data_interval_start):
    year = data_interval_start.strftime('%Y')
    list_of_files = uploader.list_files(f"{RAW_FILES_DIRECTORY}/{year}")

    uploader.upload_multiple((file for file in list_of_files))
    logging.info(f"All files for year %s from %s was uploaded to MinIO bucket %s",
                 year, f"data/raw/{year}", f"{uploader.BUCKET_NAME}")


default_args = {
    'owner': 'ntrg',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

historical_workflow = DAG(
    'HistoricalData',
    default_args=default_args,
    start_date=datetime(1901, 1, 1),
    schedule_interval='@yearly',
    catchup=True
)

with historical_workflow:
    year = '{{ data_interval_start.strftime("%Y") }}'

    start = BashOperator(
        task_id='Start',
        bash_command='echo "Start downloading historical data"',
    )

    task1 = PythonOperator(
        task_id='Download',
        python_callable=download,
    )

    # task2 = BashOperator(
    #     task_id='ExtractArchive',
    #     bash_command=f"""
    #     echo Found $(eval "find {RAW_FILES_DIRECTORY}/{year} -name \'*.gz\' | wc -l") .gz archives in /raw/{year} folder.
    #     Extracting them all now. && gunzip -fv {RAW_FILES_DIRECTORY}/{year}/*.gz  || true
    #     """
    # )

    task2 = PythonOperator(
        task_id='Upload',
        python_callable=upload,
    )

    task3 = BashOperator(
        task_id='Cleanup',
        bash_command=f"rm -rf {RAW_FILES_DIRECTORY}/{year}"
    )

    end = BashOperator(
        task_id='End',
        bash_command='echo "Upload raw data to MinIO successfully!"',
    )

start >> task1 >> task2 >> task3 >> end