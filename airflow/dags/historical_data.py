#Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#External modules
from datetime import datetime, timedelta
import logging
import os

#Utils modules
from ingest import downloader, uploader

RAW_FILES_DIRECTORY = f"{downloader.airflow_dir}/data/raw"

def download(data_interval_start):
    """
    Download data file from S3 Bucket using the given year
    :param data_interval_start:
    :type data_interval_start:
    :return:
    :rtype:
    """
    year = data_interval_start.strftime('%Y')
    list_of_objs = downloader.list_object(f"isd-lite/data/{year}")

    if not os.path.exists(f"{RAW_FILES_DIRECTORY}/{year}"):
        os.makedirs(f"{RAW_FILES_DIRECTORY}/{year}")

    downloader.download_multiple((obj.key for obj in list_of_objs))
    logging.info(f"All files for year %s retrieved from %s and saved to %s",
                 year, f"data/{year}", f"data/raw/{year}")


def upload(data_interval_start):
    year = data_interval_start.strftime('%Y')
    list_of_files = uploader.list_files(f"{RAW_FILES_DIRECTORY}/{year}")

    if uploader.check_connection():
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
    start_date=datetime(2010, 1, 1),
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

    task2 = BashOperator(
        task_id="ExtractArchive",
        do_xcom_push=False,
        bash_command=f"""
            GZ_FILES=$(find {RAW_FILES_DIRECTORY}/{year} -name '*.gz')
            GZ_COUNT=$(echo "$GZ_FILES" | grep -c "^" || echo 0)

            echo "Found $GZ_COUNT .gz archives in {RAW_FILES_DIRECTORY}/{year} folder."

            if [ $GZ_COUNT -gt 0 ]; then
                echo "Extracting archives using parallel processing..."
                echo "$GZ_FILES" | parallel -j 8 --bar 'gunzip -f {{}} && echo "Extracted: {{}}"'
                echo "All archives extracted successfully."
            else
                echo "No .gz archives found. Skipping extraction step."
            fi
            """
    )
    # Add the filename as prefix for every line on all the extracted text-based files
    task3 = BashOperator(
        task_id='AddPrefixAndCompress',
        do_xcom_push=False,
        bash_command=f"""
            # Get the list of files without extension
            FILES=$(find {RAW_FILES_DIRECTORY}/{year} -type f ! -name "*.*")

            parallel -j 8 '
                BASENAME=$(basename {{}})
                PREFIX=${{BASENAME//-{year}/}}

                awk -v prefix="$PREFIX" \\
                    "BEGIN {{ OFS=\\",\\" }}
                     {{
                        for (i=1; i<=NF; i++) {{
                            if (\\$i == \\"-9999\\") \\$i = \\"null\\"
                        }}
                        print prefix, \\$0
                     }}" {{}} | gzip > {{}}.csv.gz
            ' ::: $FILES
            
            # After successful compression, remove original files
            echo "Removing original files..."
            find {RAW_FILES_DIRECTORY}/{year} -type f ! -name "*.csv.gz" -delete
            
            # Report summary
            COMPRESSED_COUNT=$(find {RAW_FILES_DIRECTORY}/{year} -name "*.csv.gz" | wc -l)
            echo "Cleanup complete. $COMPRESSED_COUNT compressed files remain in {RAW_FILES_DIRECTORY}/{year}."
        """
    )

    task4 = PythonOperator(
        task_id='Upload',
        python_callable=upload,
    )

    task5 = BashOperator(
        task_id='Cleanup',
        bash_command=f"rm -rf {RAW_FILES_DIRECTORY}/{year}"
    )

    end = BashOperator(
        task_id='End',
        bash_command='echo "Upload raw data to MinIO successfully!"',
    )

start >> task1 >> task2 >> task3 >> task4 >> task5 >> end