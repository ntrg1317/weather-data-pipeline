#Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

#External modules
from datetime import timedelta, datetime
import logging
import os

from airflow.utils.helpers import chain

#Utils modules
from ingest import downloader, uploader

RAW_FILES_DIRECTORY = f"{downloader.airflow_dir}/data/raw"


def download(data_interval_start, **context):
    """
    Download data file from S3 Bucket using the given YEAR
    :param data_interval_start:
    :type data_interval_start:
    :return:
    :rtype:
    """

    year = data_interval_start.strftime('%Y')

    if not os.path.exists(f"{RAW_FILES_DIRECTORY}/{year}"):
        os.makedirs(f"{RAW_FILES_DIRECTORY}/{year}")

    schedule = context['dag'].schedule_interval

    if schedule == '@daily':
        list_of_objs = downloader.list_object(f"isd-lite/data/{year}")
        object_keys = (obj.key for obj in downloader.get_daily_list(list_of_objs))

        downloader.download_multiple(object_keys)
        logging.info(f"All files for year %s retrieved from %s and saved to %s",
                     year, f"data/{year}", f"data/raw/{year}")
    elif schedule == '@yearly':
        list_of_objs = downloader.list_object(f"isd-lite/data/{year}")

        downloader.download_multiple((obj.key for obj in list_of_objs))
        logging.info(f"All files for year %s retrieved from %s and saved to %s",
                     year, f"data/{year}", f"data/raw/{year}")

def upload(data_interval_start, **context):

    year = data_interval_start.strftime("%Y")
    filepath = f"{RAW_FILES_DIRECTORY}/{year}.csv.bz2"

    # list_of_files = uploader.list_files(f"{RAW_FILES_DIRECTORY}/{year}")
    #
    # if uploader.check_connection():
    #     uploader.upload_multiple((file for file in list_of_files))
    #     logging.info(f"All files for YEAR %s from %s was uploaded to MinIO bucket %s",
    #                  year, f"data/raw/{year}", f"{uploader.BUCKET_NAME}")
    uploader.upload_single(filepath)

default_args = {
    'owner': 'ntrg',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def create_my_dag(**kwargs):
    with DAG(
        dag_id=kwargs['dag_id'],
        default_args=default_args,
        start_date=kwargs['start_date'],
        schedule_interval=kwargs['schedule_interval'],
        catchup=kwargs['catchup'],
        tags=['weather', 'data-ingestion', 'minio', kwargs['tags']],
        max_active_runs=2
    ) as dag:
        year = '{{ data_interval_start.strftime("%Y") }}'

        task1 = PythonOperator(
            task_id='Download',
            python_callable=download,
            provide_context=True
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
                set -euo pipefail  # Exit on any error

                # Get the list of files without extension
                FILES=$(find {RAW_FILES_DIRECTORY}/{year} -type f ! -name "*.*")

                parallel -j 12 '
                    BASENAME=$(basename {{}})
                    PREFIX=${{BASENAME//-{year}/}}

                    TMPFILE=$(mktemp)

                    awk -v prefix="$PREFIX" \\
                        "BEGIN {{ OFS=\\",\\" }}
                        {{
                            for (i=1; i<=NF; i++) {{
                                if (\\$i == \\"-9999\\") \\$i = \\"null\\"
                            }}
                            print prefix, \\$0
                        }}" {{}} > $TMPFILE
                    bzip2 -c $TMPFILE > {{}}.csv.bz2
                    rm -f $TMPFILE
                    rm -f {{}}
                ' ::: $FILES
            """
        )

        task4 = BashOperator(
            task_id='CombineAndCompress',
            do_xcom_push=False,
            bash_command=f"""
                set -euo pipefail
        
                YEAR_DIR="{RAW_FILES_DIRECTORY}/{year}"
                FINAL_FILE="{RAW_FILES_DIRECTORY}/{year}.csv.bz2"
                
                TEMP_LIST=$(mktemp)
                find "$YEAR_DIR" -name "*.csv.bz2" | sort > "$TEMP_LIST"
                
                # Direct concatenation of bz2 files
                cat $(cat "$TEMP_LIST") > "$FINAL_FILE"
                
                # Clean up temp file
                rm -f "$TEMP_LIST"
                """
        )


        task5 = PythonOperator(
            task_id='Upload',
            python_callable=upload,
            provide_context=True
        )

        task6 = BashOperator(
            task_id='Cleanup',
            bash_command=f"""
            rm -rf {RAW_FILES_DIRECTORY}/{year}.csv.bz2
            rm -rf {RAW_FILES_DIRECTORY}/{year}
            """
        )

        if kwargs['dag_id'] == 'DailyData':
            trigger_process_daily = TriggerDagRunOperator(
                task_id="TriggerProcessDaily",
                trigger_dag_id="ProcessDailyData",
                execution_date="{{ ds }}",
                reset_dag_run=True,
                wait_for_completion=False,
            )
            task6 >> trigger_process_daily

    chain(task1, task2, task3, task4, task5, task6)
    return dag

# Create DAGs with different schedule intervals
dag_daily = create_my_dag(dag_id='DailyData', start_date=days_ago(1), schedule_interval='@daily', catchup=False, tags='daily')
dag_yearly = create_my_dag(dag_id='HistoricalData', start_date=datetime(2010, 1, 1), schedule_interval='@yearly', catchup=True, tags='yearly')
