#Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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

    list_of_files = uploader.list_files(f"{RAW_FILES_DIRECTORY}/{year}")

    if uploader.check_connection():
        uploader.upload_multiple((file for file in list_of_files))
        logging.info(f"All files for YEAR %s from %s was uploaded to MinIO bucket %s",
                     year, f"data/raw/{year}", f"{uploader.BUCKET_NAME}")

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
        max_active_runs=1
    ) as dag:
        year = '{{ data_interval_start.strftime("%Y") }}'

        start = BashOperator(
            task_id='Start',
            bash_command='echo "Start downloading historical data"',
        )

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

                    # After successful compression, remove original files
                    echo "Removing original files..."
                    find {RAW_FILES_DIRECTORY}/{year} -type f ! -name "*.csv.bz2" -delete

                    # Report summary
                    COMPRESSED_COUNT=$(find {RAW_FILES_DIRECTORY}/{year} -name "*.csv.bz2" | wc -l)
                    echo "Cleanup complete. $COMPRESSED_COUNT compressed files remain in {RAW_FILES_DIRECTORY}/{year}."
                """
        )

        task4 = BashOperator(
            task_id='CombineFiles',
            do_xcom_push=False,
            bash_command=f"""
                TARGET_DIR="{RAW_FILES_DIRECTORY}/{year}"
                TMP_DIR="$TARGET_DIR/tmp_combine"
                TARGET_SIZE_MB=128

                mkdir -p "$TMP_DIR"
                cd "$TARGET_DIR"

                echo "📦 Decompressing all .csv.bz2 into one file..."
                bzcat *.csv.bz2 > "$TMP_DIR/all_data.csv"

                echo "📊 Counting total lines..."
                TOTAL_LINES=$(wc -l < "$TMP_DIR/all_data.csv")
                echo "Total lines: $TOTAL_LINES"

                # Estimate compression ratio (adjust this if needed)
                COMPRESSION_RATIO=0.25  # avg bzip2 ratio
                BYTES_PER_LINE=$(wc -c < "$TMP_DIR/all_data.csv")
                BYTES_PER_LINE=$(($BYTES_PER_LINE / $TOTAL_LINES))
                LINES_PER_CHUNK=$(( ($TARGET_SIZE_MB * 1024 * 1024) / ($BYTES_PER_LINE * $COMPRESSION_RATIO) ))

                echo "Estimated lines per 128MB chunk: $LINES_PER_CHUNK"

                echo "🚀 Splitting file..."
                split -l $LINES_PER_CHUNK "$TMP_DIR/all_data.csv" "$TMP_DIR/chunk_"

                echo "🗜 Compressing chunks..."
                for chunk in "$TMP_DIR"/chunk_*; do
                    bzip2 -c "$chunk" > "$TARGET_DIR/combined_$(basename $chunk).csv.bz2"
                done

                echo "🧹 Cleaning up..."
                rm -rf "$TMP_DIR"
                find "$TARGET_DIR" -maxdepth 1 -name "*.csv.bz2" -not -name "combined_*" -delete

                echo "✅ Done. Final combined files:"
                du -sh "$TARGET_DIR"/combined_*.csv.bz2
            """
        )

        task5 = PythonOperator(
            task_id='Upload',
            python_callable=upload,
            provide_context=True
        )

        task6 = BashOperator(
            task_id='Cleanup',
            bash_command=f"rm -rf {RAW_FILES_DIRECTORY}/{year}"
        )

        end = BashOperator(
            task_id='End',
            bash_command='echo "Upload raw data to MinIO successfully!"',
        )

    chain(start, task1, task2, task3, task4, task5, task6, end)
    return dag

# Create DAGs with different schedule intervals
dag_daily = create_my_dag(dag_id='DailyData', start_date=days_ago(1), schedule_interval='@daily', catchup=False, tags='daily')
dag_yearly = create_my_dag(dag_id='HistoricalData', start_date=datetime(2010, 1, 1), schedule_interval='@yearly', catchup=True, tags='yearly')
