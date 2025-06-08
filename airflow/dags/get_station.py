import gzip
import logging
import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook

from ingest import downloader

# Global variables
DATA_DIR = f"{downloader.airflow_dir}/data/station"
STATION_URL = "https://noaa-isd-pds.s3.amazonaws.com/isd-history.csv"
HISTORY_URL = "https://noaa-isd-pds.s3.amazonaws.com/isd-inventory.csv.z"
STATION_FILE = f"{DATA_DIR}/isd-history.csv"
HISTORY_FILE = f"{DATA_DIR}/isd-inventory.csv.z"
STATION_DATA = f"{DATA_DIR}/station.csv.gz"
HISTORY_DATA = f"{DATA_DIR}/history.csv.gz"

def check_exist_dir():
    """
    Ensures that a specified directory exists. If the directory does not exist,
    it is created. The function performs a check using the given directory path
    and creates the directory if necessary.

    :raises OSError: If the directory creation fails due to system-related issues.
    """
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def download_file(data_url, data_file):
    """
    Download a file from a provided URL and save it to a specified location on the
    local filesystem. If the file already exists and its size matches the remote
    file size, the download will be skipped.

    :param data_url: The URL of the file to download.
    :type data_url: str
    :param data_file: The local file path to save the downloaded content.
    :type data_file: str
    :return: None
    :rtype: None
    """
    if os.path.exists(data_file):
        local_size = os.path.getsize(data_file)
        remote_size = int(os.popen(f"curl -sI {data_url} | grep -i 'Content-Length' | awk '{{print $2}}'").read())
        if local_size == remote_size:
            logging.info("File already exists with the same size. Skipping download.")
            return

    logging.info("Downloading file from %s to %s", data_url, data_file)
    os.makedirs(os.path.dirname(data_file), exist_ok=True)
    os.system(f"curl -s {data_url} -o {data_file}")

def generate_compressed_files():
    os.system(f"cat {STATION_FILE} | gzip --best > {STATION_DATA}")
    os.system(f"cat {HISTORY_FILE} | gzip -d | gzip --best > {HISTORY_DATA}")
    logging.info("Generated compressed files: %s, %s", STATION_DATA, HISTORY_DATA)

def setup():
    hook = CassandraHook(cassandra_conn_id='cassandra_conn')
    session = hook.get_conn()

    station_schema = f"{downloader.airflow_dir}/cassandra/station.cql"
    with open(station_schema, 'r') as f:
        statements = f.read().split(';')
        for statement in statements:
            if statement.strip():
                session.execute(statement)

    session.shutdown()

def ingest_data(table, query, filepath):
    """Ingest station data into a temporary table."""
    hook = CassandraHook(cassandra_conn_id='cassandra_conn')
    session = hook.get_conn()

# Define the DAG

default_args = {
    'owner': 'ntrg',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

reload_station_workflow = DAG(
    "ReloadStationData",
    default_args=default_args,
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=60),
    start_date=datetime(2025, 5, 13),
    catchup=False
)

with (reload_station_workflow):

    # Task 1: Check if the data directory exists
    task1 = PythonOperator(
        task_id="CheckDir",
        python_callable=check_exist_dir,
    )

    # Task 2: Download station and history data
    task2 = PythonOperator(
        task_id="DownloadStationData",
        python_callable=download_file,
        op_args=[STATION_URL, STATION_FILE],
    )

    task3 = PythonOperator(
        task_id="DownloadHistoryData",
        python_callable=download_file,
        op_args=[HISTORY_URL, HISTORY_FILE]
    )

    # Task 3: Generate .csv.gz files from downloaded files
    task4 = BashOperator(
        task_id="GenerateCompressedFiles",
        bash_command=f"""
            cat {STATION_FILE} | gzip --best > {STATION_DATA}
            cat {HISTORY_FILE} | gzip -d | gzip --best > {HISTORY_DATA}
            echo "Generated compressed files: {STATION_DATA}, {HISTORY_DATA}"
        """
    )

    # Task 4: Ingest station data into temporary table
    task5 = BashOperator(
        task_id="TransformStationData",
        bash_command=f"""
            zcat {STATION_DATA} | \
            awk -F',' 'BEGIN{{OFS=","}} 
            NR==1 {{ print "wsid,name,country,province,icao,latitude,longitude,elevation,begin_date,end_date"; next }}
            {{ 
                for (i=1; i<=NF; i++) gsub(/"/, "", $i);
                wsid = $1 "-" $2;
                begin_date = substr($10, 1, 4) "-" substr($10, 5, 2) "-" substr($10, 7, 2)
                end_date = substr($11, 1, 4) "-" substr($11, 5, 2) "-" substr($11, 7, 2)
    
                elevation = ($9 == "-0999" ? "null" : $9);
                printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\\n", \
                    wsid, $3, $4, $5, $6, $7, $8, elevation, begin_date, end_date
            }}' | \
            gzip --best > {DATA_DIR}/tmp_station.csv.gz && \
            mv {DATA_DIR}/tmp_station.csv.gz {STATION_DATA}
        """
    )

    task6 = BashOperator(
        task_id="TransformHistoryData",
        bash_command=f"""
            zcat {HISTORY_DATA} | \
            awk -F',' 'BEGIN{{OFS=","}}
            NR==1 {{ print "wsid,year,total,active_month,m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12"; next }}
            {{
                for (i=1; i<=NF; i++) gsub(/"/, "", $i);
                wsid = $1 "-" $2;
                year = $3;

                total = 0;
                active_month = 0;
                for (i=4; i<=15; i++) {{
                    val = ($i == "" ? 0 : $i);
                    total += val;
                    active_month += (val != 0 && val != "0" ? 1 : 0);
                }}

                printf "%s,%s,%d,%d", wsid, year, total, active_month;
                for (i=4; i<=15; i++) printf ",%s", $i;
                print ""
            }} ' | \
            gzip --best > {DATA_DIR}/tmp_history.csv.gz && \
            mv {DATA_DIR}/tmp_history.csv.gz {HISTORY_DATA}
        """
    )

    task7 = PythonOperator(
        task_id="CheckCassandraSetup",
        python_callable=setup,
    )

    # Task 6: Upsert station data into the main table
    task8= BashOperator(
        task_id="IngestStationData",
        bash_command=f"""
            gzip -dc {STATION_DATA} > {DATA_DIR}/tmp_station.csv && \
            cqlsh cassandra -k weather -e "
                COPY station (wsid, name, country, province, icao, latitude, longitude, elevation, begin_date, end_date) 
                FROM '{DATA_DIR}/tmp_station.csv' 
                WITH DELIMITER = ',' AND HEADER = TRUE
            "
        """,
    )

    task9 = BashOperator(
        task_id="IngestHistoryData",
        bash_command=f"""
            gzip -dc {HISTORY_DATA} > {DATA_DIR}/tmp_history.csv && \
            cqlsh cassandra -k weather -e "
                COPY history (
                    wsid, year, total, active_month,
                    m1, m2, m3, m4, m5, m6,
                    m7, m8, m9, m10, m11, m12
                )
                FROM '{DATA_DIR}/tmp_history.csv' 
                WITH DELIMITER = ',' AND HEADER = TRUE
            "
        """,
    )

    # Task dependencies
    task1 >> [task2, task3] >> task4 >> [task5, task6] >> task7 >> [task8, task9]
