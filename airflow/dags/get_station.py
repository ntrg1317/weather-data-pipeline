import gzip
import logging
import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from ingest import downloader, postgresDb

# Global variables
DATA_DIR = f"{downloader.airflow_dir}/data/station"
STATION_URL = "https://noaa-isd-pds.s3.amazonaws.com/isd-history.csv"
HISTORY_URL = "https://noaa-isd-pds.s3.amazonaws.com/isd-inventory.csv.z"
STATION_FILE = f"{DATA_DIR}/isd-history.csv"
HISTORY_FILE = f"{DATA_DIR}/isd-inventory.csv.z"
STATION_DATA = f"{DATA_DIR}/station.csv.gz"
HISTORY_DATA = f"{DATA_DIR}/history.csv.gz"

CONN = postgresDb.PostgresDb()

def check_exist_dir():
    """Create data directory if it doesn't exist."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def download_file(data_url, data_file):
    """Download file from URL if not already downloaded or if it differs in size."""
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
    """Generate compressed .csv.gz files from downloaded .csv files."""
    os.system(f"cat {STATION_FILE} | gzip --best > {STATION_DATA}")
    os.system(f"cat {HISTORY_FILE} | gzip -d | gzip --best > {HISTORY_DATA}")
    logging.info("Generated compressed files: %s, %s", STATION_DATA, HISTORY_DATA)

def ingest_station():
    """Ingest station data into a temporary table."""
    table_name = "isd.tmp_station"
    CONN.drop_table(table_name)
    schema = ("""
                usaf       TEXT,
                wban       TEXT,
                name       TEXT,
                ctry       TEXT,
                st         TEXT,
                icao       TEXT,
                lat        TEXT,
                lon        TEXT,
                elev       TEXT,
                begin_date DATE,
                end_date   DATE
            """)
    CONN.create_table(table_name, schema)
    # Copy the content of station data into the temp table
    gz_path = STATION_DATA
    with gzip.open(gz_path, mode='rt', encoding='utf-8') as f:
        CONN.copy_expert(
            f"""
            COPY {table_name} 
            FROM STDIN 
            WITH (FORMAT CSV, HEADER, FORCE_NULL (usaf,wban,name,ctry,st,icao,lat,lon,elev,begin_date,end_date))
            """,
            f
        )

    CONN.close()

def ingest_history():
    """Ingest history data into a temporary table."""
    table_name = "isd.tmp_history"
    CONN.drop_table(table_name)
    schema = ("""
                usaf VARCHAR(6),
                wban VARCHAR(5),
                year INTEGER,
                m1   INTEGER,
                m2   INTEGER,
                m3   INTEGER,
                m4   INTEGER,
                m5   INTEGER,
                m6   INTEGER,
                m7   INTEGER,
                m8   INTEGER,
                m9   INTEGER,
                m10  INTEGER,
                m11  INTEGER,
                m12  INTEGER,
                PRIMARY KEY (usaf, wban, year) 
                """)

    CONN.create_table(table_name, schema)

    gz_path = HISTORY_DATA
    with gzip.open(gz_path, mode='rt', encoding='utf-8') as f:
        CONN.copy_expert(
            f"""
            COPY {table_name} 
            FROM STDIN 
            WITH (FORMAT CSV, HEADER, FORCE_NULL (usaf,wban,year,m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12))
            """,
            f
        )
    CONN.close()

def upsert_data(table_name, tmp_table_name, query):
    """Upsert data from a source table to a target table."""
    CONN.truncate_table(table_name)
    CONN.execute_query(query)
    CONN.drop_table(tmp_table_name)
    logging.info(f"`{table_name}` table deleted")
    CONN.close()

# Define the DAG
reload_station_workflow = DAG(
    "ReloadStationData",
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=60),
    start_date=datetime(2025, 5, 13),
    catchup=False
)

with reload_station_workflow:

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
    task4 = PythonOperator(
        task_id="GenerateCompressedFiles",
        python_callable=generate_compressed_files,
    )

    # Task 4: Ingest station data into temporary table
    task5 = PythonOperator(
        task_id="IngestStationData",
        python_callable=ingest_station,
    )

    # Task 5: Ingest history data into temporary table
    task6 = PythonOperator(
        task_id="IngestHistoryData",
        python_callable=ingest_history,
    )

    # Task 6: Upsert station data into the main table
    task7 = PythonOperator(
        task_id="UpsertStationData",
        python_callable=upsert_data,
        op_kwargs={
            "table_name": "isd.station",
            "tmp_table_name": "isd.tmp_station",
            "query": f"""
                    INSERT INTO isd.station(station, name, country, province, icao, location, elevation, period)
                    SELECT (usaf || wban)::VARCHAR(12)                                      AS station,
                           name::VARCHAR(32),
                           ctry::VARCHAR(2)                                                 AS country,
                           st::VARCHAR(2)                                                   AS province,
                           icao::VARCHAR(4),
                           ST_SetSRID(ST_Point(lon::numeric, lat::numeric), 4326)           AS location,
                           CASE WHEN elev ~ '-0999' THEN NULL ELSE elev::NUMERIC::FLOAT END AS elevation,
                           daterange(begin_date::DATE, end_date::DATE, '[]')                AS duration
                    FROM isd.tmp_station;
                    """
        },
    )

    # Task 7: Upsert history data into the main table
    task8 = PythonOperator(
        task_id="UpsertHistoryData",
        python_callable=upsert_data,
        op_kwargs={
            "table_name": "isd.history",
            "tmp_table_name": "isd.tmp_history",
            "query": f"""
                    INSERT INTO isd.history(station, year, country, active_month, total, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12)
                    SELECT i.station,year,country,active_month,total,m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12
                    FROM (SELECT usaf || wban                                                 AS station,
                                 make_date(year, 1, 1)::DATE                                  AS year,
                                 m1 + m2 + m3 + m4 + m5 + m6 + m7 + m8 + m9 + m10 + m11 + m12 AS total,
                                 m1::BOOLEAN::INT + m2::BOOLEAN::INT + m3::BOOLEAN::INT + m4::BOOLEAN::INT + m5::BOOLEAN::INT +
                                 m6::BOOLEAN::INT + m7::BOOLEAN::INT + m8::BOOLEAN::INT + m9::BOOLEAN::INT + m10::BOOLEAN::INT +
                                 m11::BOOLEAN::INT + m12::BOOLEAN::INT                        AS active_month,
                                 m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12
                          FROM isd.tmp_history
                          ORDER BY 1, 2
                         ) i,
                         LATERAL (SELECT coalesce(country, 'NA') AS country FROM isd.station h WHERE h.station = i.station) res;
                    """
        }
    )

    # Task dependencies
    task1 >> [task2, task3] >> task4 >> [task5, task6]
    task5>> task7
    task6 >> task8
