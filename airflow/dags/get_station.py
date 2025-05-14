# Airflow modules
import gzip

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# External Modules
from ingest import downloader
from datetime import timedelta, datetime
import logging
import os
import psycopg2

# Global variables
DATA_DIR = f"{downloader.airflow_dir}/data/station"
CONN = psycopg2.connect(**{
    "host": "postgres",
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD'],
    "database": os.environ['POSTGRES_DB']
})

# Local variables
STATION_URL="https://noaa-isd-pds.s3.amazonaws.com/isd-history.csv"
HISTORY_URL="https://noaa-isd-pds.s3.amazonaws.com/isd-inventory.csv.z"
STATION_FILE=f"${DATA_DIR}/isd-history.csv"
HISTORY_FILE=f"${DATA_DIR}/isd-inventory.csv.z"
STATION_DATA=f"${DATA_DIR}/station.csv.gz"
HISTORY_DATA=f"${DATA_DIR}/history.csv.gz"

def check_exist_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def ingest_station(db):
    """
    Creates and populates a temporary database table for station data.

    This function handles creating a database table and populates it with data
    from a compressed CSV file. The data is copied into the database table
    't_station' located in the 'isd' schema. The table is dropped if it
    already exists, re-created, and populated from the CSV file. It uses a
    temporary table configuration during the operation.

    :param db: Database connection object used to execute SQL statements and
               manage the database connection.
    :type db: object
    :return: None
    :rtype: None
    """
    cursor = db.cursor()
    logging.info("Cursor created")

    # Create the temporary table
    table_name = "tmp_station"
    with db:
        cursor.execute(
            f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
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
            );
            """
        )
        logging.info("Temporary table %s created", table_name)

    # Copy the content of station data into the temp table
    gz_path = f"{DATA_DIR}/station.csv.gz"
    with gzip.open(gz_path, mode='rt', encoding='utf-8') as f:
        cursor.copy_expert(
            """
            COPY t_station 
            FROM STDIN 
            WITH (FORMAT CSV, HEADER, FORCE_NULL (usaf,wban,name,ctry,st,icao,lat,lon,elev,begin_date,end_date))
            """,
            f
        )

        CONN.commit()
        cursor.close()
        CONN.close()

def ingest_history(db):
    """
    Ingest data from a compressed CSV file into a database by creating a temporary table,
    loading the CSV data into it, and closing the database connection upon completion.

    The function first drops an existing table if it exists, creates a new table to temporarily
    store the data, and then uses the compressed CSV file to populate the table using the COPY command.

    :param db: Database connection object used for creating and managing the database cursor
        to execute queries.
    :type db: Any
    :return: None
    :rtype: None
    """
    cursor = db.cursor()
    logging.info("Cursor created")

    # Create the temporary table
    table_name = "tmp_history"
    with db:
        cursor.execute(
            f"""
                DROP TABLE IF EXISTS ${table_name};
                CREATE TABLE {table_name} (
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
                );
                """
        )
        logging.info("Temporary table %s created", table_name)

    # Copy the content of history.csv.gz file into temp table
    gz_path = f"{DATA_DIR}/history.csv.gz"
    with gzip.open(gz_path, mode='rt', encoding='utf-8') as f:
        cursor.copy_expert(
            f"""
            COPY {table_name} 
            FROM STDIN 
            WITH (FORMAT CSV, HEADER, FORCE_NULL (usaf,wban,year,m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12))
            """,
            f
        )

        CONN.commit()
        cursor.close()
        CONN.close()

def upsert_station(db):
    """
    Upserts station data from a temporary table to the main station table in the database.

    Summarizes and processes data in the specified temporary table, `tmp_station`,
    by inserting it into the `station` table. Afterwards, the temporary table is
    dropped. The function ensures that the data is properly transformed and that
    longitude-latitude pairs are stored as geospatial points, with additional
    handling for nullable elevation and date duration ranges.

    :param db: Database connection instance to facilitate data manipulation.
    :type db: Any
    :return: None
    :rtype: None
    """
    cursor = db.cursor()

    # Modify and upsert to real table
    table_name = "tmp_station"
    with db:
        logging.info("Summarizing the content of `tmp_station`")
        cursor.execute(
            f"""
            TRUNCATE {table_name};
            INSERT INTO station(station, name, country, province, icao, location, elevation, period)
            SELECT (usaf || wban)::VARCHAR(12)                                      AS station,
                   name::VARCHAR(32),
                   ctry::VARCHAR(2)                                                 AS country,
                   st::VARCHAR(2)                                                   AS province,
                   icao::VARCHAR(4),
                   ST_SetSRID(ST_Point(lon::numeric, lat::numeric), 4326)           AS location,
                   CASE WHEN elev ~ '-0999' THEN NULL ELSE elev::NUMERIC::FLOAT END AS elevation,
                   daterange(begin_date::DATE, end_date::DATE, '[]')                AS duration
            FROM {table_name};
            """
        )
        CONN.commit()

        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        CONN.commit()
        logging.info(f"`{table_name}` table deleted")

def upsert_history(db):
    cursor = db.cursor()

    # Modify and upsert to real table
    table_name = "tmp_history"
    with db:
        logging.info("Summarizing the content of `tmp_history`")
        cursor.execute(
            f"""
            TRUNCATE {table_name};
            INSERT INTO history(station, year, country, active_month, total, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12)
            SELECT i.station,year,country,active_month,total,m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12
            FROM (SELECT usaf || wban                                                 AS station,
                         make_date(year, 1, 1)::DATE                                  AS year,
                         m1 + m2 + m3 + m4 + m5 + m6 + m7 + m8 + m9 + m10 + m11 + m12 AS total,
                         m1::BOOLEAN::INT + m2::BOOLEAN::INT + m3::BOOLEAN::INT + m4::BOOLEAN::INT + m5::BOOLEAN::INT +
                         m6::BOOLEAN::INT + m7::BOOLEAN::INT + m8::BOOLEAN::INT + m9::BOOLEAN::INT + m10::BOOLEAN::INT +
                         m11::BOOLEAN::INT + m12::BOOLEAN::INT                        AS active_month,
                         m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12
                  FROM {table_name}
                  ORDER BY 1, 2
                 ) i,
                 LATERAL (SELECT coalesce(country, 'NA') AS country FROM station h WHERE h.station = i.station) res;
            """
        )
        CONN.commit()

        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        CONN.commit()
        logging.info(f"`{table_name}` table deleted")

################# DAG #################

# DAG configuration
reload_station_workflow = DAG(
    "ReloadStationData",
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=60),
    start_date=datetime(2025, 5, 13),
    catchup=False
)

with reload_station_workflow:

    task1 = PythonOperator(
        task_id="CheckDir",
        python_callable=check_exist_dir,
    )

    # Download the objects from the S3 Bucket
    task2 = BashOperator(
        task_id="DownloadData",
        do_xcom_push=False,
        bash_command=f"""
        function log_info() {{
            echo "[INFO] $1"
        }}
    
        function log_warn() {{
            echo "[WARN] $1"
        }}
    
        function log_error() {{
            echo "[ERROR] $1"
        }}
    
        function get_file_size() {{
            local filepath=${{1-}}
            local size=0
            if [[ -z $filepath ]] || [[ ! -f $filepath ]]; then
                echo "0"
                return 0
            fi
            if [ "$(uname)" == "Darwin" ]; then
                size=$(stat -f '%z' $filepath)
            else
                size=$(stat -c '%s' $filepath)
            fi
            echo $size
            return 0
        }}
    
        function download_file() {{
            local data_url=$1
            local data_file=$2
    
            if [[ -f ${{data_file}} ]]; then
                size=$(get_file_size ${{data_file}})
                log_warn "data file exists, size: ${{size}}, path: ${{data_file}}"
                curl_size=$(curl -sI ${{data_url}} | grep -i 'Content-Length' | awk '{{print $2}}' | tr -d '\r')
                
                if [[ ${{size}} -eq ${{curl_size}} ]]; then
                    log_info "data url file has same size: ${{curl_size}}, skip downloading ${{data_url}}"
                    return 10
                else
                    log_warn "data url file size ${{curl_size}} != local ${{size}}, downloading ${{data_url}}"
                fi
            fi
            
            log_info "downloading ${{data_url}} to ${{data_file}}"
            mkdir -p $(dirname ${{data_file}})
            curl ${{data_url}} -o ${{data_file}}
            return $?
        }}
    
        download_file "{STATION_URL}" "{STATION_FILE}"
        download_file "{HISTORY_URL}" "{HISTORY_FILE}"
        
        log_info "generate station.csv.gz from isd-history.csv"
        cat "{STATION_FILE}" | gzip --best > {STATION_DATA}
        
        log_info "generate history.csv.gz from isd-inventory.csv.z"
        cat "{HISTORY_FILE}" | gzip -d | gzip --best > {HISTORY_DATA}
        
        log_info "station data generated @ {DATA_DIR}"
        ls -l {DATA_DIR}/*.csv.gz
        """
    )

    # Extract the content(s) of .gz archives
    task3 = BashOperator(
        task_id="Archive",
        do_xcom_push=False,
        bash_command=f"""
        cat "${STATION_FILE}" | gzip --best > ${STATION_DATA}
        
        cat "${HISTORY_FILE}" | gzip -d | gzip --best > ${HISTORY_DATA}     
        """
    )

    # Ingest the content of the .gz files into "temporary" table
    task4 = PythonOperator(
        task_id="IngestStationData",
        python_callable=ingest_station,
        op_kwargs={
            "db": CONN
        }
    )

    # Ingest the content of the .gz files into "temporary" table
    task5 = PythonOperator(
        task_id="IngestHistoryData",
        python_callable=ingest_station,
        op_kwargs={
            "db": CONN
        }
    )

    task6 = PythonOperator(
        task_id="UpsertStationData",
        python_callable=upsert_station,
        op_kwargs={
            "db": CONN
        }
    )

    task7 = PythonOperator(
        task_id="UpsertHistoryData",
        python_callable=upsert_history,
        op_kwargs={
            "db": CONN
        }
    )

    task1 >> task2 >> task3 >> [task4, task5]
    task4 >> task6
    task5 >> task7