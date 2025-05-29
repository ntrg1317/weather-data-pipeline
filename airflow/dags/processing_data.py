import os, logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

SECURE_CONNECT_BUNDLE_PATH = "/opt/airflow/spark/data/secure-connect-weather-cluster.zip"
SECURE_CONNECT_BUNDLE = os.environ.get("SECURE_CONNECT_BUNDLE", "secure-connect-weather-cluster.zip")
CLIENT_ID = os.environ.get("ASTRA_CLIENT_ID")
CLIENT_SECRET = os.environ.get("ASTRA_CLIENT_SECRET")
ASTRA_KEYSPACE = os.environ.get("ASTRA_KEYSPACE")

# def delete_duplicates(data_interval_start):
#     auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
#     cluster = Cluster(cloud={'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH}, auth_provider=auth_provider)
#     session = cluster.connect()
#     session.set_keyspace(ASTRA_KEYSPACE)
#
#     year = data_interval_start.strftime('%Y')
#     try:
#         # You must have an index on year or know common station_ids
#         query = f"SELECT wsid FROM hourly WHERE year = {year} ALLOW FILTERING"
#         stmt = SimpleStatement(query, fetch_size=100)
#         rows = session.execute(stmt)
#         wsids = set(row.wsid for row in rows)
#     except Exception as e:
#         logging.error(f"Failed to check existing data for year {year}: {e}")
#         session.shutdown()
#         return
#
#     if not wsids:
#         logging.info(f"No data to delete for year {year}")
#         session.shutdown()
#         return
#
#     try:
#         for wsid in wsids:
#             delete_query = f"DELETE FROM hourly WHERE wsid = %s AND year = %s"
#             session.execute(delete_query, (wsid, int(year)))
#         logging.info(f"Deleted existing data for year {year} for {len(wsids)} stations")
#     except Exception as e:
#         logging.error(f"Failed to delete data: {e}")
#     finally:
#         session.shutdown()

default_args = {
    'owner': 'ntrg',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'email_on_failure': True,
    'email_on_retry': True,
    'execution_timeout': timedelta(hours=2),
    'depends_on_past': False
}

process_data_workflow = DAG(
    'ProcessData',
    default_args=default_args,
    description='Process historical data workflow',
    start_date=datetime(2010, 1, 1),
    schedule_interval='@yearly',
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'data-processing', 'spark']
)

with process_data_workflow:
    year = '{{ data_interval_start.strftime("%Y") }}'

    pre_check = BashOperator(
        task_id='Pre-flightCheck',
        bash_command='''
            echo "=== Pre-flight Checks ==="
            echo "Execution Date: {{ ds }}"
            echo "Logical Date: {{ logical_date }}"
            echo "Next Execution Date: {{ next_ds }}"
            echo "Secure Bundle Path: {{ params.secure_bundle_path }}"

            # Check if secure bundle exists
            if [ -f "{{ params.secure_bundle_path }}" ]; then
                echo "✓ Secure bundle found"
            else
                echo "✗ Secure bundle not found at {{ params.secure_bundle_path }}"
                exit 1
            fi

            echo "=== Checks Complete ==="
            ''',
        params={
            'secure_bundle_path': SECURE_CONNECT_BUNDLE_PATH
        }
    )

    # delete_duplicates = PythonOperator(
    #     task_id='DeleteDuplicates',
    #     python_callable=delete_duplicates,
    #     provide_context=True,
    # )
    
    ingest_data = SparkSubmitOperator(
        task_id='IngestRawData',
        application='app/process_raw_data.py',
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
            "com.github.jnr:jnr-posix:3.1.15"
        ),
        conf={
            'spark.files': 'spark/data/secure-connect-weather-cluster.zip',
            'spark.cassandra.connection.config.cloud.path': 'secure-connect-weather-cluster.zip',
            'spark.driver.memory': '4g',
            'spark.executor.memory': '3g',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.files.maxPartitionBytes': '134217728',
        },
        conn_id='spark_conn',
        verbose=True,
        application_args=[
            '--year', '{{ data_interval_start.strftime("%Y") }}'
        ],
    )

    # data_quality_check = BashOperator(
    #     task_id='DataQualityCheck',
    #     bash_command='''
    #         echo "=== Data Quality Checks ==="
    #         echo "Checking processed data for {{ ds }}"
    #
    #         # Add your data quality checks here
    #         # Example: Check if output files exist, row counts, etc.
    #
    #         echo "✓ Data quality checks passed"
    #         ''',
    # )

    # Cleanup and notification
    cleanup = BashOperator(
        task_id='Cleanup',
        bash_command='''
            echo "=== Processing Complete ==="
            echo "Execution Date: {{ ds }}"
            echo "Start Time: {{ task_instance.start_date }}"
            echo "End Time: $(date)"
            echo "DAG Run ID: {{ dag_run.run_id }}"

            # Calculate actual duration
            START_TIMESTAMP=$(date -d "{{ task_instance.start_date }}" +%s)
            END_TIMESTAMP=$(date +%s)
            DURATION=$((END_TIMESTAMP - START_TIMESTAMP))

            echo "Total Duration: ${DURATION} seconds"
            echo "Status: SUCCESS"

            # Optional: Send notification to Slack/Teams/Email
            # curl -X POST -H 'Content-type: application/json' \
            #   --data '{"text":"Weather data processing completed for {{ ds }}"}' \
            #   YOUR_SLACK_WEBHOOK_URL
            ''',
    )

    # Task dependencies
    pre_check >> delete_duplicates >> ingest_data  >> cleanup