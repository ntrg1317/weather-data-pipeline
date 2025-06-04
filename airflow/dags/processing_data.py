import os, logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# SECURE_CONNECT_BUNDLE_PATH = "/opt/airflow/spark/data/secure-connect-weather-cluster.zip"
# SECURE_CONNECT_BUNDLE = os.environ.get("SECURE_CONNECT_BUNDLE", "secure-connect-weather-cluster.zip")
# CLIENT_ID = os.environ.get("ASTRA_CLIENT_ID")
# CLIENT_SECRET = os.environ.get("ASTRA_CLIENT_SECRET")
# ASTRA_KEYSPACE = os.environ.get("ASTRA_KEYSPACE")

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST")
CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT")

def setup():
    try:
        hook = CassandraHook(cassandra_conn_id='cassandra_conn')
        session = hook.get_conn()

        # Create keyspace
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS weather
        WITH REPLICATION = { 
            'class' : 'SimpleStrategy', 
            'replication_factor' : 1 
            }
        """)

        session.set_keyspace('weather')

        session.execute("""
        CREATE TABLE IF NOT EXISTS hourly (
            wsid text,
            year int,
            month int,
            day int,
            hour int,
            temperature double,
            dewpoint double,
            pressure double,
            wind_direction int,
            wind_speed double,
            sky_condition int,
            one_hour_precip double,
            six_hour_precip double,
            PRIMARY KEY ((wsid), year, month, day, hour)
        ) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);
        """)

        logging.info("Cassandra keyspace and table setup completed")
    except Exception as e:
        logging.error("Error setting up Cassandra keyspace and table: %s", e)
        raise

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
    catchup=True,
    max_active_runs=1,
    tags=['weather', 'data-processing', 'spark']
)

with process_data_workflow:
    year = '{{ data_interval_start.strftime("%Y") }}'

    start = BashOperator(
        task_id='Start',
        bash_command='''
            echo "=== Start ingest raw data to cassandra ==="
            echo "Execution Date: {{ ds }}"
            echo "Logical Date: {{ logical_date }}"
            echo "Next Execution Date: {{ next_ds }}"
            '''
    )

    setup = PythonOperator(
        task_id='SetupCassandra',
        python_callable=setup,
    )
    
    ingest_data = SparkSubmitOperator(
        task_id='IngestRawData',
        application='app/process_raw_data.py',
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
            "com.github.jnr:jnr-posix:3.1.15"
        ),
        conf={
            # 'spark.files': 'spark/data/secure-connect-weather-cluster.zip',
            # 'spark.cassandra.connection.config.cloud.path': 'secure-connect-weather-cluster.zip',
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.files.maxPartitionBytes': '134217728',
            'spark.cassandra.connection.host': 'cassandra',
            'spark.cassandra.connection.port': '9042',
        },
        conn_id='spark_conn',
        verbose=True,
        application_args=[
            '--year', '{{ data_interval_start.strftime("%Y") }}'
        ],
    )

    daily = SparkSubmitOperator(
        task_id='Daily',
        application='app/daily.py',
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
            "com.github.jnr:jnr-posix:3.1.15"
        ),
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.files.maxPartitionBytes': '134217728',
            'spark.cassandra.connection.host': 'cassandra',
            'spark.cassandra.connection.port': '9042',
        },
        conn_id='spark_conn',
        verbose=True,
        application_args=[
            '--year', '{{ data_interval_start.strftime("%Y") }}'
        ],
    )

    monthly = SparkSubmitOperator(
        task_id='Monthly',
        application='app/monthly.py',
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
            "com.github.jnr:jnr-posix:3.1.15"
        ),
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.files.maxPartitionBytes': '134217728',
            'spark.cassandra.connection.host': 'cassandra',
            'spark.cassandra.connection.port': '9042',
        },
        conn_id='spark_conn',
        verbose=True,
        application_args=[
            '--year', '{{ data_interval_start.strftime("%Y") }}'
        ],
    )

    yearly = SparkSubmitOperator(
        task_id='Yearly',
        application='app/yearly.py',
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
            "com.github.jnr:jnr-posix:3.1.15"
        ),
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.files.maxPartitionBytes': '134217728',
            'spark.cassandra.connection.host': 'cassandra',
            'spark.cassandra.connection.port': '9042',
        },
        conn_id='spark_conn',
        verbose=True,
        application_args=[
            '--year', '{{ data_interval_start.strftime("%Y") }}'
        ],
    )

    end = BashOperator(
        task_id='End',
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
    start >> setup >> ingest_data >> daily >> monthly >> yearly  >> end