import os, logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from ingest import downloader

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST")
CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT")

def setupCassandra():
    hook = CassandraHook(cassandra_conn_id='cassandra_conn')
    session = hook.get_conn()

    station_schema = f"{downloader.airflow_dir}/cassandra/weather.cql"
    with open(station_schema, 'r') as f:
        statements = f.read().split(';')
        for statement in statements:
            if statement.strip():
                session.execute(statement)

    session.shutdown()

default_args = {
    'owner': 'ntrg',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(hours=2),
    'depends_on_past': False
}

def create_process_dag(**kwargs):
    with DAG(
        dag_id=kwargs['dag_id'],
        default_args=default_args,
        start_date=kwargs['start_date'],
        schedule_interval=kwargs['schedule_interval'],
        catchup=kwargs['catchup'],
        tags=['weather', 'data-processing', 'spark', kwargs['tags']],
        max_active_runs=1
    ) as dag:
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
            python_callable=setupCassandra,
            provide_context=True
        )

        ingest_data = SparkSubmitOperator(
            task_id='IngestRawData',
            application='app/ingest.py',
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
    return dag

dag_process_daily = create_process_dag(dag_id='ProcessDailyData', start_date=datetime(2010, 1, 1), schedule_interval=None, catchup=False, tags='daily')
dag_process_yearly = create_process_dag(dag_id='ProcessYearlyData', start_date=datetime(2010, 1, 1), schedule_interval='@yearly', catchup=True, tags='yearly')