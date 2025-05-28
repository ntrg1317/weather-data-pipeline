import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago


SECURE_CONNECT_BUNDLE_PATH=os.environ.get("SECURE_CONNECT_BUNDLE_PATH")
SECURE_CONNECT_BUNDLE=os.environ.get("SECURE_CONNECT_BUNDLE")

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
    start_date=days_ago(1),
    end_date=None,  # Set this based on your requirements
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
)

with process_data_workflow:
    start = BashOperator(
        task_id='Start',
        bash_command='echo "Start processing historical data at $(date)"',
    )
    
    task1 = SparkSubmitOperator(
        task_id='IngestData',
        application='app/process_raw_data.py',
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
            "com.github.jnr:jnr-posix:3.1.15,"
            "org.postgresql:postgresql:42.7.3"
        ),
        conf={
            'spark.files': 'spark/data/secure-connect-weather-cluster.zip',
            'spark.cassandra.connection.config.cloud.path': 'secure-connect-weather-cluster.zip',
        },
        conn_id='spark_conn',
        verbose=True,
        application_args=['{{ ds }}'],  # Pass execution date as parameter
    )
    
    end = BashOperator(
        task_id='End',
        bash_command='echo "End processing historical data at $(date). Duration: $SECONDS seconds"',
        env={'SECONDS': '{{ (execution_date + macros.timedelta(days=1) - execution_date).total_seconds() }}'}
    )

start >> task1 >> end