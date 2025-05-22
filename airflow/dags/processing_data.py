from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'ntrg',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

process_data_workflow = DAG(
    'ProcessData',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
)

with process_data_workflow:
    start = BashOperator(
        task_id='Start',
        bash_command='echo "Start downloading historical data"',
    )

    spark_submit_task = SparkSubmitOperator(
        task_id='SparkSubmit',
        application='/opt/spark/app/process_raw_data.py',
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        conn_id='spark_conn',
        verbose=True,
        application_args=[
            '2023',
        ]
    )

start >> spark_submit_task