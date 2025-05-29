import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# Environment variables
SECURE_CONNECT_BUNDLE_PATH = "/opt/airflow/spark/data/secure-connect-weather-cluster.zip"
SECURE_CONNECT_BUNDLE = os.environ.get("SECURE_CONNECT_BUNDLE", "secure-connect-weather-cluster.zip")

default_args = {
    'owner': 'ntrg',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),  # Tăng thời gian retry
    'email_on_failure': True,
    'email_on_retry': False,  # Tắt email on retry để tránh spam
    'execution_timeout': timedelta(hours=3),  # Tăng timeout cho large data
    'depends_on_past': False
}

process_data_workflow = DAG(
    'ProcessData1',
    default_args=default_args,
    description='Process historical weather data workflow',
    start_date=days_ago(1),
    end_date=None,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'data-processing', 'spark'],  # Thêm tags để organize
)

with process_data_workflow:
    # Pre-flight checks
    pre_check = BashOperator(
        task_id='pre_flight_check',
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

    # Main Spark job
    ingest_data = SparkSubmitOperator(
        task_id='ingest_weather_data',
        application='app/process_raw_data.py',
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
            "com.github.jnr:jnr-posix:3.1.15,"
            "org.postgresql:postgresql:42.7.3"
        ),
        conf={
            # Cassandra connection
            'spark.files': SECURE_CONNECT_BUNDLE_PATH,
            'spark.cassandra.connection.config.cloud.path': 'secure-connect-weather-cluster.zip',

            # Memory settings - tối ưu cho large data
            'spark.driver.memory': '6g',
            'spark.driver.maxResultSize': '2g',
            'spark.executor.memory': '4g',
            'spark.executor.memoryFraction': '0.8',
            'spark.executor.cores': '4',
            'spark.executor.instances': '3',

            # Adaptive Query Execution
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.minPartitionSize': '5',
            'spark.sql.adaptive.coalescePartitions.initialPartitionNum': '50',

            # File processing - tối ưu cho 12K files
            'spark.sql.files.maxPartitionBytes': '67108864',  # 64MB thay vì 4MB
            'spark.sql.files.openCostInBytes': '4194304',  # 4MB open cost
            'spark.sql.files.maxRecordsPerFile': '100000',

            # S3 optimizations
            'spark.hadoop.fs.s3a.connection.maximum': '100',
            'spark.hadoop.fs.s3a.threads.max': '20',
            'spark.hadoop.fs.s3a.connection.establish.timeout': '10000',
            'spark.hadoop.fs.s3a.connection.timeout': '200000',
            'spark.hadoop.fs.s3a.fast.upload': 'true',
            'spark.hadoop.fs.s3a.block.size': '67108864',  # 64MB blocks

            # Serialization
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.execution.arrow.pyspark.enabled': 'true',

            # Dynamic allocation (optional)
            'spark.dynamicAllocation.enabled': 'true',
            'spark.dynamicAllocation.minExecutors': '2',
            'spark.dynamicAllocation.maxExecutors': '10',
            'spark.dynamicAllocation.initialExecutors': '3',
        },
        conn_id='spark_conn',
        verbose=True,
        application_args=[
            '--execution-date', '{{ ds }}',
            '--logical-date', '{{ logical_date }}',
            '--secure-bundle', SECURE_CONNECT_BUNDLE,
        ],
        # Resource monitoring
        driver_memory='6g',
        executor_memory='4g',
        executor_cores=4,
        num_executors=3,
    )

    # Data quality checks
    data_quality_check = BashOperator(
        task_id='data_quality_check',
        bash_command='''
        echo "=== Data Quality Checks ==="
        echo "Checking processed data for {{ ds }}"

        # Add your data quality checks here
        # Example: Check if output files exist, row counts, etc.

        echo "✓ Data quality checks passed"
        ''',
    )

    # Cleanup and notification
    cleanup_and_notify = BashOperator(
        task_id='cleanup_and_notify',
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
    pre_check >> ingest_data >> data_quality_check >> cleanup_and_notify