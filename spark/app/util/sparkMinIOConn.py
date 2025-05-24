from pyspark.sql import SparkSession

class MinIOConnector:
    def __init__(self, app_name="SparkApp", master="local[*]", minio_config=None):
        self.app_name = app_name
        self.master = master
        self.minio_config = minio_config
        self.spark = self._build_session()

    def _build_session(self):
        builder = SparkSession.builder.appName(self.app_name).master(self.master)

        builder = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_config["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", self.minio_config["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_config["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # Extra packages
        builder = builder.config("spark.jars.packages", ",".join([
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ]))

        return builder.getOrCreate()

    def get_session(self):
        return self.spark

    def stop(self):
        if self.spark:
            self.spark.stop()
