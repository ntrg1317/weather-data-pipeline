from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from typing import Optional, Dict, Any, List

class SparkCassandraConnector:
    def __init__(
            self,
            app_name: str = "CassandraSparkApp",
            master: str = "local[*]",
            secure_connect_bundle_file_path: Optional[str] = None,
            secure_connect_bundle_file: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
    ):
        self.app_name = app_name
        self.master = master
        self.secure_connect_bundle_file_path = secure_connect_bundle_file_path
        self.secure_connect_bundle_file = secure_connect_bundle_file
        self.username = username
        self.password = password
        self.spark = self._build_session()

    def _build_session(self):
        builder = SparkSession.builder.appName(self.app_name).master(self.master)

        builder = builder \
            .config("spark.files", self.secure_connect_bundle_file_path) \
            .config("spark.cassandra.connection.config.cloud.path", self.secure_connect_bundle_file) \
            .config("spark.cassandra.auth.username", self.username) \
            .config("spark.cassandra.auth.password", self.password) \

        # Extra packages
        builder = builder.config("spark.jars.packages", ",".join([
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
        ]))

        return builder.getOrCreate()

    def get_session(self):
        return self.spark

    def read_from_cassandra(
            self,
            keyspace: str,
            table: str,
            columns: Optional[List[str]] = None,
            where_clause: Optional[str] = None
    ) -> DataFrame:
        """
        Read data from Cassandra table

        :param keyspace: Cassandra keyspace name
        :param table: Cassandra table name
        :param columns: List of columns to read (optional)
        :param where_clause: Optional WHERE clause for filtering
        :return: Spark DataFrame
        """
        options = {
            "keyspace": keyspace,
            "table": table
        }

        # Select specific columns if provided
        df_reader = self.spark.read.format("org.apache.spark.sql.cassandra")

        if columns:
            # Read only specified columns
            selected_df = df_reader.options(**options).load()
            return selected_df.select(*columns)

        # Read entire table
        return df_reader.options(**options).load()

    def write_to_cassandra(
            self,
            dataframe: DataFrame,
            keyspace: str,
            table: str,
            mode: str = "append"
    ) -> None:
        """
        Write Spark DataFrame to Cassandra

        :param dataframe: Spark DataFrame to write
        :param keyspace: Target Cassandra keyspace
        :param table: Target Cassandra table
        :param mode: Write mode (append/overwrite/ignore)
        """

        if mode == "overwrite":
            (dataframe.write
             .format("org.apache.spark.sql.cassandra")
             .mode(mode)
             .options(keyspace=keyspace, table=table)
             .option("confirm.truncate", "true")
             .save())
        else:
            (dataframe.write
             .format("org.apache.spark.sql.cassandra")
             .mode(mode)
             .options(keyspace=keyspace, table=table)
             .save())

    def execute_cassandra_query(
            self,
            query: str,
            params: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Execute a CQL-like query using Spark

        :param query: SQL-like query
        :param params: Optional query parameters
        :return: Spark DataFrame with query results
        """
        return self.spark.sql(query)

    def create_cassandra_table(
            self,
            keyspace: str,
            table: str,
            schema: StructType
    ) -> None:
        """
        Create a new Cassandra table from Spark DataFrame schema

        :param keyspace: Target keyspace
        :param table: Table name
        :param schema: Spark DataFrame schema
        """
        # Convert Spark schema to CREATE TABLE statement
        schema_ddl = ", ".join([
            f"{field.name} {field.dataType.simpleString()}"
            for field in schema.fields
        ])

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
            {schema_ddl}
        )
        """

        # Note: This is a simplified approach and might need adjustments
        self.spark.sql(create_table_query)

    def close(self):
        """
        Close the Spark session
        """
        self.spark.stop()