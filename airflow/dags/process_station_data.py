from datetime import timedelta

import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from cassandra.cluster import Session
from pandas.core.interchange.dataframe_protocol import DataFrame

from ingest import downloader


def preprocess_country_data(**kwargs):
    path = f"{downloader.airflow_dir}/data/station/country.csv"
    data = pd.read_csv(path)
    data = data[
        ['FIPS', 'ISO3166-1-Alpha-3', 'ISO3166-1-Alpha-2', 'is_independent', 'official_name_en', 'Region Name',
         'Sub-region Name', 'Capital', 'Continent', 'ISO4217-currency_name']]
    data = data.rename(columns={
        'FIPS': 'fips',
        'ISO3166-1-Alpha-3': 'iso3',
        'ISO3166-1-Alpha-2': 'iso2',
        'Region Name': 'region',
        'Sub-region Name': 'sub_region',
        'Capital': 'capital',
        'Continent': 'continent',
        'ISO4217-currency_name': 'currency'
    })

    data = data[data['fips'].str.strip().astype(bool)]

    kwargs['ti'].xcom_push(key='country_data', value=data.to_dict(orient='records'))

def setup_physical_schema():
    hook = CassandraHook(cassandra_conn_id='cassandra_conn')
    session = hook.get_conn()

    session.set_keyspace('station')



    create_schema = f"""
        CREATE TABLE IF NOT EXISTS station.country (
            fips TEXT,
            wsid TEXT,
            iso3 TEXT,
            iso2 TEXT,
            is_independent BOOLEAN,
            official_name_en TEXT,
            region TEXT,
            sub_region TEXT,
            capital TEXT,
            continent TEXT,
            currency TEXT,
            name TEXT,
            province TEXT,
            icao TEXT,
            latitude DOUBLE,
            longitude DOUBLE,
            elevation DOUBLE,
            PRIMARY KEY ( (fips), wsid )
        ) WITH CLUSTERING ORDER BY (wsid ASC);
        
        CREATE TABLE IF NOT EXISTS station.station_by_start_date(
            year INT,
            month INT,
            wsid TEXT,
            name TEXT,
            latitude DOUBLE,
            longitude DOUBLE,
            elevation DOUBLE,
            begin_date DATE,
            PRIMARY KEY ( (year, month), wsid )
        ) WITH CLUSTERING ORDER BY (wsid ASC);
        
        CREATE TABLE IF NOT EXISTS station.station_by_end_date(
            year INT,
            month INT,
            wsid TEXT,
            name TEXT,
            latitude DOUBLE,
            longitude DOUBLE,
            elevation DOUBLE,
            end_date DATE,
            PRIMARY KEY ( (year, month), wsid )
        ) WITH CLUSTERING ORDER BY (wsid ASC);
    """

    session.execute(create_schema)

def insert_to_cassandra(df: pd.DataFrame, session: Session, table_name: str):
    columns = df.columns.tolist()

    query = "INSERT INTO {} ({}) VALUES({})".format({table_name}, ','.join(columns),
                                                    ','.join([val.replace(val, "?") for val in columns]))
    prepared = session.prepare(query)

    for row in df.loc:
        values = [row[col] for col in columns]
        session.execute(prepared, values)


def q1_modeling(**kwargs):
    hook = CassandraHook(cassandra_conn_id='cassandra_conn')
    session = hook.get_conn()

    country_data = kwargs['ti'].xcom_pull(key='country_data', task_ids='PreprocessCountryData')
    country_df = pd.DataFrame(country_data)

    station = session.execute(f"SELECT * FROM station.station")
    station_df = pd.DataFrame(station.all())

    station_by_country = station_df.merge(country_df, left_on='country', right_on='fips', how='right')
    insert_to_cassandra(station_by_country, session, 'country')

def q4_modeling(**kwargs):
    hook = CassandraHook(cassandra_conn_id='cassandra_conn')
    session = hook.get_conn()

    station = session.execute(f"SELECT * FROM station.station")
    station_df = pd.DataFrame(station.all())

    # Tạo bảng station_by_start_date
    df_start = station_df.copy()
    df_start['year'] = df_start['begin_date'].dt.year
    df_start['month'] = df_start['begin_date'].dt.month

    station_by_start_date = df_start[
        ['year', 'month', 'wsid', 'name', 'latitude', 'longitude', 'elevation', 'begin_date']]

    insert_to_cassandra(station_by_start_date, session, 'station_by_start_date')

    # Tạo bảng station_by_end_date
    df_end = station_df.copy()
    df_end['year'] = df_end['end_date'].dt.year
    df_end['month'] = df_end['end_date'].dt.month

    station_by_end_date = df_end[['year', 'month', 'wsid', 'name', 'latitude', 'longitude', 'elevation', 'end_date']]
    insert_to_cassandra(station_by_end_date, session, 'station_by_end_date')

default_args = {
    'owner': 'ntrg',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'depends_on_past': False
}

with DAG(
    dag_id='ProcessStationData',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['station', 'data-processing', 'spark']
):
    physical_schema = PythonOperator(
        task_id='SetupPhysicalSchema',
        python_callable=setup_physical_schema
    )

    preprocess = PythonOperator(
        task_id='PreprocessCountryData',
        python_callable=preprocess_country_data
    )

    country_data_modeling = PythonOperator(
        task_id='Q1DataModeling',
        python_callable=q1_modeling,
    )

    station_data_modeling = PythonOperator(
        task_id='Q4DataModeling',
        python_callable=q4_modeling,
    )

    physical_schema >> preprocess >> [country_data_modeling, station_data_modeling]