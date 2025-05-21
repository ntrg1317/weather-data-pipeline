import psycopg2
import os
import logging

class PostgresDb:
    def __init__(self):
        self.conn = psycopg2.connect(**{
            "host": "postgres",
            "user": os.environ['POSTGRES_USER'],
            "password": os.environ['POSTGRES_PASSWORD'],
            "database": os.environ['POSTGRES_DB']
        })
        self.cursor = self.conn.cursor()

    def close(self):
        if self.conn:
            self.conn.close()

    def commit(self):
        self.conn.commit()

    def execute_query(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            self.commit()
            logging.info("Query executed successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error("Error while executing query: %s", error)
            self.conn.rollback()

    def fetchall(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Exception as error:
            logging.error("Error while fetching data: %s", error)
            return []

    def truncate_table(self, table_name):
        self.execute_query(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")

    def drop_table(self, table_name):
        self.execute_query(f"DROP TABLE IF EXISTS {table_name};")

    def create_table(self, table_name, schema):
        self.execute_query(f"CREATE TABLE {table_name} ({schema});")

    def copy_expert(self, query, file):
        try:
            self.cursor.copy_expert(query, file)
            self.commit()
            logging.info("COPY executed successfully")
        except Exception as e:
            self.conn.rollback()
            logging.error("Error during COPY: %s", e)
