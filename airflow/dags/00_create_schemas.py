from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pyspark.sql import SparkSession

PARQUET_PATH = "/opt/airflow/data"

def create_schemas():
    
    hook = PostgresHook("main_postgres")
    hook.run("""
             CREATE SCHEMA IF NOT EXISTS rdv;
             CREATE SCHEMA IF NOT EXISTS udv;
             CREATE SCHEMA IF NOT EXISTS ddv;
             """)

with DAG(
    dag_id="00_create_schemas",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["rdv","udv","ddv"]
):
    PythonOperator(
        task_id="create_schemas_rdv_udv_ddv",
        python_callable=create_schemas
    )
