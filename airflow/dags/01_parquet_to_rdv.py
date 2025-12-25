from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pyspark.sql import SparkSession

PARQUET_PATH = "/opt/airflow/data"

def parquet_to_rdv():
    
    hook = PostgresHook("main_postgres")

    spark = SparkSession.builder \
        .appName("parquet_to_rdv") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    df = spark.read.parquet(PARQUET_PATH)

    conn = hook.get_connection("main_postgres")
    jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"

    props = {
        "user": conn.login,
        "password": conn.password,
        "driver": "org.postgresql.Driver"
    }

    df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, "rdv.orders_raw", properties=props)

    spark.stop()

with DAG(
    dag_id="01_parquet_to_rdv",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["rdv"]
):
    PythonOperator(
        task_id="01_parquet_to_rdv",
        python_callable=parquet_to_rdv
    )
