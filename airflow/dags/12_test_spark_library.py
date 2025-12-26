from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession

def test_connection():
    spark = SparkSession.builder \
        .appName("spark_postgres_test") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    df = spark.sql("SELECT 1 AS test")
    df.show()

    spark.stop()

with DAG(
    dag_id="12_test_spark_connection",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    is_paused_upon_creation=False,
    catchup=False
):
    PythonOperator(
        task_id="spark_test",
        python_callable=test_connection
    )
