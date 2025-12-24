from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def test_postgres_connection():
    hook = PostgresHook(postgres_conn_id="main_postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT 1;")
    result = cursor.fetchone()

    if result[0] != 1:
        raise ValueError("Postgres test query failed")

    print("✅ Postgres connection successful")

    cursor.close()
    conn.close()

with DAG(
    dag_id="test_postgres_connection",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "postgres"]
) as dag:

    test_connection = PythonOperator(
        task_id="test_main_postgres_connection",
        python_callable=test_postgres_connection
    )