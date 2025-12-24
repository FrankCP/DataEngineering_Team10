from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="00_master_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["master", "pipeline"]
):

    run_dag_1 = TriggerDagRunOperator(
        task_id="run_parquet_to_rdv",
        trigger_dag_id="parquet_to_rdv",
        wait_for_completion=True
    )

    run_dag_2 = TriggerDagRunOperator(
        task_id="run_rdv_to_udv",
        trigger_dag_id="rdv_to_udv_sql",
        wait_for_completion=True
    )

    run_dag_3 = TriggerDagRunOperator(
        task_id="run_orders_mart",
        trigger_dag_id="03_ddv_orders_mart_pyspark",
        wait_for_completion=False
    )

    run_dag_4 = TriggerDagRunOperator(
        task_id="run_items_mart",
        trigger_dag_id="04_ddv_items_mart_pyspark",
        wait_for_completion=False
    )

    run_dag_1 >> run_dag_2 >> run_dag_3 >> run_dag_4