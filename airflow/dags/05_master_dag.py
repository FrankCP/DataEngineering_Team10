from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="05_master_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["master", "pipeline"]
):
    run_dag_0 = TriggerDagRunOperator(
        task_id="00_create_schemas",
        trigger_dag_id="00_create_schemas",
        wait_for_completion=True
    )

    run_dag_1 = TriggerDagRunOperator(
        task_id="01_parquet_to_rdv",
        trigger_dag_id="01_parquet_to_rdv",
        wait_for_completion=True
    )

    run_dag_2 = TriggerDagRunOperator(
        task_id="02_rdv_to_udv",
        trigger_dag_id="02_rdv_to_udv",
        wait_for_completion=True
    )

    run_dag_3 = TriggerDagRunOperator(
        task_id="03_udv_to_ddv_orders_mart",
        trigger_dag_id="03_udv_to_ddv_orders_mart",
        wait_for_completion=False
    )

    run_dag_4 = TriggerDagRunOperator(
        task_id="04_udv_to_ddv_items_mart",
        trigger_dag_id="04_udv_to_ddv_items_mart",
        wait_for_completion=False
    )

    run_dag_0 >>run_dag_1 >> run_dag_2 >> [
            run_dag_3,
            run_dag_4
        ]