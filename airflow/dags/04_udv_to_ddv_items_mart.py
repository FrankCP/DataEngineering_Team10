from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def build_items_mart():

    hook = PostgresHook("main_postgres")
    conn = hook.get_connection("main_postgres")

    jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
    props = {
        "user": conn.login,
        "password": conn.password,
        "driver": "org.postgresql.Driver"
    }

    spark = (
        SparkSession.builder
        .appName("ddv_items_mart")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    hook.run("""
        CREATE SCHEMA IF NOT EXISTS ddv;
        DROP TABLE IF EXISTS ddv.items_mart;
    """)

    orders   = spark.read.jdbc(jdbc_url, "udv.orders", properties=props)
    items    = spark.read.jdbc(jdbc_url, "udv.order_items", properties=props)
    products = spark.read.jdbc(jdbc_url, "udv.items", properties=props)
    stores   = spark.read.jdbc(jdbc_url, "udv.stores", properties=props)
    delivery = spark.read.jdbc(jdbc_url, "udv.deliveries", properties=props)

    df = (
        items
        .join(orders, "order_id")
        .join(products, "item_id")
        .join(stores, "store_id")
        .join(delivery, "order_id", "left")
        .withColumn("order_date", to_date("created_at"))
        .withColumn("year", year("created_at"))
        .withColumn("month", month("created_at"))
        .withColumn("day", dayofmonth("created_at"))
    )

    mart = (
        df
        .groupBy(
            "year",
            "month",
            "day",
            "order_date",
            "store_id",
            "store_address",
            "item_id",
            "item_title",
            "item_category"
        )
        .agg(
            # Orders with item
            countDistinct("order_id").alias("orders_with_item"),

            # Orders with canceled item
            countDistinct(
                when(col("item_canceled_quantity") > 0, col("order_id"))
            ).alias("orders_with_item_cancellation"),

            # Units
            sum("item_quantity").alias("units_ordered"),
            sum("item_canceled_quantity").alias("units_canceled"),

            # Turnover (ordered amount)
            sum(
                col("item_quantity") *
                col("item_price") *
                (1 - coalesce(col("item_discount"), lit(0)) / 100)
            ).alias("item_turnover"),

            # Revenue (ONLY successful deliveries)
            sum(
                when(
                    col("delivered_at").isNotNull(),
                    (col("item_quantity") - col("item_canceled_quantity")) *
                    col("item_price") *
                    (1 - coalesce(col("item_discount"), lit(0)) / 100)
                ).otherwise(0)
            ).alias("item_revenue")
        )
    )

    mart.write.jdbc(
        jdbc_url,
        "ddv.items_mart",
        mode="overwrite",
        properties=props
    )

    spark.stop()


with DAG(
    dag_id="04_ddv_items_mart_pyspark",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ddv", "items", "pyspark"]
):
    PythonOperator(
        task_id="build_items_mart",
        python_callable=build_items_mart
    )
