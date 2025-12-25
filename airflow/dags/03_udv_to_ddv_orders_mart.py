from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def build_orders_mart():

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
        .appName("ddv_orders_mart")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    hook.run("""
        DROP TABLE IF EXISTS ddv.orders_mart;
    """)

    orders   = spark.read.jdbc(jdbc_url, "udv.orders", properties=props)
    items    = spark.read.jdbc(jdbc_url, "udv.order_items", properties=props)
    products = spark.read.jdbc(jdbc_url, "udv.items", properties=props)
    stores   = spark.read.jdbc(jdbc_url, "udv.stores", properties=props)
    delivery = spark.read.jdbc(jdbc_url, "udv.deliveries", properties=props)

    df = (
        orders
        .join(items, "order_id")
        .join(products, "item_id")
        .join(stores, "store_id")
        .join(delivery, "order_id", "left")
    )

    mart = (
        df
        .withColumn("order_date", to_date("created_at"))
        .withColumn("year", year("created_at"))
        .withColumn("month", month("created_at"))
        .withColumn("day", dayofmonth("created_at"))
        .groupBy(
            "order_date", "year", "month", "day",
            "store_id", "store_address"
        )
        .agg(
            countDistinct("order_id").alias("orders_created"),

            countDistinct(
                when(col("delivered_at").isNotNull(), col("order_id"))
            ).alias("orders_delivered"),

            countDistinct(
                when(col("canceled_at").isNotNull(), col("order_id"))
            ).alias("orders_canceled"),

            countDistinct(
                when(
                    col("canceled_at").isNotNull() &
                    col("delivered_at").isNotNull(),
                    col("order_id")
                )
            ).alias("canceled_after_delivery"),

            countDistinct(
                when(
                    col("order_cancellation_reason").isin(
                        "Ошибка приложения", "Проблемы с оплатой"
                    ),
                    col("order_id")
                )
            ).alias("service_error_cancellations"),

            countDistinct("user_id").alias("buyers"),

            sum(
                col("item_quantity") * col("item_price") *
                (1 - coalesce(col("item_discount"), lit(0)) / 100)
            ).alias("turnover"),

            sum(
                when(
                    col("delivered_at").isNotNull(),
                    (col("item_quantity") - col("item_canceled_quantity"))
                    * col("item_price")
                    * (1 - coalesce(col("item_discount"), lit(0)) / 100)
                ).otherwise(0)
            ).alias("revenue"),

            sum(
                when(
                    col("delivered_at").isNotNull(),
                    (col("item_quantity") - col("item_canceled_quantity"))
                    * col("item_price")
                    * (1 - coalesce(col("item_discount"), lit(0)) / 100)
                    - coalesce(col("delivery_cost"), lit(0))
                ).otherwise(0)
            ).alias("profit"),

            countDistinct("driver_id").alias("active_couriers")
        )
    )

    mart.write.jdbc(
        jdbc_url,
        "ddv.orders_mart",
        mode="overwrite",
        properties=props
    )

    spark.stop()

with DAG(
    dag_id="03_udv_to_ddv_orders_mart",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["ddv", "orders", "pyspark"]
):
    PythonOperator(
        task_id="build_orders_mart",
        python_callable=build_orders_mart
    )