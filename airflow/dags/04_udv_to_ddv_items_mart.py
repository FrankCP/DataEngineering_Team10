from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

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
        DROP TABLE IF EXISTS ddv.items_mart;
    """)

    orders   = spark.read.jdbc(jdbc_url, "udv.orders", properties=props)
    orders_items    = spark.read.jdbc(jdbc_url, "udv.order_items", properties=props)
    items = spark.read.jdbc(jdbc_url, "udv.items", properties=props)
    stores   = spark.read.jdbc(jdbc_url, "udv.stores", properties=props)
    delivery = spark.read.jdbc(jdbc_url, "udv.deliveries", properties=props)

    base = (
        orders_items
        .join(orders, "order_id")
        .join(items, "item_id","left")
        .join(stores, "store_id","left")
        .join(delivery, "order_id", "left")
        .withColumn("order_date", to_date("created_at"))
        .withColumn("year", year("created_at"))
        .withColumn("month", month("created_at"))
        .withColumn("day", dayofmonth("created_at"))
        .withColumn("week", weekofyear("created_at"))
        .withColumn("city", split(col("address_text"), ",").getItem(0))
    )

    enriched = (
        base
        .withColumn(
            "item_turnover",
            col("item_quantity")
            * col("item_price")
            * (1 - coalesce(col("item_discount"), lit(0)) / 100)
            * (1 - coalesce(col("order_discount"), lit(0)) / 100)
        )
    )

    mart_base = (
        enriched
        .groupBy(
            "year",
            "month",
            "day",
            "week",
            "city",
            "store_id",
            "item_category",
            "item_id",
            "item_title"
        )
        .agg(
            sum("item_turnover").alias("item_turnover"),
            sum("item_quantity").alias("tot_items_ordered"),
            sum("item_canceled_quantity").alias("tot_canceled_units"),

            countDistinct("order_id").alias("tot_orders_item"),

            countDistinct(
                when(col("item_canceled_quantity") > 0, col("order_id"))
            ).alias("tot_order_cancellation")
        )
    )


    pop_day = (
        mart_base
        .groupBy("year", "month", "day", "city", "store_id")
        .agg(
            max(struct(col("tot_items_ordered"), col("item_title"))).alias("max_item"),
            min(struct(col("tot_items_ordered"), col("item_title"))).alias("min_item")
        )
        .select(
            "year", "month", "day", "city", "store_id",
            col("max_item.item_title").alias("most_popular_day"),
            col("min_item.item_title").alias("least_popular_day")
        )
    )

    # ----------------------------------------------------
    # MOST / LEAST POPULAR — WEEK
    # ----------------------------------------------------
    pop_week = (
        mart_base
        .groupBy("year", "week", "city", "store_id")
        .agg(
            max(struct(col("tot_items_ordered"), col("item_title"))).alias("max_item"),
            min(struct(col("tot_items_ordered"), col("item_title"))).alias("min_item")
        )
        .select(
            "year", "week", "city", "store_id",
            col("max_item.item_title").alias("most_popular_week"),
            col("min_item.item_title").alias("least_popular_week")
        )
    )

    # ----------------------------------------------------
    # MOST / LEAST POPULAR — MONTH
    # ----------------------------------------------------
    pop_month = (
        mart_base
        .groupBy("year", "month", "city", "store_id")
        .agg(
            max(struct(col("tot_items_ordered"), col("item_title"))).alias("max_item"),
            min(struct(col("tot_items_ordered"), col("item_title"))).alias("min_item")
        )
        .select(
            "year", "month", "city", "store_id",
            col("max_item.item_title").alias("most_popular_month"),
            col("min_item.item_title").alias("least_popular_month")
        )
    )

    # ----------------------------------------------------
    # Final mart
    # ----------------------------------------------------
    final_mart = (
        mart_base
        .join(pop_day,   ["year", "month", "day", "city", "store_id"], "left")
        .join(pop_week,  ["year", "week", "city", "store_id"], "left")
        .join(pop_month, ["year", "month", "city", "store_id"], "left")
    )

    final_mart.write.jdbc(
        jdbc_url,
        "ddv.items_mart",
        mode="overwrite",
        properties=props
    )

    spark.stop()


with DAG(
    dag_id="04_udv_to_ddv_items_mart",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["ddv", "items", "pyspark"]
):
    PythonOperator(
        task_id="build_items_mart",
        python_callable=build_items_mart
    )