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

    orders      = spark.read.jdbc(jdbc_url, "udv.orders", properties=props)
    deliveries  = spark.read.jdbc(jdbc_url, "udv.deliveries", properties=props)
    order_items = spark.read.jdbc(jdbc_url, "udv.order_items", properties=props)
    items       = spark.read.jdbc(jdbc_url, "udv.items", properties=props)

    # Order item money
    item_money = (
        order_items
        .join(items, "item_id")
        .withColumn(
            "item_net",
            col("item_quantity") *
            col("item_price") *
            (1 - coalesce(col("item_discount"), lit(0)) / 100)
        )
        .groupBy("order_id")
        .agg(
            sum("item_net").alias("order_gross"),
            sum(
                (col("item_quantity") - col("item_canceled_quantity")) *
                col("item_price") *
                (1 - coalesce(col("item_discount"), lit(0)) / 100)
            ).alias("order_revenue_items")
        )
    )

    # Delivery stats
    delivery_stats = (
        deliveries
        .groupBy("order_id")
        .agg(
            countDistinct("driver_id").alias("driver_cnt"),
            sum("delivery_cost").alias("delivery_cost"),
            max(when(col("delivered_at").isNotNull(), 1).otherwise(0)).alias("delivered_flag")
        )
    )

    mart = (
        orders
        .join(item_money, "order_id", "left")
        .join(delivery_stats, "order_id", "left")
        .withColumn("order_date", to_date("created_at"))
        .withColumn("year", year("created_at"))
        .withColumn("month", month("created_at"))
        .withColumn("day", dayofmonth("created_at"))
        .withColumn("city", split(col("address_text"), ",").getItem(0))

        # Discounts
        .withColumn(
            "turnover",
            col("order_gross") * (1 - coalesce(col("order_discount"), lit(0)) / 100)
        )
        .withColumn(
            "revenue",
            when(
                col("delivered_flag") == 1,
                col("order_revenue_items") * (1 - coalesce(col("order_discount"), lit(0)) / 100)
            ).otherwise(0)
        )
        .withColumn(
            "profit",
            col("revenue") - when(col("delivered_flag") == 1, col("delivery_cost")).otherwise(0)
        )

        # Counters
        .withColumn("orders_created", lit(1))
        .withColumn("orders_delivered", when(col("delivered_flag") == 1, 1).otherwise(0))
        .withColumn("orders_canceled", when(col("canceled_at").isNotNull(), 1).otherwise(0))
        .withColumn(
            "canceled_after_delivery",
            when(col("canceled_at").isNotNull() & (col("delivered_flag") == 1), 1).otherwise(0)
        )
        .withColumn(
            "canceled_service_error",
            when(
                col("order_cancellation_reason")
                .isin("Ошибка приложения", "Проблемы с оплатой"),
                1
            ).otherwise(0)
        )
        .withColumn("courier_shift", when(col("driver_cnt") > 1, 1).otherwise(0))
    )

    final_mart = (
        mart
        .groupBy("year", "month", "day", "city", "store_id")
        .agg(
            sum("turnover").alias("turnover"),
            sum("revenue").alias("revenue"),
            sum("profit").alias("profit"),
            sum("orders_created").alias("orders_created"),
            sum("orders_delivered").alias("orders_delivered"),
            sum("orders_canceled").alias("orders_canceled"),
            sum("canceled_after_delivery").alias("canceled_after_delivery"),
            sum("canceled_service_error").alias("canceled_service_error"),
            countDistinct("user_id").alias("qty_customers"),
            (sum("revenue") / sum("orders_delivered")).alias("avg_check"),
            (sum("orders_created") / countDistinct("user_id")).alias("tot_orders_customer"),
            (sum("revenue") / countDistinct("user_id")).alias("revenue_per_customer"),
            sum("courier_shift").alias("tot_courier_shifts"),
            sum(
            when(col("delivered_flag") == 1, 1).otherwise(0)).alias("tot_active_couriers")
        )
    )

    final_mart.write.jdbc(
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