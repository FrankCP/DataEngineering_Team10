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
    items    = spark.read.jdbc(jdbc_url, "udv.items", properties=props)
    oi       = spark.read.jdbc(jdbc_url, "udv.order_items", properties=props)

    df = (
        oi
        .join(orders, "order_id")
        .join(items, "item_id")
        .filter(col("canceled_at").isNull() )
    )

    df = (
        df
        .withColumn("order_date", to_date("created_at"))
        .withColumn("year", year("created_at"))
        .withColumn("month", month("created_at"))
        .withColumn("week",weekofyear("created_at"))
        .withColumn("day", dayofmonth("created_at"))
        .withColumn("city", trim(split(col("address_text"), ",").getItem(0)))
        .withColumn(
            "item_turnover",
            col("item_quantity") *
            col("item_price") *
            (1 - coalesce(col("item_discount"), lit(0)) / 100)
        )
    )

    mart_base = (
        df
        .groupBy(
            "year", "month", "week", "day",
            "city", "store_id",
            "item_category", "item_id", "item_title"
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
    
    daily_pop = (
        df
        .groupBy("year", "month", "day", "city", "store_id", "item_title")
        .agg(
            sum(
                col("item_quantity") - coalesce(col("item_canceled_quantity"), lit(0))
            ).alias("qty")
        )
    )

    day_desc = Window.partitionBy(
        "year", "month", "day","city", "store_id"
    ).orderBy(col("qty").desc())

    day_asc = Window.partitionBy(
        "year", "month", "day","city", "store_id"
    ).orderBy(col("qty").asc())


    daily_ranked = (
    daily_pop
    .withColumn("rn_max", row_number().over(day_desc))
    .withColumn("rn_min", row_number().over(day_asc))
)

    daily_final = (
        daily_ranked
        .select(
            "year", "month", "day",
            "city", "store_id",
            when(col("rn_max") == 1, col("item_title")).alias("most_pop_item_per_day"),
            when(col("rn_min") == 1, col("item_title")).alias("lst_pop_item_per_day")
        )
        .groupBy(
            "year", "month", "day",
            "city", "store_id"
        )
        .agg(
            max("most_pop_item_per_day").alias("most_pop_item_per_day"),
            max("lst_pop_item_per_day").alias("lst_pop_item_per_day")
        )
    )

    weekly_pop = (
        df
        .groupBy("year", "week", "city", "store_id", "item_title")
        .agg(
            sum(
                col("item_quantity") - coalesce(col("item_canceled_quantity"), lit(0))
            ).alias("qty")
        )
    )
    week_desc = Window.partitionBy(
        "year", "week",
        "city", "store_id"
    ).orderBy(col("qty").desc())

    week_asc = Window.partitionBy(
        "year", "week",
        "city", "store_id"
    ).orderBy(col("qty").asc())

    weekly_ranked = (
        weekly_pop
        .withColumn("rn_max", row_number().over(week_desc))
        .withColumn("rn_min", row_number().over(week_asc))
    )

    weekly_final = (
        weekly_ranked
        .select(
            "year", "week",
            "city", "store_id",
            when(col("rn_max") == 1, col("item_title")).alias("most_pop_item_per_week"),
            when(col("rn_min") == 1, col("item_title")).alias("lst_pop_item_per_week")
        )
        .groupBy(
            "year", "week",
            "city", "store_id"
        )
        .agg(
            max("most_pop_item_per_week").alias("most_pop_item_per_week"),
            max("lst_pop_item_per_week").alias("lst_pop_item_per_week")
        )
    )

    
    monthly_pop = (
        df
        .groupBy("year", "month", "city", "store_id", "item_title")
        .agg(
            sum(
                col("item_quantity") - coalesce(col("item_canceled_quantity"), lit(0))
            ).alias("qty")
        )
    )
    month_desc = Window.partitionBy(
        "year", "month",
        "city", "store_id"
    ).orderBy(col("qty").desc())

    month_asc = Window.partitionBy(
        "year", "month",
        "city", "store_id"
    ).orderBy(col("qty").asc())

    monthly_ranked = (
        monthly_pop
        .withColumn("rn_max", row_number().over(month_desc))
        .withColumn("rn_min", row_number().over(month_asc))
    )

    monthly_final = (
        monthly_ranked
        .select(
            "year", "month",
            "city", "store_id", 
            when(col("rn_max") == 1, col("item_title")).alias("most_pop_item_per_month"),
            when(col("rn_min") == 1, col("item_title")).alias("lst_pop_item_per_month")
        )
        .groupBy(
            "year", "month",
            "city", "store_id"
        )
        .agg(
            max("most_pop_item_per_month").alias("most_pop_item_per_month"),
            max("lst_pop_item_per_month").alias("lst_pop_item_per_month")
        )
    )


    final_mart = (
        mart_base
        .join(daily_final, ["year", "month", "day", "city", "store_id"], "left")
        .join(weekly_final, ["year", "week", "city", "store_id"], "left")
        .join(monthly_final, ["year", "month", "city", "store_id"], "left")
        .select(
            "year",
            "month",
            "day",
            "city",
            "store_id",
            "item_category",
            "item_title",
            "item_turnover",
            "tot_items_ordered",
            "tot_canceled_units",
            "tot_orders_item",
            "tot_order_cancellation",
            "most_pop_item_per_day",
            "most_pop_item_per_week",
            "most_pop_item_per_month",
            "lst_pop_item_per_day",
            "lst_pop_item_per_week",
            "lst_pop_item_per_month"
        )
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