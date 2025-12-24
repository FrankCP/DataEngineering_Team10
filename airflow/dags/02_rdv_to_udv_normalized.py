from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="rdv_to_udv_sql",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["udv", "sql"]
) as dag:

    create_udv_schema = PostgresOperator(
        task_id="create_udv_schema",
        postgres_conn_id="main_postgres",
        sql="""
        CREATE SCHEMA IF NOT EXISTS udv;
        """
    )

    create_udv_tables = PostgresOperator(
        task_id="create_udv_tables",
        postgres_conn_id="main_postgres",
        sql="""

        CREATE TABLE IF NOT EXISTS udv.users (
            user_id BIGINT PRIMARY KEY,
            user_phone VARCHAR(20)
        );

        CREATE TABLE IF NOT EXISTS udv.items (
            item_id BIGINT PRIMARY KEY,
            item_title TEXT,
            item_price NUMERIC(10,2),
            item_category TEXT
        );

        CREATE TABLE IF NOT EXISTS udv.drivers (
            driver_id BIGINT PRIMARY KEY,
            driver_phone VARCHAR(20)
        );

        CREATE TABLE IF NOT EXISTS udv.stores (
            store_id BIGINT PRIMARY KEY,
            store_address TEXT
        );

        CREATE TABLE IF NOT EXISTS udv.orders (
            order_id BIGINT PRIMARY KEY,
            address_text TEXT,
            created_at TIMESTAMP,
            paid_at TIMESTAMP,
            canceled_at TIMESTAMP,
            payment_type TEXT,
            order_discount NUMERIC(5,2),
            order_cancellation_reason TEXT,
            user_id BIGINT,
            store_id BIGINT,

            CONSTRAINT fk_orders_user
                FOREIGN KEY (user_id)
                REFERENCES udv.users (user_id),

            CONSTRAINT fk_orders_store
                FOREIGN KEY (store_id)
                REFERENCES udv.stores (store_id)
        );

        CREATE TABLE IF NOT EXISTS udv.deliveries (
            order_id BIGINT,
            delivery_started_at TIMESTAMP,
            delivered_at TIMESTAMP,
            delivery_cost NUMERIC(10,2),
            driver_id BIGINT,

            CONSTRAINT fk_delivery_order
                FOREIGN KEY (order_id)
                REFERENCES udv.orders (order_id),

            CONSTRAINT fk_delivery_driver
                FOREIGN KEY (driver_id)
                REFERENCES udv.drivers (driver_id)
        );

        CREATE TABLE IF NOT EXISTS udv.order_items (
            order_id BIGINT,
            item_id BIGINT,
            item_quantity INT,
            item_canceled_quantity INT,
            item_discount NUMERIC(5,2),
            item_replaced_id BIGINT,

            CONSTRAINT fk_order_items_order
                FOREIGN KEY (order_id)
                REFERENCES udv.orders (order_id),

            CONSTRAINT fk_order_items_item
                FOREIGN KEY (item_id)
                REFERENCES udv.items (item_id),

            CONSTRAINT fk_order_items_replaced_item
                FOREIGN KEY (item_replaced_id)
                REFERENCES udv.items (item_id)
        );
        """
    )

    truncate_udv_tables = PostgresOperator(
        task_id="truncate_udv_tables",
        postgres_conn_id="main_postgres",
        sql="""
        TRUNCATE TABLE
            udv.order_items,
            udv.deliveries,
            udv.orders,
            udv.users,
            udv.items,
            udv.drivers,
            udv.stores;
        """
    )

    load_udv_users = PostgresOperator(
        task_id="load_udv_users",
        postgres_conn_id="main_postgres",
        sql="""
        INSERT INTO udv.users (user_id, user_phone)
        SELECT DISTINCT
            user_id,
            user_phone
        FROM rdv.orders_raw
        WHERE user_id IS NOT NULL;
        """
    )

    load_udv_items = PostgresOperator(
        task_id="load_udv_items",
        postgres_conn_id="main_postgres",
        sql="""
        INSERT INTO udv.items (item_id, item_title, item_price, item_category)
        SELECT DISTINCT
            item_id,
            item_title,
            item_price,
            item_category
        FROM rdv.orders_raw
        WHERE item_id IS NOT NULL;
        """
    )

    load_udv_drivers = PostgresOperator(
        task_id="load_udv_drivers",
        postgres_conn_id="main_postgres",
        sql="""
        INSERT INTO udv.drivers (driver_id, driver_phone)
        SELECT DISTINCT
            driver_id,
            driver_phone
        FROM rdv.orders_raw
        WHERE driver_id IS NOT NULL;
        """
    )

    load_udv_stores = PostgresOperator(
        task_id="load_udv_stores",
        postgres_conn_id="main_postgres",
        sql="""
        INSERT INTO udv.stores (store_id, store_address)
        SELECT DISTINCT
            store_id,
            store_address
        FROM rdv.orders_raw
        WHERE store_id IS NOT NULL;
        """
    )

    load_udv_orders = PostgresOperator(
        task_id="load_udv_orders",
        postgres_conn_id="main_postgres",
        sql="""
        INSERT INTO udv.orders (
            order_id,
            address_text,
            created_at,
            paid_at,
            canceled_at,
            payment_type,
            order_discount,
            order_cancellation_reason,
            user_id,
            store_id
        )
        SELECT DISTINCT
            order_id,
            address_text,
            created_at,
            paid_at,
            canceled_at,
            payment_type,
            order_discount,
            order_cancellation_reason,
            user_id,
            store_id
        FROM rdv.orders_raw;
        """
    )

    load_udv_deliveries = PostgresOperator(
        task_id="load_udv_deliveries",
        postgres_conn_id="main_postgres",
        sql="""
        INSERT INTO udv.deliveries (
            order_id,
            delivery_started_at,
            delivered_at,
            delivery_cost,
            driver_id
        )
        SELECT DISTINCT
            order_id,
            delivery_started_at,
            delivered_at,
            delivery_cost,
            driver_id
        FROM rdv.orders_raw
        WHERE driver_id IS NOT NULL;
        """
    )

    load_udv_order_items = PostgresOperator(
        task_id="load_udv_order_items",
        postgres_conn_id="main_postgres",
        sql="""
        INSERT INTO udv.order_items (
            order_id,
            item_id,
            item_quantity,
            item_canceled_quantity,
            item_discount,
            item_replaced_id
        )
        SELECT
            order_id,
            item_id,
            sum(item_quantity) as item_quantity,
            sum(item_canceled_quantity) as item_quantity,
            max(item_discount),
            item_replaced_id
        FROM rdv.orders_raw
        WHERE item_id IS NOT NULL
        group by order_id,item_id,item_replaced_id;
        """
    )

    (
        create_udv_schema
        >> create_udv_tables
        >> truncate_udv_tables
        >> [
            load_udv_users,
            load_udv_items,
            load_udv_drivers,
            load_udv_stores
        ]
        >> load_udv_orders
        >> load_udv_deliveries
        >> load_udv_order_items
    )
