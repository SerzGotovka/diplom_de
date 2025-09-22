import os
from datetime import datetime
from textwrap import dedent
from dotenv import load_dotenv

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

# Загружаем переменные окружения из .env файла
load_dotenv()

from etl.create_database import create_pg_database, create_clickhouse_database
from etl.init_schemas_clickhouse import run_ch_sql_folder
from etl.init_schemas_neo4j import run_cypher_files
from etl.init_schemas_postgres import run_sql_files
from etl.utils.telegram_notifier import telegram_notifier

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

default_args = {
    'start_date': datetime(2024, 7, 29),
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier
}

with DAG(
    dag_id='0_INIT_POSTGRES_NEO4J_CLICKHOUSE_schemas',
    description="Инициализация БД/схем и таблиц в Postgres, Neo4j и ClickHouse. Запускать один раз после поднятия окружения.",
    doc_md=dedent("""
    ### Что делает DAG
    - Создаёт БД DWH в Postgres, схемы RAW/DDS, таблицы и индексы.
    - Создаёт констрейнты/индексы в Neo4j.
    - Создаёт БД/таблицы витрин в ClickHouse.
    """),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["DDL", "init", "raw", "dds","data_mart", "postgres", "neo4j", "clickhouse"],
) as dag:

    # 0. Postgres: Создание базы DWH
    create_dwh_database = PythonOperator(
        task_id='create_dwh_database',
        python_callable=create_pg_database
    )

    # 1. Postgres: Инициализация RAW и DDS схем
    init_pg_schemas = PythonOperator(
        task_id='init_postgres_schemas',
        python_callable=run_sql_files,
        op_args=[os.path.join(BASE, "sql", "ddl", "init_schemas.sql")]
    )

    # 2. Postgres: Создание таблиц в RAW схеме
    create_raw_tables = PythonOperator(
        task_id='create_raw_tables',
        python_callable=run_sql_files,
        op_args=[os.path.join(BASE, "sql", "ddl", "raw")]
    )

    # 3. Postgres: Создание таблиц в DDS схеме
    create_dds_tables = PythonOperator(
        task_id='create_dds_tables',
        python_callable=run_sql_files,
        op_args=[os.path.join(BASE, "sql", "ddl", "dds")]
    )

    # 4. Postgres: Создание таблиц в DDS схеме
    create_dds_indexes = PythonOperator(
        task_id='create_dds_indexes',
        python_callable=run_sql_files,
        op_args=[os.path.join(BASE, "sql", "ddl", "dds/indexes")]
    )

    # 5. Neo4j: создание констрейнтов
    create_neo4j_constraints = PythonOperator(
        task_id='create_neo4j_constraints',
        python_callable=run_cypher_files,
        op_args=[os.path.join(BASE, "cypher", "ddl")]
    )

    # 6. Clickhouse: Создание базы Clickhouse
    create_ch_database = PythonOperator(
        task_id="create_clickhouse_db",
        python_callable=create_clickhouse_database,
    )

    # 7. Clickhouse: Создание таблиц в Clickhouse
    create_ch_tables = PythonOperator(
        task_id="create_clickhouse_tables",
        python_callable=run_ch_sql_folder,  # sql/ddl/data_mart/*.sql
        op_kwargs={"layer": "ddl", "subdir": "data_mart"},
    )
    # Граф выполнения
    create_dwh_database >> init_pg_schemas >> create_raw_tables >> create_dds_tables >> create_dds_indexes
    [create_raw_tables, create_dds_tables] >> create_neo4j_constraints
    create_ch_database >> create_ch_tables