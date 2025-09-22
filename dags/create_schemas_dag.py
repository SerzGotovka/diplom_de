import os
from datetime import datetime
from dotenv import load_dotenv
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from processed_data.create_database import create_pg_database, create_clickhouse_database
from processed_data.init_schemas_clickhouse import run_ch_sql_folder
from processed_data.init_schemas_neo4j import run_cypher_files
from processed_data.init_schemas_postgres import run_sql_files
from processed_data.utils.telegram_notifier import telegram_notifier


load_dotenv()

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

default_args = {
    'start_date': datetime(2024, 7, 29),
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier
}

with DAG(
    dag_id='0_INIT_schemas',
    description="Создание БД/схем и таблиц в Postgres, Neo4j и ClickHouse.",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["DDL", "raw", "dds","data_mart", "postgres", "neo4j", "clickhouse"],
) as dag:

    # Создание базы DWH в Postgres
    create_dwh_database = PythonOperator(
        task_id='create_dwh_database',
        python_callable=create_pg_database
    )

    # создание RAW и DDS схем в Postgres
    init_pg_schemas = PythonOperator(
        task_id='init_postgres_schemas',
        python_callable=run_sql_files,
        op_args=[os.path.join(BASE, "sql", "ddl", "init_schemas.sql")]
    )

    # Создание таблиц в RAW схеме в Postgres
    create_raw_tables = PythonOperator(
        task_id='create_raw_tables',
        python_callable=run_sql_files,
        op_args=[os.path.join(BASE, "sql", "ddl", "raw")]
    )

    # Создание таблиц в DDS схеме 
    create_dds_tables = PythonOperator(
        task_id='create_dds_tables',
        python_callable=run_sql_files,
        op_args=[os.path.join(BASE, "sql", "ddl", "dds")]
    )

    # создание в Neo4j
    create_neo4j = PythonOperator(
        task_id='create_neo4j',
        python_callable=run_cypher_files,
        op_args=[os.path.join(BASE, "cypher", "ddl")]
    )

    # Создание базы Clickhouse
    create_ch_database = PythonOperator(
        task_id="create_clickhouse_db",
        python_callable=create_clickhouse_database,
    )

    # Создание таблиц в Clickhouse
    create_ch_tables = PythonOperator(
        task_id="create_clickhouse_tables",
        python_callable=run_ch_sql_folder,  
        op_kwargs={"layer": "ddl", "subdir": "data_mart"},
    )
    # Граф выполнения
    create_dwh_database >> init_pg_schemas >> create_raw_tables >> create_dds_tables
    [create_raw_tables, create_dds_tables] >> create_neo4j
    create_ch_database >> create_ch_tables