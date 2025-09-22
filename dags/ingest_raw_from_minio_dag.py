from datetime import datetime, timedelta
from textwrap import dedent
from dotenv import load_dotenv

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

# Загружаем переменные окружения из .env файла
load_dotenv()

from etl.ingestion import ingest_from_minio
from etl.utils.telegram_notifier import telegram_notifier

default_args = {
    "owner": "serzik",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier
}

with DAG(
    dag_id="2_INGEST_RAW_FROM_MINIO",
    description="Импорт батчей JSON из MinIO в RAW (Postgres) c пометкой обработанных объектов.",
    doc_md=dedent("""
    ### Что делает DAG
    - Читает JSON / JSONL файлы из MinIO.
    - Не удаляет источник; записывает отметку в raw.processed_objects, чтобы не обрабатывать повторно.
    - Поддерживает группировку по типам и batched insert.
    """),
    default_args=default_args,
    start_date=datetime(2024, 7, 29),
    schedule_interval="*/2 * * * *",  # каждые 5 минут
    catchup=False,
    max_active_runs=1,
    tags=["raw", "ingest", "minio"],
) as dag:

    ingest_minio = PythonOperator(
        task_id="ingest_from_minio",
        python_callable=ingest_from_minio
    )
