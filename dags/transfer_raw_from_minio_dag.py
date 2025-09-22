from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from processed_data.ingestion import ingest_from_minio
from processed_data.utils.telegram_notifier import telegram_notifier

load_dotenv()

default_args = {
    "owner": "serzik",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier
}

with DAG(
    dag_id="2_TRANSFER_RAW_FROM_MINIO",
    description="Перенос данных из MinIO в RAW.",
    default_args=default_args,
    start_date=datetime(2024, 7, 29),
    schedule_interval="*/2 * * * *", 
    catchup=True,
    max_active_runs=1,
    tags=["raw", "ingest", "minio"],
) as dag:

    ingest_minio = PythonOperator(
        task_id="ingest_from_minio",
        python_callable=ingest_from_minio
    )
