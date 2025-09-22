import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

# Загружаем переменные окружения из .env файла
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), "data_generator"))
from generate_data.generate_events import generate_to_kafka, generate_to_minio, generate_all_data_and_return
from processed_data.utils.telegram_notifier import telegram_notifier

default_args = {
    "owner": "serzik",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier
}

with DAG(
    dag_id="1_DATA_GENERATE_to_kafka_and_minio",
    description="Генерация данных в Kafka и MinIO",
    default_args=default_args,
    schedule_interval="* * * * *", #генерация каждую минуту
    start_date=datetime(2024, 7, 1),
    catchup=True,
    max_active_runs=1,
    tags=["generate_data", "raw", "minio", "kafka"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=generate_all_data_and_return,
        op_kwargs={"day": "{{ ds }}"}
    )

    kafka_task = PythonOperator(
        task_id="generate_kafka_events",
        python_callable=generate_to_kafka,
    )

    minio_task = PythonOperator(
        task_id="generate_minio_batch",
        python_callable=generate_to_minio,
        op_kwargs={"day": "{{ ds }}"}
    )

    generate_data >> [kafka_task, minio_task]