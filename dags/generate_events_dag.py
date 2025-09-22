import os
import sys
from datetime import datetime, timedelta
from textwrap import dedent
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator

# Загружаем переменные окружения из .env файла
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), "data_generator"))
from generator.generate_events import generate_to_kafka, generate_to_minio, generate_all_data_and_return
from etl.utils.telegram_notifier import telegram_notifier

default_args = {
    "owner": "serzik",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier
}

with DAG(
    dag_id="1_DATA_GENERATOR_to_kafka_and_minio_each_minute",
    description="Генерация исторических/стриминговых данных (Faker) → Kafka и MinIO (батчи) каждую минуту",
    doc_md=dedent("""
    ### Что делает DAG
    - Генерирует данные пользователей/посты/комменты/реакции/дружбу/сообщества каждую минуту.
    - Пишет в Kafka топики и складывает батчи JSON в MinIO под датированными именами.
    - Нежелательно использовать для backfill.
    """),
    default_args=default_args,
    schedule_interval="* * * * *", #генерируем данные каждую минуту
    start_date=datetime(2024, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["generator", "raw", "minio", "kafka"],
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