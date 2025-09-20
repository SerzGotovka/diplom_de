from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from etl.utils.telegram_notifier import telegram_notifier
from generator.generate_events import generate_to_kafka, generate_to_minio, generate_all_data_and_return

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier
}

with DAG(
    dag_id="1_DATA_GENERATOR_daily",
    description="Генерация исторических/стриминговых данных (Faker) → Kafka и MinIO (батчи) за 1 ДЕНЬ",
    doc_md=dedent("""
    ### Что делает DAG
    - Генерирует данные пользователей/посты/комменты/реакции/дружбу/сообщества за 1 день.
    - Пишет в Kafka топики и складывает батчи JSON в MinIO под датированными именами.
    - Можно  и нужно использовать для backfill.
    """),
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 7, 1),
    catchup=True,
    max_active_runs=1,
    tags=["generator", "raw", "minio", "kafka"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=generate_all_data_and_return,
        op_kwargs={"day": "{{ ds }}"}   # <- ключ: передаём дату запуска 'YYYY-MM-DD'
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

    # «Слип» на 2 минуты
    throttle_2m = BashOperator(
        task_id="throttle",
        bash_command="sleep 10"
    )

    generate_data >> [kafka_task, minio_task]
    [kafka_task, minio_task] >> throttle_2m
