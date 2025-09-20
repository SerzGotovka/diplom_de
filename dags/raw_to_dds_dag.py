from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.loaders.raw_to_dds import process_raw_entity
from etl.utils.telegram_notifier import telegram_notifier

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier,
    "pool": "postgres_dwh",   # дефолтный пул для всех тасков DAG'а
    "pool_slots": 1,          # сколько слотов за раз занимает таск
}

with DAG(
    dag_id="3_RAW_to_DDS_dag",
    description="Перенос сущностей RAW → DDS в корректном порядке зависимостей, дедуп и FK/PK.",
    doc_md=dedent("""
    ### Что делает DAG
    - Последовательно загружает user → friend → post → comment → reaction → community → group_member → media → pinned_post.
    - Обеспечивает идемпотентность (уникальные индексы/merge).
    - Готовит данные для Neo4j/витрин.
    """),
    default_args=default_args,
    schedule_interval="*/5 * * * *", # запуск каждые 5 минут
    start_date=datetime(2024, 7, 31),
    catchup=False,
    max_active_runs=1,
    tags=["ddl", "dds", "postgres"]
) as dag:
    entity_type = [
        "user", "community",
        "post", "comment", "media",
        "pinned_post", "group_member", "friend",
        "like", "reaction",
    ]

    tasks = {
        entity: PythonOperator(task_id=f"raw_to_dds_{entity}", python_callable=process_raw_entity, op_args=[entity])
        for entity in entity_type
    }

    # База
    tasks["user"] >> [tasks["post"], tasks["friend"], tasks["group_member"]]
    tasks["community"] >> [tasks["group_member"], tasks["pinned_post"]]

    # Контент и зависящие
    tasks["post"] >> [tasks["comment"], tasks["media"], tasks["pinned_post"], tasks["like"], tasks["reaction"]]
    tasks["comment"] >> [tasks["like"], tasks["reaction"]]