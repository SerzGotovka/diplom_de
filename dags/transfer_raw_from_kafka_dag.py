from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from processed_data.ingestion import ingest_from_kafka_topics
from processed_data.utils.telegram_notifier import telegram_notifier

load_dotenv()

default_args = {
    "owner": "serzik",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": telegram_notifier,
}

HEAVY = ["likes", "comments", "reactions", "posts"]
LIGHT = ["users", "friends", "communities", "group_members", "media", "pinned_posts"]

with DAG(
    dag_id="2_TRANSFER_RAW_FROM_KAFKA",
    description="Перенос данных из Kafka в RAW.",
    default_args=default_args,
    start_date=datetime(2024, 7, 29),
    schedule_interval="*/2 * * * *", 
    catchup=True,
    max_active_runs=1,               
    tags=["raw", "ingest", "kafka"],
) as dag:

    # тяжёлые топики — отдельные воркеры (делят партиции)
    heavy_tasks = []
    for topic in HEAVY:
        heavy_tasks.append(
            PythonOperator(
                task_id=f"consume_{topic}",
                python_callable=ingest_from_kafka_topics,
                op_kwargs={"topics": [topic], "batch_size": 2000, "group_id": "raw-loader"},
            )
        )

    # лёгкие — одним воркером
    light_task = PythonOperator(
        task_id="consume_light",
        python_callable=ingest_from_kafka_topics,
        op_kwargs={"topics": LIGHT, "batch_size": 1000, "group_id": "raw-loader"},
    )

    heavy_tasks >> light_task

