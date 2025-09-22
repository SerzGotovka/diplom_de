from datetime import datetime
from textwrap import dedent
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator

# Загружаем переменные окружения из .env файла
load_dotenv()

from etl.loaders.dds_to_clickhouse_community_stats_metric import upsert_community_stats
from etl.loaders.dds_to_clickhouse_daily_platform_stats_metric import upsert_daily_platform_stats
from etl.loaders.neo4j_dds_to_clickhouse_social_graph_stats_metric import upsert_social_graph_stats
from etl.utils.telegram_notifier import telegram_notifier

default_args = {
    "owner": "serzik",
    "retries": 3,
    "on_failure_callback": telegram_notifier,
    "on_success_callback": telegram_notifier
}


with DAG(
    dag_id="5_BUILD_MARTS_TO_CLICKHOUSE",
    description="Расчёт агрегатов (DAU/WAU/MAU, сообщества, граф) и загрузка витрин в ClickHouse.",
    doc_md=dedent("""
    ### Что делает DAG
    - Считает метрики платформы (окна [d0,d1), [w0,d1), [m0,d1)).
    - Строит суточную статистику по сообществам.
    - Выгружает summary графа и degree centrality из Neo4j.
    """),
    schedule_interval="@daily",
    start_date=datetime(2024, 7, 1),
    catchup=True,
    max_active_runs=1,
    tags=["data_mart", "clickhouse"],
    default_args=default_args,
) as dag:

    dm_daily = PythonOperator(
        task_id="dm_daily_platform_stats",
        python_callable=upsert_daily_platform_stats,
        op_kwargs={"as_of_date": "{{ ds }}"}
    )

    dm_social = PythonOperator(
        task_id="dm_social_graph_stats",
        python_callable=upsert_social_graph_stats,
        op_kwargs={
            "as_of_date": "{{ ds }}",  # строка 'YYYY-MM-DD'
            "as_of_end_ts": "{{ data_interval_end | ts }}"
        }
    )

    dm_comm = PythonOperator(
        task_id="dm_community_stats",
        python_callable=upsert_community_stats,
        op_kwargs={"as_of_date": "{{ ds }}"}
    )

dm_daily >> dm_comm
dm_social