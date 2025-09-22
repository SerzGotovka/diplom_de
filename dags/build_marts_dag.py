from datetime import datetime
from dotenv import load_dotenv
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from processed_data.loaders.dds_to_clickhouse_community_stats_metric import upsert_community_stats
from processed_data.loaders.dds_to_clickhouse_daily_platform_stats_metric import upsert_daily_platform_stats
from processed_data.loaders.neo4j_dds_to_clickhouse_social_graph_stats_metric import upsert_social_graph_stats
from processed_data.utils.telegram_notifier import telegram_notifier

load_dotenv()

default_args = {
    "owner": "serzik",
    "retries": 3,
    "on_failure_callback": telegram_notifier,
    "on_success_callback": telegram_notifier
}


with DAG(
    dag_id="5_CREATE_DATA_MARTS_TO_CLICKHOUSE",
    description="Расчёт агрегатов и загрузка витрин в ClickHouse.",
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
            "as_of_date": "{{ ds }}",  
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