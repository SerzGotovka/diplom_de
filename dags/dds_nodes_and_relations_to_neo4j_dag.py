from datetime import datetime
from textwrap import dedent
from dotenv import load_dotenv

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

# Загружаем переменные окружения из .env файла
load_dotenv()

from etl.loaders.dds_to_neo4j_nodes import copy_nodes_to_neo4j
from etl.loaders.dds_to_neo4j_relations import copy_all_relations
from etl.utils.telegram_notifier import telegram_notifier

default_args = {
    "owner": "serzik",
    "retries": 3,
    "on_success_callback": telegram_notifier,
    "on_failure_callback": telegram_notifier
}

with DAG(
    dag_id="4_DDS_to_NEO4J_dag",
    description="Материализация графа в Neo4j (узлы/связи) из DDS, идемпотентные MERGE.",
    doc_md=dedent("""
    ### Что делает DAG
    - Создаёт (:User), (:Post), (:Comment), (:Community), (:Media) и связи (:FRIENDS_WITH), (:POSTED), (:COMMENTED) и т.д.
    - Использует MERGE + уникальные констрейнты.
    - Годится для расчёта графовых метрик.
    """),
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 7, 30),
    catchup=False,
    max_active_runs=1,
    tags=["dds", "neo4j"],
) as dag:
    create_nodes = PythonOperator(
        task_id="copy_nodes_to_neo4j",
        python_callable=copy_nodes_to_neo4j
        
    )

    create_relations = PythonOperator(
        task_id="copy_relations_to_neo4j",
        python_callable=copy_all_relations
       
    )

    create_nodes >> create_relations

