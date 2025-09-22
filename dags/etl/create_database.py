import logging
import os
from dotenv import load_dotenv

from clickhouse_driver import Client
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from .config import get_clickhouse_config

# Загружаем переменные окружения из .env файла
load_dotenv()

logger = logging.getLogger(__name__)

def create_pg_database(db_name='airflow'):
    # Создаем PostgresHook с конфигурацией
    pg_hook = PostgresHook(postgres_conn_id="my_postgress_conn")
    
    # Проверяем, существует ли БД
    exists_query = "SELECT 1 FROM pg_database WHERE datname = %s;"
    result = pg_hook.get_first(exists_query, parameters=(db_name,))
    exists = result is not None

    if not exists:
        # CREATE DATABASE не может выполняться внутри транзакции, используем autocommit
        pg_hook.run(f"CREATE DATABASE {db_name};", autocommit=True)
        logger.info(f"[PG] Database '{db_name}' created")
    else:
        logger.info(f"[PG] Database '{db_name}' already exists")

def create_clickhouse_database(db_name=None):
    if db_name is None:
        db_name = "data_mart"  # Всегда создаем базу data_mart для витрин данных
    config = get_clickhouse_config()
    config["database"] = "default"
    client = Client(**config)
    client.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
    logger.info(f"[CH] Database ensured: {db_name}")