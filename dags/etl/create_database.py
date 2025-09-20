import logging
import os

import psycopg2
from clickhouse_driver import Client
from .config import get_postgres_config, get_clickhouse_config

logger = logging.getLogger(__name__)

def create_pg_database(db_name=os.environ.get("POSTGRES_DB")):
    config = get_postgres_config()
    config["dbname"] = "postgres"   # <-- меняем значение, только для создания БД!
    conn = psycopg2.connect(**config)
    conn.autocommit = True
    cur = conn.cursor()
    # проверяем, существует ли БД
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
    exists = cur.fetchone() is not None

    if not exists:
        cur.execute(f"CREATE DATABASE {db_name};")
        logger.info(f"[PG] Database '{db_name}' created")
    else:
        logger.info(f"[PG] Database '{db_name}' already exists")
    cur.close()
    conn.close()

def create_clickhouse_database(db_name=os.environ.get("CLICKHOUSE_DB")):
    config = get_clickhouse_config()
    config["database"] = "default"
    client = Client(**config)
    client.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
    logger.info(f"[CH] Database ensured: {db_name}")