import datetime as dt
import logging
from dotenv import load_dotenv

from clickhouse_driver import Client
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore

from ..config import get_clickhouse_config
from ..loaders_utils.load_sql import load_sql

# Загружаем переменные окружения из .env файла
load_dotenv()

logger = logging.getLogger(__name__)

def _fetch_rows_from_pg(as_of_date: dt.date):
    sql = load_sql("select_community_stats.sql", layer="dql", subdir="data_mart")
    pg_hook = PostgresHook(postgres_conn_id="my_postgress_conn")
    # В SQL-запросе используется %s три раза, поэтому передаем параметр три раза
    rows = pg_hook.get_records(sql, parameters=[as_of_date, as_of_date, as_of_date])
    return rows  # [(dt, group_id, new_members, posts, comments, reactions), ...]

def upsert_community_stats(as_of_date: dt.date = None):
    if as_of_date is None:
        as_of_date = dt.date.today()

    rows = _fetch_rows_from_pg(as_of_date)

    ch = get_clickhouse_config()
    client = Client(host=ch["host"], port=int(ch["port"]),
                    user=ch["user"], password=ch["password"],
                    database=ch["database"])

    client.execute("ALTER TABLE data_mart.community_daily_stats DELETE WHERE dt = %(dt)s",
                   params={"dt": as_of_date})
    if rows:
        client.execute(
            "INSERT INTO data_mart.community_daily_stats (dt, community_id, new_members, posts, comments, reactions) VALUES",
            rows
        )
    client.disconnect()
    logger.info(f"[CH] community_daily_stats upserted for {as_of_date}, rows={len(rows)}")
