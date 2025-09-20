import datetime as dt
import logging

import psycopg2
from clickhouse_driver import Client

from ..config import get_postgres_config, get_clickhouse_config
from ..loaders_utils.load_sql import load_sql

logger = logging.getLogger(__name__)

def _fetch_metrics_from_pg(as_of_date: dt.date):
    sql = load_sql("select_daily_platform_stats.sql", layer="dql", subdir="data_mart")
    pg = get_postgres_config()
    with psycopg2.connect(**pg) as conn, conn.cursor() as cur:
        cur.execute(sql, {"as_of_date": as_of_date})
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))

def upsert_daily_platform_stats(as_of_date: dt.date = None):
    """
    Тянем агрегаты из PG и вставляем в CH (data_mart.daily_platform_stats)
    """
    if as_of_date is None:
        as_of_date = dt.date.today()

    metrics = _fetch_metrics_from_pg(as_of_date)
    # подготовка значений для вставки в CH
    values = (
        metrics["dt"],
        int(metrics["total_posts"] or 0),
        int(metrics["total_comments"] or 0),
        int(metrics["total_reactions"] or 0),
        float(metrics["interactions_per_post"] or 0.0),
        int(metrics["dau"] or 0),
        int(metrics["wau"] or 0),
        int(metrics["mau"] or 0),
        float(metrics["dau_wau_ratio"] or 0.0),
        float(metrics["wau_mau_ratio"] or 0.0),
    )

    ch_cfg = get_clickhouse_config()
    # теперь подключаемся уже к целевой БД
    client = Client(host=ch_cfg["host"], port=int(ch_cfg["port"]),
                    user=ch_cfg["user"], password=ch_cfg["password"],
                    database=ch_cfg["database"])

    # простая идемпотентность: удалим строку за дату и вставим новую
    delete_dt = metrics["dt"]
    delete_sql = load_sql("delete_daily_platform_stats.sql", layer="dml", subdir="data_mart")
    if isinstance(delete_dt, str):
        delete_dt = dt.date.fromisoformat(delete_dt)
    client.execute(
        delete_sql,
        params={"dt": delete_dt}
    )

    # ClickHouse асинхронно мутирует, но для простоты вставим сразу
    insert_sql = load_sql("insert_daily_platform_stats.sql", layer="dml", subdir="data_mart")
    client.execute(
        insert_sql,
        [values]
    )

    flush_sql = load_sql("system_flush_logs.sql", layer="dml", subdir="data_mart")
    client.execute(flush_sql)

    optimize_sql = load_sql("optimize_daily_platform_stats.sql", layer="dml", subdir="data_mart")
    client.execute(optimize_sql)

    client.disconnect()
    logger.info(f"[CH] daily_platform_stats upserted for {as_of_date}")
