import json
import logging

import psycopg2

from .config import get_postgres_config
from .loaders_utils.load_sql import load_sql

logger = logging.getLogger(__name__)

_sql_cache = {}
_sql_map = {
    "user": "insert_raw_user.sql",
    "post": "insert_raw_post.sql",
    "comment": "insert_raw_comment.sql",
    "like": "insert_raw_like.sql",
    "reaction": "insert_raw_reaction.sql",
    "community": "insert_raw_community.sql",
    "media": "insert_raw_media.sql",
    "pinned_post": "insert_raw_pinned_post.sql",
    "friend": "insert_raw_friend.sql",
    "group_member": "insert_raw_group_member.sql",
}

# Cлужебные SQL для статуса обработанных объектов
_proc_sql_map = {
    "select_processed": ("dql", "raw", "select_processed_object.sql"),
    "insert_processed": ("dml", "raw", "insert_processed_object.sql"),
}

def _get_sql(event_type):
    fname = _sql_map[event_type]
    if fname not in _sql_cache:
        # поправь layer/subdir под свою структуру, если нужно
        _sql_cache[fname] = load_sql(fname, layer="dml", subdir="raw")
    return _sql_cache[fname]

def _get_proc_sql(key):
    layer, subdir, fname = _proc_sql_map[key]
    cache_key = f"{layer}/{subdir}/{fname}"
    if cache_key not in _sql_cache:
        _sql_cache[cache_key] = load_sql(fname, layer=layer, subdir=subdir)
    return _sql_cache[cache_key]

# ---- API для отметки/проверки статуса файла ----
def is_object_processed(storage: str, object_key: str, cur) -> bool:
    """
        Проверяем, обрабатывали ли уже объект из MinIO (или другого стораджа).
    """
    sql = _get_proc_sql("select_processed")
    cur.execute(sql, {"storage": storage, "object_key": object_key})
    return cur.fetchone() is not None

def mark_object_processed(storage: str, object_key: str, etag: str, cur) -> None:
    """
        Помечаем объект как обработанный.
    """
    sql = _get_proc_sql("insert_processed")
    cur.execute(sql, {"storage": storage, "object_key": object_key, "etag": etag})

def save_to_raw(event, event_type):
    cfg = get_postgres_config()
    with psycopg2.connect(**cfg) as conn, conn.cursor() as cur:
        if event_type not in _sql_map:
            logger.warning(f"[WARN] Unknown event type for RAW: {event_type}")
            return
        sql = _get_sql(event_type)
        cur.execute(sql, {"event_json": json.dumps(event)})

def save_to_raw_bulk(event_type, events, cur):
    if not events:
        return
    if event_type not in _sql_map:
        logger.warning(f"[WARN] Unknown event type for RAW: {event_type}")
        return
    sql = _get_sql(event_type)
    params = [{"event_json": json.dumps(e)} for e in events]
    cur.executemany(sql, params)
