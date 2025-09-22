import logging
from dotenv import load_dotenv

from airflow.providers.postgres.hooks.postgres import PostgresHook
from .loaders_utils.load_sql import load_sql

# Загружаем переменные окружения из .env файла
load_dotenv()

logger = logging.getLogger(__name__)

def save_to_dds(event: dict, entity_type: str):
    pg_hook = PostgresHook(postgres_conn_id="my_postgress_conn")
    sql_map = {
        "user": "insert_user.sql",
        "friend": "insert_friend.sql",
        "post": "insert_post.sql",
        "comment": "insert_comment.sql",
        "like": "insert_like.sql",
        "reaction": "insert_reaction.sql",
        "community": "insert_community.sql",
        "group_member": "insert_group_member.sql",
        "media": "insert_media.sql",
        "pinned_post": "insert_pinned_post.sql"
    }
    if entity_type not in sql_map:
        logger.warning(f"[WARN] Unknown entity type for dds: {entity_type}")
        return

    sql = load_sql(sql_map[entity_type], subdir="dds")
    pg_hook.run(sql, parameters=event)
