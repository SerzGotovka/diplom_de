import logging

import psycopg2

from .config import get_postgres_config
from .loaders_utils.load_sql import load_sql

logger = logging.getLogger(__name__)

def save_to_dds(event: dict, entity_type: str):
    config = get_postgres_config()
    conn = psycopg2.connect(**config)
    cur = conn.cursor()
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
        cur.close()
        conn.close()
        return

    sql = load_sql(sql_map[entity_type], subdir="dds")
    cur.execute(sql, event)
    conn.commit()
    cur.close()
    conn.close()
