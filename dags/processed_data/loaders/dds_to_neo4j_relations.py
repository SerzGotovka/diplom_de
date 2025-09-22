import logging
from dotenv import load_dotenv

from neo4j import GraphDatabase
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore

from ..config import get_neo4j_config
from ..loaders_utils.load_cypher import load_cypher
from ..loaders_utils.load_sql import load_sql

# Загружаем переменные окружения из .env файла
load_dotenv()

logger = logging.getLogger(__name__)

ENTITY_META = {
    "friend": {
        "sql_file": "select_friend.sql",
        "cypher_file": "insert_friend.cypher",
        "columns": ["user_id", "friend_id", "created_at"]
    },
    "posted": {
        "sql_file": "select_posted.sql",
        "cypher_file": "insert_posted.cypher",
        "columns": ["user_id", "post_id", "created_at"]
    },
    "group_member": {
        "sql_file": "select_group_member.sql",
        "cypher_file": "insert_group_member.cypher",
        "columns": ["user_id", "community_id", "joined_at"]
    },
    "commented": {
        "sql_file": "select_commented.sql",
        "cypher_file": "insert_commented.cypher",
        "columns": ["comment_id", "user_id", "post_id", "created_at"]
    },
    "reacted_post": {
        "sql_file": "select_reacted_post.sql",    # этот select должен выбирать только реакции на post
        "cypher_file": "insert_reacted_post.cypher",
        "columns": ["user_id", "target_id", "reaction", "created_at"]
    },
    "reacted_comment": {
        "sql_file": "select_reacted_comment.sql", # этот select должен выбирать только реакции на comment
        "cypher_file": "insert_reacted_comment.cypher",
        "columns": ["user_id", "target_id", "reaction", "created_at"]
    },
    "pinned_post": {
        "sql_file": "select_pinned_post.sql",
        "cypher_file": "insert_pinned_post.cypher",
        "columns": ["community_id", "post_id"]
    },
    "attached_media": {
        "sql_file": "select_attached_media.sql",
        "cypher_file": "insert_attached_media.cypher",
        "columns": ["attached_to_post", "media_id"]
    }
}

def copy_relation_to_neo4j(entity):
    pg_hook = PostgresHook(postgres_conn_id="my_postgress_conn")
    meta = ENTITY_META[entity]
    sql = load_sql(meta["sql_file"], layer="dql", subdir="dds/relations")
    
    # Получаем данные из PostgreSQL через PostgresHook
    rows = pg_hook.get_records(sql)
    
    # Фильтруем пустые строки и строки с None значениями
    valid_rows = []
    for row in rows:
        if row and all(value is not None for value in row):
            valid_rows.append(row)
    
    # Если нет валидных данных, выходим из функции
    if not valid_rows:
        logger.info(f"[INFO] No valid data found for {entity}, skipping...")
        return
    
    rows = valid_rows
    
    # Используем предопределенные колонки
    columns = meta["columns"]
    logger.info(f"[INFO] Processing {len(rows)} rows for {entity} with columns: {columns}")

    cypher = load_cypher(meta["cypher_file"], layer="dml", subdir="relations")
    logger.info(f"[INFO] Cypher query for {entity}: {cypher}")
    
    neo_cfg = get_neo4j_config()
    driver = GraphDatabase.driver(neo_cfg["uri"], auth=(neo_cfg["user"], neo_cfg["password"]))
    with driver.session() as session:
        for i, row in enumerate(rows):
            params = dict(zip(columns, row))
            logger.debug(f"[DEBUG] Row {i+1} params: {params}")
            try:
                session.run(cypher, **params)
            except Exception as e:
                logger.error(f"[ERROR] Failed to process row {i+1} for {entity}: {e}")
                logger.error(f"[ERROR] Row data: {row}")
                raise
    driver.close()
    logger.info(f"[INFO] Successfully processed {len(rows)} rows for {entity}")

def copy_all_relations():
    for entity in ENTITY_META:
        logger.info(f"[INFO] Copying {entity}...")
        copy_relation_to_neo4j(entity)
