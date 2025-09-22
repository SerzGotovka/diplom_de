import logging
from dotenv import load_dotenv

import pandas as pd
from neo4j import GraphDatabase
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ..config import get_neo4j_config
from ..loaders_utils.load_cypher import load_cypher
from ..loaders_utils.load_sql import load_sql

# Загружаем переменные окружения из .env файла
load_dotenv()

logger = logging.getLogger(__name__)

# ENTITY_META: имя, select-скрипт, cypher-скрипт
ENTITY_META = {
    "user": {
        "sql_file": "select_users.sql",
        "cypher_file": "insert_user.cypher",
    },
    "post": {
        "sql_file": "select_posts.sql",
        "cypher_file": "insert_post.cypher",
    },
    "comment": {
        "sql_file": "select_comments.sql",
        "cypher_file": "insert_comment.cypher",
    },
    "community": {
        "sql_file": "select_communities.sql",
        "cypher_file": "insert_community.cypher",
    },
    "media": {
        "sql_file": "select_media.sql",
        "cypher_file": "insert_media.cypher",
    },
    "pinned_post": {
        "sql_file": "select_pinned_posts.sql",
        "cypher_file": "insert_pinned_post.cypher",
    },
    "reaction": {
        "sql_file": "select_reactions.sql",
        "cypher_file": "insert_reaction.cypher",
    }
}

def copy_nodes_to_neo4j():
    pg_hook = PostgresHook(postgres_conn_id="my_postgress_conn")
    neo_cfg = get_neo4j_config()
    driver = GraphDatabase.driver(neo_cfg["uri"], auth=(neo_cfg["user"], neo_cfg["password"]))

    for entity, meta in ENTITY_META.items():
        logger.info(f"[INFO] Copying {entity} nodes...")
        sql = load_sql(meta["sql_file"], layer="dql", subdir="dds/nodes")
        
        # Получаем данные из PostgreSQL через PostgresHook с использованием pandas
        df = pg_hook.get_pandas_df(sql)
        
        # Если нет данных, пропускаем эту сущность
        if df.empty:
            logger.info(f"[INFO] No data found for {entity} nodes, skipping...")
            continue
            
        rows = df.values.tolist()
        columns = df.columns.tolist()
        
        # Фильтруем строки с None значениями
        valid_rows = []
        for row in rows:
            if all(pd.notna(value) for value in row):
                valid_rows.append(row)
        
        if not valid_rows:
            logger.info(f"[INFO] No valid data found for {entity} nodes, skipping...")
            continue
            
        rows = valid_rows
        
        cypher = load_cypher(meta["cypher_file"], layer="dml", subdir="nodes")
        logger.info(f"[INFO] Columns: {columns}")
        logger.info(f"[INFO] Cypher query: {cypher}")
        with driver.session() as session:
            for i, row in enumerate(rows):
                params = dict(zip(columns, row))
                logger.debug(f"[DEBUG] Row {i+1} parameters: {params}")
                try:
                    session.run(cypher, **params)
                except Exception as e:
                    logger.error(f"[ERROR] Failed to process row {i+1} for {entity}: {e}")
                    logger.error(f"[ERROR] Row data: {row}")
                    raise

    driver.close()
