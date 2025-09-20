import logging

import psycopg2
from neo4j import GraphDatabase

from ..config import get_postgres_config, get_neo4j_config
from ..loaders_utils.load_cypher import load_cypher
from ..loaders_utils.load_sql import load_sql

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
    pg_cfg = get_postgres_config()
    neo_cfg = get_neo4j_config()
    conn = psycopg2.connect(**pg_cfg)
    driver = GraphDatabase.driver(neo_cfg["uri"], auth=(neo_cfg["user"], neo_cfg["password"]))

    for entity, meta in ENTITY_META.items():
        logger.info(f"[INFO] Copying {entity} nodes...")
        sql = load_sql(meta["sql_file"], layer="dql", subdir="dds/nodes")
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]  # получаем названия колонок
            cypher = load_cypher(meta["cypher_file"], layer="dml", subdir="nodes")
            with driver.session() as session:
                for row in rows:
                    params = dict(zip(columns, row))
                    session.run(cypher, **params)

    driver.close()
    conn.close()
