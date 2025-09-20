import logging

import psycopg2
from neo4j import GraphDatabase

from ..config import get_postgres_config, get_neo4j_config
from ..loaders_utils.load_cypher import load_cypher
from ..loaders_utils.load_sql import load_sql

logger = logging.getLogger(__name__)

ENTITY_META = {
    "friend": {
        "sql_file": "select_friend.sql",
        "cypher_file": "insert_friend.cypher"
    },
    "posted": {
        "sql_file": "select_posted.sql",
        "cypher_file": "insert_posted.cypher"
    },
    "group_member": {
        "sql_file": "select_group_member.sql",
        "cypher_file": "insert_group_member.cypher"
    },
    "commented": {
        "sql_file": "select_commented.sql",
        "cypher_file": "insert_commented.cypher"
    },
    "reacted_post": {
        "sql_file": "select_reacted_post.sql",    # этот select должен выбирать только реакции на post
        "cypher_file": "insert_reacted_post.cypher",
    },
    "reacted_comment": {
        "sql_file": "select_reacted_comment.sql", # этот select должен выбирать только реакции на comment
        "cypher_file": "insert_reacted_comment.cypher",
    },
    "pinned_post": {
        "sql_file": "select_pinned_post.sql",
        "cypher_file": "insert_pinned_post.cypher"
    },
    "attached_media": {
        "sql_file": "select_attached_media.sql",
        "cypher_file": "insert_attached_media.cypher"
    }
}

def copy_relation_to_neo4j(entity):
    pg_cfg = get_postgres_config()
    meta = ENTITY_META[entity]
    sql = load_sql(meta["sql_file"], layer="dql", subdir="dds/relations")
    conn = psycopg2.connect(**pg_cfg)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()

    cypher = load_cypher(meta["cypher_file"], layer="dml", subdir="relations")
    neo_cfg = get_neo4j_config()
    driver = GraphDatabase.driver(neo_cfg["uri"], auth=(neo_cfg["user"], neo_cfg["password"]))
    with driver.session() as session:
        for row in rows:
            params = dict(zip(columns, row))
            logger.debug(params)
            session.run(cypher, **params)
    driver.close()

def copy_all_relations():
    for entity in ENTITY_META:
        logger.info(f"[INFO] Copying {entity}...")
        copy_relation_to_neo4j(entity)
