import os

import psycopg2


def load_cypher(query_file, layer="dml", subdir=""):
    """
        query_file: имя cypher-файла, например 'insert_friend.cypher' или 'insert_attached_media.cypher'
        layer: dml / dql / ddl
        subdir: raw / dds / или любой другой подкаталог, если нужен
    """
    base_cypher_dir = os.environ.get("CYPHER_DIR", "/opt/airflow/cypher")
    cypher_dir = os.path.join(base_cypher_dir, layer, subdir)
    path = os.path.join(cypher_dir, query_file)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Cypher-файл не найден: {path}")
    with open(path, encoding='utf-8') as f:
        return f.read()

