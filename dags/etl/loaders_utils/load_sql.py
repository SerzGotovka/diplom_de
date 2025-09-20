import os

import psycopg2


def load_sql(query_file, layer="dml", subdir="raw"):
    """
        query_file: имя файла, например 'insert_user.sql' или 'select_attached_media.sql'
        layer: dml / dql / ddl
        subdir: raw / dds / или любой другой подкаталог, если нужен
    """
    base_sql_dir = os.environ.get("SQL_DIR", "/opt/airflow/sql")
    sql_dir = os.path.join(base_sql_dir, layer, subdir)
    path = os.path.join(sql_dir, query_file)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"SQL-файл не найден: {path}")
    with open(path, encoding='utf-8') as f:
        return f.read()
