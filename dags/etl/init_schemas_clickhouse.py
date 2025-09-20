import logging
import os

from clickhouse_driver import Client

from .config import get_clickhouse_config
from .loaders_utils.load_sql import load_sql

logger = logging.getLogger(__name__)

def _client():
    ch = get_clickhouse_config()
    return Client(
        host=ch["host"],
        port=ch["port"],
        user=ch["user"],
        password=ch["password"],
        database=ch["database"],
    )

def run_ch_sql_folder(layer="ddl", subdir="data_mart"):
    """
        Выполняет ВСЕ .sql файлы из sql/{layer}/{subdir} по алфавиту.
    """
    base_sql_dir = os.environ.get("SQL_DIR", "/opt/airflow/sql")
    folder = os.path.join(base_sql_dir, layer, subdir)

    client = _client()
    files = [f for f in sorted(os.listdir(folder)) if f.endswith(".sql")]
    for fname in files:
        sql = load_sql(fname, layer=layer, subdir=subdir)
        logger.info(f"[CH] Running {os.path.join(folder, fname)}")
        client.execute(sql)
