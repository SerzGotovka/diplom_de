import logging
import os

import psycopg2

from .config import get_postgres_config

logger = logging.getLogger(__name__)

def run_sql_files(folder_or_file):
    config = get_postgres_config()
    conn = psycopg2.connect(**config)
    cur = conn.cursor()

    # Если это файл — выполняем просто его, если папка — все файлы по алфавиту
    if os.path.isfile(folder_or_file):
        files = [folder_or_file]
    else:
        files = [os.path.join(folder_or_file, file) for file in sorted(os.listdir(folder_or_file)) if file.endswith('.sql')]
    for file in files:
        with open(file, encoding='utf-8') as f:
            sql = f.read()
            logger.info(f"Running {file}...")
            cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()