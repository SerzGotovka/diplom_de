import logging
import os
from dotenv import load_dotenv

from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore

# Загружаем переменные окружения из .env файла
load_dotenv()

logger = logging.getLogger(__name__)

def run_sql_files(folder_or_file):
    pg_hook = PostgresHook(postgres_conn_id="my_postgress_conn")

    # Если это файл — выполняем просто его, если папка — все файлы по алфавиту
    if os.path.isfile(folder_or_file):
        files = [folder_or_file]
    else:
        files = [os.path.join(folder_or_file, file) for file in sorted(os.listdir(folder_or_file)) if file.endswith('.sql')]
    for file in files:
        with open(file, encoding='utf-8') as f:
            sql = f.read()
            logger.info(f"Running {file}...")
            pg_hook.run(sql)