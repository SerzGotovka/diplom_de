import logging
import os
from dotenv import load_dotenv

from neo4j import GraphDatabase

from .config import get_neo4j_config

# Загружаем переменные окружения из .env файла
load_dotenv()

logger = logging.getLogger(__name__)

def run_cypher_files(folder):
    cfg = get_neo4j_config()
    driver = GraphDatabase.driver(cfg["uri"], auth=(cfg["user"], cfg["password"]))
    files = [file for file in sorted(os.listdir(folder)) if file.endswith('.cypher')]
    with driver.session() as session:
        for file in files:
            with open(os.path.join(folder, file), encoding='utf-8') as f:
                cypher = f.read()
                logger.info(f"Running {file}...")
                session.run(cypher)
    driver.close()