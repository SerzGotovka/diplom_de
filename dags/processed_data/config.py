import os
import logging
from typing import Dict, Any
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
# Можно указать конкретный путь к .env файлу: load_dotenv('/path/to/.env')
load_dotenv()

logger = logging.getLogger(__name__)

def get_postgres_config() -> Dict[str, Any]:
    """
    Получает конфигурацию PostgreSQL из переменных окружения
    """
    required_vars = [
        'POSTGRES_HOST',
        'POSTGRES_PORT', 
        'POSTGRES_USER',
        'POSTGRES_PASSWORD',
        'POSTGRES_DB'
    ]
    
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        logger.error(f"Отсутствуют переменные окружения: {missing_vars}")
        raise RuntimeError("One or more PostgreSQL env vars are not set")
    
    config = {
        'host': os.environ.get('POSTGRES_HOST', 'postgres'),
        'port': int(os.environ.get('POSTGRES_PORT', '5432')),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD'),
        'dbname': os.environ.get('POSTGRES_DB')
    }
    
    logger.info(f"PostgreSQL config loaded: host={config['host']}, port={config['port']}, user={config['user']}, db={config['dbname']}")
    return config

def get_clickhouse_config() -> Dict[str, Any]:
    """
    Получает конфигурацию ClickHouse из переменных окружения
    """
    required_vars = [
        'CLICKHOUSE_HOST',
        'CLICKHOUSE_PORT',
        'CLICKHOUSE_USER', 
        'CLICKHOUSE_PASSWORD',
        'CLICKHOUSE_DB'
    ]
    
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        logger.error(f"Отсутствуют переменные окружения: {missing_vars}")
        raise RuntimeError("One or more ClickHouse env vars are not set")
    
    config = {
        'host': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        'port': int(os.environ.get('CLICKHOUSE_PORT', '9000')),
        'user': os.environ.get('CLICKHOUSE_USER'),
        'password': os.environ.get('CLICKHOUSE_PASSWORD'),
        'database': os.environ.get('CLICKHOUSE_DB')
    }
    
    logger.info(f"ClickHouse config loaded: host={config['host']}, port={config['port']}, user={config['user']}, db={config['database']}")
    return config

def get_neo4j_config() -> Dict[str, Any]:
    """
    Получает конфигурацию Neo4j из переменных окружения
    """
    required_vars = [
        'NEO4J_HOST',
        'NEO4J_PORT',
        'NEO4J_USER',
        'NEO4J_PASSWORD'
    ]
    
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        logger.error(f"Отсутствуют переменные окружения: {missing_vars}")
        raise RuntimeError("One or more Neo4j env vars are not set")
    
    config = {
        'uri': f"bolt://{os.environ.get('NEO4J_HOST', 'neo4j')}:{os.environ.get('NEO4J_PORT', '7687')}",
        'user': os.environ.get('NEO4J_USER'),
        'password': os.environ.get('NEO4J_PASSWORD')
    }
    
    logger.info(f"Neo4j config loaded: uri={config['uri']}, user={config['user']}")
    return config

def get_kafka_bootstrap_servers() -> str:
    """
    Получает адреса брокеров Kafka из переменных окружения
    """
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    logger.info(f"Kafka bootstrap servers: {bootstrap_servers}")
    return bootstrap_servers

def get_minio_bucket() -> str:
    """
    Получает имя бакета MinIO из переменных окружения
    """
    bucket = os.environ.get('MINIO_BUCKET', 'raw-data')
    logger.info(f"MinIO bucket: {bucket}")
    return bucket