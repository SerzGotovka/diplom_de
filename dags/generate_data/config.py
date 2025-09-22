import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

def get_kafka_bootstrap_servers():
    value = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return value

def get_minio_bucket():
    return os.environ.get("MINIO_BUCKET", "events")
