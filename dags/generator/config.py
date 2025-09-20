import os

def get_kafka_bootstrap_servers():
    value = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not value:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS env var is not set")
    return value

def get_minio_endpoint():
    value = os.environ.get("MINIO_ENDPOINT")
    if not value:
        raise RuntimeError("MINIO_ENDPOINT env var is not set")
    return value

def get_minio_access_key():
    value = os.environ.get("MINIO_ROOT_USER")
    if not value:
        raise RuntimeError("MINIO_ROOT_USER env var is not set")
    return value

def get_minio_secret_key():
    value = os.environ.get("MINIO_ROOT_PASSWORD")
    if not value:
        raise RuntimeError("MINIO_ROOT_PASSWORD env var is not set")
    return value

def get_minio_bucket():
    return os.environ.get("MINIO_BUCKET", "events")

def get_minio_use_ssl():
    return os.environ.get("MINIO_USE_SSL", "False").lower() == "true"
