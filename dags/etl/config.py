import os

def get_kafka_bootstrap_servers():
    value = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not value:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS env var is not set")
    return value.split(',')

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
    value = os.environ.get("MINIO_BUCKET")
    if not value:
        raise RuntimeError("MINIO_BUCKET env var is not set")
    return value

def get_minio_use_ssl():
    value = os.environ.get("MINIO_USE_SSL")
    if value is None:
        raise RuntimeError("MINIO_USE_SSL env var is not set")
    return value.lower() == "true"

def get_postgres_config():
    dbname = os.environ.get("POSTGRES_DB")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("POSTGRES_HOST")
    if not all([dbname, user, password, host]):
        raise RuntimeError("One or more PostgreSQL env vars are not set")
    return {
        "dbname": dbname,
        "user": user,
        "password": password,
        "host": host,
    }

def get_neo4j_config():
    uri = os.environ.get("NEO4J_URI")
    user = os.environ.get("NEO4J_USER")
    password = os.environ.get("NEO4J_PASSWORD")
    if not all([uri, user, password]):
        raise RuntimeError("One or more Neo4j env vars are not set")
    return {
        "uri": uri,
        "user": user,
        "password": password,
    }

def get_clickhouse_config():
    host = os.environ.get("CLICKHOUSE_HOST")
    port = os.environ.get("CLICKHOUSE_PORT")
    user = os.environ.get("CLICKHOUSE_USER")
    password = os.environ.get("CLICKHOUSE_PASSWORD")
    database = os.environ.get("CLICKHOUSE_DB")
    if not all([host, port, user, password, database]):
        raise RuntimeError("One or more ClickHouse env vars are not set")
    return {
        "host": host,
        "port": int(port),
        "user": user,
        "password": password,
        "database": database,
    }
