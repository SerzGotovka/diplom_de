import io
import json

from kafka import KafkaProducer
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # type: ignore

from .config import (
    get_kafka_bootstrap_servers,
    get_minio_bucket,
)


def write_to_kafka(topic, event):
    producer = KafkaProducer(
        bootstrap_servers=get_kafka_bootstrap_servers(),
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8")
    )
    producer.send(topic, event)
    producer.flush()

def write_to_minio(filename, events):
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    bucket = get_minio_bucket()
    
    # Проверяем существование bucket и создаем если нужно
    if not s3_hook.check_for_bucket(bucket):
        s3_hook.create_bucket(bucket)
    
    data = json.dumps([event for event in events], default=str).encode("utf-8")
    s3_hook.load_bytes(
        data,
        key=filename,
        bucket_name=bucket
    )

def write_to_file(filename, events):
    with open(filename, "w") as f:
        json.dump([event for event in events], f, default=str)
