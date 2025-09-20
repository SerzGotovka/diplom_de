import io
import json

from kafka import KafkaProducer
from minio import Minio

from .config import (
    get_kafka_bootstrap_servers,
    get_minio_endpoint,
    get_minio_access_key,
    get_minio_secret_key,
    get_minio_bucket,
    get_minio_use_ssl,
)


def write_to_kafka(topic, event):
    producer = KafkaProducer(
        bootstrap_servers=get_kafka_bootstrap_servers(),
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8")
    )
    producer.send(topic, event)
    producer.flush()

def write_to_minio(filename, events):
    client = Minio(
        get_minio_endpoint(),
        access_key=get_minio_access_key(),
        secret_key=get_minio_secret_key(),
        secure=get_minio_use_ssl()
    )
    if not client.bucket_exists(get_minio_bucket()):
        client.make_bucket(get_minio_bucket())
    data = json.dumps([event for event in events], default=str).encode("utf-8")
    client.put_object(
        get_minio_bucket(),
        filename,
        io.BytesIO(data),
        length=len(data)
    )

def write_to_file(filename, events):
    with open(filename, "w") as f:
        json.dump([event for event in events], f, default=str)
