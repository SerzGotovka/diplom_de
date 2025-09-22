import json
import logging
import time
from collections import defaultdict
from typing import Dict, List, Any, Iterable
from dotenv import load_dotenv

from kafka import KafkaConsumer
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # type: ignore

# Загружаем переменные окружения из .env файла
load_dotenv()

from .config import (
    get_kafka_bootstrap_servers,
    get_minio_bucket
)
from .save_to_raw import (
    save_to_raw_bulk,
    is_object_processed,
    mark_object_processed,
)

logger = logging.getLogger(__name__)

RAW_ENTITY_TYPES = {
    "user", "post", "comment", "like", "reaction",
    "community", "media", "pinned_post", "friend", "group_member"
}


# -------- Kafka --------

def ingest_from_kafka_topics(
    topics: Iterable[str],
    batch_size: int = 2000,
    flush_sec: int = 5,
    idle_sec: int = 8,
    group_id: str = "raw-loader",
) -> None:
    """
    Читает один/несколько топиков Kafka, буферизует события по типу,
    пишет батчами в RAW.* и коммитит оффсеты только после успешной записи.
    Завершается, если нет новых сообщений idle_sec секунд.
    """
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=get_kafka_bootstrap_servers(),
        group_id=group_id,
        enable_auto_commit=False,                 # сами коммитим после записи
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=batch_size,
        fetch_max_bytes=50 * 1024 * 1024,
        fetch_min_bytes=1 * 1024 * 1024,
        fetch_max_wait_ms=500,
    )

    pg_hook = PostgresHook(postgres_conn_id="my_postgress_conn")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    last_flush = time.time()
    last_seen = time.time()

    def flush():
        total = 0
        for etype, rows in list(buffers.items()):
            if not rows:
                continue
            if etype in RAW_ENTITY_TYPES:
                save_to_raw_bulk(etype, rows, cur)
                total += len(rows)
            buffers[etype].clear()
        if total:
            conn.commit()
            consumer.commit()
            logger.info(f"[Kafka] Flushed {total} events to RAW and committed offsets")

    try:
        logger.info(f"[Kafka] Start consume: topics={list(topics)}, group_id={group_id}")
        while True:
            polled = consumer.poll(timeout_ms=1000, max_records=batch_size)
            if not polled:
                if time.time() - last_seen > idle_sec:
                    logger.info(f"[Kafka] Idle for {idle_sec}s → exit")
                    break
                continue

            last_seen = time.time()

            for _, msgs in polled.items():
                for m in msgs:
                    v = m.value
                    etype = v.get("type")
                    if etype:
                        buffers[etype].append(v)

            if time.time() - last_flush >= flush_sec:
                flush()
                last_flush = time.time()

        # финальный сброс
        if any(buffers.values()):
            flush()
    finally:
        try:
            cur.close()
            conn.close()
        finally:
            consumer.close()
        logger.info("[Kafka] Consumer closed")

# -------- MinIO --------

def _parse_minio_payload(data: bytes) -> List[Dict[str, Any]]:
    """
    Пытается разобрать содержимое файла:
    - JSON-объект → [obj]
    - JSON-массив → list
    - JSON Lines → построчно
    """
    try:
        obj = json.loads(data)
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            return [obj]
    except Exception:
        pass

    # JSONL fallback
    events: List[Dict[str, Any]] = []
    for line in data.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except Exception:
            logger.warning("Skip invalid JSONL line")
    return events


def ingest_from_minio(prefix: str = "", recursive: bool = True, batch_size: int = 1000) -> None:
    """
    Читает батчи из MinIO и пишет в RAW.*.
    Объекты НЕ удаляются. Вместо этого, в raw.processed_objects ставится отметка,
    чтобы не обрабатывать повторно.
    """
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    bucket = get_minio_bucket()

    pg_hook = PostgresHook(postgres_conn_id="my_postgress_conn")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    processed = 0
    skipped = 0

    try:
        logger.info(f"[MinIO] Scan bucket={bucket} prefix='{prefix}' recursive={recursive}")
        
        # Получаем список объектов из S3
        objects = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
        
        for key in objects:
            # проверяем - уже обработан?
            if is_object_processed("minio", key, cur):
                skipped += 1
                if skipped % 100 == 0:
                    logger.info(f"[MinIO] Skipped {skipped} already processed objects so far")
                continue

            # читаем и парсим
            try:
                data = s3_hook.read_key(key, bucket_name=bucket)
                if isinstance(data, str):
                    data = data.encode('utf-8')
            except Exception as e:
                logger.error(f"[MinIO] Failed to read object {key}: {e}")
                continue

            events = _parse_minio_payload(data)
            if not events:
                logger.warning(f"[MinIO] Empty or unparsable object: {key}")
                continue

            # группируем по типам и вставляем батчами
            grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for ev in events:
                etype = ev.get("type")
                if etype in RAW_ENTITY_TYPES:
                    grouped[etype].append(ev)

            total_rows = 0
            for etype, rows in grouped.items():
                if not rows:
                    continue
                if len(rows) <= batch_size:
                    save_to_raw_bulk(etype, rows, cur)
                    total_rows += len(rows)
                else:
                    # режем на чанки, чтобы держать память под контролем
                    for i in range(0, len(rows), batch_size):
                        chunk = rows[i : i + batch_size]
                        save_to_raw_bulk(etype, chunk, cur)
                        total_rows += len(chunk)

            # коммитим вставки
            conn.commit()

            # достаём etag и помечаем как обработанный
            try:
                # S3Hook не предоставляет прямой доступ к etag, используем ключ как идентификатор
                etag = key
            except Exception:
                etag = None

            mark_object_processed("minio", key, etag, cur)
            conn.commit()

            processed += 1
            logger.info(f"[MinIO] Processed object: {key}, events={len(events)}, rows_inserted={total_rows}")

        logger.info(f"[MinIO] Done. processed={processed}, skipped={skipped}")
    finally:
        cur.close()
        conn.close()
