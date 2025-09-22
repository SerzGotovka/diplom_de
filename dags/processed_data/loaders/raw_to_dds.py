import logging
from dotenv import load_dotenv

from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from ..save_to_dds import save_to_dds
from generate_data.models import User, Friend, Post, Comment, Like, Reaction, Community, GroupMember, Media, PinnedPost

# Загружаем переменные окружения из .env файла
load_dotenv()

logger = logging.getLogger(__name__)

# Маппинг: сущность → Pydantic модель → таблица
ENTITY_META = {
    "user":      {"model": User,      "table": "users"},
    "friend":    {"model": Friend,    "table": "friends"},
    "post":      {"model": Post,      "table": "posts"},
    "comment":   {"model": Comment,   "table": "comments"},
    "like":      {"model": Like,      "table": "likes"},
    "reaction":  {"model": Reaction,  "table": "reactions"},
    "community": {"model": Community, "table": "communities"},
    "group_member": {"model": GroupMember, "table": "group_members"},
    "media":     {"model": Media,     "table": "media"},
    "pinned_post": {"model": PinnedPost, "table": "pinned_posts"},
}

def process_raw_entity(entity_type):
    pg_hook = PostgresHook(postgres_conn_id="my_postgress_conn")
    meta = ENTITY_META[entity_type]
    table = meta["table"]
    Model = meta["model"]
    
    rows = pg_hook.get_records(f"SELECT event_json FROM raw.{table}")
    valid = 0
    errors = 0
    for row in rows:
        try:
            raw = row[0]
            # Если jsonb → dict, если text → str
            if isinstance(raw, dict):
                data = Model.parse_obj(raw)
            else:
                data = Model.parse_raw(raw)
            save_to_dds(data.dict(), entity_type)
            valid += 1
        except Exception as e:
            errors += 1
            logger.error(f"[ERROR] Invalid {entity_type}: {e}")
    logger.info(f"{entity_type}: OK {valid}, ERRORS {errors}")

def process_all_entities():
    for entity_type in ENTITY_META:
        process_raw_entity(entity_type)

