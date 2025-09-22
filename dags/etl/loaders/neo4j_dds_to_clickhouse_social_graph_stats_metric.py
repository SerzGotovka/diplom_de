import datetime as dt
import logging
from datetime import timezone, time
from dotenv import load_dotenv

from clickhouse_driver import Client
from neo4j import GraphDatabase

from ..config import get_neo4j_config, get_clickhouse_config
from ..loaders_utils.load_cypher import load_cypher

# Загружаем переменные окружения из .env файла
load_dotenv()

logger = logging.getLogger(__name__)

def _to_date(as_date):
    if isinstance(as_date, dt.date):
        return as_date
    if isinstance(as_date, str):
        return dt.date.fromisoformat(as_date)
    raise TypeError(f"Unsupported as_of_date type: {type(as_date)}")

def _cutoff_iso(as_of_date: dt.date) -> str:
    """Эксклюзивная верхняя граница: 00:00 следующего дня в UTC, ISO-строка."""
    cutoff = dt.datetime.combine(as_of_date, time(23, 59, 59), tzinfo=timezone.utc)
    return cutoff.isoformat()  # 'YYYY-MM-DDTHH:MM:SS'

def _fetch_summary_from_neo4j(as_of_date: dt.date):
    neo = get_neo4j_config()
    driver = GraphDatabase.driver(neo["uri"], auth=(neo["user"], neo["password"]))
    cypher = load_cypher("summary_degree.cypher", layer="dql", subdir="data_mart")
    params = {"cutoff": _cutoff_iso(as_of_date)}
    with driver.session() as s:
        rec = s.run(cypher, **params).single()
    driver.close()

    if not rec:
        return {"avg_friends": 0.0, "total_friendships": 0}

    d = dict(rec)
    return {
        "avg_friends": float(d.get("avg_friends") or 0.0),
        "total_friendships": int(d.get("total_friendships") or 0),
    }

def _fetch_degree_by_user(as_of_date: dt.date):
    neo = get_neo4j_config()
    driver = GraphDatabase.driver(neo["uri"], auth=(neo["user"], neo["password"]))
    cypher = load_cypher("degree_by_user.cypher", layer="dql", subdir="data_mart")
    params = {"cutoff": _cutoff_iso(as_of_date)}
    rows = []
    with driver.session() as s:
        for r in s.run(cypher, **params):
            d = dict(r)
            uid = d.get("user_id")
            deg = int(d.get("degree", 0))
            if uid is not None:
                rows.append((uid, deg))
    driver.close()
    return rows

def upsert_social_graph_stats(as_of_date: dt.date = None):
    as_of_date = _to_date(as_of_date or dt.date.today())

    summary = _fetch_summary_from_neo4j(as_of_date)
    degrees = _fetch_degree_by_user(as_of_date)
    max_deg = max((d for _, d in degrees), default=1)

    ch = get_clickhouse_config()
    client = Client(host=ch["host"], port=int(ch["port"]),
                    user=ch["user"], password=ch["password"],
                    database=ch["database"])

    logger.debug(
        f"[NEO] cutoff={as_of_date}\n"
        f"[NEO] summary={summary}\n"
        f"[NEO] degrees_n={len(degrees)}"
    )

    # 1) summary
    client.execute(
        "ALTER TABLE data_mart.social_graph_stats DELETE WHERE dt = %(dt)s",
        params={"dt": as_of_date}
    )
    client.execute(
        "INSERT INTO data_mart.social_graph_stats (dt, avg_friends, total_friendships) VALUES",
        [(as_of_date, summary["avg_friends"], summary["total_friendships"])]
    )

    # 2) centrality proxy
    client.execute(
        "ALTER TABLE data_mart.social_user_centrality DELETE WHERE dt = %(dt)s",
        params={"dt": as_of_date}
    )
    if degrees:
        batch = [
            (as_of_date, uid, deg, (float(deg) / float(max_deg)) if max_deg else 0.0)
            for uid, deg in degrees
        ]
        client.execute(
            "INSERT INTO data_mart.social_user_centrality (dt, user_id, degree, degree_centrality) VALUES",
            batch
        )

    client.disconnect()
    logger.info(f"[CH] social_graph_stats + social_user_centrality upserted for {as_of_date}")
