import json
import logging
import os
from typing import Any, Dict

import redis

LOG = logging.getLogger("metadata_generator")

DEFAULT_REDIS_HOST = os.environ.get("METADATA_REDIS_HOST", "metadata-redis")
DEFAULT_REDIS_PORT = int(os.environ.get("METADATA_REDIS_PORT", "6379"))
DEFAULT_REDIS_DB = int(os.environ.get("METADATA_REDIS_DB", "0"))
DEFAULT_REDIS_KEY = os.environ.get("METADATA_REDIS_KEY", "pipelines")


def fetch_payload_from_redis(
    host: str = DEFAULT_REDIS_HOST,
    port: int = DEFAULT_REDIS_PORT,
    db: int = DEFAULT_REDIS_DB,
    key: str = DEFAULT_REDIS_KEY,
) -> Dict[str, Any]:
    client = redis.Redis(
        host=host, port=port, db=db, socket_connect_timeout=5, socket_timeout=5
    )
    raw = client.get(key)
    if not raw:
        LOG.warning("No metadata found in Redis key %s", key)
        return {}
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    try:
        return json.loads(raw)
    except Exception:
        LOG.exception("Failed to parse metadata JSON from Redis")
        return {}


def write_generated_dags(payload: Dict[str, Any], out_dir: str = None) -> None:
    if out_dir is None:
        out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "generated"))
    os.makedirs(out_dir, exist_ok=True)
    dags = payload.get("dags") or []
    for dag in dags:
        name = dag.get("dag_name") or dag.get("dag_id") or "unknown"
        safe_name = name.replace("/", "_")
        path = os.path.join(out_dir, f"dag_{safe_name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(dag, f, indent=2, ensure_ascii=False)
        LOG.info("Wrote generated DAG metadata to %s", path)


def generate_from_redis(
    redis_host: str = DEFAULT_REDIS_HOST,
    redis_port: int = DEFAULT_REDIS_PORT,
    redis_db: int = DEFAULT_REDIS_DB,
    redis_key: str = DEFAULT_REDIS_KEY,
    out_dir: str = None,
) -> Dict[str, Any]:
    payload = fetch_payload_from_redis(
        host=redis_host, port=redis_port, db=redis_db, key=redis_key
    )
    write_generated_dags(payload, out_dir=out_dir)
    return payload


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_from_redis()
