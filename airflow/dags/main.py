import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable

import redis

from airflow import DAG  # imported so Airflow safe mode parses this file
from generator.datasource_to_dwh import DatasourceToDwhGenerator

try:
    from generator.slave_to_bq import GeneratorPipeline
except Exception:
    GeneratorPipeline = None

REDIS_HOST = os.environ.get("METADATA_REDIS_HOST", "metadata-redis")
REDIS_PORT = int(os.environ.get("METADATA_REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("METADATA_REDIS_DB", "0"))
REDIS_KEY = os.environ.get("METADATA_REDIS_KEY", "pipelines")


@dataclass(frozen=True)
class GeneratorSpec:
    name: str
    redis_key: str
    extract_dags: Callable[[Any], Iterable[Dict[str, Any]]]
    build_dag: Callable[[Dict[str, Any]], Any]


def _load_payload(redis_key: str):
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        raw = client.get(redis_key)
        if not raw:
            logging.warning("No metadata found in Redis key %s", redis_key)
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        return json.loads(raw)
    except Exception as exc:
        logging.warning("Redis metadata unavailable for key %s: %s", redis_key, exc)
        return None


def _extract_dags(payload: Any) -> Iterable[Dict[str, Any]]:
    if not payload:
        return []
    if isinstance(payload, dict):
        payload = payload.get("dags", [])
    if not isinstance(payload, list):
        logging.warning("Invalid metadata payload format for dags")
        return []
    return [item for item in payload if isinstance(item, dict)]


def _looks_like_slave(pipelines: Any) -> bool:
    if not isinstance(pipelines, list):
        return False
    for pipeline in pipelines:
        if not isinstance(pipeline, dict):
            continue
        if "slave_task" in pipeline or "database_conn" in pipeline or "processor" in pipeline:
            return True
    return False


DATASOURCE_GENERATOR = DatasourceToDwhGenerator()
SLAVE_GENERATOR = GeneratorPipeline() if GeneratorPipeline else None


def _build_metadata_dag(dag_cfg: Dict[str, Any]):
    pipelines = dag_cfg.get("pipelines") or []
    if SLAVE_GENERATOR and _looks_like_slave(pipelines):
        default_args = {
            "owner": dag_cfg.get("owner", "data-eng"),
            "retries": dag_cfg.get("retries", 1),
            "start_date": None,
        }
        return SLAVE_GENERATOR.generate_dag(
            dag_id=dag_cfg.get("dag_name"),
            default_args=default_args,
            pipelines=pipelines,
            tags=dag_cfg.get("tags"),
            schedule=dag_cfg.get("schedule") or dag_cfg.get("schedule_cron"),
            max_active_tasks=dag_cfg.get("max_active_tasks", 8),
        )
    return DATASOURCE_GENERATOR.generate_dag(dag_cfg)


def _register_dags(spec: GeneratorSpec, seen: set) -> None:
    payload = _load_payload(spec.redis_key)
    dag_cfgs = spec.extract_dags(payload)
    for dag_cfg in dag_cfgs:
        if not dag_cfg.get("enabled", True):
            continue
        dag_name = dag_cfg.get("dag_name") or dag_cfg.get("dag_id")
        if not dag_name:
            logging.warning("Skipping DAG without name from %s", spec.name)
            continue
        if dag_name in seen:
            logging.warning("DAG %s already registered; skipping %s", dag_name, spec.name)
            continue
        try:
            dag = spec.build_dag(dag_cfg)
        except Exception as exc:
            logging.warning("Failed to build DAG %s from %s: %s", dag_name, spec.name, exc)
            continue
        globals()[dag_name] = dag
        seen.add(dag_name)


def _load_generators() -> None:
    seen: set = set()
    for spec in GENERATORS:
        _register_dags(spec, seen)


# Add more generator specs here to register additional DAG families.
GENERATORS = [
    GeneratorSpec(
        name="metadata",
        redis_key=REDIS_KEY,
        extract_dags=_extract_dags,
        build_dag=_build_metadata_dag,
    ),
]

_load_generators()
