import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable

from airflow import DAG  # imported so Airflow safe mode parses this file
from generator.datasource_to_dwh import DatasourceToDwhGenerator

try:
    from generator.slave_to_bq import GeneratorPipeline
except Exception:
    GeneratorPipeline = None

METADATA_SOURCE = os.environ.get("METADATA_SOURCE", "auto")
REDIS_KEY = os.environ.get("METADATA_REDIS_KEY", "pipelines")


@dataclass(frozen=True)
class GeneratorSpec:
    name: str
    load_configs: Callable[[], Iterable[Dict[str, Any]]]
    build_dag: Callable[[Dict[str, Any]], Any]


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


def _load_metadata_configs():
    logging.info(
        "Loading metadata configs source=%s redis_key=%s",
        METADATA_SOURCE,
        REDIS_KEY,
    )
    return DATASOURCE_GENERATOR.load_configs(
        source=METADATA_SOURCE,
        redis_key=REDIS_KEY,
    )


def _build_metadata_dag(dag_cfg: Dict[str, Any]):
    pipelines = dag_cfg.get("pipelines") or []
    logging.info(
        "Building DAG %s with %s pipelines",
        dag_cfg.get("dag_name") or dag_cfg.get("dag_id"),
        len(pipelines),
    )
    if SLAVE_GENERATOR and _looks_like_slave(pipelines):
        logging.info("Using slave generator for DAG %s", dag_cfg.get("dag_name"))
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
    dag_cfgs = list(spec.load_configs() or [])
    logging.info("Registering %s DAG configs for %s", len(dag_cfgs), spec.name)
    if not dag_cfgs:
        logging.warning("No DAG configs loaded for %s", spec.name)
    for dag_cfg in dag_cfgs:
        if not isinstance(dag_cfg, dict):
            continue
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
        logging.info("Registered DAG %s from %s", dag_name, spec.name)


def _load_generators() -> None:
    seen: set = set()
    for spec in GENERATORS:
        _register_dags(spec, seen)


# Add more generator specs here to register additional DAG families.
GENERATORS = [
    GeneratorSpec(
        name="datasource_to_dwh",
        load_configs=_load_metadata_configs,
        build_dag=_build_metadata_dag,
    ),
]

_load_generators()
