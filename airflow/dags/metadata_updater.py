import json
import logging
import os

import redis

try:
    from metadata.query import MetadataQuery
except Exception:
    MetadataQuery = None

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

REDIS_HOST = os.environ.get("METADATA_REDIS_HOST", "metadata-redis")
REDIS_PORT = int(os.environ.get("METADATA_REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("METADATA_REDIS_DB", "0"))
REDIS_KEY = os.environ.get("METADATA_REDIS_KEY", "pipelines")


def _ensure_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    if isinstance(value, set):
        return list(value)
    return []


def _parse_json_value(value):
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def _fetch_dag_configs(hook):
    rows = hook.get_records(
        """
        SELECT id, dag_name, enabled, schedule_cron, timezone, owner, tags, max_active_tasks
        FROM control.dag_configs
        WHERE enabled = true
        ORDER BY dag_name
        """
    )
    logging.info("Loaded %s DAG configs from control.dag_configs", len(rows))
    dag_configs = {}
    for row in rows:
        dag_id, dag_name, enabled, schedule_cron, timezone, owner, tags, max_active_tasks = row
        dag_configs[dag_id] = {
            "dag_id": dag_id,
            "dag_name": dag_name,
            "enabled": bool(enabled),
            "schedule_cron": schedule_cron,
            "timezone": timezone or "Asia/Jakarta",
            "owner": owner,
            "tags": _ensure_list(tags),
            "max_active_tasks": max_active_tasks or 8,
            "pipelines": [],
        }
    return dag_configs


def _normalize_pipeline_tables(
    source_table_name,
    datasource_table,
    target_schema,
    target_table_name,
    datawarehouse_table,
):
    source_table_name = source_table_name or datasource_table
    datasource_table = datasource_table or source_table_name
    if (not target_schema or not target_table_name) and datawarehouse_table:
        if "." in datawarehouse_table:
            schema, table = datawarehouse_table.split(".", 1)
            target_schema = target_schema or schema
            target_table_name = target_table_name or table
    if not datawarehouse_table and target_schema and target_table_name:
        datawarehouse_table = f"{target_schema}.{target_table_name}"
    return (
        source_table_name,
        datasource_table,
        target_schema,
        target_table_name,
        datawarehouse_table,
    )


def _fetch_pipelines(hook, dag_configs):
    rows = hook.get_records(
        """
        SELECT p.pipeline_id, p.dag_id, dc.dag_name, p.enabled, p.description,
               p.source_table_name, p.source_sql_query, p.datasource_timestamp_column,
               p.target_schema, p.target_table_name, p.target_table_schema,
               p.datasource_table, p.datawarehouse_table,
               p.unique_key, p.merge_window_minutes, p.expected_columns,
               p.sql_merge_path, p.freshness_threshold_minutes, p.sla_minutes
        FROM control.datasource_to_dwh_pipelines p
        INNER JOIN control.dag_configs dc ON p.dag_id = dc.id
        WHERE p.enabled = true
        ORDER BY dc.dag_name, p.pipeline_id
        """
    )
    appended = 0
    skipped = 0
    for row in rows:
        (
            pipeline_id,
            dag_id,
            dag_name,
            enabled,
            description,
            source_table_name,
            source_sql_query,
            datasource_timestamp_column,
            target_schema,
            target_table_name,
            target_table_schema,
            datasource_table,
            datawarehouse_table,
            unique_key,
            merge_window_minutes,
            expected_columns,
            sql_merge_path,
            freshness_threshold_minutes,
            sla_minutes,
        ) = row
        (
            source_table_name,
            datasource_table,
            target_schema,
            target_table_name,
            datawarehouse_table,
        ) = _normalize_pipeline_tables(
            source_table_name,
            datasource_table,
            target_schema,
            target_table_name,
            datawarehouse_table,
        )
        dag_cfg = dag_configs.get(dag_id)
        if not dag_cfg:
            skipped += 1
            continue
        dag_cfg["pipelines"].append(
            {
                "pipeline_id": pipeline_id,
                "dag_id": dag_id,
                "enabled": bool(enabled),
                "description": description,
                "source_table_name": source_table_name,
                "source_sql_query": source_sql_query,
                "datasource_table": datasource_table,
                "datasource_timestamp_column": datasource_timestamp_column,
                "target_schema": target_schema,
                "target_table_name": target_table_name,
                "target_table_schema": target_table_schema,
                "datawarehouse_table": datawarehouse_table,
                "unique_key": unique_key,
                "merge_window_minutes": merge_window_minutes,
                "expected_columns": _ensure_list(expected_columns),
                "sql_merge_path": sql_merge_path,
                "freshness_threshold_minutes": freshness_threshold_minutes,
                "sla_minutes": sla_minutes,
            }
        )
        appended += 1
    logging.info(
        "Loaded %s pipelines from control.datasource_to_dwh_pipelines (appended=%s skipped=%s)",
        len(rows),
        appended,
        skipped,
    )


def _fetch_dags_from_metadata_query(hook, mq):
    sql = getattr(mq, "datasource_to_dwh", None)
    if not sql:
        logging.warning("MetadataQuery.datasource_to_dwh is missing")
        return []
    row = hook.get_first(sql)
    if not row:
        logging.info("Metadata query returned no rows for datasource_to_dwh")
        return []
    parsed = _parse_json_value(row[0])
    if not isinstance(parsed, list):
        logging.warning("Metadata query returned non-list payload")
        return []
    logging.info("Loaded %s DAG configs from MetadataQuery.datasource_to_dwh", len(parsed))
    return parsed


def _run_metadata_queries(hook, mq):
    metadata = {}
    for name in ["database_connections", "dag_configs", "datasource_to_dwh"]:
        sql = getattr(mq, name, None)
        if not sql:
            continue
        try:
            row = hook.get_first(sql)
            metadata[name] = _parse_json_value(row[0]) if row else None
            if metadata[name] is None:
                logging.warning("Metadata query %s returned no data", name)
            elif isinstance(metadata[name], list):
                logging.info("Metadata query %s returned %s rows", name, len(metadata[name]))
        except Exception as exc:  # pragma: no cover - runtime DB errors
            logging.exception("Error running metadata query %s: %s", name, exc)
            metadata[name] = None
    return metadata


def _build_payload():
    hook = PostgresHook(postgres_conn_id="analytics_db")
    payload = {"dags": []}
    mq = MetadataQuery() if MetadataQuery is not None else None

    dags = []
    if mq:
        try:
            dags = _fetch_dags_from_metadata_query(hook, mq)
        except Exception as exc:
            logging.exception("Error building DAGs from metadata query: %s", exc)
            dags = []

    if dags:
        payload["dags"] = dags
        logging.info("Using metadata query payload (dags=%s)", len(dags))
    else:
        logging.info("Metadata query empty; falling back to control tables")
        dag_configs = _fetch_dag_configs(hook)
        if not dag_configs:
            payload["dags"] = []
        else:
            _fetch_pipelines(hook, dag_configs)
            payload["dags"] = list(dag_configs.values())
        total_pipelines = sum(
            len(dag.get("pipelines", [])) for dag in payload.get("dags", [])
        )
        logging.info(
            "Fallback payload built from tables (dags=%s pipelines=%s)",
            len(payload.get("dags", [])),
            total_pipelines,
        )

    payload["metadata_queries"] = _run_metadata_queries(hook, mq) if mq else {}
    return payload


def _write_to_redis(payload):
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    payload_json = json.dumps(payload)
    client.set(REDIS_KEY, payload_json)
    logging.info("Wrote metadata payload to Redis key %s (bytes=%s)", REDIS_KEY, len(payload_json))


def update_metadata():
    logging.info("Starting metadata refresh into Redis")
    payload = _build_payload()
    _write_to_redis(payload)
    logging.info("Metadata updated in Redis key %s", REDIS_KEY)


default_args = {
    "owner": "data-eng",
    "retries": 1,
}


with DAG(
    dag_id="metadata_updater",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["metadata", "redis"],
) as dag:
    update_task = PythonOperator(
        task_id="update_metadata",
        python_callable=update_metadata,
    )
