import json
import logging
import os
import re
import subprocess
import sys
import uuid
from typing import Any, Dict, Iterable, List, Optional

import redis
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

try:
    from metadata.query import MetadataQuery
except Exception:
    MetadataQuery = None

DEFAULT_POSTGRES_CONN = os.environ.get("METADATA_POSTGRES_CONN", "analytics_db")
DEFAULT_REDIS_HOST = os.environ.get("METADATA_REDIS_HOST", "metadata-redis")
DEFAULT_REDIS_PORT = int(os.environ.get("METADATA_REDIS_PORT", "6379"))
DEFAULT_REDIS_DB = int(os.environ.get("METADATA_REDIS_DB", "0"))

SODA_CONFIG_PATH = os.environ.get(
    "SODA_CONFIG_PATH", "/opt/airflow/include/dq/soda_config.yml"
)
SODA_CHECKS_PATH = os.environ.get(
    "SODA_CHECKS_PATH", "/opt/airflow/include/dq/soda_checks.yml"
)
SODA_DATASOURCE = os.environ.get("SODA_DATASOURCE", "analytics")

QUALIFIED_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

TYPE_ALIASES = {
    "timestamptz": "timestamp with time zone",
    "timestamp with time zone": "timestamp with time zone",
    "timestamp without time zone": "timestamp without time zone",
    "timestamp": "timestamp without time zone",
    "int": "integer",
    "int4": "integer",
    "integer": "integer",
    "bigint": "bigint",
    "int8": "bigint",
    "smallint": "smallint",
    "int2": "smallint",
    "double precision": "double precision",
    "float8": "double precision",
    "float": "double precision",
    "numeric": "numeric",
    "decimal": "numeric",
    "bool": "boolean",
    "boolean": "boolean",
    "varchar": "character varying",
    "character varying": "character varying",
    "char": "character",
    "character": "character",
    "jsonb": "jsonb",
    "json": "json",
    "inet": "inet",
    "text": "text",
    "uuid": "uuid",
    "date": "date",
    "time": "time without time zone",
    "timetz": "time with time zone",
    "bytea": "bytea",
}


class NoTemplatePythonOperator(PythonOperator):
    template_fields: tuple[str, ...] = ()


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


def _extract_dags(payload: Any) -> List[Dict[str, Any]]:
    if not payload:
        return []
    if isinstance(payload, dict):
        payload = payload.get("dags", [])
    if not isinstance(payload, list):
        logging.warning("Invalid metadata payload format for dags")
        return []
    return [item for item in payload if isinstance(item, dict)]


def load_sql(path):
    if isinstance(path, str) and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as handle:
            return handle.read()
    return path


def normalize_pipeline(pipeline):
    normalized = dict(pipeline)
    for key in ("gold_tables", "gold_sql_paths"):
        normalized[key] = _ensure_list(normalized.get(key))
    return normalized


def _get_hook():
    return PostgresHook(postgres_conn_id="analytics_db")


def _normalize_type(value):
    if value is None:
        return None
    if not isinstance(value, str):
        return str(value)
    key = value.strip().lower()
    return TYPE_ALIASES.get(key, key)


def _normalize_schema(schema):
    entries = _ensure_list(schema)
    normalized = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name") or entry.get("column_name") or entry.get("column")
        col_type = entry.get("type") or entry.get("data_type")
        if not name:
            continue
        normalized.append({"name": name, "type": col_type})
    return normalized


def _normalize_manual_ts(value):
    if not value:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return None


def _get_manual_window(context):
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    start_ts = _normalize_manual_ts(conf.get("start_ts") or params.get("start_ts"))
    end_ts = _normalize_manual_ts(conf.get("end_ts") or params.get("end_ts"))
    if start_ts and end_ts:
        return start_ts, end_ts
    return None, None


def _normalize_pipeline_filter(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return None


def should_run_pipeline(pipeline_id, **context):
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    requested = _normalize_pipeline_filter(
        conf.get("pipeline_id") or params.get("pipeline_id")
    )
    if not requested:
        return True
    if requested == pipeline_id:
        logging.info("Pipeline filter matched %s", pipeline_id)
        return True
    logging.info("Skipping pipeline %s due to filter %s", pipeline_id, requested)
    return False

def _require_qualified_name(value, field_name):
    if not isinstance(value, str) or not QUALIFIED_NAME_RE.match(value):
        raise AirflowException(f"Invalid {field_name}: {value}")
    return value


def _require_identifier(value, field_name):
    if not isinstance(value, str) or not IDENTIFIER_RE.match(value):
        raise AirflowException(f"Invalid {field_name}: {value}")
    return value


def _render_merge_sql_text(sql_text, replacements):
    sql_template = sql_text
    for key, value in replacements.items():
        sql_template = sql_template.replace(f"{{{{{key}}}}}", value)
    return sql_template


def start_pipeline_run(pipeline_id, **context):
    run_id = context["run_id"]
    logging.info("Starting pipeline run %s for %s", run_id, pipeline_id)
    hook = _get_hook()
    hook.run(
        """
        INSERT INTO monitoring.pipeline_runs
            (run_id, pipeline_id, run_ts, status, notes, started_at)
        VALUES (%s, %s, now(), %s, %s, now())
        ON CONFLICT (run_id) DO NOTHING
        """,
        parameters=(run_id, pipeline_id, "running", "started"),
    )


def end_pipeline_run(pipeline_id, **context):
    run_id = context["run_id"]
    dag_run = context["dag_run"]
    tis = dag_run.get_task_instances()
    failed_tasks = [ti.task_id for ti in tis if ti.state == State.FAILED]
    status = "failed" if failed_tasks else "success"
    logging.info(
        "Ending pipeline run %s for %s status=%s failed_tasks=%s",
        run_id,
        pipeline_id,
        status,
        failed_tasks,
    )
    hook = _get_hook()
    hook.run(
        """
        UPDATE monitoring.pipeline_runs
        SET status = %s, ended_at = now()
        WHERE run_id = %s
        """,
        parameters=(status, run_id),
    )


def compute_lag(pipeline_id, bronze_table, **_):
    logging.info("Computing lag metrics for %s from %s", pipeline_id, bronze_table)
    hook = _get_hook()
    row = hook.get_first(f"SELECT max(event_ts) FROM {bronze_table}")
    max_ts = row[0] if row else None
    lag_seconds = None
    if max_ts is not None:
        lag_row = hook.get_first(
            "SELECT EXTRACT(EPOCH FROM (now() - %s::timestamptz))",
            parameters=(max_ts,),
        )
        lag_seconds = lag_row[0] if lag_row else None
    hook.run(
        """
        INSERT INTO monitoring.lag_metrics
            (pipeline_id, observed_at, max_event_ts, lag_seconds)
        VALUES (%s, now(), %s, %s)
        """,
        parameters=(pipeline_id, max_ts, lag_seconds),
    )
    logging.info(
        "Lag metrics for %s: max_event_ts=%s lag_seconds=%s",
        pipeline_id,
        max_ts,
        lag_seconds,
    )

def schema_drift_check(
    pipeline_id,
    bronze_table,
    expected_schema=None,
    expected_columns=None,
    **_,
):
    logging.info("Running schema drift check for %s on %s", pipeline_id, bronze_table)
    schema_name, table_name = bronze_table.split(".")
    hook = _get_hook()
    rows = hook.get_records(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        parameters=(schema_name, table_name),
    )
    actual = {row[0]: _normalize_type(row[1]) for row in rows}
    normalized_schema = _normalize_schema(expected_schema)
    if normalized_schema:
        expected_map = {
            entry["name"]: _normalize_type(entry.get("type"))
            for entry in normalized_schema
        }
        columns = list(expected_map.keys())
    else:
        columns = _ensure_list(expected_columns)
        expected_map = {col: None for col in columns}
    if not columns:
        logging.info("Schema drift check skipped for %s: no expected schema", pipeline_id)
        return
    missing_count = 0
    mismatch_count = 0
    for column_name in columns:
        actual_type = actual.get(column_name)
        expected_type = expected_map.get(column_name)
        if actual_type is None:
            status = "missing"
            missing_count += 1
        elif expected_type and actual_type != expected_type:
            status = "mismatch"
            mismatch_count += 1
        else:
            status = "ok"
        hook.run(
            """
            INSERT INTO monitoring.schema_drift
                (pipeline_id, observed_at, column_name, expected_type, actual_type, status)
            VALUES (%s, now(), %s, %s, %s, %s)
            """,
            parameters=(pipeline_id, column_name, expected_type, actual_type, status),
        )
    logging.info(
        "Schema drift recorded for %s: columns=%s missing=%s mismatch=%s",
        pipeline_id,
        len(columns),
        missing_count,
        mismatch_count,
    )


def volume_check(pipeline_id, bronze_table, **_):
    logging.info("Running volume check for %s on %s", pipeline_id, bronze_table)
    hook = _get_hook()
    recent = hook.get_first(
        f"""
        SELECT count(*)
        FROM {bronze_table}
        WHERE event_ts >= now() - interval '5 minutes'
        """
    )[0]
    last_hour = hook.get_first(
        f"""
        SELECT count(*)
        FROM {bronze_table}
        WHERE event_ts >= now() - interval '1 hour'
        """
    )[0]
    baseline = int(last_hour / 12) if last_hour else 0
    status = "ok"
    if baseline > 0 and recent < baseline * 0.5:
        status = "low"
    hook.run(
        """
        INSERT INTO monitoring.volume_metrics
            (pipeline_id, observed_at, window_minutes, event_count, baseline_count, status)
        VALUES (%s, now(), %s, %s, %s, %s)
        """,
        parameters=(pipeline_id, 5, recent, baseline, status),
    )
    logging.info(
        "Volume check for %s: recent=%s baseline=%s status=%s",
        pipeline_id,
        recent,
        baseline,
        status,
    )


def run_soda_scan(pipeline_id, **_):
    output_path = f"/tmp/soda_scan_{pipeline_id}.json"
    logging.info("Running Soda scan for %s output=%s", pipeline_id, output_path)
    cmd = [
        sys.executable,
        "-m",
        "soda",
        "scan",
        "-d",
        SODA_DATASOURCE,
        "-c",
        SODA_CONFIG_PATH,
        SODA_CHECKS_PATH,
        "--scan-results-file",
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    logging.info("Soda scan for %s finished with code %s", pipeline_id, result.returncode)
    if result.returncode not in (0, 1):
        logging.error("Soda scan failed for %s: %s", pipeline_id, result.stderr)
        raise AirflowException(f"Soda failed: {result.stderr}")
    if not os.path.exists(output_path):
        logging.error("Soda output missing for %s", pipeline_id)
        raise AirflowException("Soda output file missing")

    with open(output_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    failed = bool(data.get("hasFailures")) or bool(data.get("hasErrors"))
    for check in data.get("checks", []):
        if check.get("outcome") in ("fail", "error"):
            failed = True
            break

    status = "fail" if failed else "pass"
    hook = _get_hook()
    hook.run(
        """
        INSERT INTO gold.dq_results
            (run_id, pipeline_id, run_ts, status, results_json)
        VALUES (%s::uuid, %s, now(), %s, %s::jsonb)
        """,
        parameters=(str(uuid.uuid4()), pipeline_id, status, json.dumps(data)),
    )
    logging.info("Soda scan status for %s: %s", pipeline_id, status)

    if failed:
        logging.error("Data quality checks failed for %s", pipeline_id)
        raise AirflowException("Data quality checks failed")

def alerting(pipeline_id, freshness_threshold_minutes, **context):
    threshold_seconds = int(freshness_threshold_minutes or 10) * 60
    hook = _get_hook()
    issues = []

    lag_row = hook.get_first(
        """
        SELECT lag_seconds
        FROM monitoring.lag_metrics
        WHERE pipeline_id = %s
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        parameters=(pipeline_id,),
    )
    if lag_row and lag_row[0] is not None and lag_row[0] > threshold_seconds:
        issues.append(("lag", "warning", f"lag_seconds={lag_row[0]}"))

    vol_row = hook.get_first(
        """
        SELECT status, event_count, baseline_count
        FROM monitoring.volume_metrics
        WHERE pipeline_id = %s
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        parameters=(pipeline_id,),
    )
    if vol_row and vol_row[0] == "low":
        issues.append(
            (
                "volume",
                "warning",
                f"event_count={vol_row[1]} baseline_count={vol_row[2]}",
            )
        )

    drift_row = hook.get_first(
        """
        SELECT count(*)
        FROM monitoring.schema_drift
        WHERE pipeline_id = %s
          AND observed_at >= now() - interval '15 minutes'
          AND status != 'ok'
        """,
        parameters=(pipeline_id,),
    )
    if drift_row and drift_row[0] > 0:
        issues.append(("schema_drift", "critical", "schema drift detected"))

    dq_row = hook.get_first(
        """
        SELECT status
        FROM gold.dq_results
        WHERE pipeline_id = %s
        ORDER BY run_ts DESC
        LIMIT 1
        """,
        parameters=(pipeline_id,),
    )
    if dq_row and dq_row[0] == "fail":
        issues.append(("dq", "critical", "dq checks failed"))

    if not issues:
        logging.info("No alerts for pipeline %s", pipeline_id)
        return
    logging.info("Alerts for pipeline %s: %s", pipeline_id, issues)

    for alert_type, severity, message in issues:
        hook.run(
            """
            INSERT INTO monitoring.alerts
                (pipeline_id, alert_type, severity, message)
            VALUES (%s, %s, %s, %s)
            """,
            parameters=(pipeline_id, alert_type, severity, message),
        )

    webhook = os.environ.get("ALERT_WEBHOOK_URL")
    if webhook:
        payload = {
            "pipeline_id": pipeline_id,
            "issues": [
                {"type": t, "severity": s, "message": m} for t, s, m in issues
            ],
        }
        try:
            import urllib.request

            req = urllib.request.Request(
                webhook,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception as exc:
            logging.warning("Alert webhook failed: %s", exc)


def create_snapshot_id():
    return str(uuid.uuid4())

def build_pipeline_taskgroup(pipeline, dag):
    pipeline = normalize_pipeline(pipeline)
    pipeline_id = pipeline["pipeline_id"]
    bronze_table = pipeline["bronze_table"]
    silver_sql_path = pipeline["silver_sql_path"]
    silver_sql = load_sql(silver_sql_path)
    gold_sql_paths = pipeline.get("gold_sql_paths", [])
    gold_tables = pipeline.get("gold_tables", [])
    snapshot_sql_path = pipeline.get("snapshot_sql_path")
    snapshot_sql = load_sql(snapshot_sql_path) if snapshot_sql_path else None
    expected_schema = pipeline.get("expected_schema")
    expected_columns = pipeline.get("expected_columns")

    optimize_sql = f"ANALYZE {pipeline['silver_table']};"
    for table in gold_tables:
        optimize_sql += f"\nANALYZE {table};"

    with TaskGroup(group_id=f"{pipeline_id}_pipeline", dag=dag) as taskgroup:
        start_run = PythonOperator(
            task_id="start_run",
            python_callable=start_pipeline_run,
            op_kwargs={"pipeline_id": pipeline_id},
        )

        compute_lag_task = PythonOperator(
            task_id="compute_lag",
            python_callable=compute_lag,
            op_kwargs={"pipeline_id": pipeline_id, "bronze_table": bronze_table},
        )

        schema_drift_task = PythonOperator(
            task_id="schema_drift_check",
            python_callable=schema_drift_check,
            op_kwargs={
                "pipeline_id": pipeline_id,
                "bronze_table": bronze_table,
                "expected_schema": expected_schema,
                "expected_columns": expected_columns,
            },
        )

        volume_task = PythonOperator(
            task_id="volume_check",
            python_callable=volume_check,
            op_kwargs={"pipeline_id": pipeline_id, "bronze_table": bronze_table},
        )

        build_silver = PostgresOperator(
            task_id="build_silver",
            postgres_conn_id="analytics_db",
            sql=silver_sql,
        )

        gold_tasks = []
        for path in gold_sql_paths:
            sql = load_sql(path)
            base = os.path.basename(path).replace(".sql", "")
            gold_tasks.append(
                PostgresOperator(
                    task_id=f"build_{base}",
                    postgres_conn_id="analytics_db",
                    sql=sql,
                )
            )

        dq_scan = PythonOperator(
            task_id="run_dq",
            python_callable=run_soda_scan,
            op_kwargs={"pipeline_id": pipeline_id},
        )

        optimize = PostgresOperator(
            task_id="optimize",
            postgres_conn_id="analytics_db",
            sql=optimize_sql,
        )

        alert_task = PythonOperator(
            task_id="alerting",
            python_callable=alerting,
            op_kwargs={
                "pipeline_id": pipeline_id,
                "freshness_threshold_minutes": pipeline.get(
                    "freshness_threshold_minutes", 10
                ),
            },
            trigger_rule=TriggerRule.ALL_DONE,
        )

        end_run = PythonOperator(
            task_id="end_run",
            python_callable=end_pipeline_run,
            op_kwargs={"pipeline_id": pipeline_id},
            trigger_rule=TriggerRule.ALL_DONE,
        )

        start_run >> [compute_lag_task, schema_drift_task, volume_task] >> build_silver

        if gold_tasks:
            build_silver >> gold_tasks[0]
            for upstream, downstream in zip(gold_tasks, gold_tasks[1:]):
                upstream >> downstream
            gold_tasks[-1] >> dq_scan
        else:
            build_silver >> dq_scan

        if snapshot_sql:
            snapshot_id = PythonOperator(
                task_id="snapshot_id",
                python_callable=create_snapshot_id,
            )

            snapshot = PostgresOperator(
                task_id="snapshot_gold",
                postgres_conn_id="analytics_db",
                sql=snapshot_sql,
                params={
                    "pipeline_id": pipeline_id,
                    "snapshot_task_id": f"{taskgroup.group_id}.snapshot_id",
                },
            )

            dq_scan >> optimize >> snapshot_id >> snapshot
            snapshot >> alert_task >> end_run
        else:
            dq_scan >> optimize >> alert_task >> end_run

    return taskgroup

def freshness_check(
    pipeline_id,
    datasource_table,
    datasource_timestamp_column,
    freshness_threshold_minutes,
    **_,
):
    logging.info(
        "Running freshness check for %s on %s",
        pipeline_id,
        datasource_table,
    )
    datasource_table = _require_qualified_name(datasource_table, "datasource_table")
    ts_col = _require_identifier(
        datasource_timestamp_column, "datasource_timestamp_column"
    )
    hook = _get_hook()
    row = hook.get_first(
        f"SELECT EXTRACT(EPOCH FROM (now() - max({ts_col}))) AS lag_seconds FROM {datasource_table}"
    )
    lag_seconds = row[0] if row else None
    if lag_seconds is None:
        logging.error("Freshness check failed for %s: no data", pipeline_id)
        raise AirflowException(
            f"Freshness check failed for {pipeline_id}: no data in {datasource_table}"
        )
    threshold_seconds = int(freshness_threshold_minutes or 0) * 60
    if lag_seconds > threshold_seconds:
        logging.error(
            "Freshness check failed for %s: lag=%s threshold=%s",
            pipeline_id,
            lag_seconds,
            threshold_seconds,
        )
        raise AirflowException(
            f"Freshness check failed for {pipeline_id}: lag {lag_seconds}s exceeds {threshold_seconds}s"
        )
    logging.info(
        "Freshness check passed for %s: lag_seconds=%s threshold_seconds=%s",
        pipeline_id,
        lag_seconds,
        threshold_seconds,
    )


def schema_check(pipeline_id, datasource_table, expected_columns, **_):
    logging.info("Running schema check for %s on %s", pipeline_id, datasource_table)
    datasource_table = _require_qualified_name(datasource_table, "datasource_table")
    expected = _ensure_list(expected_columns)
    if not expected:
        logging.info("Schema check skipped for %s: no expected columns", pipeline_id)
        return
    for column_name in expected:
        _require_identifier(column_name, "expected_columns")
    schema_name, table_name = datasource_table.split(".")
    hook = _get_hook()
    rows = hook.get_records(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        parameters=(schema_name, table_name),
    )
    actual = {row[0] for row in rows}
    missing = sorted(set(expected) - actual)
    if missing:
        logging.error(
            "Schema check failed for %s: missing columns %s",
            pipeline_id,
            missing,
        )
        raise AirflowException(
            f"Schema check failed for {pipeline_id}: missing columns {missing}"
        )
    logging.info("Schema check passed for %s", pipeline_id)


def ensure_target_schema(pipeline_id, datawarehouse_table, target_table_schema, **_):
    logging.info(
        "Ensuring target schema for %s on %s", pipeline_id, datawarehouse_table
    )
    if not target_table_schema:
        logging.info(
            "Target schema check skipped for %s: no target_table_schema", pipeline_id
        )
        return
    datawarehouse_table = _require_qualified_name(
        datawarehouse_table, "datawarehouse_table"
    )
    schema_name, table_name = datawarehouse_table.split(".", 1)
    _require_identifier(schema_name, "target_schema")
    _require_identifier(table_name, "target_table_name")
    normalized_schema = _normalize_schema(target_table_schema)
    if not normalized_schema:
        logging.info("Target schema check skipped for %s: empty schema", pipeline_id)
        return
    expected = []
    for entry in normalized_schema:
        name = entry.get("name")
        col_type = entry.get("type")
        if not name:
            continue
        if not col_type:
            raise AirflowException(
                f"Target schema missing type for {pipeline_id}.{name}"
            )
        _require_identifier(name, "target_table_schema")
        expected.append((name, _normalize_type(col_type)))
    if not expected:
        logging.info("Target schema check skipped for %s: no columns", pipeline_id)
        return

    hook = _get_hook()
    table_exists = hook.get_first(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        parameters=(schema_name, table_name),
    )
    if not table_exists:
        columns_sql = ",\n  ".join(f"{name} {col_type}" for name, col_type in expected)
        hook.run(
            f"CREATE SCHEMA IF NOT EXISTS {schema_name};\n"
            f"CREATE TABLE IF NOT EXISTS {datawarehouse_table} (\n  {columns_sql}\n);"
        )

    rows = hook.get_records(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        parameters=(schema_name, table_name),
    )
    actual = {row[0]: _normalize_type(row[1]) for row in rows}
    missing = []
    for name, col_type in expected:
        actual_type = actual.get(name)
        if actual_type is None:
            missing.append((name, col_type))
        elif actual_type != col_type:
            raise AirflowException(
                f"Target schema mismatch for {datawarehouse_table}.{name}: "
                f"expected {col_type}, found {actual_type}"
            )
    if missing:
        for name, col_type in missing:
            hook.run(
                f"ALTER TABLE {datawarehouse_table} "
                f"ADD COLUMN IF NOT EXISTS {name} {col_type};"
            )
        logging.info(
            "Added %s columns to %s: %s",
            len(missing),
            datawarehouse_table,
            [name for name, _ in missing],
        )
    extras = sorted(set(actual) - {name for name, _ in expected})
    if extras:
        logging.info(
            "Target table %s has extra columns not in metadata: %s",
            datawarehouse_table,
            extras,
        )


def merge_to_datawarehouse(
    pipeline_id,
    datasource_table,
    datasource_timestamp_column,
    datawarehouse_table,
    unique_key,
    merge_window_minutes,
    expected_columns,
    merge_sql_text,
    **context,
):
    logging.info(
        "Starting merge for %s: %s -> %s",
        pipeline_id,
        datasource_table,
        datawarehouse_table,
    )
    merge_sql_text = load_sql(merge_sql_text)
    datasource_table = _require_qualified_name(datasource_table, "datasource_table")
    datawarehouse_table = _require_qualified_name(
        datawarehouse_table, "datawarehouse_table"
    )
    ts_col = _require_identifier(
        datasource_timestamp_column, "datasource_timestamp_column"
    )
    unique_key = _require_identifier(unique_key, "unique_key")
    columns = [
        _require_identifier(col, "expected_columns")
        for col in _ensure_list(expected_columns)
    ]
    if not merge_sql_text:
        logging.error("Merge failed for %s: merge_sql_text is empty", pipeline_id)
        raise AirflowException(
            f"Merge failed for {pipeline_id}: merge_sql_text is required"
        )
    window_minutes = int(merge_window_minutes or 0)
    column_list = ", ".join(columns)
    update_set = ",\n  ".join(f"{col} = EXCLUDED.{col}" for col in columns)
    merge_update_cols = [col for col in columns if col != unique_key]
    merge_update_set = ",\n  ".join(f"{col} = source.{col}" for col in merge_update_cols)
    source_column_list = ", ".join(f"source.{col}" for col in columns)
    index_name = f"ux_{datawarehouse_table.replace('.', '_')}_{unique_key}"
    start_ts, end_ts = _get_manual_window(context)
    if start_ts and end_ts:
        time_filter = f"{ts_col} >= '{start_ts}' AND {ts_col} < '{end_ts}'"
        logging.info(
            "Using manual window for %s: %s -> %s", pipeline_id, start_ts, end_ts
        )
    else:
        time_filter = f"{ts_col} >= now() - interval '{window_minutes} minutes'"
    logging.info(
        "Merge parameters for %s: columns=%s window_minutes=%s time_filter=%s",
        pipeline_id,
        len(columns),
        window_minutes,
        time_filter,
    )
    replacements = {
        "DATASOURCE_TABLE": datasource_table,
        "DATAWAREHOUSE_TABLE": datawarehouse_table,
        "TS_COL": ts_col,
        "UNIQUE_KEY": unique_key,
        "WINDOW_MINUTES": str(window_minutes),
        "TIME_FILTER": time_filter,
        "COLUMN_LIST": column_list,
        "UPDATE_SET": update_set,
        "UNIQUE_INDEX_NAME": index_name,
    }
    if not column_list and any(
        token in merge_sql_text
        for token in (
            "{{COLUMN_LIST}}",
            "{{UPDATE_SET}}",
            "{{MERGE_UPDATE_SET}}",
            "{{SOURCE_COLUMN_LIST}}",
        )
    ):
        logging.error("Merge failed for %s: expected_columns is empty", pipeline_id)
        raise AirflowException(
            f"Merge failed for {pipeline_id}: expected_columns is required"
        )
    if not merge_update_set and "{{MERGE_UPDATE_SET}}" in merge_sql_text:
        logging.error("Merge failed for %s: merge update columns are empty", pipeline_id)
        raise AirflowException(
            f"Merge failed for {pipeline_id}: merge update columns are required"
        )
    replacements["START_TS"] = start_ts or ""
    replacements["END_TS"] = end_ts or ""
    replacements["MERGE_UPDATE_SET"] = merge_update_set
    replacements["SOURCE_COLUMN_LIST"] = source_column_list
    logging.info("Using merge SQL text for %s", pipeline_id)
    merge_sql = _render_merge_sql_text(merge_sql_text, replacements)
    hook = _get_hook()
    hook.run(merge_sql)
    logging.info("Merge completed for %s into %s", pipeline_id, datawarehouse_table)

def build_datasource_to_dwh_taskgroup(pipeline, dag):
    pipeline_id = pipeline["pipeline_id"]
    datasource_table = pipeline["datasource_table"]
    datasource_timestamp_column = pipeline["datasource_timestamp_column"]
    datawarehouse_table = pipeline["datawarehouse_table"]
    unique_key = pipeline["unique_key"]
    merge_window_minutes = pipeline.get("merge_window_minutes", 10)
    expected_columns = _ensure_list(pipeline.get("expected_columns"))
    target_table_schema = pipeline.get("target_table_schema")
    if not expected_columns and target_table_schema:
        normalized_schema = _normalize_schema(target_table_schema)
        expected_columns = [entry["name"] for entry in normalized_schema if entry.get("name")]
    freshness_threshold_minutes = pipeline.get("freshness_threshold_minutes", 2)

    with TaskGroup(group_id=f"{pipeline_id}_pipeline", dag=dag) as taskgroup:
        gate = ShortCircuitOperator(
            task_id="should_run",
            python_callable=should_run_pipeline,
            op_kwargs={"pipeline_id": pipeline_id},
        )

        freshness_task = PythonOperator(
            task_id="freshness_check",
            python_callable=freshness_check,
            op_kwargs={
                "pipeline_id": pipeline_id,
                "datasource_table": datasource_table,
                "datasource_timestamp_column": datasource_timestamp_column,
                "freshness_threshold_minutes": freshness_threshold_minutes,
            },
        )

        schema_task = PythonOperator(
            task_id="schema_check",
            python_callable=schema_check,
            op_kwargs={
                "pipeline_id": pipeline_id,
                "datasource_table": datasource_table,
                "expected_columns": expected_columns,
            },
        )

        target_schema_task = PythonOperator(
            task_id="ensure_target_schema",
            python_callable=ensure_target_schema,
            op_kwargs={
                "pipeline_id": pipeline_id,
                "datawarehouse_table": datawarehouse_table,
                "target_table_schema": target_table_schema,
            },
        )

        merge_task = NoTemplatePythonOperator(
            task_id="merge_to_datawarehouse",
            python_callable=merge_to_datawarehouse,
            op_kwargs={
                "pipeline_id": pipeline_id,
                "datasource_table": datasource_table,
                "datasource_timestamp_column": datasource_timestamp_column,
                "datawarehouse_table": datawarehouse_table,
                "unique_key": unique_key,
                "merge_window_minutes": merge_window_minutes,
                "expected_columns": expected_columns,
                "merge_sql_text": pipeline.get("merge_sql_text"),
            },
        )

        analyze_task = PostgresOperator(
            task_id="analyze_target",
            postgres_conn_id="analytics_db",
            sql=f"ANALYZE {datawarehouse_table};",
        )

        gate >> freshness_task >> schema_task >> target_schema_task >> merge_task >> analyze_task

    return taskgroup


def build_datasource_to_dwh_dag(dag_cfg):
    dag_name = dag_cfg["dag_name"]
    schedule_cron = dag_cfg.get("schedule_cron") or "*/5 * * * *"
    timezone = dag_cfg.get("timezone") or "Asia/Jakarta"
    owner = dag_cfg.get("owner") or "data-eng"
    tags = _ensure_list(dag_cfg.get("tags")) or []
    max_active_tasks = int(dag_cfg.get("max_active_tasks") or 8)
    pipelines = dag_cfg.get("pipelines") or []

    default_args = {
        "owner": owner,
        "retries": 1,
    }

    with DAG(
        dag_id=dag_name,
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval=schedule_cron,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=max_active_tasks,
        tags=tags,
        params={
            "pipeline_id": Param("", type="string"),
            "start_ts": Param("", type="string"),
            "end_ts": Param("", type="string"),
        },
    ) as dag:
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")

        for pipeline in pipelines:
            if not pipeline.get("enabled", True):
                continue
            taskgroup = build_datasource_to_dwh_taskgroup(pipeline, dag)
            start >> taskgroup >> end

    return dag

def _fetch_dag_configs(hook):
    rows = hook.get_records(
        """
        SELECT id, dag_name, enabled, schedule_cron, timezone, owner, tags, max_active_tasks
        FROM control.dag_configs
        WHERE enabled = true
        ORDER BY dag_name
        """
    )
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
        SELECT p.pipeline_id, p.id, p.dag_id, dc.dag_name, p.enabled, p.description,
               p.source_table_name, p.datasource_timestamp_column,
               p.target_schema, p.target_table_name, p.target_table_schema,
               p.datasource_table, p.datawarehouse_table,
               p.unique_key, p.merge_window_minutes, p.expected_columns,
               p.merge_sql_text, p.freshness_threshold_minutes, p.sla_minutes
        FROM control.datasource_to_dwh_pipelines p
        INNER JOIN control.dag_configs dc ON p.dag_id = dc.id
        WHERE p.enabled = true
        ORDER BY dc.dag_name, p.pipeline_id
        """
    )
    for row in rows:
        (
            pipeline_id,
            pipeline_db_id,
            dag_id,
            dag_name,
            enabled,
            description,
            source_table_name,
            datasource_timestamp_column,
            target_schema,
            target_table_name,
            target_table_schema,
            datasource_table,
            datawarehouse_table,
            unique_key,
            merge_window_minutes,
            expected_columns,
            merge_sql_text,
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
        target_table_schema = _parse_json_value(target_table_schema)
        dag_cfg = dag_configs.get(dag_id)
        if not dag_cfg:
            continue
        dag_cfg["pipelines"].append(
            {
                "pipeline_id": pipeline_id,
                "pipeline_db_id": pipeline_db_id,
                "dag_id": dag_id,
                "enabled": bool(enabled),
                "description": description,
                "source_table_name": source_table_name,
                "datasource_table": datasource_table,
                "datasource_timestamp_column": datasource_timestamp_column,
                "target_schema": target_schema,
                "target_table_name": target_table_name,
                "target_table_schema": _ensure_list(target_table_schema),
                "datawarehouse_table": datawarehouse_table,
                "unique_key": unique_key,
                "merge_window_minutes": merge_window_minutes,
                "expected_columns": _ensure_list(expected_columns),
                "merge_sql_text": merge_sql_text,
                "freshness_threshold_minutes": freshness_threshold_minutes,
                "sla_minutes": sla_minutes,
            }
        )


def _fetch_from_metadata_query(hook) -> List[Dict[str, Any]]:
    if MetadataQuery is None:
        return []
    mq = MetadataQuery()
    sql = getattr(mq, "datasource_to_dwh", None)
    if not sql:
        return []
    row = hook.get_first(sql)
    parsed = _parse_json_value(row[0]) if row else None
    return parsed if isinstance(parsed, list) else []

class DatasourceToDwhGenerator:
    """Generate datasource-to-dwh DAGs from metadata configs."""

    def __init__(self, postgres_conn_id: str = DEFAULT_POSTGRES_CONN):
        self._postgres_conn_id = postgres_conn_id

    def generate_dag(self, dag_cfg: Dict[str, Any]):
        return build_datasource_to_dwh_dag(dag_cfg)

    def load_from_postgres(self) -> List[Dict[str, Any]]:
        try:
            hook = PostgresHook(postgres_conn_id=self._postgres_conn_id)
            dags = _fetch_from_metadata_query(hook)
            if dags:
                return dags
            dag_configs = _fetch_dag_configs(hook)
            if not dag_configs:
                return []
            _fetch_pipelines(hook, dag_configs)
            return list(dag_configs.values())
        except Exception as exc:
            logging.warning("Postgres metadata unavailable: %s", exc)
            return []

    def load_from_redis(
        self,
        redis_key: str,
        redis_host: str = DEFAULT_REDIS_HOST,
        redis_port: int = DEFAULT_REDIS_PORT,
        redis_db: int = DEFAULT_REDIS_DB,
    ) -> List[Dict[str, Any]]:
        try:
            client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            raw = client.get(redis_key)
            if not raw:
                logging.warning("No metadata found in Redis key %s", redis_key)
                return []
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode()
            payload = json.loads(raw)
            return _extract_dags(payload)
        except Exception as exc:
            logging.warning("Redis metadata unavailable for key %s: %s", redis_key, exc)
            return []

    def load_configs(
        self,
        source: Optional[str],
        redis_key: str,
        redis_host: str = DEFAULT_REDIS_HOST,
        redis_port: int = DEFAULT_REDIS_PORT,
        redis_db: int = DEFAULT_REDIS_DB,
    ) -> List[Dict[str, Any]]:
        mode = (source or "postgres").lower()
        if mode == "redis":
            return self.load_from_redis(redis_key, redis_host, redis_port, redis_db)
        if mode == "postgres":
            return self.load_from_postgres()
        dags = self.load_from_postgres()
        if dags:
            return dags
        return self.load_from_redis(redis_key, redis_host, redis_port, redis_db)
