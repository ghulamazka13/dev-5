import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_PARAMS = {
    "pipeline_id": "security_events",
    "start_ts": "",
    "end_ts": "",
}


def should_backfill(**context):
    params = context.get("params", {})
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    pipeline_id = conf.get("pipeline_id", params.get("pipeline_id"))
    start_ts = conf.get("start_ts", params.get("start_ts"))
    end_ts = conf.get("end_ts", params.get("end_ts"))
    should_run = bool(pipeline_id and start_ts and end_ts)
    if not pipeline_id:
        logging.warning("Backfill skipped: missing pipeline_id")
    if not start_ts or not end_ts:
        logging.warning(
            "Backfill skipped: missing window start_ts=%s end_ts=%s",
            start_ts,
            end_ts,
        )
    logging.info(
        "Backfill decision pipeline_id=%s start_ts=%s end_ts=%s should_run=%s",
        pipeline_id,
        start_ts,
        end_ts,
        should_run,
    )
    return should_run


BACKFILL_SILVER_SQL = """
CREATE TABLE IF NOT EXISTS silver.security_events (
  event_id text PRIMARY KEY,
  event_ts timestamptz,
  event_date date,
  event_hour timestamptz,
  sensor_type text,
  sensor_name text,
  event_type text,
  severity text,
  severity_score int,
  is_alert boolean,
  src_ip inet,
  dest_ip inet,
  src_port int,
  dest_port int,
  protocol text,
  bytes bigint,
  packets bigint,
  uid text,
  conn_state text,
  duration double precision,
  signature text,
  signature_id int,
  category text,
  alert_action text,
  tags jsonb,
  message text,
  ingested_at timestamptz default now()
);

WITH src AS (
  SELECT
    event_id,
    event_ts,
    (event_ts AT TIME ZONE 'UTC')::date AS event_date,
    date_trunc('hour', event_ts) AS event_hour,
    lower(sensor_type) AS sensor_type,
    sensor_name,
    lower(event_type) AS event_type,
    lower(severity) AS severity,
    CASE lower(severity)
      WHEN 'low' THEN 1
      WHEN 'medium' THEN 5
      WHEN 'high' THEN 10
      ELSE 0
    END AS severity_score,
    (lower(event_type) = 'alert') AS is_alert,
    src_ip,
    dest_ip,
    src_port,
    dest_port,
    lower(protocol) AS protocol,
    bytes,
    packets,
    uid,
    conn_state,
    duration,
    signature,
    signature_id,
    category,
    alert_action,
    tags,
    message
  FROM bronze.security_events_raw
  WHERE event_ts >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::timestamptz
    AND event_ts < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::timestamptz
    AND event_ts IS NOT NULL
)
INSERT INTO silver.security_events (
  event_id, event_ts, event_date, event_hour, sensor_type, sensor_name, event_type, severity,
  severity_score, is_alert, src_ip, dest_ip, src_port, dest_port, protocol, bytes, packets,
  uid, conn_state, duration, signature, signature_id, category, alert_action, tags, message
)
SELECT
  event_id, event_ts, event_date, event_hour, sensor_type, sensor_name, event_type, severity,
  severity_score, is_alert, src_ip, dest_ip, src_port, dest_port, protocol, bytes, packets,
  uid, conn_state, duration, signature, signature_id, category, alert_action, tags, message
FROM src
ON CONFLICT (event_id) DO UPDATE SET
  event_ts = EXCLUDED.event_ts,
  event_date = EXCLUDED.event_date,
  event_hour = EXCLUDED.event_hour,
  sensor_type = EXCLUDED.sensor_type,
  sensor_name = EXCLUDED.sensor_name,
  event_type = EXCLUDED.event_type,
  severity = EXCLUDED.severity,
  severity_score = EXCLUDED.severity_score,
  is_alert = EXCLUDED.is_alert,
  src_ip = EXCLUDED.src_ip,
  dest_ip = EXCLUDED.dest_ip,
  src_port = EXCLUDED.src_port,
  dest_port = EXCLUDED.dest_port,
  protocol = EXCLUDED.protocol,
  bytes = EXCLUDED.bytes,
  packets = EXCLUDED.packets,
  uid = EXCLUDED.uid,
  conn_state = EXCLUDED.conn_state,
  duration = EXCLUDED.duration,
  signature = EXCLUDED.signature,
  signature_id = EXCLUDED.signature_id,
  category = EXCLUDED.category,
  alert_action = EXCLUDED.alert_action,
  tags = EXCLUDED.tags,
  message = EXCLUDED.message;
"""

BACKFILL_GOLD_5M_SQL = """
CREATE TABLE IF NOT EXISTS gold.alerts_5m (
  window_start timestamptz,
  window_end timestamptz,
  severity text,
  alert_count bigint,
  unique_src_ips bigint,
  unique_dest_ips bigint,
  max_severity_score int,
  PRIMARY KEY (window_start, severity)
);

DELETE FROM gold.alerts_5m
WHERE window_start >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::timestamptz
  AND window_start < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::timestamptz;

WITH windowed AS (
  SELECT
    date_trunc('minute', event_ts) - (extract(minute from event_ts)::int % 5) * interval '1 minute' AS window_start,
    severity,
    src_ip,
    dest_ip,
    severity_score
  FROM silver.security_events
  WHERE event_ts >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::timestamptz
    AND event_ts < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::timestamptz
    AND is_alert = true
)
INSERT INTO gold.alerts_5m (
  window_start,
  window_end,
  severity,
  alert_count,
  unique_src_ips,
  unique_dest_ips,
  max_severity_score
)
SELECT
  window_start,
  window_start + interval '5 minutes' AS window_end,
  severity,
  count(*) AS alert_count,
  count(distinct src_ip) AS unique_src_ips,
  count(distinct dest_ip) AS unique_dest_ips,
  max(severity_score) AS max_severity_score
FROM windowed
GROUP BY window_start, severity
ON CONFLICT (window_start, severity) DO UPDATE SET
  window_end = EXCLUDED.window_end,
  alert_count = EXCLUDED.alert_count,
  unique_src_ips = EXCLUDED.unique_src_ips,
  unique_dest_ips = EXCLUDED.unique_dest_ips,
  max_severity_score = EXCLUDED.max_severity_score;
"""

BACKFILL_GOLD_1H_SQL = """
CREATE TABLE IF NOT EXISTS gold.alerts_1h (
  window_start timestamptz,
  window_end timestamptz,
  severity text,
  alert_count bigint,
  unique_src_ips bigint,
  unique_dest_ips bigint,
  max_severity_score int,
  PRIMARY KEY (window_start, severity)
);

DELETE FROM gold.alerts_1h
WHERE window_start >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::timestamptz
  AND window_start < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::timestamptz;

WITH windowed AS (
  SELECT
    date_trunc('hour', event_ts) AS window_start,
    severity,
    src_ip,
    dest_ip,
    severity_score
  FROM silver.security_events
  WHERE event_ts >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::timestamptz
    AND event_ts < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::timestamptz
    AND is_alert = true
)
INSERT INTO gold.alerts_1h (
  window_start,
  window_end,
  severity,
  alert_count,
  unique_src_ips,
  unique_dest_ips,
  max_severity_score
)
SELECT
  window_start,
  window_start + interval '1 hour' AS window_end,
  severity,
  count(*) AS alert_count,
  count(distinct src_ip) AS unique_src_ips,
  count(distinct dest_ip) AS unique_dest_ips,
  max(severity_score) AS max_severity_score
FROM windowed
GROUP BY window_start, severity
ON CONFLICT (window_start, severity) DO UPDATE SET
  window_end = EXCLUDED.window_end,
  alert_count = EXCLUDED.alert_count,
  unique_src_ips = EXCLUDED.unique_src_ips,
  unique_dest_ips = EXCLUDED.unique_dest_ips,
  max_severity_score = EXCLUDED.max_severity_score;
"""

BACKFILL_GOLD_DAILY_SQL = """
CREATE TABLE IF NOT EXISTS gold.alerts_daily (
  event_date date,
  severity text,
  alert_count bigint,
  unique_src_ips bigint,
  unique_dest_ips bigint,
  max_severity_score int,
  PRIMARY KEY (event_date, severity)
);

DELETE FROM gold.alerts_daily
WHERE event_date >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::date
  AND event_date < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::date;

WITH daily AS (
  SELECT
    event_date,
    severity,
    src_ip,
    dest_ip,
    severity_score
  FROM silver.security_events
  WHERE event_ts >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::timestamptz
    AND event_ts < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::timestamptz
    AND is_alert = true
)
INSERT INTO gold.alerts_daily (
  event_date,
  severity,
  alert_count,
  unique_src_ips,
  unique_dest_ips,
  max_severity_score
)
SELECT
  event_date,
  severity,
  count(*) AS alert_count,
  count(distinct src_ip) AS unique_src_ips,
  count(distinct dest_ip) AS unique_dest_ips,
  max(severity_score) AS max_severity_score
FROM daily
GROUP BY event_date, severity
ON CONFLICT (event_date, severity) DO UPDATE SET
  alert_count = EXCLUDED.alert_count,
  unique_src_ips = EXCLUDED.unique_src_ips,
  unique_dest_ips = EXCLUDED.unique_dest_ips,
  max_severity_score = EXCLUDED.max_severity_score;
"""

BACKFILL_TOP_TALKERS_SQL = """
CREATE TABLE IF NOT EXISTS gold.top_talkers_daily (
  event_date date,
  src_ip inet,
  dest_ip inet,
  total_bytes bigint,
  event_count bigint,
  rank int,
  PRIMARY KEY (event_date, rank, src_ip, dest_ip)
);

DELETE FROM gold.top_talkers_daily
WHERE event_date >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::date
  AND event_date < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::date;

WITH daily AS (
  SELECT
    event_date,
    src_ip,
    dest_ip,
    sum(bytes) AS total_bytes,
    count(*) AS event_count
  FROM silver.security_events
  WHERE event_ts >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::timestamptz
    AND event_ts < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::timestamptz
  GROUP BY event_date, src_ip, dest_ip
),
ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY event_date ORDER BY total_bytes DESC) AS rank
  FROM daily
)
INSERT INTO gold.top_talkers_daily (
  event_date, src_ip, dest_ip, total_bytes, event_count, rank
)
SELECT event_date, src_ip, dest_ip, total_bytes, event_count, rank
FROM ranked
WHERE rank <= 20
ON CONFLICT (event_date, rank, src_ip, dest_ip) DO UPDATE SET
  total_bytes = EXCLUDED.total_bytes,
  event_count = EXCLUDED.event_count;
"""

BACKFILL_TOP_SIGNATURES_SQL = """
CREATE TABLE IF NOT EXISTS gold.top_signatures_daily (
  event_date date,
  signature text,
  signature_id int,
  alert_count bigint,
  rank int,
  PRIMARY KEY (event_date, rank, signature_id)
);

DELETE FROM gold.top_signatures_daily
WHERE event_date >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::date
  AND event_date < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::date;

WITH daily AS (
  SELECT
    event_date,
    signature,
    signature_id,
    count(*) AS alert_count
  FROM silver.security_events
  WHERE event_ts >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::timestamptz
    AND event_ts < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::timestamptz
    AND signature IS NOT NULL
  GROUP BY event_date, signature, signature_id
),
ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY event_date ORDER BY alert_count DESC) AS rank
  FROM daily
)
INSERT INTO gold.top_signatures_daily (
  event_date, signature, signature_id, alert_count, rank
)
SELECT event_date, signature, signature_id, alert_count, rank
FROM ranked
WHERE rank <= 20
ON CONFLICT (event_date, rank, signature_id) DO UPDATE SET
  signature = EXCLUDED.signature,
  alert_count = EXCLUDED.alert_count;
"""

BACKFILL_PROTOCOL_MIX_SQL = """
CREATE TABLE IF NOT EXISTS gold.protocol_mix_daily (
  event_date date,
  protocol text,
  event_count bigint,
  bytes_sum bigint,
  pct_of_total numeric(5,2),
  PRIMARY KEY (event_date, protocol)
);

DELETE FROM gold.protocol_mix_daily
WHERE event_date >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::date
  AND event_date < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::date;

WITH daily AS (
  SELECT
    event_date,
    COALESCE(protocol, 'unknown') AS protocol,
    count(*) AS event_count,
    sum(bytes) AS bytes_sum
  FROM silver.security_events
  WHERE event_ts >= '{{ (dag_run.conf or {}).get("start_ts", params.start_ts) }}'::timestamptz
    AND event_ts < '{{ (dag_run.conf or {}).get("end_ts", params.end_ts) }}'::timestamptz
  GROUP BY event_date, COALESCE(protocol, 'unknown')
),
totals AS (
  SELECT event_date, sum(event_count) AS total_count
  FROM daily
  GROUP BY event_date
)
INSERT INTO gold.protocol_mix_daily (
  event_date, protocol, event_count, bytes_sum, pct_of_total
)
SELECT
  d.event_date,
  d.protocol,
  d.event_count,
  d.bytes_sum,
  CASE WHEN t.total_count > 0
    THEN round(d.event_count::numeric / t.total_count * 100, 2)
    ELSE 0
  END AS pct_of_total
FROM daily d
JOIN totals t ON d.event_date = t.event_date
ON CONFLICT (event_date, protocol) DO UPDATE SET
  event_count = EXCLUDED.event_count,
  bytes_sum = EXCLUDED.bytes_sum,
  pct_of_total = EXCLUDED.pct_of_total;
"""

MAINTENANCE_SQL = """
VACUUM ANALYZE silver.security_events;
VACUUM ANALYZE gold.alerts_5m;
VACUUM ANALYZE gold.alerts_1h;
VACUUM ANALYZE gold.alerts_daily;
VACUUM ANALYZE gold.top_talkers_daily;
VACUUM ANALYZE gold.top_signatures_daily;
VACUUM ANALYZE gold.protocol_mix_daily;
"""

with DAG(
    dag_id="backfill_maintenance_dag",
    default_args={"owner": "data-eng", "retries": 1},
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    params=DEFAULT_PARAMS,
    tags=["backfill", "maintenance"],
) as dag:
    start = EmptyOperator(task_id="start")

    maintenance = PostgresOperator(
        task_id="maintenance",
        postgres_conn_id="analytics_db",
        sql=MAINTENANCE_SQL,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    backfill_gate = ShortCircuitOperator(
        task_id="should_backfill",
        python_callable=should_backfill,
    )

    backfill_silver = PostgresOperator(
        task_id="backfill_silver",
        postgres_conn_id="analytics_db",
        sql=BACKFILL_SILVER_SQL,
    )

    backfill_gold_5m = PostgresOperator(
        task_id="backfill_gold_5m",
        postgres_conn_id="analytics_db",
        sql=BACKFILL_GOLD_5M_SQL,
    )

    backfill_gold_1h = PostgresOperator(
        task_id="backfill_gold_1h",
        postgres_conn_id="analytics_db",
        sql=BACKFILL_GOLD_1H_SQL,
    )

    backfill_gold_daily = PostgresOperator(
        task_id="backfill_gold_daily",
        postgres_conn_id="analytics_db",
        sql=BACKFILL_GOLD_DAILY_SQL,
    )

    backfill_top_talkers = PostgresOperator(
        task_id="backfill_top_talkers",
        postgres_conn_id="analytics_db",
        sql=BACKFILL_TOP_TALKERS_SQL,
    )

    backfill_top_signatures = PostgresOperator(
        task_id="backfill_top_signatures",
        postgres_conn_id="analytics_db",
        sql=BACKFILL_TOP_SIGNATURES_SQL,
    )

    backfill_protocol_mix = PostgresOperator(
        task_id="backfill_protocol_mix",
        postgres_conn_id="analytics_db",
        sql=BACKFILL_PROTOCOL_MIX_SQL,
    )

    start >> maintenance
    start >> backfill_gate >> backfill_silver
    backfill_silver >> backfill_gold_5m >> backfill_gold_1h >> backfill_gold_daily
    backfill_gold_daily >> backfill_top_talkers >> backfill_top_signatures
    backfill_top_signatures >> backfill_protocol_mix
