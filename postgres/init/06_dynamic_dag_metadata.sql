CREATE TABLE IF NOT EXISTS control.dag_configs (
  dag_name text PRIMARY KEY,
  enabled boolean NOT NULL DEFAULT true,
  schedule_cron text NOT NULL,
  timezone text NOT NULL DEFAULT 'Asia/Jakarta',
  owner text,
  tags jsonb DEFAULT '[]'::jsonb,
  max_active_tasks int DEFAULT 8
);

CREATE TABLE IF NOT EXISTS control.datasource_to_dwh_pipelines (
  pipeline_id text PRIMARY KEY,
  dag_name text NOT NULL REFERENCES control.dag_configs(dag_name),
  enabled boolean NOT NULL DEFAULT true,
  description text,
  datasource_table text NOT NULL,
  datasource_timestamp_column text NOT NULL,
  datawarehouse_table text NOT NULL,
  unique_key text NOT NULL,
  merge_window_minutes int NOT NULL DEFAULT 10,
  expected_columns jsonb NOT NULL DEFAULT '[]'::jsonb,
  sql_merge_path text NOT NULL,
  freshness_threshold_minutes int NOT NULL DEFAULT 2,
  sla_minutes int NOT NULL DEFAULT 10
);

INSERT INTO control.dag_configs (
  dag_name, enabled, schedule_cron, timezone, owner, tags, max_active_tasks
) VALUES (
  'security_dwh',
  true,
  '*/5 * * * *',
  'Asia/Jakarta',
  'data-eng',
  '["security","dwh"]'::jsonb,
  8
)
ON CONFLICT (dag_name) DO UPDATE SET
  enabled = EXCLUDED.enabled,
  schedule_cron = EXCLUDED.schedule_cron,
  timezone = EXCLUDED.timezone,
  owner = EXCLUDED.owner,
  tags = EXCLUDED.tags,
  max_active_tasks = EXCLUDED.max_active_tasks;

INSERT INTO control.datasource_to_dwh_pipelines (
  pipeline_id,
  dag_name,
  enabled,
  description,
  datasource_table,
  datasource_timestamp_column,
  datawarehouse_table,
  unique_key,
  merge_window_minutes,
  expected_columns,
  sql_merge_path,
  freshness_threshold_minutes,
  sla_minutes
) VALUES (
  'security_events',
  'security_dwh',
  true,
  'Security events bronze to gold datawarehouse',
  'bronze.security_events_raw',
  'event_ts',
  'gold.security_events_dwh',
  'event_id',
  10,
  '[
    "event_id",
    "event_ts",
    "sensor_type",
    "sensor_name",
    "event_type",
    "severity",
    "src_ip",
    "dest_ip",
    "src_port",
    "dest_port",
    "protocol",
    "bytes",
    "packets",
    "uid",
    "conn_state",
    "duration",
    "signature",
    "signature_id",
    "category",
    "alert_action",
    "tags",
    "message"
  ]'::jsonb,
  'airflow/include/sql/security_events/datawarehouse_base_merge.sql',
  2,
  10
)
ON CONFLICT (pipeline_id) DO UPDATE SET
  dag_name = EXCLUDED.dag_name,
  enabled = EXCLUDED.enabled,
  description = EXCLUDED.description,
  datasource_table = EXCLUDED.datasource_table,
  datasource_timestamp_column = EXCLUDED.datasource_timestamp_column,
  datawarehouse_table = EXCLUDED.datawarehouse_table,
  unique_key = EXCLUDED.unique_key,
  merge_window_minutes = EXCLUDED.merge_window_minutes,
  expected_columns = EXCLUDED.expected_columns,
  sql_merge_path = EXCLUDED.sql_merge_path,
  freshness_threshold_minutes = EXCLUDED.freshness_threshold_minutes,
  sla_minutes = EXCLUDED.sla_minutes;
