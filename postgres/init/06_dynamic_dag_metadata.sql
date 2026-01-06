CREATE TABLE IF NOT EXISTS control.database_connections (
  id serial PRIMARY KEY,
  db_name text NOT NULL,
  db_type text NOT NULL,
  db_host text,
  db_port int,
  username text,
  db_conn_name text NOT NULL UNIQUE,
  gsm_path text
);

CREATE TABLE IF NOT EXISTS control.dag_configs (
  id serial PRIMARY KEY,
  dag_name text NOT NULL UNIQUE,
  enabled boolean NOT NULL DEFAULT true,
  schedule_cron text NOT NULL,
  timezone text NOT NULL DEFAULT 'Asia/Jakarta',
  owner text,
  tags jsonb DEFAULT '[]'::jsonb,
  max_active_tasks int DEFAULT 8
);

CREATE TABLE IF NOT EXISTS control.datasource_to_dwh_pipelines (
  id serial PRIMARY KEY,
  pipeline_id text NOT NULL UNIQUE,
  dag_id int NOT NULL REFERENCES control.dag_configs(id),
  enabled boolean NOT NULL DEFAULT true,
  description text,
  datasource_table text,
  datasource_timestamp_column text NOT NULL,
  datawarehouse_table text,
  unique_key text NOT NULL,
  merge_window_minutes int NOT NULL DEFAULT 10,
  expected_columns jsonb NOT NULL DEFAULT '[]'::jsonb,
  sql_merge_path text NOT NULL,
  freshness_threshold_minutes int NOT NULL DEFAULT 2,
  sla_minutes int NOT NULL DEFAULT 10,
  source_db_id int REFERENCES control.database_connections(id),
  target_db_id int REFERENCES control.database_connections(id),
  source_table_name text,
  source_sql_query text,
  target_schema text,
  target_table_name text,
  target_table_schema jsonb
);

CREATE TABLE IF NOT EXISTS control.datasource_to_dwh_sql_steps (
  id serial PRIMARY KEY,
  pipeline_db_id int NOT NULL REFERENCES control.datasource_to_dwh_pipelines(id),
  step_name text NOT NULL,
  step_order int NOT NULL DEFAULT 1,
  sql_text text NOT NULL,
  enabled boolean NOT NULL DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_dwh_sql_steps_pipeline
  ON control.datasource_to_dwh_sql_steps(pipeline_db_id, step_order);

INSERT INTO control.database_connections (
  db_name, db_type, db_host, db_port, username, db_conn_name, gsm_path
) VALUES (
  'analytics',
  'postgres',
  'postgres',
  5432,
  'etl_runner',
  'analytics_db',
  NULL
)
ON CONFLICT (db_conn_name) DO UPDATE SET
  db_name = EXCLUDED.db_name,
  db_type = EXCLUDED.db_type,
  db_host = EXCLUDED.db_host,
  db_port = EXCLUDED.db_port,
  username = EXCLUDED.username,
  gsm_path = EXCLUDED.gsm_path;

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
  dag_id,
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
  sla_minutes,
  source_db_id,
  target_db_id,
  source_table_name,
  source_sql_query,
  target_schema,
  target_table_name,
  target_table_schema
) VALUES (
  'security_events',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'security_dwh'),
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
  10,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.security_events_raw',
  NULL,
  'gold',
  'security_events_dwh',
  '[
    {"name":"event_id","type":"text"},
    {"name":"event_ts","type":"timestamptz"},
    {"name":"sensor_type","type":"text"},
    {"name":"sensor_name","type":"text"},
    {"name":"event_type","type":"text"},
    {"name":"severity","type":"text"},
    {"name":"src_ip","type":"inet"},
    {"name":"dest_ip","type":"inet"},
    {"name":"src_port","type":"int"},
    {"name":"dest_port","type":"int"},
    {"name":"protocol","type":"text"},
    {"name":"bytes","type":"bigint"},
    {"name":"packets","type":"bigint"},
    {"name":"uid","type":"text"},
    {"name":"conn_state","type":"text"},
    {"name":"duration","type":"double precision"},
    {"name":"signature","type":"text"},
    {"name":"signature_id","type":"int"},
    {"name":"category","type":"text"},
    {"name":"alert_action","type":"text"},
    {"name":"tags","type":"jsonb"},
    {"name":"message","type":"text"}
  ]'::jsonb
)
ON CONFLICT (pipeline_id) DO UPDATE SET
  dag_id = EXCLUDED.dag_id,
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
  sla_minutes = EXCLUDED.sla_minutes,
  source_db_id = EXCLUDED.source_db_id,
  target_db_id = EXCLUDED.target_db_id,
  source_table_name = EXCLUDED.source_table_name,
  source_sql_query = EXCLUDED.source_sql_query,
  target_schema = EXCLUDED.target_schema,
  target_table_name = EXCLUDED.target_table_name,
  target_table_schema = EXCLUDED.target_table_schema;
