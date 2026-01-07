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
  merge_sql_text text NOT NULL,
  freshness_threshold_minutes int NOT NULL DEFAULT 2,
  sla_minutes int NOT NULL DEFAULT 10,
  source_db_id int REFERENCES control.database_connections(id),
  target_db_id int REFERENCES control.database_connections(id),
  source_table_name text,
  target_schema text,
  target_table_name text,
  target_table_schema jsonb
);

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

DELETE FROM control.datasource_to_dwh_pipelines
WHERE pipeline_id = 'security_events';

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
  merge_sql_text,
  freshness_threshold_minutes,
  sla_minutes,
  source_db_id,
  target_db_id,
  source_table_name,
  target_schema,
  target_table_name,
  target_table_schema
) VALUES (
  'suricata_events',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'security_dwh'),
  true,
  'Suricata bronze to gold datawarehouse',
  'bronze.suricata_events_raw',
  'event_ts',
  'gold.suricata_events_dwh',
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
    "flow_id",
    "signature",
    "signature_id",
    "category",
    "alert_action",
    "http_url",
    "tags",
    "message",
    "raw_data"
  ]'::jsonb,
  $$
  CREATE TABLE IF NOT EXISTS {{DATAWAREHOUSE_TABLE}} (
    LIKE {{DATASOURCE_TABLE}} INCLUDING ALL
  );

  CREATE UNIQUE INDEX IF NOT EXISTS {{UNIQUE_INDEX_NAME}}
    ON {{DATAWAREHOUSE_TABLE}} ({{UNIQUE_KEY}});

  WITH source_data AS (
    SELECT {{COLUMN_LIST}}
    FROM {{DATASOURCE_TABLE}}
    WHERE {{TIME_FILTER}}
  )
  MERGE INTO {{DATAWAREHOUSE_TABLE}} AS target
  USING source_data AS source
    ON target.{{UNIQUE_KEY}} = source.{{UNIQUE_KEY}}
  WHEN MATCHED THEN
    UPDATE SET
      {{MERGE_UPDATE_SET}}
  WHEN NOT MATCHED THEN
    INSERT ({{COLUMN_LIST}})
    VALUES ({{SOURCE_COLUMN_LIST}});
  $$,
  10,
  10,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.suricata_events_raw',
  'gold',
  'suricata_events_dwh',
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
    {"name":"flow_id","type":"text"},
    {"name":"signature","type":"text"},
    {"name":"signature_id","type":"int"},
    {"name":"category","type":"text"},
    {"name":"alert_action","type":"text"},
    {"name":"http_url","type":"text"},
    {"name":"tags","type":"jsonb"},
    {"name":"message","type":"text"},
    {"name":"raw_data","type":"jsonb"}
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
  merge_sql_text = EXCLUDED.merge_sql_text,
  freshness_threshold_minutes = EXCLUDED.freshness_threshold_minutes,
  sla_minutes = EXCLUDED.sla_minutes,
  source_db_id = EXCLUDED.source_db_id,
  target_db_id = EXCLUDED.target_db_id,
  source_table_name = EXCLUDED.source_table_name,
  target_schema = EXCLUDED.target_schema,
  target_table_name = EXCLUDED.target_table_name,
  target_table_schema = EXCLUDED.target_table_schema;

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
  merge_sql_text,
  freshness_threshold_minutes,
  sla_minutes,
  source_db_id,
  target_db_id,
  source_table_name,
  target_schema,
  target_table_name,
  target_table_schema
) VALUES (
  'wazuh_events',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'security_dwh'),
  true,
  'Wazuh bronze to gold datawarehouse',
  'bronze.wazuh_events_raw',
  'event_ts',
  'gold.wazuh_events_dwh',
  'event_id',
  10,
  '[
    "event_id",
    "event_ts",
    "event_ingested_ts",
    "event_start_ts",
    "event_end_ts",
    "event_dataset",
    "event_kind",
    "event_module",
    "event_provider",
    "agent_name",
    "agent_ip",
    "host_name",
    "host_ip",
    "rule_id",
    "rule_level",
    "rule_name",
    "rule_ruleset",
    "tags",
    "message",
    "raw_data"
  ]'::jsonb,
  $$
  CREATE TABLE IF NOT EXISTS {{DATAWAREHOUSE_TABLE}} (
    LIKE {{DATASOURCE_TABLE}} INCLUDING ALL
  );

  CREATE UNIQUE INDEX IF NOT EXISTS {{UNIQUE_INDEX_NAME}}
    ON {{DATAWAREHOUSE_TABLE}} ({{UNIQUE_KEY}});

  WITH source_data AS (
    SELECT {{COLUMN_LIST}}
    FROM {{DATASOURCE_TABLE}}
    WHERE {{TIME_FILTER}}
  )
  MERGE INTO {{DATAWAREHOUSE_TABLE}} AS target
  USING source_data AS source
    ON target.{{UNIQUE_KEY}} = source.{{UNIQUE_KEY}}
  WHEN MATCHED THEN
    UPDATE SET
      {{MERGE_UPDATE_SET}}
  WHEN NOT MATCHED THEN
    INSERT ({{COLUMN_LIST}})
    VALUES ({{SOURCE_COLUMN_LIST}});
  $$,
  10,
  10,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.wazuh_events_raw',
  'gold',
  'wazuh_events_dwh',
  '[
    {"name":"event_id","type":"text"},
    {"name":"event_ts","type":"timestamptz"},
    {"name":"event_ingested_ts","type":"timestamptz"},
    {"name":"event_start_ts","type":"timestamptz"},
    {"name":"event_end_ts","type":"timestamptz"},
    {"name":"event_dataset","type":"text"},
    {"name":"event_kind","type":"text"},
    {"name":"event_module","type":"text"},
    {"name":"event_provider","type":"text"},
    {"name":"agent_name","type":"text"},
    {"name":"agent_ip","type":"inet"},
    {"name":"host_name","type":"text"},
    {"name":"host_ip","type":"inet"},
    {"name":"rule_id","type":"text"},
    {"name":"rule_level","type":"int"},
    {"name":"rule_name","type":"text"},
    {"name":"rule_ruleset","type":"jsonb"},
    {"name":"tags","type":"jsonb"},
    {"name":"message","type":"text"},
    {"name":"raw_data","type":"jsonb"}
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
  merge_sql_text = EXCLUDED.merge_sql_text,
  freshness_threshold_minutes = EXCLUDED.freshness_threshold_minutes,
  sla_minutes = EXCLUDED.sla_minutes,
  source_db_id = EXCLUDED.source_db_id,
  target_db_id = EXCLUDED.target_db_id,
  source_table_name = EXCLUDED.source_table_name,
  target_schema = EXCLUDED.target_schema,
  target_table_name = EXCLUDED.target_table_name,
  target_table_schema = EXCLUDED.target_table_schema;
