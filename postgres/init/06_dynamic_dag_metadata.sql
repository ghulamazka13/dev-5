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

INSERT INTO control.dag_configs (
  dag_name, enabled, schedule_cron, timezone, owner, tags, max_active_tasks
) VALUES (
  'gold_star_schema',
  true,
  '*/5 * * * *',
  'Asia/Jakarta',
  'data-eng',
  '["gold","star-schema","security"]'::jsonb,
  6
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

DELETE FROM control.datasource_to_dwh_pipelines
WHERE pipeline_id = 'gold_star_schema';

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
  '["event_id", "event_ts", "sensor_type", "sensor_name", "event_type", "severity", "src_ip", "dest_ip", "src_port", "dest_port", "protocol", "bytes", "packets", "flow_id", "signature", "signature_id", "category", "alert_action", "http_url", "tags", "message", "raw_data"]'::jsonb,
  $suricata_events_sql$

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
$suricata_events_sql$,
  720,
  10,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.suricata_events_raw',
  'gold',
  'suricata_events_dwh',
  '[{"name": "event_id", "type": "text"}, {"name": "event_ts", "type": "timestamptz"}, {"name": "sensor_type", "type": "text"}, {"name": "sensor_name", "type": "text"}, {"name": "event_type", "type": "text"}, {"name": "severity", "type": "text"}, {"name": "src_ip", "type": "inet"}, {"name": "dest_ip", "type": "inet"}, {"name": "src_port", "type": "int"}, {"name": "dest_port", "type": "int"}, {"name": "protocol", "type": "text"}, {"name": "bytes", "type": "bigint"}, {"name": "packets", "type": "bigint"}, {"name": "flow_id", "type": "text"}, {"name": "signature", "type": "text"}, {"name": "signature_id", "type": "int"}, {"name": "category", "type": "text"}, {"name": "alert_action", "type": "text"}, {"name": "http_url", "type": "text"}, {"name": "tags", "type": "jsonb"}, {"name": "message", "type": "text"}, {"name": "raw_data", "type": "jsonb"}]'::jsonb
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
  'zeek_events',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'security_dwh'),
  true,
  'Zeek bronze to gold datawarehouse',
  'bronze.zeek_events_raw',
  'event_ts',
  'gold.zeek_events_dwh',
  'event_id',
  10,
  '["event_id", "event_ts", "event_ingested_ts", "event_start_ts", "event_end_ts", "event_dataset", "event_kind", "event_module", "event_provider", "zeek_uid", "sensor_name", "src_ip", "dest_ip", "src_port", "dest_port", "protocol", "application", "network_type", "direction", "community_id", "bytes", "packets", "orig_bytes", "resp_bytes", "orig_pkts", "resp_pkts", "conn_state", "conn_state_description", "duration", "history", "vlan_id", "tags", "message", "raw_data"]'::jsonb,
  $zeek_events_sql$

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
$zeek_events_sql$,
  720,
  10,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.zeek_events_raw',
  'gold',
  'zeek_events_dwh',
  '[{"name": "event_id", "type": "text"}, {"name": "event_ts", "type": "timestamptz"}, {"name": "event_ingested_ts", "type": "timestamptz"}, {"name": "event_start_ts", "type": "timestamptz"}, {"name": "event_end_ts", "type": "timestamptz"}, {"name": "event_dataset", "type": "text"}, {"name": "event_kind", "type": "text"}, {"name": "event_module", "type": "text"}, {"name": "event_provider", "type": "text"}, {"name": "zeek_uid", "type": "text"}, {"name": "sensor_name", "type": "text"}, {"name": "src_ip", "type": "inet"}, {"name": "dest_ip", "type": "inet"}, {"name": "src_port", "type": "int"}, {"name": "dest_port", "type": "int"}, {"name": "protocol", "type": "text"}, {"name": "application", "type": "text"}, {"name": "network_type", "type": "text"}, {"name": "direction", "type": "text"}, {"name": "community_id", "type": "text"}, {"name": "bytes", "type": "bigint"}, {"name": "packets", "type": "bigint"}, {"name": "orig_bytes", "type": "bigint"}, {"name": "resp_bytes", "type": "bigint"}, {"name": "orig_pkts", "type": "bigint"}, {"name": "resp_pkts", "type": "bigint"}, {"name": "conn_state", "type": "text"}, {"name": "conn_state_description", "type": "text"}, {"name": "duration", "type": "double precision"}, {"name": "history", "type": "text"}, {"name": "vlan_id", "type": "text"}, {"name": "tags", "type": "jsonb"}, {"name": "message", "type": "text"}, {"name": "raw_data", "type": "jsonb"}]'::jsonb
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
  '["event_id", "event_ts", "event_ingested_ts", "event_start_ts", "event_end_ts", "event_dataset", "event_kind", "event_module", "event_provider", "agent_name", "agent_ip", "host_name", "host_ip", "rule_id", "rule_level", "rule_name", "rule_ruleset", "tags", "message", "raw_data"]'::jsonb,
  $wazuh_events_sql$

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
$wazuh_events_sql$,
  120,
  10,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.wazuh_events_raw',
  'gold',
  'wazuh_events_dwh',
  '[{"name": "event_id", "type": "text"}, {"name": "event_ts", "type": "timestamptz"}, {"name": "event_ingested_ts", "type": "timestamptz"}, {"name": "event_start_ts", "type": "timestamptz"}, {"name": "event_end_ts", "type": "timestamptz"}, {"name": "event_dataset", "type": "text"}, {"name": "event_kind", "type": "text"}, {"name": "event_module", "type": "text"}, {"name": "event_provider", "type": "text"}, {"name": "agent_name", "type": "text"}, {"name": "agent_ip", "type": "inet"}, {"name": "host_name", "type": "text"}, {"name": "host_ip", "type": "inet"}, {"name": "rule_id", "type": "text"}, {"name": "rule_level", "type": "int"}, {"name": "rule_name", "type": "text"}, {"name": "rule_ruleset", "type": "jsonb"}, {"name": "tags", "type": "jsonb"}, {"name": "message", "type": "text"}, {"name": "raw_data", "type": "jsonb"}]'::jsonb
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
  'gold_dim_date',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold dim_date from bronze events',
  'bronze.wazuh_events_raw',
  'event_ts',
  'gold.dim_date',
  'date_key',
  15,
  '["event_ts"]'::jsonb,
  $gold_dim_date_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  INSERT INTO gold.dim_date (
    date_key,
    date,
    year,
    quarter,
    month,
    day,
    week_of_year,
    day_of_week
  )
  SELECT
    to_char(d, 'YYYYMMDD')::int,
    d,
    EXTRACT(year FROM d)::int,
    EXTRACT(quarter FROM d)::int,
    EXTRACT(month FROM d)::int,
    EXTRACT(day FROM d)::int,
    EXTRACT(week FROM d)::int,
    EXTRACT(dow FROM d)::int
  FROM (
    SELECT DISTINCT event_ts::date AS d
    FROM bronze.wazuh_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
    UNION
    SELECT DISTINCT event_ts::date AS d
    FROM bronze.suricata_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
    UNION
    SELECT DISTINCT event_ts::date AS d
    FROM bronze.zeek_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
  ) dates
  WHERE d IS NOT NULL
  ON CONFLICT (date_key) DO NOTHING;
END $$;
$gold_dim_date_sql$,
  120,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.wazuh_events_raw',
  'gold',
  'dim_date',
  '[{"name": "date_key", "type": "int"}, {"name": "date", "type": "date"}, {"name": "year", "type": "int"}, {"name": "quarter", "type": "int"}, {"name": "month", "type": "int"}, {"name": "day", "type": "int"}, {"name": "week_of_year", "type": "int"}, {"name": "day_of_week", "type": "int"}]'::jsonb
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
  'gold_dim_time',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold dim_time from bronze events',
  'bronze.wazuh_events_raw',
  'event_ts',
  'gold.dim_time',
  'time_key',
  15,
  '["event_ts"]'::jsonb,
  $gold_dim_time_sql$


DO $$
BEGIN
  INSERT INTO gold.dim_time (
    time_key,
    hour,
    minute,
    second
  )
  SELECT
    to_char(t, 'HH24MISS')::int,
    EXTRACT(hour FROM t)::int,
    EXTRACT(minute FROM t)::int,
    EXTRACT(second FROM t)::int
  FROM (
    SELECT (timestamp '2000-01-01' + s * interval '1 second')::time AS t
    FROM generate_series(0, 86399) AS s
  ) times
  ON CONFLICT (time_key) DO NOTHING;
END $$;
$gold_dim_time_sql$,
  120,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.wazuh_events_raw',
  'gold',
  'dim_time',
  '[{"name": "time_key", "type": "int"}, {"name": "hour", "type": "int"}, {"name": "minute", "type": "int"}, {"name": "second", "type": "int"}]'::jsonb
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
  'gold_dim_event',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold dim_event from bronze events',
  'bronze.wazuh_events_raw',
  'event_ts',
  'gold.dim_event',
  'event_key',
  15,
  '["event_ts", "event_dataset", "event_kind", "event_module", "event_provider"]'::jsonb,
  $gold_dim_event_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  INSERT INTO gold.dim_event (
    event_dataset,
    event_kind,
    event_module,
    event_provider
  )
  SELECT DISTINCT
    event_dataset,
    event_kind,
    event_module,
    event_provider
  FROM (
    SELECT event_dataset, event_kind, event_module, event_provider
    FROM bronze.wazuh_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
    UNION
    SELECT event_dataset, event_kind, event_module, event_provider
    FROM bronze.zeek_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
  ) events
  WHERE event_dataset IS NOT NULL
    AND event_kind IS NOT NULL
    AND event_module IS NOT NULL
    AND event_provider IS NOT NULL
  ON CONFLICT (event_dataset, event_kind, event_module, event_provider) DO NOTHING;
END $$;
$gold_dim_event_sql$,
  120,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.wazuh_events_raw',
  'gold',
  'dim_event',
  '[{"name": "event_key", "type": "bigint"}, {"name": "event_dataset", "type": "text"}, {"name": "event_kind", "type": "text"}, {"name": "event_module", "type": "text"}, {"name": "event_provider", "type": "text"}]'::jsonb
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
  'gold_dim_sensor',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold dim_sensor from bronze events',
  'bronze.suricata_events_raw',
  'event_ts',
  'gold.dim_sensor',
  'sensor_key',
  15,
  '["event_ts", "sensor_type", "sensor_name"]'::jsonb,
  $gold_dim_sensor_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  INSERT INTO gold.dim_sensor (
    sensor_type,
    sensor_name
  )
  SELECT DISTINCT
    sensor_type,
    sensor_name
  FROM (
    SELECT sensor_type, sensor_name
    FROM bronze.suricata_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND sensor_name IS NOT NULL
    UNION
    SELECT 'zeek'::text AS sensor_type, sensor_name
    FROM bronze.zeek_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND sensor_name IS NOT NULL
  ) sensors
  ON CONFLICT (sensor_type, sensor_name) DO NOTHING;
END $$;
$gold_dim_sensor_sql$,
  720,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.suricata_events_raw',
  'gold',
  'dim_sensor',
  '[{"name": "sensor_key", "type": "bigint"}, {"name": "sensor_type", "type": "text"}, {"name": "sensor_name", "type": "text"}]'::jsonb
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
  'gold_dim_protocol',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold dim_protocol from bronze events',
  'bronze.suricata_events_raw',
  'event_ts',
  'gold.dim_protocol',
  'protocol_key',
  15,
  '["event_ts", "protocol"]'::jsonb,
  $gold_dim_protocol_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  INSERT INTO gold.dim_protocol (protocol)
  SELECT DISTINCT protocol
  FROM (
    SELECT protocol
    FROM bronze.suricata_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND protocol IS NOT NULL
    UNION
    SELECT protocol
    FROM bronze.zeek_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND protocol IS NOT NULL
  ) protocols
  ON CONFLICT (protocol) DO NOTHING;
END $$;
$gold_dim_protocol_sql$,
  720,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.suricata_events_raw',
  'gold',
  'dim_protocol',
  '[{"name": "protocol_key", "type": "bigint"}, {"name": "protocol", "type": "text"}]'::jsonb
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
  'gold_dim_signature',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold dim_signature from suricata events',
  'bronze.suricata_events_raw',
  'event_ts',
  'gold.dim_signature',
  'signature_key',
  15,
  '["event_ts", "signature_id", "signature", "category", "alert_action"]'::jsonb,
  $gold_dim_signature_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  INSERT INTO gold.dim_signature (
    signature_id,
    signature,
    category,
    alert_action
  )
  SELECT DISTINCT ON (signature_id)
    signature_id,
    signature,
    category,
    alert_action
  FROM bronze.suricata_events_raw
  WHERE event_ts >= start_ts AND event_ts < end_ts
    AND signature_id IS NOT NULL
  ORDER BY signature_id, event_ts DESC
  ON CONFLICT (signature_id) WHERE signature_id IS NOT NULL DO UPDATE SET
    signature = EXCLUDED.signature,
    category = EXCLUDED.category,
    alert_action = EXCLUDED.alert_action;
END $$;
$gold_dim_signature_sql$,
  720,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.suricata_events_raw',
  'gold',
  'dim_signature',
  '[{"name": "signature_key", "type": "bigint"}, {"name": "signature_id", "type": "int"}, {"name": "signature", "type": "text"}, {"name": "category", "type": "text"}, {"name": "alert_action", "type": "text"}]'::jsonb
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
  'gold_dim_tag',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold dim_tag from bronze events',
  'bronze.wazuh_events_raw',
  'event_ts',
  'gold.dim_tag',
  'tag_key',
  15,
  '["event_ts", "tags"]'::jsonb,
  $gold_dim_tag_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  INSERT INTO gold.dim_tag (tag_value)
  SELECT DISTINCT tag_value
  FROM (
    SELECT jsonb_array_elements_text(tags) AS tag_value
    FROM bronze.wazuh_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND tags IS NOT NULL
      AND jsonb_typeof(tags) = 'array'
    UNION ALL
    SELECT jsonb_array_elements_text(tags) AS tag_value
    FROM bronze.suricata_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND tags IS NOT NULL
      AND jsonb_typeof(tags) = 'array'
    UNION ALL
    SELECT jsonb_array_elements_text(tags) AS tag_value
    FROM bronze.zeek_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND tags IS NOT NULL
      AND jsonb_typeof(tags) = 'array'
  ) tags
  WHERE tag_value IS NOT NULL AND btrim(tag_value) <> ''
  ON CONFLICT (tag_value) DO NOTHING;
END $$;
$gold_dim_tag_sql$,
  120,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.wazuh_events_raw',
  'gold',
  'dim_tag',
  '[{"name": "tag_key", "type": "bigint"}, {"name": "tag_value", "type": "text"}]'::jsonb
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
  'gold_dim_agent',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold dim_agent from wazuh events',
  'bronze.wazuh_events_raw',
  'event_ts',
  'gold.dim_agent',
  'agent_key',
  15,
  '["event_ts", "agent_name", "agent_ip"]'::jsonb,
  $gold_dim_agent_sql$

DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  WITH src AS (
    SELECT DISTINCT ON (agent_name)
      agent_name,
      agent_ip
    FROM bronze.wazuh_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND agent_name IS NOT NULL
    ORDER BY agent_name, event_ts DESC
  ), changed AS (
    SELECT d.agent_key
    FROM gold.dim_agent d
    JOIN src s ON d.agent_name = s.agent_name
    WHERE d.is_current
      AND d.agent_ip IS DISTINCT FROM s.agent_ip
  )
  UPDATE gold.dim_agent d
  SET effective_to = now(), is_current = false
  FROM changed c
  WHERE d.agent_key = c.agent_key;

  WITH src AS (
    SELECT DISTINCT ON (agent_name)
      agent_name,
      agent_ip
    FROM bronze.wazuh_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND agent_name IS NOT NULL
    ORDER BY agent_name, event_ts DESC
  )
  INSERT INTO gold.dim_agent (
    agent_name,
    agent_ip,
    effective_from,
    is_current
  )
  SELECT s.agent_name, s.agent_ip, now(), true
  FROM src s
  LEFT JOIN gold.dim_agent d
    ON d.agent_name = s.agent_name
   AND d.is_current
   AND d.agent_ip IS NOT DISTINCT FROM s.agent_ip
  WHERE d.agent_key IS NULL;
END $$;
$gold_dim_agent_sql$,
  120,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.wazuh_events_raw',
  'gold',
  'dim_agent',
  '[{"name": "agent_key", "type": "bigint"}, {"name": "agent_name", "type": "text"}, {"name": "agent_ip", "type": "inet"}, {"name": "effective_from", "type": "timestamptz"}, {"name": "effective_to", "type": "timestamptz"}, {"name": "is_current", "type": "boolean"}]'::jsonb
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
  'gold_dim_host',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold dim_host from wazuh events',
  'bronze.wazuh_events_raw',
  'event_ts',
  'gold.dim_host',
  'host_key',
  15,
  '["event_ts", "host_name", "host_ip"]'::jsonb,
  $gold_dim_host_sql$

DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  WITH src AS (
    SELECT DISTINCT ON (host_name)
      host_name,
      host_ip
    FROM bronze.wazuh_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND host_name IS NOT NULL
    ORDER BY host_name, event_ts DESC
  ), changed AS (
    SELECT d.host_key
    FROM gold.dim_host d
    JOIN src s ON d.host_name = s.host_name
    WHERE d.is_current
      AND d.host_ip IS DISTINCT FROM s.host_ip
  )
  UPDATE gold.dim_host d
  SET effective_to = now(), is_current = false
  FROM changed c
  WHERE d.host_key = c.host_key;

  WITH src AS (
    SELECT DISTINCT ON (host_name)
      host_name,
      host_ip
    FROM bronze.wazuh_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND host_name IS NOT NULL
    ORDER BY host_name, event_ts DESC
  )
  INSERT INTO gold.dim_host (
    host_name,
    host_ip,
    effective_from,
    is_current
  )
  SELECT s.host_name, s.host_ip, now(), true
  FROM src s
  LEFT JOIN gold.dim_host d
    ON d.host_name = s.host_name
   AND d.is_current
   AND d.host_ip IS NOT DISTINCT FROM s.host_ip
  WHERE d.host_key IS NULL;
END $$;
$gold_dim_host_sql$,
  120,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.wazuh_events_raw',
  'gold',
  'dim_host',
  '[{"name": "host_key", "type": "bigint"}, {"name": "host_name", "type": "text"}, {"name": "host_ip", "type": "inet"}, {"name": "effective_from", "type": "timestamptz"}, {"name": "effective_to", "type": "timestamptz"}, {"name": "is_current", "type": "boolean"}]'::jsonb
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
  'gold_dim_rule',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold dim_rule from wazuh events',
  'bronze.wazuh_events_raw',
  'event_ts',
  'gold.dim_rule',
  'rule_key',
  15,
  '["event_ts", "rule_id", "rule_level", "rule_name", "rule_ruleset"]'::jsonb,
  $gold_dim_rule_sql$

DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  WITH src AS (
    SELECT DISTINCT ON (rule_id)
      rule_id,
      rule_level,
      rule_name,
      rule_ruleset
    FROM bronze.wazuh_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND rule_id IS NOT NULL
    ORDER BY rule_id, event_ts DESC
  ), changed AS (
    SELECT d.rule_key
    FROM gold.dim_rule d
    JOIN src s ON d.rule_id = s.rule_id
    WHERE d.is_current
      AND (
        d.rule_level IS DISTINCT FROM s.rule_level
        OR d.rule_name IS DISTINCT FROM s.rule_name
        OR d.rule_ruleset IS DISTINCT FROM s.rule_ruleset
      )
  )
  UPDATE gold.dim_rule d
  SET effective_to = now(), is_current = false
  FROM changed c
  WHERE d.rule_key = c.rule_key;

  WITH src AS (
    SELECT DISTINCT ON (rule_id)
      rule_id,
      rule_level,
      rule_name,
      rule_ruleset
    FROM bronze.wazuh_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
      AND rule_id IS NOT NULL
    ORDER BY rule_id, event_ts DESC
  )
  INSERT INTO gold.dim_rule (
    rule_id,
    rule_level,
    rule_name,
    rule_ruleset,
    effective_from,
    is_current
  )
  SELECT s.rule_id, s.rule_level, s.rule_name, s.rule_ruleset, now(), true
  FROM src s
  LEFT JOIN gold.dim_rule d
    ON d.rule_id = s.rule_id
   AND d.is_current
   AND d.rule_level IS NOT DISTINCT FROM s.rule_level
   AND d.rule_name IS NOT DISTINCT FROM s.rule_name
   AND d.rule_ruleset IS NOT DISTINCT FROM s.rule_ruleset
  WHERE d.rule_key IS NULL;
END $$;
$gold_dim_rule_sql$,
  120,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.wazuh_events_raw',
  'gold',
  'dim_rule',
  '[{"name": "rule_key", "type": "bigint"}, {"name": "rule_id", "type": "text"}, {"name": "rule_level", "type": "int"}, {"name": "rule_name", "type": "text"}, {"name": "rule_ruleset", "type": "jsonb"}, {"name": "effective_from", "type": "timestamptz"}, {"name": "effective_to", "type": "timestamptz"}, {"name": "is_current", "type": "boolean"}]'::jsonb
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
  'gold_fact_wazuh_events',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold fact_wazuh_events from wazuh events',
  'bronze.wazuh_events_raw',
  'event_ts',
  'gold.fact_wazuh_events',
  'event_id',
  15,
  '["event_id", "event_ts", "event_ingested_ts", "event_start_ts", "event_end_ts", "event_dataset", "event_kind", "event_module", "event_provider", "agent_name", "agent_ip", "host_name", "host_ip", "rule_id", "message"]'::jsonb,
  $gold_fact_wazuh_events_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  PERFORM gold.create_daily_fact_partitions((start_ts::date - 1), (end_ts::date + 2));

  MERGE INTO gold.fact_wazuh_events AS target
  USING (
    SELECT
      w.event_id,
      w.event_ts,
      w.event_ingested_ts,
      w.event_start_ts,
      w.event_end_ts,
      to_char(w.event_ts, 'YYYYMMDD')::int AS date_key,
      to_char(w.event_ts, 'HH24MISS')::int AS time_key,
      a.agent_key,
      h.host_key,
      r.rule_key,
      e.event_key,
      CASE
        WHEN w.event_ingested_ts IS NULL THEN NULL
        ELSE EXTRACT(EPOCH FROM (w.event_ingested_ts - w.event_ts))
      END AS lag_seconds,
      CASE
        WHEN w.event_start_ts IS NULL OR w.event_end_ts IS NULL THEN NULL
        ELSE EXTRACT(EPOCH FROM (w.event_end_ts - w.event_start_ts))
      END AS duration_seconds,
      w.message
    FROM bronze.wazuh_events_raw w
    LEFT JOIN gold.dim_agent a
      ON a.agent_name = w.agent_name
     AND a.is_current
     AND a.agent_ip IS NOT DISTINCT FROM w.agent_ip
    LEFT JOIN gold.dim_host h
      ON h.host_name = w.host_name
     AND h.is_current
     AND h.host_ip IS NOT DISTINCT FROM w.host_ip
    LEFT JOIN gold.dim_rule r
      ON r.rule_id = w.rule_id
     AND r.is_current
    LEFT JOIN gold.dim_event e
      ON e.event_dataset IS NOT DISTINCT FROM w.event_dataset
     AND e.event_kind IS NOT DISTINCT FROM w.event_kind
     AND e.event_module IS NOT DISTINCT FROM w.event_module
     AND e.event_provider IS NOT DISTINCT FROM w.event_provider
    WHERE w.event_ts >= start_ts AND w.event_ts < end_ts
      AND w.event_id IS NOT NULL
  ) AS source
  ON (target.event_id = source.event_id AND target.event_ts = source.event_ts)
  WHEN MATCHED THEN
    UPDATE SET
      event_ingested_ts = source.event_ingested_ts,
      event_start_ts = source.event_start_ts,
      event_end_ts = source.event_end_ts,
      date_key = source.date_key,
      time_key = source.time_key,
      agent_key = source.agent_key,
      host_key = source.host_key,
      rule_key = source.rule_key,
      event_key = source.event_key,
      lag_seconds = source.lag_seconds,
      duration_seconds = source.duration_seconds,
      message = source.message
  WHEN NOT MATCHED THEN
    INSERT (
      event_id,
      event_ts,
      event_ingested_ts,
      event_start_ts,
      event_end_ts,
      date_key,
      time_key,
      agent_key,
      host_key,
      rule_key,
      event_key,
      lag_seconds,
      duration_seconds,
      message
    )
    VALUES (
      source.event_id,
      source.event_ts,
      source.event_ingested_ts,
      source.event_start_ts,
      source.event_end_ts,
      source.date_key,
      source.time_key,
      source.agent_key,
      source.host_key,
      source.rule_key,
      source.event_key,
      source.lag_seconds,
      source.duration_seconds,
      source.message
    );
END $$;
$gold_fact_wazuh_events_sql$,
  120,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.wazuh_events_raw',
  'gold',
  'fact_wazuh_events',
  '[{"name": "event_id", "type": "text"}, {"name": "event_ts", "type": "timestamptz"}, {"name": "event_ingested_ts", "type": "timestamptz"}, {"name": "event_start_ts", "type": "timestamptz"}, {"name": "event_end_ts", "type": "timestamptz"}, {"name": "date_key", "type": "int"}, {"name": "time_key", "type": "int"}, {"name": "agent_key", "type": "bigint"}, {"name": "host_key", "type": "bigint"}, {"name": "rule_key", "type": "bigint"}, {"name": "event_key", "type": "bigint"}, {"name": "lag_seconds", "type": "double precision"}, {"name": "duration_seconds", "type": "double precision"}, {"name": "message", "type": "text"}]'::jsonb
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
  'gold_fact_suricata_events',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold fact_suricata_events from suricata events',
  'bronze.suricata_events_raw',
  'event_ts',
  'gold.fact_suricata_events',
  'event_id',
  15,
  '["event_id", "event_ts", "sensor_type", "sensor_name", "signature_id", "protocol", "event_type", "severity", "src_ip", "dest_ip", "src_port", "dest_port", "bytes", "packets", "flow_id", "http_url", "message"]'::jsonb,
  $gold_fact_suricata_events_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  PERFORM gold.create_daily_fact_partitions((start_ts::date - 1), (end_ts::date + 2));

  MERGE INTO gold.fact_suricata_events AS target
  USING (
    SELECT
      s.event_id,
      s.event_ts,
      to_char(s.event_ts, 'YYYYMMDD')::int AS date_key,
      to_char(s.event_ts, 'HH24MISS')::int AS time_key,
      se.sensor_key,
      sig.signature_key,
      p.protocol_key,
      s.event_type,
      s.severity,
      s.src_ip,
      s.dest_ip,
      s.src_port,
      s.dest_port,
      s.bytes,
      s.packets,
      s.flow_id,
      s.http_url,
      s.message
    FROM bronze.suricata_events_raw s
    LEFT JOIN gold.dim_sensor se
      ON se.sensor_type IS NOT DISTINCT FROM s.sensor_type
     AND se.sensor_name IS NOT DISTINCT FROM s.sensor_name
    LEFT JOIN gold.dim_signature sig
      ON sig.signature_id = s.signature_id
    LEFT JOIN gold.dim_protocol p
      ON p.protocol IS NOT DISTINCT FROM s.protocol
    WHERE s.event_ts >= start_ts AND s.event_ts < end_ts
      AND s.event_id IS NOT NULL
  ) AS source
  ON (target.event_id = source.event_id AND target.event_ts = source.event_ts)
  WHEN MATCHED THEN
    UPDATE SET
      date_key = source.date_key,
      time_key = source.time_key,
      sensor_key = source.sensor_key,
      signature_key = source.signature_key,
      protocol_key = source.protocol_key,
      event_type = source.event_type,
      severity = source.severity,
      src_ip = source.src_ip,
      dest_ip = source.dest_ip,
      src_port = source.src_port,
      dest_port = source.dest_port,
      bytes = source.bytes,
      packets = source.packets,
      flow_id = source.flow_id,
      http_url = source.http_url,
      message = source.message
  WHEN NOT MATCHED THEN
    INSERT (
      event_id,
      event_ts,
      date_key,
      time_key,
      sensor_key,
      signature_key,
      protocol_key,
      event_type,
      severity,
      src_ip,
      dest_ip,
      src_port,
      dest_port,
      bytes,
      packets,
      flow_id,
      http_url,
      message
    )
    VALUES (
      source.event_id,
      source.event_ts,
      source.date_key,
      source.time_key,
      source.sensor_key,
      source.signature_key,
      source.protocol_key,
      source.event_type,
      source.severity,
      source.src_ip,
      source.dest_ip,
      source.src_port,
      source.dest_port,
      source.bytes,
      source.packets,
      source.flow_id,
      source.http_url,
      source.message
    );
END $$;
$gold_fact_suricata_events_sql$,
  720,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.suricata_events_raw',
  'gold',
  'fact_suricata_events',
  '[{"name": "event_id", "type": "text"}, {"name": "event_ts", "type": "timestamptz"}, {"name": "date_key", "type": "int"}, {"name": "time_key", "type": "int"}, {"name": "sensor_key", "type": "bigint"}, {"name": "signature_key", "type": "bigint"}, {"name": "protocol_key", "type": "bigint"}, {"name": "event_type", "type": "text"}, {"name": "severity", "type": "text"}, {"name": "src_ip", "type": "inet"}, {"name": "dest_ip", "type": "inet"}, {"name": "src_port", "type": "int"}, {"name": "dest_port", "type": "int"}, {"name": "bytes", "type": "bigint"}, {"name": "packets", "type": "bigint"}, {"name": "flow_id", "type": "text"}, {"name": "http_url", "type": "text"}, {"name": "message", "type": "text"}]'::jsonb
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
  'gold_fact_zeek_events',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold fact_zeek_events from zeek events',
  'bronze.zeek_events_raw',
  'event_ts',
  'gold.fact_zeek_events',
  'event_id',
  15,
  '["event_id", "event_ts", "event_ingested_ts", "event_start_ts", "event_end_ts", "event_dataset", "event_kind", "event_module", "event_provider", "sensor_name", "protocol", "zeek_uid", "src_ip", "dest_ip", "src_port", "dest_port", "application", "network_type", "direction", "community_id", "bytes", "packets", "orig_bytes", "resp_bytes", "orig_pkts", "resp_pkts", "conn_state", "conn_state_description", "duration", "history", "vlan_id", "message"]'::jsonb,
  $gold_fact_zeek_events_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  PERFORM gold.create_daily_fact_partitions((start_ts::date - 1), (end_ts::date + 2));

  MERGE INTO gold.fact_zeek_events AS target
  USING (
    SELECT
      z.event_id,
      z.event_ts,
      z.event_ingested_ts,
      z.event_start_ts,
      z.event_end_ts,
      to_char(z.event_ts, 'YYYYMMDD')::int AS date_key,
      to_char(z.event_ts, 'HH24MISS')::int AS time_key,
      se.sensor_key,
      p.protocol_key,
      e.event_key,
      z.zeek_uid,
      z.src_ip,
      z.dest_ip,
      z.src_port,
      z.dest_port,
      z.application,
      z.network_type,
      z.direction,
      z.community_id,
      z.bytes,
      z.packets,
      z.orig_bytes,
      z.resp_bytes,
      z.orig_pkts,
      z.resp_pkts,
      z.conn_state,
      z.conn_state_description,
      COALESCE(z.duration, CASE
        WHEN z.event_start_ts IS NULL OR z.event_end_ts IS NULL THEN NULL
        ELSE EXTRACT(EPOCH FROM (z.event_end_ts - z.event_start_ts))
      END) AS duration_seconds,
      z.history,
      z.vlan_id,
      z.message
    FROM bronze.zeek_events_raw z
    LEFT JOIN gold.dim_sensor se
      ON se.sensor_type = 'zeek'
     AND se.sensor_name IS NOT DISTINCT FROM z.sensor_name
    LEFT JOIN gold.dim_protocol p
      ON p.protocol IS NOT DISTINCT FROM z.protocol
    LEFT JOIN gold.dim_event e
      ON e.event_dataset IS NOT DISTINCT FROM z.event_dataset
     AND e.event_kind IS NOT DISTINCT FROM z.event_kind
     AND e.event_module IS NOT DISTINCT FROM z.event_module
     AND e.event_provider IS NOT DISTINCT FROM z.event_provider
    WHERE z.event_ts >= start_ts AND z.event_ts < end_ts
      AND z.event_id IS NOT NULL
  ) AS source
  ON (target.event_id = source.event_id AND target.event_ts = source.event_ts)
  WHEN MATCHED THEN
    UPDATE SET
      event_ingested_ts = source.event_ingested_ts,
      event_start_ts = source.event_start_ts,
      event_end_ts = source.event_end_ts,
      date_key = source.date_key,
      time_key = source.time_key,
      sensor_key = source.sensor_key,
      protocol_key = source.protocol_key,
      event_key = source.event_key,
      zeek_uid = source.zeek_uid,
      src_ip = source.src_ip,
      dest_ip = source.dest_ip,
      src_port = source.src_port,
      dest_port = source.dest_port,
      application = source.application,
      network_type = source.network_type,
      direction = source.direction,
      community_id = source.community_id,
      bytes = source.bytes,
      packets = source.packets,
      orig_bytes = source.orig_bytes,
      resp_bytes = source.resp_bytes,
      orig_pkts = source.orig_pkts,
      resp_pkts = source.resp_pkts,
      conn_state = source.conn_state,
      conn_state_description = source.conn_state_description,
      duration_seconds = source.duration_seconds,
      history = source.history,
      vlan_id = source.vlan_id,
      message = source.message
  WHEN NOT MATCHED THEN
    INSERT (
      event_id,
      event_ts,
      event_ingested_ts,
      event_start_ts,
      event_end_ts,
      date_key,
      time_key,
      sensor_key,
      protocol_key,
      event_key,
      zeek_uid,
      src_ip,
      dest_ip,
      src_port,
      dest_port,
      application,
      network_type,
      direction,
      community_id,
      bytes,
      packets,
      orig_bytes,
      resp_bytes,
      orig_pkts,
      resp_pkts,
      conn_state,
      conn_state_description,
      duration_seconds,
      history,
      vlan_id,
      message
    )
    VALUES (
      source.event_id,
      source.event_ts,
      source.event_ingested_ts,
      source.event_start_ts,
      source.event_end_ts,
      source.date_key,
      source.time_key,
      source.sensor_key,
      source.protocol_key,
      source.event_key,
      source.zeek_uid,
      source.src_ip,
      source.dest_ip,
      source.src_port,
      source.dest_port,
      source.application,
      source.network_type,
      source.direction,
      source.community_id,
      source.bytes,
      source.packets,
      source.orig_bytes,
      source.resp_bytes,
      source.orig_pkts,
      source.resp_pkts,
      source.conn_state,
      source.conn_state_description,
      source.duration_seconds,
      source.history,
      source.vlan_id,
      source.message
    );
END $$;
$gold_fact_zeek_events_sql$,
  720,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.zeek_events_raw',
  'gold',
  'fact_zeek_events',
  '[{"name": "event_id", "type": "text"}, {"name": "event_ts", "type": "timestamptz"}, {"name": "event_ingested_ts", "type": "timestamptz"}, {"name": "event_start_ts", "type": "timestamptz"}, {"name": "event_end_ts", "type": "timestamptz"}, {"name": "date_key", "type": "int"}, {"name": "time_key", "type": "int"}, {"name": "sensor_key", "type": "bigint"}, {"name": "protocol_key", "type": "bigint"}, {"name": "event_key", "type": "bigint"}, {"name": "zeek_uid", "type": "text"}, {"name": "src_ip", "type": "inet"}, {"name": "dest_ip", "type": "inet"}, {"name": "src_port", "type": "int"}, {"name": "dest_port", "type": "int"}, {"name": "application", "type": "text"}, {"name": "network_type", "type": "text"}, {"name": "direction", "type": "text"}, {"name": "community_id", "type": "text"}, {"name": "bytes", "type": "bigint"}, {"name": "packets", "type": "bigint"}, {"name": "orig_bytes", "type": "bigint"}, {"name": "resp_bytes", "type": "bigint"}, {"name": "orig_pkts", "type": "bigint"}, {"name": "resp_pkts", "type": "bigint"}, {"name": "conn_state", "type": "text"}, {"name": "conn_state_description", "type": "text"}, {"name": "duration_seconds", "type": "double precision"}, {"name": "history", "type": "text"}, {"name": "vlan_id", "type": "text"}, {"name": "message", "type": "text"}]'::jsonb
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
  'gold_bridge_wazuh_event_tag',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold bridge_wazuh_event_tag from wazuh events',
  'bronze.wazuh_events_raw',
  'event_ts',
  'gold.bridge_wazuh_event_tag',
  'event_id',
  15,
  '["event_id", "event_ts", "tags"]'::jsonb,
  $gold_bridge_wazuh_event_tag_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  INSERT INTO gold.bridge_wazuh_event_tag (
    event_id,
    event_ts,
    tag_key
  )
  SELECT DISTINCT
    w.event_id,
    w.event_ts,
    t.tag_key
  FROM bronze.wazuh_events_raw w
  JOIN LATERAL (
    SELECT jsonb_array_elements_text(w.tags) AS tag_value
    WHERE w.tags IS NOT NULL
      AND jsonb_typeof(w.tags) = 'array'
  ) tag_row ON true
  JOIN gold.dim_tag t ON t.tag_value = btrim(tag_row.tag_value)
  JOIN gold.fact_wazuh_events f
    ON f.event_id = w.event_id
   AND f.event_ts = w.event_ts
  WHERE w.event_ts >= start_ts AND w.event_ts < end_ts
  ON CONFLICT DO NOTHING;
END $$;
$gold_bridge_wazuh_event_tag_sql$,
  120,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.wazuh_events_raw',
  'gold',
  'bridge_wazuh_event_tag',
  '[{"name": "event_id", "type": "text"}, {"name": "event_ts", "type": "timestamptz"}, {"name": "tag_key", "type": "bigint"}]'::jsonb
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
  'gold_bridge_suricata_event_tag',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold bridge_suricata_event_tag from suricata events',
  'bronze.suricata_events_raw',
  'event_ts',
  'gold.bridge_suricata_event_tag',
  'event_id',
  15,
  '["event_id", "event_ts", "tags"]'::jsonb,
  $gold_bridge_suricata_event_tag_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  INSERT INTO gold.bridge_suricata_event_tag (
    event_id,
    event_ts,
    tag_key
  )
  SELECT DISTINCT
    s.event_id,
    s.event_ts,
    t.tag_key
  FROM bronze.suricata_events_raw s
  JOIN LATERAL (
    SELECT jsonb_array_elements_text(s.tags) AS tag_value
    WHERE s.tags IS NOT NULL
      AND jsonb_typeof(s.tags) = 'array'
  ) tag_row ON true
  JOIN gold.dim_tag t ON t.tag_value = btrim(tag_row.tag_value)
  JOIN gold.fact_suricata_events f
    ON f.event_id = s.event_id
   AND f.event_ts = s.event_ts
  WHERE s.event_ts >= start_ts AND s.event_ts < end_ts
  ON CONFLICT DO NOTHING;
END $$;
$gold_bridge_suricata_event_tag_sql$,
  720,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.suricata_events_raw',
  'gold',
  'bridge_suricata_event_tag',
  '[{"name": "event_id", "type": "text"}, {"name": "event_ts", "type": "timestamptz"}, {"name": "tag_key", "type": "bigint"}]'::jsonb
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
  'gold_bridge_zeek_event_tag',
  (SELECT id FROM control.dag_configs WHERE dag_name = 'gold_star_schema'),
  true,
  'Gold bridge_zeek_event_tag from zeek events',
  'bronze.zeek_events_raw',
  'event_ts',
  'gold.bridge_zeek_event_tag',
  'event_id',
  15,
  '["event_id", "event_ts", "tags"]'::jsonb,
  $gold_bridge_zeek_event_tag_sql$


DO $$
DECLARE
  window_interval interval := interval '{{WINDOW_MINUTES}} minutes';
  start_ts timestamptz := COALESCE(NULLIF('{{START_TS}}', '')::timestamptz, now() - window_interval);
  end_ts timestamptz := COALESCE(NULLIF('{{END_TS}}', '')::timestamptz, now());
BEGIN
  IF end_ts <= start_ts THEN
    RAISE EXCEPTION 'end_ts must be greater than start_ts';
  END IF;

  INSERT INTO gold.bridge_zeek_event_tag (
    event_id,
    event_ts,
    tag_key
  )
  SELECT DISTINCT
    z.event_id,
    z.event_ts,
    t.tag_key
  FROM bronze.zeek_events_raw z
  JOIN LATERAL (
    SELECT jsonb_array_elements_text(z.tags) AS tag_value
    WHERE z.tags IS NOT NULL
      AND jsonb_typeof(z.tags) = 'array'
  ) tag_row ON true
  JOIN gold.dim_tag t ON t.tag_value = btrim(tag_row.tag_value)
  JOIN gold.fact_zeek_events f
    ON f.event_id = z.event_id
   AND f.event_ts = z.event_ts
  WHERE z.event_ts >= start_ts AND z.event_ts < end_ts
  ON CONFLICT DO NOTHING;
END $$;
$gold_bridge_zeek_event_tag_sql$,
  720,
  15,
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  (SELECT id FROM control.database_connections WHERE db_conn_name = 'analytics_db'),
  'bronze.zeek_events_raw',
  'gold',
  'bridge_zeek_event_tag',
  '[{"name": "event_id", "type": "text"}, {"name": "event_ts", "type": "timestamptz"}, {"name": "tag_key", "type": "bigint"}]'::jsonb
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


