CREATE TABLE IF NOT EXISTS bronze.suricata_events_raw (
  event_id text PRIMARY KEY,
  event_ts timestamptz,
  sensor_type text,
  sensor_name text,
  event_type text,
  severity text,
  src_ip inet,
  dest_ip inet,
  src_port int,
  dest_port int,
  protocol text,
  bytes bigint,
  packets bigint,
  flow_id text,
  signature text,
  signature_id int,
  category text,
  alert_action text,
  http_url text,
  tags jsonb,
  message text,
  raw_data jsonb
);

CREATE INDEX IF NOT EXISTS idx_suricata_events_raw_event_ts ON bronze.suricata_events_raw(event_ts);
CREATE INDEX IF NOT EXISTS idx_suricata_events_raw_severity ON bronze.suricata_events_raw(severity);
CREATE INDEX IF NOT EXISTS idx_suricata_events_raw_src_ip ON bronze.suricata_events_raw(src_ip);
CREATE INDEX IF NOT EXISTS idx_suricata_events_raw_dest_ip ON bronze.suricata_events_raw(dest_ip);

CREATE TABLE IF NOT EXISTS bronze.wazuh_events_raw (
  event_id text PRIMARY KEY,
  event_ts timestamptz,
  event_ingested_ts timestamptz,
  event_start_ts timestamptz,
  event_end_ts timestamptz,
  event_dataset text,
  event_kind text,
  event_module text,
  event_provider text,
  agent_name text,
  agent_ip inet,
  host_name text,
  host_ip inet,
  rule_id text,
  rule_level int,
  rule_name text,
  rule_ruleset jsonb,
  tags jsonb,
  message text,
  raw_data jsonb
);

CREATE INDEX IF NOT EXISTS idx_wazuh_events_raw_event_ts ON bronze.wazuh_events_raw(event_ts);
CREATE INDEX IF NOT EXISTS idx_wazuh_events_raw_agent_name ON bronze.wazuh_events_raw(agent_name);
CREATE INDEX IF NOT EXISTS idx_wazuh_events_raw_rule_id ON bronze.wazuh_events_raw(rule_id);

CREATE TABLE IF NOT EXISTS bronze.zeek_events_raw (
  event_id text PRIMARY KEY,
  event_ts timestamptz,
  event_ingested_ts timestamptz,
  event_start_ts timestamptz,
  event_end_ts timestamptz,
  event_dataset text,
  event_kind text,
  event_module text,
  event_provider text,
  zeek_uid text,
  sensor_name text,
  src_ip inet,
  dest_ip inet,
  src_port int,
  dest_port int,
  protocol text,
  application text,
  network_type text,
  direction text,
  community_id text,
  bytes bigint,
  packets bigint,
  orig_bytes bigint,
  resp_bytes bigint,
  orig_pkts bigint,
  resp_pkts bigint,
  conn_state text,
  conn_state_description text,
  duration double precision,
  history text,
  vlan_id text,
  tags jsonb,
  message text,
  raw_data jsonb
);

CREATE INDEX IF NOT EXISTS idx_zeek_events_raw_event_ts ON bronze.zeek_events_raw(event_ts);
CREATE INDEX IF NOT EXISTS idx_zeek_events_raw_src_ip ON bronze.zeek_events_raw(src_ip);
CREATE INDEX IF NOT EXISTS idx_zeek_events_raw_dest_ip ON bronze.zeek_events_raw(dest_ip);

-- RisingWave JDBC sink uses INSERT ... RETURNING, which requires SELECT on target columns.
GRANT SELECT ON bronze.suricata_events_raw TO rw_writer;
GRANT SELECT ON bronze.wazuh_events_raw TO rw_writer;
GRANT SELECT ON bronze.zeek_events_raw TO rw_writer;
