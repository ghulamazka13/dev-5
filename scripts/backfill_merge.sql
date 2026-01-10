\set ON_ERROR_STOP on

BEGIN;

INSERT INTO bronze.suricata_events_raw (
  event_id,
  event_ts,
  sensor_type,
  sensor_name,
  event_type,
  severity,
  src_ip,
  dest_ip,
  src_port,
  dest_port,
  protocol,
  bytes,
  packets,
  flow_id,
  signature,
  signature_id,
  category,
  alert_action,
  http_url,
  tags,
  message,
  raw_data
)
SELECT DISTINCT ON (event_id)
  event_id,
  event_ts,
  sensor_type,
  sensor_name,
  event_type,
  severity,
  src_ip,
  dest_ip,
  src_port,
  dest_port,
  protocol,
  bytes,
  packets,
  flow_id,
  signature,
  signature_id,
  category,
  alert_action,
  http_url,
  tags,
  message,
  raw_data
FROM staging.suricata_events_backfill
WHERE event_ts >= :'start_ts'::timestamptz
  AND event_ts < :'end_ts'::timestamptz
ORDER BY event_id, event_ts DESC
ON CONFLICT (event_id) DO NOTHING;

INSERT INTO bronze.wazuh_events_raw (
  event_id,
  event_ts,
  event_ingested_ts,
  event_start_ts,
  event_end_ts,
  event_dataset,
  event_kind,
  event_module,
  event_provider,
  agent_name,
  agent_ip,
  host_name,
  host_ip,
  rule_id,
  rule_level,
  rule_name,
  rule_ruleset,
  tags,
  message,
  raw_data
)
SELECT DISTINCT ON (event_id)
  event_id,
  event_ts,
  event_ingested_ts,
  event_start_ts,
  event_end_ts,
  event_dataset,
  event_kind,
  event_module,
  event_provider,
  agent_name,
  agent_ip,
  host_name,
  host_ip,
  rule_id,
  rule_level,
  rule_name,
  rule_ruleset,
  tags,
  message,
  raw_data
FROM staging.wazuh_events_backfill
WHERE event_ts >= :'start_ts'::timestamptz
  AND event_ts < :'end_ts'::timestamptz
ORDER BY event_id, event_ts DESC
ON CONFLICT (event_id) DO NOTHING;

INSERT INTO bronze.zeek_events_raw (
  event_id,
  event_ts,
  event_ingested_ts,
  event_start_ts,
  event_end_ts,
  event_dataset,
  event_kind,
  event_module,
  event_provider,
  zeek_uid,
  sensor_name,
  src_ip,
  dest_ip,
  src_port,
  dest_port,
  protocol,
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
  duration,
  history,
  vlan_id,
  tags,
  message,
  raw_data
)
SELECT DISTINCT ON (event_id)
  event_id,
  event_ts,
  event_ingested_ts,
  event_start_ts,
  event_end_ts,
  event_dataset,
  event_kind,
  event_module,
  event_provider,
  zeek_uid,
  sensor_name,
  src_ip,
  dest_ip,
  src_port,
  dest_port,
  protocol,
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
  duration,
  history,
  vlan_id,
  tags,
  message,
  raw_data
FROM staging.zeek_events_backfill
WHERE event_ts >= :'start_ts'::timestamptz
  AND event_ts < :'end_ts'::timestamptz
ORDER BY event_id, event_ts DESC
ON CONFLICT (event_id) DO NOTHING;

COMMIT;
