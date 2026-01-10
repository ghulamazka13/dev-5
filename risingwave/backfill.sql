-- Backfill pipeline (no downtime). Requires psql vars: start_ts, end_ts, topic, bootstrap_server.

DROP SINK IF EXISTS suricata_backfill_sink;
DROP SINK IF EXISTS wazuh_backfill_sink;
DROP SINK IF EXISTS zeek_backfill_sink;
DROP MATERIALIZED VIEW IF EXISTS suricata_backfill_mv;
DROP MATERIALIZED VIEW IF EXISTS wazuh_backfill_mv;
DROP MATERIALIZED VIEW IF EXISTS zeek_backfill_mv;
DROP SOURCE IF EXISTS security_events_backfill_source;

CREATE SOURCE IF NOT EXISTS security_events_backfill_source (
  event jsonb,
  suricata jsonb,
  zeek jsonb,
  source jsonb,
  destination jsonb,
  network jsonb,
  agent jsonb,
  host jsonb,
  rule jsonb,
  tags jsonb,
  protocol jsonb,
  input jsonb,
  client jsonb,
  server jsonb,
  related jsonb,
  ecs jsonb,
  log jsonb,
  node text,
  "rootId" text,
  "@timestamp" text,
  "@version" text,
  "firstPacket" bigint,
  "lastPacket" bigint,
  "ipProtocol" int,
  "totDataBytes" bigint,
  timestamp bigint,
  length int,
  message text
)
WITH (
  connector = 'kafka',
  topic = :'topic',
  properties.bootstrap.server = :'bootstrap_server',
  scan.startup.mode = 'earliest'
)
FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW IF NOT EXISTS suricata_backfill_mv AS
SELECT
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
FROM (
  SELECT
    event->>'hash' AS event_id,
    COALESCE(
      NULLIF("@timestamp", ''),
      suricata->>'timestamp'
    )::timestamptz AS event_ts,
    COALESCE(event->>'provider', event->>'module') AS sensor_type,
    COALESCE(agent->>'name', host->>'name', node) AS sensor_name,
    COALESCE(event->>'dataset', event->>'kind') AS event_type,
    COALESCE(suricata->'alert'->>'severity', event->>'severity') AS severity,
    source->>'ip' AS src_ip,
    destination->>'ip' AS dest_ip,
    (source->>'port')::int AS src_port,
    (destination->>'port')::int AS dest_port,
    COALESCE(
      network->>'application',
      network->'transport'->>0,
      network->'protocol'->>0,
      protocol->>0
    ) AS protocol,
    COALESCE(
      "totDataBytes",
      (network->>'bytes')::bigint,
      (client->>'bytes')::bigint,
      (server->>'bytes')::bigint
    ) AS bytes,
    COALESCE(
      (network->>'packets')::bigint,
      (client->>'packets')::bigint,
      (server->>'packets')::bigint
    ) AS packets,
    suricata->>'flow_id' AS flow_id,
    COALESCE(rule->>'name', suricata->'alert'->>'signature') AS signature,
    (rule->>'id')::int AS signature_id,
    rule->'category'->>0 AS category,
    suricata->'alert'->>'action' AS alert_action,
    suricata->'http'->>'url' AS http_url,
    COALESCE(tags, event->'severity_tags') AS tags,
    COALESCE(message, event->>'original', rule->>'name') AS message,
    jsonb_build_object(
      'event', event,
      'suricata', suricata,
      'source', source,
      'destination', destination,
      'network', network,
      'agent', agent,
      'host', host,
      'rule', rule,
      'tags', tags,
      'protocol', protocol,
      'input', input,
      'client', client,
      'server', server,
      'related', related,
      'ecs', ecs,
      'log', log,
      'node', node,
      'rootId', "rootId",
      '@timestamp', "@timestamp",
      '@version', "@version",
      'firstPacket', "firstPacket",
      'lastPacket', "lastPacket",
      'ipProtocol', "ipProtocol",
      'totDataBytes', "totDataBytes",
      'timestamp', timestamp,
      'length', length,
      'message', message
    ) AS raw_data
  FROM security_events_backfill_source
  WHERE suricata IS NOT NULL
    AND event->>'hash' IS NOT NULL
) t
WHERE t.event_ts >= :'start_ts'::timestamptz
  AND t.event_ts < :'end_ts'::timestamptz;

CREATE SINK IF NOT EXISTS suricata_backfill_sink
FROM suricata_backfill_mv
WITH (
  connector = 'jdbc',
  jdbc.url = 'jdbc:postgresql://postgres:5432/analytics?user=rw_writer&password=rw_writer',
  schema.name = 'staging',
  table.name = 'suricata_events_backfill',
  type = 'append-only'
);

CREATE MATERIALIZED VIEW IF NOT EXISTS wazuh_backfill_mv AS
SELECT
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
FROM (
  SELECT
    event->>'hash' AS event_id,
    COALESCE(
      NULLIF("@timestamp", '')::timestamptz,
      (event->>'ingested')::timestamptz,
      to_timestamp((event->>'start')::double precision / 1000.0),
      to_timestamp((event->>'end')::double precision / 1000.0)
    ) AS event_ts,
    (event->>'ingested')::timestamptz AS event_ingested_ts,
    to_timestamp((event->>'start')::double precision / 1000.0) AS event_start_ts,
    to_timestamp((event->>'end')::double precision / 1000.0) AS event_end_ts,
    event->>'dataset' AS event_dataset,
    event->>'kind' AS event_kind,
    event->>'module' AS event_module,
    event->>'provider' AS event_provider,
    agent->>'name' AS agent_name,
    agent->>'ip' AS agent_ip,
    host->>'name' AS host_name,
    host->>'ip' AS host_ip,
    rule->>'id' AS rule_id,
    (rule->>'level')::int AS rule_level,
    rule->>'name' AS rule_name,
    rule->'ruleset' AS rule_ruleset,
    tags AS tags,
    COALESCE(message, rule->>'name') AS message,
    jsonb_build_object(
      'event', event,
      'agent', agent,
      'host', host,
      'rule', rule,
      'tags', tags,
      'input', input,
      'related', related,
      'ecs', ecs,
      'log', log,
      'node', node,
      'rootId', "rootId",
      '@timestamp', "@timestamp",
      '@version', "@version",
      'firstPacket', "firstPacket",
      'lastPacket', "lastPacket",
      'ipProtocol', "ipProtocol",
      'totDataBytes', "totDataBytes",
      'timestamp', timestamp,
      'length', length,
      'message', message
    ) AS raw_data
  FROM security_events_backfill_source
  WHERE event->>'provider' = 'wazuh'
    AND event->>'hash' IS NOT NULL
) t
WHERE t.event_ts >= :'start_ts'::timestamptz
  AND t.event_ts < :'end_ts'::timestamptz;

CREATE SINK IF NOT EXISTS wazuh_backfill_sink
FROM wazuh_backfill_mv
WITH (
  connector = 'jdbc',
  jdbc.url = 'jdbc:postgresql://postgres:5432/analytics?user=rw_writer&password=rw_writer',
  schema.name = 'staging',
  table.name = 'wazuh_events_backfill',
  type = 'append-only'
);

CREATE MATERIALIZED VIEW IF NOT EXISTS zeek_backfill_mv AS
SELECT
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
FROM (
  SELECT
    event->>'hash' AS event_id,
    COALESCE(
      NULLIF("@timestamp", '')::timestamptz,
      NULLIF(zeek->>'ts', '')::timestamptz,
      NULLIF(event->>'ingested', '')::timestamptz,
      to_timestamp((event->>'start')::double precision / 1000.0)
    ) AS event_ts,
    (event->>'ingested')::timestamptz AS event_ingested_ts,
    to_timestamp((event->>'start')::double precision / 1000.0) AS event_start_ts,
    to_timestamp((event->>'end')::double precision / 1000.0) AS event_end_ts,
    event->>'dataset' AS event_dataset,
    event->>'kind' AS event_kind,
    event->>'module' AS event_module,
    event->>'provider' AS event_provider,
    COALESCE(zeek->>'uid', event->'id'->>0) AS zeek_uid,
    COALESCE(agent->>'name', host->>'name', node) AS sensor_name,
    source->>'ip' AS src_ip,
    destination->>'ip' AS dest_ip,
    (source->>'port')::int AS src_port,
    (destination->>'port')::int AS dest_port,
    COALESCE(
      network->>'application',
      network->'transport'->>0,
      network->'protocol'->>0,
      protocol->>0
    ) AS protocol,
    network->>'application' AS application,
    network->>'type' AS network_type,
    network->>'direction' AS direction,
    network->>'community_id' AS community_id,
    COALESCE(
      "totDataBytes",
      (network->>'bytes')::bigint,
      (source->>'bytes')::bigint,
      (destination->>'bytes')::bigint
    ) AS bytes,
    COALESCE(
      (network->>'packets')::bigint,
      (source->>'packets')::bigint,
      (destination->>'packets')::bigint
    ) AS packets,
    COALESCE(
      (zeek->'conn'->>'orig_bytes')::bigint,
      (zeek->'conn'->>'orig_ip_bytes')::bigint
    ) AS orig_bytes,
    COALESCE(
      (zeek->'conn'->>'resp_bytes')::bigint,
      (zeek->'conn'->>'resp_ip_bytes')::bigint
    ) AS resp_bytes,
    (zeek->'conn'->>'orig_pkts')::bigint AS orig_pkts,
    (zeek->'conn'->>'resp_pkts')::bigint AS resp_pkts,
    zeek->'conn'->>'conn_state' AS conn_state,
    zeek->'conn'->>'conn_state_description' AS conn_state_description,
    (zeek->'conn'->>'duration')::double precision AS duration,
    zeek->'conn'->>'history' AS history,
    COALESCE(zeek->'conn'->>'vlan', network->'vlan'->'id'->>0) AS vlan_id,
    COALESCE(tags, event->'category', event->'severity_tags') AS tags,
    COALESCE(message, event->>'original', zeek->'conn'->>'conn_state_description') AS message,
    jsonb_build_object(
      'event', event,
      'zeek', zeek,
      'source', source,
      'destination', destination,
      'network', network,
      'agent', agent,
      'host', host,
      'rule', rule,
      'tags', tags,
      'protocol', protocol,
      'input', input,
      'client', client,
      'server', server,
      'related', related,
      'ecs', ecs,
      'log', log,
      'node', node,
      'rootId', "rootId",
      '@timestamp', "@timestamp",
      '@version', "@version",
      'firstPacket', "firstPacket",
      'lastPacket', "lastPacket",
      'ipProtocol', "ipProtocol",
      'totDataBytes', "totDataBytes",
      'timestamp', timestamp,
      'length', length,
      'message', message
    ) AS raw_data
  FROM security_events_backfill_source
  WHERE zeek IS NOT NULL
    AND event->>'hash' IS NOT NULL
) t
WHERE t.event_ts >= :'start_ts'::timestamptz
  AND t.event_ts < :'end_ts'::timestamptz;

CREATE SINK IF NOT EXISTS zeek_backfill_sink
FROM zeek_backfill_mv
WITH (
  connector = 'jdbc',
  jdbc.url = 'jdbc:postgresql://postgres:5432/analytics?user=rw_writer&password=rw_writer',
  schema.name = 'staging',
  table.name = 'zeek_events_backfill',
  type = 'append-only'
);
