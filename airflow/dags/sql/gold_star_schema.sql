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

  INSERT INTO gold.dim_time (
    time_key,
    hour,
    minute,
    second
  )
  SELECT DISTINCT
    to_char(t, 'HH24MISS')::int,
    EXTRACT(hour FROM t)::int,
    EXTRACT(minute FROM t)::int,
    EXTRACT(second FROM t)::int
  FROM (
    SELECT event_ts::time AS t
    FROM bronze.wazuh_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
    UNION
    SELECT event_ts::time AS t
    FROM bronze.suricata_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
    UNION
    SELECT event_ts::time AS t
    FROM bronze.zeek_events_raw
    WHERE event_ts >= start_ts AND event_ts < end_ts
  ) times
  WHERE t IS NOT NULL
  ON CONFLICT (time_key) DO NOTHING;

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
  ON CONFLICT (signature_id) DO UPDATE SET
    signature = EXCLUDED.signature,
    category = EXCLUDED.category,
    alert_action = EXCLUDED.alert_action;

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
  ) tag_value ON true
  JOIN gold.dim_tag t ON t.tag_value = btrim(tag_value)
  JOIN gold.fact_wazuh_events f
    ON f.event_id = w.event_id
   AND f.event_ts = w.event_ts
  WHERE w.event_ts >= start_ts AND w.event_ts < end_ts
  ON CONFLICT DO NOTHING;

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
  ) tag_value ON true
  JOIN gold.dim_tag t ON t.tag_value = btrim(tag_value)
  JOIN gold.fact_suricata_events f
    ON f.event_id = s.event_id
   AND f.event_ts = s.event_ts
  WHERE s.event_ts >= start_ts AND s.event_ts < end_ts
  ON CONFLICT DO NOTHING;

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
  ) tag_value ON true
  JOIN gold.dim_tag t ON t.tag_value = btrim(tag_value)
  JOIN gold.fact_zeek_events f
    ON f.event_id = z.event_id
   AND f.event_ts = z.event_ts
  WHERE z.event_ts >= start_ts AND z.event_ts < end_ts
  ON CONFLICT DO NOTHING;
END $$;

ANALYZE gold.dim_date;
ANALYZE gold.dim_time;
ANALYZE gold.dim_host;
ANALYZE gold.dim_tag;
ANALYZE gold.dim_agent;
ANALYZE gold.dim_rule;
ANALYZE gold.dim_event;
ANALYZE gold.dim_sensor;
ANALYZE gold.dim_signature;
ANALYZE gold.dim_protocol;
ANALYZE gold.fact_wazuh_events;
ANALYZE gold.fact_suricata_events;
ANALYZE gold.fact_zeek_events;
ANALYZE gold.bridge_wazuh_event_tag;
ANALYZE gold.bridge_suricata_event_tag;
ANALYZE gold.bridge_zeek_event_tag;
