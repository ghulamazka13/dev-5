# Fact and Dimension Architecture for Wazuh + Suricata + Zeek

Scope
- Source data: bronze.wazuh_events_raw, bronze.suricata_events_raw, bronze.zeek_events_raw
- Target: gold star schema for analytics, BI, and monitoring
- Fact and dimension tables live in the gold schema; bronze tables remain raw
- ETL: Airflow DAGs load bronze -> gold dimensions, facts, and bridges
- Volume: ~300k events/15 min; partition fact tables by event_ts (daily) for pruning and retention
- Grain: one row per event_id in each fact table

Design principles
- Conformed dimensions shared across sources
- Mostly additive facts; keep raw_data in bronze for audit
- Use SCD2 only where attributes can change and history matters

Conformed dimensions (shared)
- gold.dim_date
  - date_key (YYYYMMDD), date, year, quarter, month, day, week_of_year, day_of_week
- gold.dim_time
  - time_key (HHMMSS), hour, minute, second
- gold.dim_host (SCD2)
  - host_key, host_name, host_ip, effective_from, effective_to, is_current
- gold.dim_tag
  - tag_key, tag_value

Wazuh star schema
- Fact: gold.fact_wazuh_events
  - Grain: one Wazuh event_id
  - Keys: date_key, time_key, agent_key, host_key, rule_key, event_key
  - Measures: lag_seconds, duration_seconds
  - Degenerate attributes: event_id, message

- Dimensions
  - gold.dim_agent (SCD2)
    - agent_key, agent_name, agent_ip, effective_from, effective_to, is_current
  - gold.dim_rule (SCD2 if rule_name/level changes, else SCD1)
    - rule_key, rule_id, rule_level, rule_name, rule_ruleset
  - gold.dim_event (SCD1)
    - event_key, event_dataset, event_kind, event_module, event_provider

Wazuh mapping (bronze -> gold)
- event_id -> fact.event_id
- event_ts -> fact.event_ts + dim_date + dim_time
- event_ingested_ts -> fact.event_ingested_ts
- event_start_ts/event_end_ts -> fact.event_start_ts/fact.event_end_ts, duration_seconds
- event_dataset/event_kind/event_module/event_provider -> dim_event
- agent_name/agent_ip -> dim_agent
- host_name/host_ip -> dim_host
- rule_id/rule_level/rule_name/rule_ruleset -> dim_rule
- tags -> dim_tag + bridge_wazuh_event_tag
- message -> fact.message

Suricata star schema
- Fact: gold.fact_suricata_events
  - Grain: one Suricata event_id
  - Keys: date_key, time_key, sensor_key, signature_key, protocol_key
  - Measures: bytes, packets
  - Degenerate attributes: event_id, flow_id, src_ip, dest_ip, src_port, dest_port, http_url, message

- Dimensions
  - gold.dim_sensor (SCD1)
    - sensor_key, sensor_type, sensor_name
  - gold.dim_signature (SCD1)
    - signature_key, signature_id, signature, category, alert_action
  - gold.dim_protocol (SCD1)
    - protocol_key, protocol

Suricata mapping (bronze -> gold)
- event_id -> fact.event_id
- event_ts -> fact.event_ts + dim_date + dim_time
- sensor_type/sensor_name -> dim_sensor
- signature_id/signature/category/alert_action -> dim_signature
- protocol -> dim_protocol
- severity/event_type -> keep in fact as attributes or create small dims if needed
- bytes/packets -> fact measures
- src_ip/dest_ip/src_port/dest_port -> keep in fact; optional dim_ip or dim_endpoint for enrichment
- tags -> dim_tag + bridge_suricata_event_tag
- message/http_url -> fact attributes

Zeek star schema
- Fact: gold.fact_zeek_events
  - Grain: one Zeek event_id
  - Keys: date_key, time_key, sensor_key, protocol_key, event_key
  - Measures: bytes, packets, orig_bytes, resp_bytes, orig_pkts, resp_pkts, duration_seconds
  - Degenerate attributes: event_id, zeek_uid, community_id, src_ip, dest_ip, src_port, dest_port, message

- Dimensions
  - gold.dim_sensor (SCD1)
    - sensor_key, sensor_name
  - gold.dim_protocol (SCD1)
    - protocol_key, protocol
  - gold.dim_event (SCD1)
    - event_key, event_dataset, event_kind, event_module, event_provider

Zeek mapping (bronze -> gold)
- event_id -> fact.event_id
- event_ts -> fact.event_ts + dim_date + dim_time
- event_ingested_ts -> fact.event_ingested_ts
- event_start_ts/event_end_ts -> fact.event_start_ts/fact.event_end_ts, duration_seconds
- event_dataset/event_kind/event_module/event_provider -> dim_event
- sensor_name -> dim_sensor (sensor_type = 'zeek' for uniqueness)
- protocol -> dim_protocol
- zeek_uid -> fact.zeek_uid
- bytes/packets/orig_bytes/resp_bytes/orig_pkts/resp_pkts -> fact measures
- src_ip/dest_ip/src_port/dest_port -> keep in fact; optional dim_ip or dim_endpoint for enrichment
- application/network_type/direction/community_id/conn_state/conn_state_description/history/vlan_id -> keep in fact as attributes or create small dims if needed
- tags -> dim_tag + bridge_zeek_event_tag
- message -> fact.message

Bridge tables (many-to-many)
- gold.bridge_wazuh_event_tag
  - event_id, event_ts, tag_key
- gold.bridge_suricata_event_tag
  - event_id, event_ts, tag_key
- gold.bridge_zeek_event_tag
  - event_id, event_ts, tag_key

Table connections (gold)
- Wazuh fact -> dims
  - gold.fact_wazuh_events.date_key -> gold.dim_date.date_key
  - gold.fact_wazuh_events.time_key -> gold.dim_time.time_key
  - gold.fact_wazuh_events.agent_key -> gold.dim_agent.agent_key
  - gold.fact_wazuh_events.host_key -> gold.dim_host.host_key
  - gold.fact_wazuh_events.rule_key -> gold.dim_rule.rule_key
  - gold.fact_wazuh_events.event_key -> gold.dim_event.event_key
- Suricata fact -> dims
  - gold.fact_suricata_events.date_key -> gold.dim_date.date_key
  - gold.fact_suricata_events.time_key -> gold.dim_time.time_key
  - gold.fact_suricata_events.sensor_key -> gold.dim_sensor.sensor_key
  - gold.fact_suricata_events.signature_key -> gold.dim_signature.signature_key
  - gold.fact_suricata_events.protocol_key -> gold.dim_protocol.protocol_key
- Zeek fact -> dims
  - gold.fact_zeek_events.date_key -> gold.dim_date.date_key
  - gold.fact_zeek_events.time_key -> gold.dim_time.time_key
  - gold.fact_zeek_events.sensor_key -> gold.dim_sensor.sensor_key
  - gold.fact_zeek_events.protocol_key -> gold.dim_protocol.protocol_key
  - gold.fact_zeek_events.event_key -> gold.dim_event.event_key
- Tag bridges
  - gold.bridge_wazuh_event_tag.event_id, event_ts -> gold.fact_wazuh_events.event_id, event_ts
  - gold.bridge_suricata_event_tag.event_id, event_ts -> gold.fact_suricata_events.event_id, event_ts
  - gold.bridge_zeek_event_tag.event_id, event_ts -> gold.fact_zeek_events.event_id, event_ts
  - bridge tag_key -> gold.dim_tag.tag_key

Key and partitioning notes
- Fact tables are range-partitioned by event_ts (daily) at current volumes
- Chosen approach: composite key (event_id, event_ts) on facts; bridge tables carry event_ts
- Keep ETL time-windowed to target only the active partitions

Load strategy
- Airflow orchestrates the bronze -> gold load for dimensions, facts, and bridges
1) Load dim_date and dim_time (calendar)
2) Upsert dimensions by natural key (SCD2 for host/agent/rule if needed)
3) Load facts and compute lag_seconds and duration_seconds
4) Load tag bridges

Partitioning and indexes
- Partition facts by event_ts (daily) to enable pruning and retention
- Create per-partition indexes on event_ts and FK keys (rule_key, agent_key, host_key, signature_key, protocol_key, sensor_key)
- Index (event_id, event_ts) on facts and bridge tables; also index tag_key on bridges
- Pre-create future partitions and drop old partitions for retention

Optional enrichments
- dim_ip + GeoIP attributes
- dim_asset or dim_user if you can map IP/host/agent to assets
- threat_intel dimension for IP or signature tags

Notes
- Keep bronze raw tables unchanged for audit and replay.
- Airflow DAGs handle bronze -> gold loads for facts, dims, and tag bridges.
- If you already maintain wide gold tables (gold.wazuh_events_dwh, gold.suricata_events_dwh, gold.zeek_events_dwh), you can feed dims/facts from them.
