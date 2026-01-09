# Fact and Dimension Architecture for Wazuh + Suricata

Scope
- Source data: bronze.wazuh_events_raw and bronze.suricata_events_raw
- Target: gold star schema for analytics, BI, and monitoring
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

Bridge tables (many-to-many)
- gold.bridge_wazuh_event_tag
  - event_id (or fact surrogate key), tag_key
- gold.bridge_suricata_event_tag
  - event_id (or fact surrogate key), tag_key

Load strategy
1) Load dim_date and dim_time (calendar)
2) Upsert dimensions by natural key (SCD2 for host/agent/rule if needed)
3) Load facts and compute lag_seconds and duration_seconds
4) Load tag bridges

Partitioning and indexes
- Partition facts by event date (derived from event_ts)
- Index on event_ts, rule_key, agent_key, host_key, signature_key
- Bridge indexes on (event_id) and (tag_key)

Optional enrichments
- dim_ip + GeoIP attributes
- dim_asset or dim_user if you can map IP/host/agent to assets
- threat_intel dimension for IP or signature tags

Notes
- Keep bronze raw tables unchanged for audit and replay.
- If you already maintain a wide gold table (gold.wazuh_events_dwh), you can feed dims/facts from it.
