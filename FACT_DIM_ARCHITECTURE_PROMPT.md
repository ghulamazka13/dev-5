# Diagram Prompt: Fact and Dimension Architecture

Use FACT_DIM_ARCHITECTURE.md as the source of truth. Create a diagram that shows both the ETL flow and the gold star schema for Wazuh, Suricata, and Zeek.

Requirements
- Note that all gold fact tables are range-partitioned by event_ts (daily).
- Show bronze raw tables: bronze.wazuh_events_raw, bronze.suricata_events_raw, bronze.zeek_events_raw.
- Show Airflow as the ETL orchestrator between bronze and gold.
- Show gold fact tables: gold.fact_wazuh_events, gold.fact_suricata_events, gold.fact_zeek_events.
- Show gold dimensions: gold.dim_date, gold.dim_time, gold.dim_host, gold.dim_tag, gold.dim_agent, gold.dim_rule, gold.dim_event, gold.dim_sensor, gold.dim_signature, gold.dim_protocol.
- Show gold bridge tables: gold.bridge_wazuh_event_tag, gold.bridge_suricata_event_tag, gold.bridge_zeek_event_tag.

Relationships to depict
- Fact tables use composite keys (event_id, event_ts) to align with daily partitions.
- gold.fact_wazuh_events connects to gold.dim_date, gold.dim_time, gold.dim_agent, gold.dim_host, gold.dim_rule, gold.dim_event.
- gold.fact_suricata_events connects to gold.dim_date, gold.dim_time, gold.dim_sensor, gold.dim_signature, gold.dim_protocol.
- gold.fact_zeek_events connects to gold.dim_date, gold.dim_time, gold.dim_sensor, gold.dim_protocol, gold.dim_event.
- Each bridge table connects its fact table using (event_id, event_ts) to gold.dim_tag.

Partitioning note
- Add a small annotation or note node that fact tables are partitioned by event_ts (daily).

Output format
- Provide Mermaid `flowchart LR` in a single fenced code block.
- Use subgraphs labeled Bronze, Airflow, and Gold.
- Only show table names and key relationships (no full column lists).
- Keep the diagram readable on a single page; avoid crossing lines where possible.
