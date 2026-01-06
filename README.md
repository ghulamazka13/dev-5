# Near Real-Time Security Analytics POC

This repo is an end-to-end open source near real-time analytics POC using a Medallion architecture and metadata-driven Airflow.

Dataflow
- Dummy producer -> Kafka -> Kafka UI -> RisingWave -> Postgres bronze
- Airflow metadata-driven generator merges bronze -> gold datawarehouse tables (dedupe/upsert), plus monitoring and data quality
- Superset reads gold only using bi_reader

## Quickstart

1) Start the stack

```bash
docker compose up -d
```

2) Wait for services

```bash
docker compose ps
```

3) Open UIs
- Kafka UI: http://localhost:18080
- Airflow: http://localhost:8088 (admin/admin)
- Superset: http://localhost:8089 (admin/admin)

4) Trigger the pipeline (optional)

```bash
docker compose exec -T airflow-webserver airflow dags trigger metadata_updater
docker compose exec -T airflow-webserver airflow dags trigger security_dwh
```

If `security_dwh` doesn't appear immediately, wait for the scheduler to pick up the new dynamic DAG.

5) Check data in Postgres

```bash
docker compose exec -T postgres psql -U postgres -d analytics
```

## Access control (Medallion + grants)
- bronze: raw, restricted
- silver: cleaned, restricted
- gold: business, BI readable
- control: metadata
- monitoring: pipeline metrics

Roles
- rw_writer: insert only to bronze
- etl_runner: select bronze, DML silver/gold/monitoring
- bi_reader: select only on gold (Superset must use this)

## pg_duckdb
The Postgres image includes pg_duckdb. It is enabled on init:

```sql
CREATE EXTENSION IF NOT EXISTS pg_duckdb;
```

## Airflow metadata-driven pipelines
- Control tables live in control.*
- metadata_updater uses metadata/query.py (MetadataQuery.datasource_to_dwh) to read control.database_connections, control.dag_configs, and control.datasource_to_dwh_pipelines, then writes a payload to Redis
- main.py loads metadata from Postgres (with Redis fallback) and builds dynamic DAGs via generator/datasource_to_dwh.py
- Update pipeline configs by editing control.* metadata in Postgres (source_table_name, source_sql_query, target_schema, target_table_schema)
- Optional: control.datasource_to_dwh_sql_steps stores per-pipeline SQL transforms that run after the merge
- Optional: scripts/metadata-generator/main.py exports the Redis payload to JSON under airflow/dags/generated

## Superset
Use the bi_reader account so only gold schema is visible. See superset/bootstrap/README_superset.md.

## Producer configuration
Environment variables for producer/producer.py:
- EVENTS_PER_SEC (default 10)
- MIX_ZEEK_PERCENT (default 60)
- MIX_SURICATA_PERCENT (default 40)
- SEED (default 42)
- KAFKA_BROKERS (default kafka:9092)
- KAFKA_TOPIC (default raw.security_events)

## Smoke test
Use scripts/smoke_test.sh (bash shell). On Windows, run via WSL or Git Bash.

## Backfill
Trigger the backfill DAG with dag_run.conf:

```bash
docker compose exec -T airflow-webserver airflow dags trigger backfill_maintenance_dag -c '{"pipeline_id":"security_events","start_ts":"2026-01-01T00:00:00Z","end_ts":"2026-01-02T00:00:00Z"}'
```

## Logstash future (Kafka output)
To replace the dummy producer with Logstash later:

```conf
output {
  kafka {
    bootstrap_servers => "kafka:9092"
    topic_id => "raw.security_events"
    codec => json
  }
}
```

Downstream remains unchanged.

## Troubleshooting
- If RisingWave init fails, rerun: docker compose run --rm risingwave-init
- If Airflow DAGs are missing, wait for the scheduler to parse or restart airflow-scheduler
- If Superset is empty, add the Postgres database using bi_reader and create datasets from gold schema
