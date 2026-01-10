# Near Real-Time Security Analytics POC

This repo is an end-to-end open source near real-time analytics POC using a Medallion architecture and metadata-driven Airflow.

Dataflow
- Redpanda (remote) -> RisingWave -> Postgres bronze (bronze.suricata_events_raw + bronze.wazuh_events_raw + bronze.zeek_events_raw)
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

Example counts:

```sql
SELECT count(*) FROM bronze.suricata_events_raw;
SELECT count(*) FROM bronze.wazuh_events_raw;
SELECT count(*) FROM bronze.zeek_events_raw;
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
- Update pipeline configs by editing control.* metadata in Postgres (source_table_name, target_schema, target_table_schema)
- control.datasource_to_dwh_pipelines.merge_sql_text stores the merge SQL (required; supports placeholders like {{DATAWAREHOUSE_TABLE}}, {{TIME_FILTER}}, {{MERGE_UPDATE_SET}}, {{SOURCE_COLUMN_LIST}})
- Optional: scripts/metadata-generator/main.py exports the Redis payload to JSON under airflow/dags/generated

## Superset
Use the bi_reader account so only gold schema is visible. See superset/bootstrap/README_superset.md.

## Redpanda source
This stack expects a remote Redpanda/Kafka-compatible broker:
- bootstrap: `10.110.12.20:9092`
- topic: `malcolm-logs`

## Smoke test
Use scripts/smoke_test.sh (bash shell). On Windows, run via WSL or Git Bash.

## Backfill
Trigger the pipeline DAG with dag_run.conf:

```bash
docker compose exec -T airflow-webserver airflow dags trigger security_dwh -c '{"pipeline_id":"suricata_events","start_ts":"2026-01-01T00:00:00Z","end_ts":"2026-01-02T00:00:00Z"}'
```

Use `pipeline_id` `wazuh_events` to backfill Wazuh data.
Use `pipeline_id` `zeek_events` to backfill Zeek data.

## RisingWave backfill (no downtime)
The live RisingWave pipeline can stay on `scan.startup.mode = 'latest'`. Backfill uses a separate pipeline that writes to staging tables, then merges into bronze.

If the Postgres container already exists, apply the staging tables once:

```bash
docker compose exec -T postgres psql -U postgres -d analytics -f postgres/init/02_staging_tables.sql
```

1) Run backfill (defaults to last 7 days):

  $env:RW_BACKFILL_START_TS="2026-01-07T00:00:00Z"                                                               
  $env:RW_BACKFILL_END_TS="2026-01-08T00:00:00Z"                                                                 
 docker compose --profile backfill run --rm risingwave-backfill                                                    
  Remove-Item Env:\RW_BACKFILL_START_TS, Env:\RW_BACKFILL_END_TS    

```bash
docker compose --profile backfill run --rm risingwave-backfill
```

Optional overrides:
- RW_KAFKA_BOOTSTRAP (default 10.110.12.20:9092)
- RW_BACKFILL_TOPIC (default malcolm-logs)
- RW_BACKFILL_DAYS (default 7)
- RW_BACKFILL_START_TS / RW_BACKFILL_END_TS (ISO8601 UTC)

2) Merge staging -> bronze using the same time window:

 docker compose cp scripts/backfill_merge.sql postgres:/tmp/backfill_merge.sql   

docker compose exec -T postgres psql -U postgres -d analytics -v start_ts="1970
-01-01T00:00:00Z" -v end_ts="2100-01-01T00:00:00Z" -f /tmp/backfill_merge.sql

```bash
docker compose exec -T postgres psql -U postgres -d analytics -v start_ts="2026-01-01T00:00:00Z" -v end_ts="2026-01-08T00:00:00Z" -f scripts/backfill_merge.sql
```

3) Optional cleanup:
- Truncate staging tables in `staging.*`.
- Drop backfill objects in RisingWave using `risingwave/backfill_cleanup.sql`.

## Logstash future (Kafka output)
To send data into Redpanda:

```conf
output {
  kafka {
    bootstrap_servers => "10.110.12.20:9092"
    topic_id => "malcolm-logs"
    codec => json
  }
}
```

Downstream remains unchanged.

## Troubleshooting
- If RisingWave init fails, rerun: docker compose run --rm risingwave-init
- If Airflow DAGs are missing, wait for the scheduler to parse or restart airflow-scheduler
- If Superset is empty, add the Postgres database using bi_reader and create datasets from gold schema
