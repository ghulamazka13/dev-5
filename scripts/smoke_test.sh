#!/usr/bin/env bash
set -euo pipefail

docker compose up -d

echo "Waiting for services..."
sleep 30

echo "Bronze row count:"
docker compose exec -T postgres psql -U postgres -d analytics -c "SELECT count(*) FROM bronze.suricata_events_raw;"
docker compose exec -T postgres psql -U postgres -d analytics -c "SELECT count(*) FROM bronze.wazuh_events_raw;"
docker compose exec -T postgres psql -U postgres -d analytics -c "SELECT count(*) FROM bronze.zeek_events_raw;"

echo "Triggering Airflow metadata updater..."
docker compose exec -T airflow-webserver airflow dags trigger metadata_updater
sleep 10
echo "Triggering dynamic DAG..."
docker compose exec -T airflow-webserver airflow dags trigger security_dwh

echo "Open Airflow: http://localhost:8088"
echo "Open Superset: http://localhost:8089"
