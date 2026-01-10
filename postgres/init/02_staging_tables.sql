CREATE SCHEMA IF NOT EXISTS staging;
REVOKE ALL ON SCHEMA staging FROM PUBLIC;
GRANT USAGE ON SCHEMA staging TO rw_writer, etl_runner;

CREATE TABLE IF NOT EXISTS staging.suricata_events_backfill (
  LIKE bronze.suricata_events_raw
);

CREATE INDEX IF NOT EXISTS idx_staging_suricata_events_backfill_event_id
  ON staging.suricata_events_backfill(event_id);
CREATE INDEX IF NOT EXISTS idx_staging_suricata_events_backfill_event_ts
  ON staging.suricata_events_backfill(event_ts);

CREATE TABLE IF NOT EXISTS staging.wazuh_events_backfill (
  LIKE bronze.wazuh_events_raw
);

CREATE INDEX IF NOT EXISTS idx_staging_wazuh_events_backfill_event_id
  ON staging.wazuh_events_backfill(event_id);
CREATE INDEX IF NOT EXISTS idx_staging_wazuh_events_backfill_event_ts
  ON staging.wazuh_events_backfill(event_ts);

CREATE TABLE IF NOT EXISTS staging.zeek_events_backfill (
  LIKE bronze.zeek_events_raw
);

CREATE INDEX IF NOT EXISTS idx_staging_zeek_events_backfill_event_id
  ON staging.zeek_events_backfill(event_id);
CREATE INDEX IF NOT EXISTS idx_staging_zeek_events_backfill_event_ts
  ON staging.zeek_events_backfill(event_ts);

GRANT INSERT ON staging.suricata_events_backfill TO rw_writer;
GRANT INSERT ON staging.wazuh_events_backfill TO rw_writer;
GRANT INSERT ON staging.zeek_events_backfill TO rw_writer;
GRANT SELECT ON staging.suricata_events_backfill TO rw_writer, etl_runner;
GRANT SELECT ON staging.wazuh_events_backfill TO rw_writer, etl_runner;
GRANT SELECT ON staging.zeek_events_backfill TO rw_writer, etl_runner;
