DROP SINK IF EXISTS suricata_backfill_sink;
DROP SINK IF EXISTS wazuh_backfill_sink;
DROP SINK IF EXISTS zeek_backfill_sink;
DROP MATERIALIZED VIEW IF EXISTS suricata_backfill_mv;
DROP MATERIALIZED VIEW IF EXISTS wazuh_backfill_mv;
DROP MATERIALIZED VIEW IF EXISTS zeek_backfill_mv;
DROP SOURCE IF EXISTS security_events_backfill_source;
