SELECT count(*) FROM bronze.suricata_events_raw;
SELECT count(*) FROM bronze.wazuh_events_raw;
SELECT count(*) FROM bronze.zeek_events_raw;
SELECT count(*) FROM silver.security_events;
SELECT * FROM gold.alerts_5m ORDER BY window_start DESC LIMIT 10;
SELECT * FROM monitoring.lag_metrics ORDER BY observed_at DESC LIMIT 5;
