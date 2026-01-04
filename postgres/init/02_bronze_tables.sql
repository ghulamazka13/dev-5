CREATE TABLE IF NOT EXISTS bronze.security_events_raw (
  event_id text PRIMARY KEY,
  event_ts timestamptz,
  sensor_type text,
  sensor_name text,
  event_type text,
  severity text,
  src_ip inet,
  dest_ip inet,
  src_port int,
  dest_port int,
  protocol text,
  bytes bigint,
  packets bigint,
  uid text null,
  conn_state text null,
  duration double precision null,
  signature text null,
  signature_id int null,
  category text null,
  alert_action text null,
  tags jsonb,
  message text
);

CREATE INDEX IF NOT EXISTS idx_security_events_raw_event_ts ON bronze.security_events_raw(event_ts);
CREATE INDEX IF NOT EXISTS idx_security_events_raw_severity ON bronze.security_events_raw(severity);
CREATE INDEX IF NOT EXISTS idx_security_events_raw_sensor_type ON bronze.security_events_raw(sensor_type);
CREATE INDEX IF NOT EXISTS idx_security_events_raw_src_ip ON bronze.security_events_raw(src_ip);
CREATE INDEX IF NOT EXISTS idx_security_events_raw_dest_ip ON bronze.security_events_raw(dest_ip);

-- RisingWave JDBC sink uses INSERT ... RETURNING, which requires SELECT on target columns.
GRANT SELECT ON bronze.security_events_raw TO rw_writer;
