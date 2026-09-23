CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS iot_events (
    event_id      UUID        NOT NULL,
    sensor_id     TEXT        NOT NULL,
    ts            TIMESTAMPTZ NOT NULL,
    humidity      DOUBLE PRECISION,
    temperature   DOUBLE PRECISION,
    soil_state    TEXT,
    received_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (event_id, ts)
);

SELECT create_hypertable('iot_events', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_iot_events_sensor_ts
    ON iot_events (sensor_id, ts DESC);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id    UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);