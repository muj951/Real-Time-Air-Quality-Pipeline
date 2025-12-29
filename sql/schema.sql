-- Table structure for Real-Time Air Quality Monitoring System

CREATE TABLE IF NOT EXISTS air_quality_all_sensors (
    location_id BIGINT,
    station_name VARCHAR(255),
    sensor_id BIGINT,
    sensor_value DOUBLE PRECISION,
    parameter VARCHAR(50),
    measurement_unit VARCHAR(50),
    sensor_timestamp_utc TIMESTAMP,
    scrape_timestamp_utc TIMESTAMP,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

-- Optional: Create an index on timestamp for faster Grafana querying
CREATE INDEX idx_scrape_time ON air_quality_all_sensors(scrape_timestamp_utc);