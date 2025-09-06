CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.sensor_data (
    id SERIAL PRIMARY KEY,
    machine_id VARCHAR(50),
    sensor VARCHAR(50),
    reading DOUBLE PRECISION,
    t_stamp TIMESTAMP
);
