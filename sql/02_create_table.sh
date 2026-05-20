#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "weather_db" <<-EOSQL
    CREATE TABLE IF NOT EXISTS weather_data (
        id              SERIAL PRIMARY KEY,
        city            VARCHAR(100)    NOT NULL,
        date            DATE            NOT NULL,
        temperature_max NUMERIC(5,2),
        temperature_min NUMERIC(5,2),
        precipitation   NUMERIC(6,2),
        windspeed_max   NUMERIC(5,2),
        ingested_at     TIMESTAMP       DEFAULT NOW(),
        UNIQUE(city, date)
    );
    CREATE INDEX IF NOT EXISTS idx_weather_date ON weather_data(date);
EOSQL