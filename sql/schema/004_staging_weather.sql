CREATE TABLE IF NOT EXISTS staging.weather_hourly (
    observed_at       timestamptz NOT NULL,
    temperature_c     numeric(5, 2),
    precipitation_mm  numeric(6, 2),
    wind_speed_kmh    numeric(6, 2),
    humidity_pct      integer,
    snapshot_id       bigint      NOT NULL,
    fetched_at        timestamptz NOT NULL,
    loaded_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_weather_hourly PRIMARY KEY (observed_at)
);
