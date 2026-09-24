INSERT INTO staging.weather_hourly (
    observed_at, temperature_c, precipitation_mm, wind_speed_kmh, humidity_pct,
    snapshot_id, fetched_at
)
SELECT
    to_timestamp(t.epoch::bigint),
    (r.payload -> 'hourly' -> 'temperature_2m'       ->> (t.idx - 1)::int)::numeric,
    (r.payload -> 'hourly' -> 'precipitation'        ->> (t.idx - 1)::int)::numeric,
    (r.payload -> 'hourly' -> 'wind_speed_10m'       ->> (t.idx - 1)::int)::numeric,
    (r.payload -> 'hourly' -> 'relative_humidity_2m' ->> (t.idx - 1)::int)::int,
    r.snapshot_id,
    r.fetched_at
FROM raw.api_snapshots AS r
CROSS JOIN LATERAL jsonb_array_elements_text(r.payload -> 'hourly' -> 'time')
    WITH ORDINALITY AS t(epoch, idx)
WHERE r.snapshot_id = %(snapshot_id)s
  AND r.source = 'open_meteo_hourly'
ON CONFLICT (observed_at) DO UPDATE SET
    temperature_c    = EXCLUDED.temperature_c,
    precipitation_mm = EXCLUDED.precipitation_mm,
    wind_speed_kmh   = EXCLUDED.wind_speed_kmh,
    humidity_pct     = EXCLUDED.humidity_pct,
    snapshot_id      = EXCLUDED.snapshot_id,
    fetched_at       = EXCLUDED.fetched_at,
    loaded_at        = now()
WHERE EXCLUDED.fetched_at > staging.weather_hourly.fetched_at;
