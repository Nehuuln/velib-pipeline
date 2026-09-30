CREATE MATERIALIZED VIEW IF NOT EXISTS marts.station_hourly_usage AS
WITH par_heure AS (
    SELECT
        f.station_id,
        date_trunc('hour', f.last_reported)          AS hour_utc,
        count(*)                                     AS observations,
        round(avg(f.num_bikes_available), 2)         AS avg_bikes,
        min(f.num_bikes_available)                   AS min_bikes,
        max(f.num_bikes_available)                   AS max_bikes,
        round(avg(f.num_ebike), 2)                   AS avg_ebike,
        round(avg(f.num_docks_available), 2)         AS avg_docks,
        count(*) FILTER (WHERE f.num_bikes_available = 0)::numeric / count(*) AS empty_ratio,
        count(*) FILTER (WHERE f.num_docks_available = 0)::numeric / count(*) AS full_ratio,
        (array_agg(f.num_bikes_available ORDER BY f.last_reported DESC))[1] AS bikes_fin_heure
    FROM core.fact_station_status AS f
    WHERE f.is_installed
    GROUP BY f.station_id, date_trunc('hour', f.last_reported)
),
avec_flux AS (
    SELECT
        p.*,
        lag(p.bikes_fin_heure) OVER w AS bikes_fin_heure_precedente,
        extract(EPOCH FROM p.hour_utc - lag(p.hour_utc) OVER w) / 3600 AS ecart_heures
    FROM par_heure AS p
    WINDOW w AS (PARTITION BY p.station_id ORDER BY p.hour_utc)
)
SELECT
    a.station_id,
    a.hour_utc,
    d.name          AS station_name,
    d.station_code,
    d.lat,
    d.lon,
    d.capacity,
    a.observations,
    a.avg_bikes,
    a.min_bikes,
    a.max_bikes,
    a.avg_ebike,
    a.avg_docks,
    round(a.empty_ratio, 4) AS empty_ratio,
    round(a.full_ratio, 4)  AS full_ratio,
    round(a.avg_bikes / nullif(d.capacity, 0), 4) AS fill_ratio,
    CASE WHEN a.ecart_heures = 1
         THEN a.bikes_fin_heure - a.bikes_fin_heure_precedente
    END AS net_flow,
    w.temperature_c,
    w.precipitation_mm,
    w.wind_speed_kmh,
    (w.precipitation_mm > 0) AS is_rainy
FROM avec_flux AS a
LEFT JOIN LATERAL (
    SELECT s.name, s.station_code, s.lat, s.lon, s.capacity
    FROM core.dim_station AS s
    WHERE s.station_id = a.station_id
    ORDER BY (s.valid_from <= a.hour_utc) DESC,
             abs(extract(EPOCH FROM s.valid_from - a.hour_utc))
    LIMIT 1
) AS d ON true
LEFT JOIN staging.weather_hourly AS w
       ON w.observed_at = a.hour_utc;

CREATE UNIQUE INDEX IF NOT EXISTS uq_station_hourly_usage
    ON marts.station_hourly_usage (station_id, hour_utc);

CREATE INDEX IF NOT EXISTS ix_station_hourly_usage_hour
    ON marts.station_hourly_usage (hour_utc);
