WITH curseur AS (
    SELECT coalesce(max(last_reported), '-infinity'::timestamptz) - interval '1 hour'
           AS depuis
    FROM core.fact_station_status
)
INSERT INTO core.fact_station_status (
    station_id, last_reported, station_sk,
    num_bikes_available, num_mechanical, num_ebike, num_docks_available,
    is_installed, is_renting, is_returning
)
SELECT
    s.station_id,
    s.last_reported,
    d.station_sk,
    s.num_bikes_available,
    s.num_mechanical,
    s.num_ebike,
    s.num_docks_available,
    s.is_installed,
    s.is_renting,
    s.is_returning
FROM staging.station_status AS s
CROSS JOIN curseur AS c
LEFT JOIN LATERAL (
    SELECT d.station_sk
    FROM core.dim_station AS d
    WHERE d.station_id = s.station_id
    ORDER BY (s.last_reported >= d.valid_from
              AND s.last_reported < coalesce(d.valid_to, 'infinity'::timestamptz)) DESC,
             abs(extract(EPOCH FROM d.valid_from - s.last_reported))
    LIMIT 1
) AS d ON true
WHERE s.last_reported >= c.depuis
ON CONFLICT (station_id, last_reported) DO NOTHING;
