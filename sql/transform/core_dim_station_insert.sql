WITH incoming AS (
    SELECT DISTINCT ON ((s ->> 'station_id')::bigint)
        (s ->> 'station_id')::bigint  AS station_id,
        s ->> 'stationCode'           AS station_code,
        s ->> 'name'                  AS name,
        (s ->> 'lat')::numeric(9, 6)  AS lat,
        (s ->> 'lon')::numeric(9, 6)  AS lon,
        (s ->> 'capacity')::int       AS capacity,
        r.fetched_at,
        r.snapshot_id
    FROM raw.api_snapshots AS r
    CROSS JOIN LATERAL jsonb_array_elements(r.payload -> 'data' -> 'stations') AS s
    WHERE r.snapshot_id = %(snapshot_id)s
      AND r.source = 'velib_station_information'
)
INSERT INTO core.dim_station (
    station_id, station_code, name, lat, lon, capacity,
    valid_from, snapshot_id
)
SELECT
    i.station_id, i.station_code, i.name, i.lat, i.lon, i.capacity,
    i.fetched_at, i.snapshot_id
FROM incoming AS i
WHERE NOT EXISTS (
    SELECT 1 FROM core.dim_station AS d
     WHERE d.station_id = i.station_id
       AND d.is_current
)
ON CONFLICT DO NOTHING;
