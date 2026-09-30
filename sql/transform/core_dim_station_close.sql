WITH incoming AS (
    SELECT
        (s ->> 'station_id')::bigint  AS station_id,
        s ->> 'stationCode'           AS station_code,
        s ->> 'name'                  AS name,
        (s ->> 'lat')::numeric(9, 6)  AS lat,
        (s ->> 'lon')::numeric(9, 6)  AS lon,
        (s ->> 'capacity')::int       AS capacity,
        r.fetched_at
    FROM raw.api_snapshots AS r
    CROSS JOIN LATERAL jsonb_array_elements(r.payload -> 'data' -> 'stations') AS s
    WHERE r.snapshot_id = %(snapshot_id)s
      AND r.source = 'velib_station_information'
)
UPDATE core.dim_station AS d
   SET valid_to   = (SELECT min(fetched_at) FROM incoming),
       is_current = false
 WHERE d.is_current
   AND EXISTS (SELECT 1 FROM incoming)  
   AND (
        EXISTS (
            SELECT 1 FROM incoming AS i
             WHERE i.station_id = d.station_id
               AND (i.station_code, i.name, i.lat, i.lon, i.capacity)
                   IS DISTINCT FROM (d.station_code, d.name, d.lat, d.lon, d.capacity)
        )
        OR NOT EXISTS (
            SELECT 1 FROM incoming AS i WHERE i.station_id = d.station_id
        )
   );
