INSERT INTO staging.station_status (
    station_id, station_code, last_reported,
    num_bikes_available, num_mechanical, num_ebike, num_docks_available,
    is_installed, is_renting, is_returning,
    snapshot_id, fetched_at
)
SELECT
    (s ->> 'station_id')::bigint,
    s ->> 'stationCode',
    to_timestamp((s ->> 'last_reported')::bigint),
    (s ->> 'num_bikes_available')::int,
    (jsonb_path_query_first(s, '$.num_bikes_available_types[*].mechanical') #>> '{}')::int,
    (jsonb_path_query_first(s, '$.num_bikes_available_types[*].ebike') #>> '{}')::int,
    (s ->> 'num_docks_available')::int,
    (s ->> 'is_installed')::int = 1,
    (s ->> 'is_renting')::int = 1,
    (s ->> 'is_returning')::int = 1,
    r.snapshot_id,
    r.fetched_at
FROM raw.api_snapshots AS r
CROSS JOIN LATERAL jsonb_array_elements(r.payload -> 'data' -> 'stations') AS s
WHERE r.snapshot_id = %(snapshot_id)s
  AND r.source = 'velib_station_status'
ON CONFLICT (station_id, last_reported) DO NOTHING;
