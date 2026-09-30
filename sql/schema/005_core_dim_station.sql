CREATE TABLE IF NOT EXISTS core.dim_station (
    station_sk   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id   bigint      NOT NULL,
    station_code text,
    name         text        NOT NULL,
    lat          numeric(9, 6),
    lon          numeric(9, 6),
    capacity     integer,
    valid_from   timestamptz NOT NULL,
    valid_to     timestamptz,
    is_current   boolean     NOT NULL DEFAULT true,
    snapshot_id  bigint      NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_dim_station_version UNIQUE (station_id, valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_station_current
    ON core.dim_station (station_id) WHERE is_current;
