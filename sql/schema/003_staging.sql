CREATE TABLE IF NOT EXISTS staging.station_status (
    station_id          bigint      NOT NULL,
    station_code        text,
    last_reported       timestamptz NOT NULL,
    num_bikes_available integer     NOT NULL,
    num_mechanical      integer,
    num_ebike           integer,
    num_docks_available integer     NOT NULL,
    is_installed        boolean     NOT NULL,
    is_renting          boolean     NOT NULL,
    is_returning        boolean     NOT NULL,
    snapshot_id         bigint      NOT NULL,
    fetched_at          timestamptz NOT NULL,
    loaded_at           timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_station_status PRIMARY KEY (station_id, last_reported)
);

CREATE INDEX IF NOT EXISTS ix_station_status_last_reported
    ON staging.station_status (last_reported);
