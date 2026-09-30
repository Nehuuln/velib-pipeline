CREATE TABLE IF NOT EXISTS core.fact_station_status (
    station_id          bigint      NOT NULL,
    last_reported       timestamptz NOT NULL,
    station_sk          bigint,     
    num_bikes_available integer     NOT NULL,
    num_mechanical      integer,
    num_ebike           integer,
    num_docks_available integer     NOT NULL,
    is_installed        boolean     NOT NULL,
    is_renting          boolean     NOT NULL,
    is_returning        boolean     NOT NULL,
    loaded_at           timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_fact_station_status PRIMARY KEY (station_id, last_reported)
);

CREATE INDEX IF NOT EXISTS ix_fact_station_status_last_reported
    ON core.fact_station_status (last_reported);
