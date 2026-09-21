CREATE TABLE IF NOT EXISTS raw.api_snapshots (
    snapshot_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source            text        NOT NULL,  
    fetched_at        timestamptz NOT NULL,  
    source_updated_at timestamptz,          
    payload           jsonb       NOT NULL,
    payload_md5       text GENERATED ALWAYS AS (md5(payload::text)) STORED,
    CONSTRAINT uq_api_snapshots_source_payload UNIQUE (source, payload_md5)
);

CREATE INDEX IF NOT EXISTS ix_api_snapshots_source_fetched_at
    ON raw.api_snapshots (source, fetched_at DESC);
