CREATE TABLE IF NOT EXISTS marts.quality_check_results (
    check_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    checked_at  timestamptz NOT NULL DEFAULT now(),
    check_name  text        NOT NULL,
    status      text        NOT NULL,  
    observed    numeric,            
    expected    text,                
    details     text,
    CONSTRAINT ck_quality_status CHECK (status IN ('ok', 'warn', 'fail'))
);

CREATE INDEX IF NOT EXISTS ix_quality_check_results_name_time
    ON marts.quality_check_results (check_name, checked_at DESC);

CREATE TABLE IF NOT EXISTS marts.pipeline_metrics (
    metric_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recorded_at  timestamptz NOT NULL DEFAULT now(),
    dag_id       text        NOT NULL,
    task_id      text        NOT NULL,
    run_id       text,
    rows_written integer,
    duration_ms  integer,
    CONSTRAINT ck_pipeline_metrics_duration CHECK (duration_ms >= 0)
);

CREATE INDEX IF NOT EXISTS ix_pipeline_metrics_dag_time
    ON marts.pipeline_metrics (dag_id, recorded_at DESC);
