"""Alimentation des couches core et marts, toutes les 15 minutes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from src.loaders.postgres import (
    refresh_materialized_view,
    transform_fact_station_status,
)

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "velib_db"
MART = "marts.station_hourly_usage"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


@dag(
    dag_id="transform_core_marts",
    description="staging -> core.fact_station_status -> marts.station_hourly_usage",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["core", "marts", "transformation"],
)
def transform_core_marts():
    @task
    def load_fact_station_status() -> int:
        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with conn:
            return transform_fact_station_status(conn)

    @task
    def refresh_mart(inserted: int) -> None:
        """Rafraîchit le mart après l'alimentation des faits."""
        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {MART}")
                deja_peuplee = cur.fetchone()[0] > 0
            refresh_materialized_view(conn, MART, concurrently=deja_peuplee)

    refresh_mart(load_fact_station_status())


transform_core_marts()
