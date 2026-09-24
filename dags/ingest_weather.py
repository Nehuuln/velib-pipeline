"""Ingestion de la météo horaire de Paris (Open-Meteo), toutes les heures.

    API -> raw.api_snapshots -> staging.weather_hourly
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from src.clients.open_meteo import OpenMeteoClient
from src.loaders.postgres import insert_snapshot, transform_weather_hourly

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "velib_db"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=3),
}


@dag(
    dag_id="ingest_weather",
    description="Météo horaire Paris (Open-Meteo) : API -> raw -> staging",
    schedule="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["meteo", "ingestion"],
)
def ingest_weather():
    @task
    def fetch_to_raw() -> int:
        snapshot = OpenMeteoClient().fetch_hourly(past_days=1, forecast_days=1)

        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with conn:
            snapshot_id = insert_snapshot(conn, snapshot)

        if snapshot_id is None:
            raise AirflowSkipException("Réponse Open-Meteo déjà présente dans le raw")
        return snapshot_id

    @task
    def raw_to_staging(snapshot_id: int) -> int:
        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with conn:
            rows = transform_weather_hourly(conn, snapshot_id)
        return rows

    raw_to_staging(fetch_to_raw())


ingest_weather()
