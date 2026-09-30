"""Ingestion des caractéristiques des stations Vélib', toutes les heures.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from src.clients.velib import VelibClient, stations_of
from src.loaders.postgres import insert_snapshot, transform_dim_station

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "velib_db"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=3),
}


@dag(
    dag_id="ingest_station_information",
    description="Caractéristiques des stations Vélib' : API -> raw -> core.dim_station",
    schedule="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["velib", "ingestion", "core"],
)
def ingest_station_information():
    @task
    def fetch_to_raw() -> int:
        snapshot = VelibClient().fetch_station_information()
        logger.info("Snapshot récupéré : %s stations", len(stations_of(snapshot)))

        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with conn:
            snapshot_id = insert_snapshot(conn, snapshot)

        if snapshot_id is None:
            raise AirflowSkipException("Réponse API déjà présente dans le raw")
        return snapshot_id

    @task
    def raw_to_dim_station(snapshot_id: int) -> int:
        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with conn:
            versions = transform_dim_station(conn, snapshot_id)
        return versions

    raw_to_dim_station(fetch_to_raw())


ingest_station_information()
