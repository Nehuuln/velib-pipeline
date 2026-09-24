"""Ingestion du statut des stations Vélib', toutes les 5 minutes.

    API -> raw.api_snapshots (JSONB brut) -> staging.station_status (typé)

Deux tâches séparées : si la transformation échoue, la réponse API est déjà
sauvegardée dans le raw et peut être rejouée sans rappeler l'API.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from src.clients.velib import VelibClient
from src.loaders.postgres import insert_snapshot, transform_station_status

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "velib_db"

default_args = {
    # Retries Airflow : si la tâche échoue, on retente plus tard
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=3),
}


@dag(
    dag_id="ingest_velib",
    description="Statut des stations Vélib' : API -> raw -> staging",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False, 
    max_active_runs=1,  
    default_args=default_args,
    tags=["velib", "ingestion"],
)
def ingest_velib():
    @task
    def fetch_to_raw() -> int:
        """Appelle l'API et stocke la réponse brute. Renvoie le snapshot_id."""
        snapshot = VelibClient().fetch_station_status()
        logger.info(
            "Snapshot récupéré : %s stations, source_updated_at=%s",
            len(snapshot.stations), snapshot.source_updated_at,
        )

        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with conn:  
            snapshot_id = insert_snapshot(conn, snapshot)

        if snapshot_id is None:
            raise AirflowSkipException("Réponse API déjà présente dans le raw")
        return snapshot_id

    @task
    def raw_to_staging(snapshot_id: int) -> int:
        """Transforme le snapshot en lignes typées."""
        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with conn:
            inserted = transform_station_status(conn, snapshot_id)
        return inserted

    raw_to_staging(fetch_to_raw())


ingest_velib()
