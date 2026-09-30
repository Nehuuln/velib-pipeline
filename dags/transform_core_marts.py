"""Alimentation des couches core et marts, puis contrôles qualité.

    staging -> core.fact_station_status -> marts.station_hourly_usage -> qualité

Séparé des DAGs d'ingestion : la modélisation ne doit pas ralentir la
collecte, et on peut la rejouer sans rappeler aucune API.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from src.loaders.postgres import (
    record_metric,
    refresh_materialized_view,
    transform_fact_station_status,
)
from src.quality.checks import failures, run_checks, warnings

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "velib_db"
MART = "marts.station_hourly_usage"
DAG_ID = "transform_core_marts"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


@dag(
    dag_id=DAG_ID,
    description="staging -> core -> marts, avec contrôles qualité",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["core", "marts", "transformation", "qualite"],
)
def transform_core_marts():
    @task
    def load_fact_station_status(**context) -> int:
        started = time.monotonic()
        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with conn:
            inserted = transform_fact_station_status(conn)
            record_metric(
                conn,
                dag_id=DAG_ID,
                task_id="load_fact_station_status",
                run_id=context["run_id"],
                rows_written=inserted,
                duration_ms=int((time.monotonic() - started) * 1000),
            )
        return inserted

    @task
    def refresh_mart(inserted: int, **context) -> None:
        started = time.monotonic()
        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {MART}")
                deja_peuplee = cur.fetchone()[0] > 0
            refresh_materialized_view(conn, MART, concurrently=deja_peuplee)
            record_metric(
                conn,
                dag_id=DAG_ID,
                task_id="refresh_mart",
                run_id=context["run_id"],
                rows_written=inserted,
                duration_ms=int((time.monotonic() - started) * 1000),
            )

    @task
    def quality_checks() -> None:
        """Exécute tous les contrôles, puis échoue s'il reste un fail.

        Les avertissements (anomalies connues de la source Vélib') sont
        tracés mais ne font pas échouer le DAG : sinon l'alerte serait
        permanente et on cesserait de la regarder.
        """
        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with conn:
            results = run_checks(conn)

        for result in warnings(results):
            logger.warning(
                "Avertissement qualité %s : mesuré=%s, attendu %s (%s)",
                result.name, result.observed, result.expected, result.details,
            )

        echecs = failures(results)
        if echecs:
            raise AirflowFailException(
                "Contrôles qualité en échec : "
                + ", ".join(
                    f"{r.name} (mesuré={r.observed}, attendu {r.expected})" for r in echecs
                )
            )

        logger.info("%s contrôles exécutés, aucun échec", len(results))

    refresh_mart(load_fact_station_status()) >> quality_checks()


transform_core_marts()
