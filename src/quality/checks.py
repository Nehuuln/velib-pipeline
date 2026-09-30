"""Contrôles qualité du pipeline Vélib'.

Trois niveaux :
    ok   : valeur dans les bornes
    warn : anomalie connue des données sources, on ne bloque pas le pipeline
    fail : le pipeline est cassé ou les données sont inexploitables
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

OK = "ok"
WARN = "warn"
FAIL = "fail"


@dataclass(frozen=True)
class Check:
    name: str
    sql: str
    expected: str
    severity: str = FAIL 
    min_value: float | None = None
    max_value: float | None = None
    description: str = ""

    def evaluate(self, observed: float | None) -> str:
        if observed is None:
            return self.severity
        if self.min_value is not None and observed < self.min_value:
            return self.severity
        if self.max_value is not None and observed > self.max_value:
            return self.severity
        return OK


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    observed: float | None
    expected: str
    details: str


CHECKS: tuple[Check, ...] = (
    Check(
        name="pipeline_freshness_minutes",
        description="Âge du dernier appel API Vélib' réussi",
        sql="""
            SELECT extract(EPOCH FROM now() - max(fetched_at)) / 60
            FROM raw.api_snapshots
            WHERE source = 'velib_station_status'
        """,
        expected="< 15 min",
        max_value=15,
        severity=FAIL,
    ),
    Check(
        name="weather_hours_last_24h",
        description="Heures de météo disponibles sur les dernières 24 h",
        sql="""
            SELECT count(*) FROM staging.weather_hourly
            WHERE observed_at BETWEEN now() - interval '24 hours' AND now()
        """,
        expected=">= 20 heures",
        min_value=20,
        severity=FAIL,
    ),
    Check(
        name="stations_in_last_snapshot",
        description="Nombre de stations dans le dernier snapshot",
        sql="""
            SELECT jsonb_array_length(payload -> 'data' -> 'stations')
            FROM raw.api_snapshots
            WHERE source = 'velib_station_status'
            ORDER BY fetched_at DESC
            LIMIT 1
        """,
        expected="entre 1400 et 1700",
        min_value=1400,
        max_value=1700,
        severity=FAIL,
    ),
    Check(
        name="negative_values",
        description="Vélos ou bornes négatifs dans le staging",
        sql="""
            SELECT count(*) FROM staging.station_status
            WHERE num_bikes_available < 0
               OR num_docks_available < 0
               OR num_mechanical < 0
               OR num_ebike < 0
        """,
        expected="= 0",
        max_value=0,
        severity=FAIL,
    ),
    Check(
        name="facts_without_station_version",
        description="Faits non rattachés à une version de la dimension",
        sql="""
            SELECT count(*) FROM core.fact_station_status
            WHERE station_sk IS NULL
              AND last_reported > now() - interval '24 hours'
        """,
        expected="= 0",
        max_value=0,
        severity=WARN, 
    ),
    Check(
        name="dim_stations_multiple_current",
        description="Stations avec plusieurs versions courantes (SCD 2 cassé)",
        sql="""
            SELECT count(*) FROM (
                SELECT station_id FROM core.dim_station
                WHERE is_current GROUP BY station_id HAVING count(*) > 1
            ) AS anomalies
        """,
        expected="= 0",
        max_value=0,
        severity=FAIL,
    ),
    Check(
        name="source_lag_minutes",
        description="Retard du last_reported le plus récent de Vélib'",
        sql="""
            SELECT extract(EPOCH FROM now() - max(last_reported)) / 60
            FROM staging.station_status
        """,
        expected="< 90 min",
        max_value=90,
        severity=WARN,
    ),
    Check(
        name="capacity_overflow_ratio",
        description="Part des stations où vélos + bornes dépassent la capacité",
        sql="""
            WITH dernier AS (
                SELECT DISTINCT ON (station_id) station_id,
                       num_bikes_available, num_docks_available
                FROM staging.station_status
                ORDER BY station_id, last_reported DESC
            )
            SELECT coalesce(
                count(*) FILTER (
                    WHERE s.num_bikes_available + s.num_docks_available > d.capacity
                )::numeric / nullif(count(*), 0), 0)
            FROM dernier AS s
            JOIN core.dim_station AS d
              ON d.station_id = s.station_id AND d.is_current
        """,
        expected="<= 15 % des stations",
        max_value=0.15,
        severity=WARN,
    ),
    Check(
        name="stale_stations",
        description="Stations muettes depuis plus de 24 h",
        sql="""
            WITH dernier AS (
                SELECT DISTINCT ON (station_id) station_id, last_reported
                FROM staging.station_status
                ORDER BY station_id, last_reported DESC
            )
            SELECT count(*) FROM dernier
            WHERE last_reported < now() - interval '24 hours'
        """,
        expected="<= 50 stations",
        max_value=50,
        severity=WARN,
    ),
)

INSERT_RESULT = """
INSERT INTO marts.quality_check_results (check_name, status, observed, expected, details)
VALUES (%(check_name)s, %(status)s, %(observed)s, %(expected)s, %(details)s)
"""


def run_checks(conn: Any, checks: tuple[Check, ...] = CHECKS) -> list[CheckResult]:
    """Exécute tous les contrôles et enregistre les résultats en base.

    Ne lève rien : c'est à l'appelant (le DAG) de décider quoi faire des
    statuts fail. On veut TOUS les résultats, pas seulement jusqu'au premier
    problème.
    """
    results: list[CheckResult] = []

    with conn.cursor() as cur:
        for check in checks:
            cur.execute(check.sql)
            row = cur.fetchone()
            observed = None if row is None or row[0] is None else float(row[0])
            status = check.evaluate(observed)

            result = CheckResult(
                name=check.name,
                status=status,
                observed=observed,
                expected=check.expected,
                details=check.description,
            )
            results.append(result)

            cur.execute(
                INSERT_RESULT,
                {
                    "check_name": result.name,
                    "status": result.status,
                    "observed": result.observed,
                    "expected": result.expected,
                    "details": result.details,
                },
            )

            log = logger.info if status == OK else logger.warning
            log(
                "Contrôle %s : %s (mesuré=%s, attendu %s)",
                result.name, status, observed, check.expected,
            )

    return results


def failures(results: list[CheckResult]) -> list[CheckResult]:
    return [r for r in results if r.status == FAIL]


def warnings(results: list[CheckResult]) -> list[CheckResult]:
    return [r for r in results if r.status == WARN]
