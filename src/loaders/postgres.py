"""Écriture des snapshots API dans Postgres.

Les fonctions reçoivent une connexion ouverte. 
"""

from __future__ import annotations

import json
import logging
from functools import cache
from pathlib import Path
from typing import Any

from src.clients.http import ApiSnapshot

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"
REFRESHABLE_VIEWS = frozenset({"marts.station_hourly_usage"})

INSERT_SNAPSHOT = """
INSERT INTO raw.api_snapshots (source, fetched_at, source_updated_at, payload)
VALUES (%(source)s, %(fetched_at)s, %(source_updated_at)s, %(payload)s)
ON CONFLICT (source, payload_md5) DO NOTHING
RETURNING snapshot_id
"""


@cache
def read_sql(relative_path: str) -> str:
    """Lit un fichier SQL du dossier sql/."""
    return (SQL_DIR / relative_path).read_text(encoding="utf-8")


def insert_snapshot(conn: Any, snapshot: ApiSnapshot) -> int | None:
    """Insère la réponse brute. Renvoie None si elle est déjà en base."""
    with conn.cursor() as cur:
        cur.execute(
            INSERT_SNAPSHOT,
            {
                "source": snapshot.source,
                "fetched_at": snapshot.fetched_at,
                "source_updated_at": snapshot.source_updated_at,
                "payload": json.dumps(snapshot.payload),
            },
        )
        row = cur.fetchone()

    if row is None:
        logger.info("Snapshot %s déjà présent en base, rien à insérer", snapshot.source)
        return None

    snapshot_id = row[0]
    logger.info("Snapshot %s inséré (snapshot_id=%s)", snapshot.source, snapshot_id)
    return snapshot_id


def transform_station_status(conn: Any, snapshot_id: int) -> int:
    """raw -> staging.station_status. Renvoie le nombre de lignes insérées.

    Idempotent : ON CONFLICT DO NOTHING sur (station_id, last_reported).
    """
    with conn.cursor() as cur:
        cur.execute(
            read_sql("transform/staging_station_status.sql"),
            {"snapshot_id": snapshot_id},
        )
        inserted = cur.rowcount

    logger.info("staging.station_status : %s ligne(s) insérée(s)", inserted)
    return inserted


def transform_weather_hourly(conn: Any, snapshot_id: int) -> int:
    """raw -> staging.weather_hourly. Renvoie le nombre de lignes écrites.

    Une heure déjà connue n'est mise à jour que si l'appel est plus récent
    (une prévision devient une observation).
    """
    with conn.cursor() as cur:
        cur.execute(
            read_sql("transform/staging_weather_hourly.sql"),
            {"snapshot_id": snapshot_id},
        )
        written = cur.rowcount

    logger.info("staging.weather_hourly : %s ligne(s) écrite(s)", written)
    return written


def transform_dim_station(conn: Any, snapshot_id: int) -> int:
    """raw -> core.dim_station (SCD type 2), en deux étapes.

    D'abord fermer les versions obsolètes, ensuite ouvrir les nouvelles.
    L'ordre compte, et les deux doivent partager la même transaction, sinon
    une station peut se retrouver sans version courante.

    Renvoie le nombre de nouvelles versions créées : 0 signifie qu'aucune
    station n'a changé depuis le snapshot précédent.
    """
    params = {"snapshot_id": snapshot_id}
    with conn.cursor() as cur:
        cur.execute(read_sql("transform/core_dim_station_close.sql"), params)
        closed = cur.rowcount
        cur.execute(read_sql("transform/core_dim_station_insert.sql"), params)
        opened = cur.rowcount

    logger.info("core.dim_station : %s version(s) fermée(s), %s ouverte(s)", closed, opened)
    return opened


def transform_fact_station_status(conn: Any) -> int:
    """staging -> core.fact_station_status, en incrémental.

    Pas de paramètre : le SQL repart du fait le plus récent, moins une heure
    de marge. Idempotent grâce à ON CONFLICT DO NOTHING.
    """
    with conn.cursor() as cur:
        cur.execute(read_sql("transform/core_fact_station_status.sql"))
        inserted = cur.rowcount

    logger.info("core.fact_station_status : %s ligne(s) insérée(s)", inserted)
    return inserted


def refresh_materialized_view(conn: Any, view: str, concurrently: bool = True) -> None:
    """Rafraîchit une vue matérialisée des marts.

    CONCURRENTLY ne bloque pas les lectures (dashboards Grafana), mais exige
    un index unique et une vue déjà peuplée : au premier rafraîchissement, il
    faut donc passer concurrently=False.
    """
    if view not in REFRESHABLE_VIEWS:
        raise ValueError(f"Vue inconnue : {view}")

    option = "CONCURRENTLY " if concurrently else ""
    with conn.cursor() as cur:
        cur.execute(f"REFRESH MATERIALIZED VIEW {option}{view}")

    logger.info("Vue %s rafraîchie (concurrently=%s)", view, concurrently)
