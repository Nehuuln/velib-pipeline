"""Écriture des snapshots API dans Postgres.

Les fonctions reçoivent une connexion ouverte. 
"""

from __future__ import annotations

import json
import logging
from functools import cache
from pathlib import Path
from typing import Any

from src.clients.velib import ApiSnapshot

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"

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
