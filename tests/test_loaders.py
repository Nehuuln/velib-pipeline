"""Tests du chargeur Postgres, avec une fausse connexion."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from src.clients.http import ApiSnapshot
from src.loaders.postgres import (
    insert_snapshot,
    read_sql,
    transform_station_status,
)


class FakeCursor:
    """Enregistre les requêtes exécutées et renvoie des résultats."""

    def __init__(self, fetch_result=None, rowcount=0):
        self.executed: list[tuple[str, dict]] = []
        self._fetch_result = fetch_result
        self.rowcount = rowcount

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._fetch_result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


SNAPSHOT = ApiSnapshot(
    source="velib_station_status",
    fetched_at=datetime(2026, 9, 21, 13, 27, 17, tzinfo=UTC),
    source_updated_at=datetime(2026, 9, 21, 13, 27, 0, tzinfo=UTC),
    payload={"data": {"stations": [{"station_id": 1}]}},
)


def test_insert_snapshot_renvoie_id():
    cur = FakeCursor(fetch_result=(42,))

    assert insert_snapshot(FakeConnection(cur), SNAPSHOT) == 42

    sql, params = cur.executed[0]
    assert "ON CONFLICT (source, payload_md5) DO NOTHING" in sql
    assert params["source"] == "velib_station_status"
    assert params["fetched_at"].tzinfo is UTC
    assert json.loads(params["payload"]) == SNAPSHOT.payload


def test_insert_snapshot_doublon_renvoie_none():
    """RETURNING ne renvoie rien quand ON CONFLICT DO NOTHING a joué."""
    cur = FakeCursor(fetch_result=None)

    assert insert_snapshot(FakeConnection(cur), SNAPSHOT) is None


def test_transform_station_status_renvoie_nombre_de_lignes():
    cur = FakeCursor(rowcount=1518)

    assert transform_station_status(FakeConnection(cur), 42) == 1518

    sql, params = cur.executed[0]
    assert params == {"snapshot_id": 42}
    assert "staging.station_status" in sql


def test_le_fichier_sql_de_transformation_est_lisible():
    sql = read_sql("transform/staging_station_status.sql")

    assert "ON CONFLICT (station_id, last_reported) DO NOTHING" in sql
    assert "%(snapshot_id)s" in sql
