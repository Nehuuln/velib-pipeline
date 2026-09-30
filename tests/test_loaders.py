"""Tests du chargeur Postgres, avec une fausse connexion."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from src.clients.http import ApiSnapshot
from src.loaders.postgres import (
    insert_snapshot,
    read_sql,
    record_metric,
    refresh_materialized_view,
    transform_dim_station,
    transform_fact_station_status,
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


def test_transform_dim_station_execute_les_deux_etapes_dans_le_bon_ordre():
    """Fermer avant d'ouvrir : sinon une station perd sa version courante."""
    cur = FakeCursor(rowcount=3)

    assert transform_dim_station(FakeConnection(cur), 42) == 3

    assert len(cur.executed) == 2
    close_sql, close_params = cur.executed[0]
    insert_sql, insert_params = cur.executed[1]
    assert "UPDATE core.dim_station" in close_sql
    assert "INSERT INTO core.dim_station" in insert_sql
    assert close_params == insert_params == {"snapshot_id": 42}


def test_le_sql_du_scd2_arrondit_les_coordonnees():
    """Sans arrondi, les 14 décimales de l'API créent une version par run."""
    for name in ("core_dim_station_close.sql", "core_dim_station_insert.sql"):
        assert "::numeric(9, 6)" in read_sql(f"transform/{name}")


def test_le_sql_de_fermeture_refuse_un_flux_vide():
    """Garde-fou : un flux vide ne doit pas retirer toutes les stations."""
    assert "EXISTS (SELECT 1 FROM incoming)" in read_sql(
        "transform/core_dim_station_close.sql"
    )


def test_transform_fact_station_status_sans_parametre():
    """Le curseur d'avancement est calculé en SQL, pas passé par Airflow."""
    cur = FakeCursor(rowcount=1519)

    assert transform_fact_station_status(FakeConnection(cur)) == 1519

    sql, params = cur.executed[0]
    assert params is None
    assert "ON CONFLICT (station_id, last_reported) DO NOTHING" in sql


def test_le_sql_des_faits_rattache_la_version_de_la_dimension():
    sql = read_sql("transform/core_fact_station_status.sql")

    assert "FROM core.dim_station AS d" in sql
    assert "coalesce(d.valid_to, 'infinity'::timestamptz)" in sql


def test_refresh_materialized_view():
    cur = FakeCursor()

    refresh_materialized_view(FakeConnection(cur), "marts.station_hourly_usage")

    assert cur.executed[0][0] == (
        "REFRESH MATERIALIZED VIEW CONCURRENTLY marts.station_hourly_usage"
    )


def test_refresh_materialized_view_refuse_une_vue_inconnue():
    """Le nom part dans du SQL non paramétrable : liste blanche obligatoire."""
    cur = FakeCursor()

    with pytest.raises(ValueError, match="Vue inconnue"):
        refresh_materialized_view(FakeConnection(cur), "marts.station_hourly_usage; DROP TABLE")

    assert cur.executed == []


def test_record_metric():
    cur = FakeCursor()

    record_metric(
        FakeConnection(cur),
        dag_id="transform_core_marts",
        task_id="load_fact_station_status",
        run_id="manual__2026-09-30",
        rows_written=1519,
        duration_ms=842,
    )

    sql, params = cur.executed[0]
    assert "marts.pipeline_metrics" in sql
    assert params["rows_written"] == 1519
    assert params["duration_ms"] == 842
