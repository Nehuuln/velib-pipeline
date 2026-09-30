"""Tests des contrôles qualité : logique de seuils et enregistrement."""

from __future__ import annotations

import pytest

from src.quality.checks import (
    CHECKS,
    FAIL,
    OK,
    WARN,
    Check,
    CheckResult,
    failures,
    run_checks,
    warnings,
)


class FakeCursor:
    """Renvoie les valeurs mesurées une par une, et note les insertions."""

    def __init__(self, observed_values):
        self._values = list(observed_values)
        self.inserts: list[dict] = []

    def execute(self, sql, params=None):
        if params is not None: 
            self.inserts.append(params)
        else:  
            self._current = self._values.pop(0)

    def fetchone(self):
        return self._current

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


MAX_5 = Check(name="max_5", sql="SELECT 1", expected="<= 5", max_value=5)
MIN_10 = Check(name="min_10", sql="SELECT 1", expected=">= 10", min_value=10)
FOURCHETTE = Check(
    name="entre_2_et_4", sql="SELECT 1", expected="2 à 4", min_value=2, max_value=4
)
AVERTISSEMENT = Check(
    name="souple", sql="SELECT 1", expected="<= 1", max_value=1, severity=WARN
)


@pytest.mark.parametrize(
    ("check", "observed", "attendu"),
    [
        (MAX_5, 3, OK),
        (MAX_5, 5, OK), 
        (MAX_5, 6, FAIL),
        (MIN_10, 10, OK),
        (MIN_10, 9, FAIL),
        (FOURCHETTE, 3, OK),
        (FOURCHETTE, 1, FAIL),
        (FOURCHETTE, 5, FAIL),
        (AVERTISSEMENT, 2, WARN),
        (MAX_5, None, FAIL),  
    ],
)
def test_evaluation_des_seuils(check, observed, attendu):
    assert check.evaluate(observed) == attendu


def test_run_checks_execute_tout_et_enregistre():
    cur = FakeCursor([(3,), (12,)])

    results = run_checks(FakeConnection(cur), checks=(MAX_5, MIN_10))

    assert [r.status for r in results] == [OK, OK]
    assert [r.observed for r in results] == [3.0, 12.0]
    assert [i["check_name"] for i in cur.inserts] == ["max_5", "min_10"]
    assert cur.inserts[0]["status"] == OK


def test_run_checks_ne_sarrete_pas_au_premier_echec():
    """On veut le bilan complet, pas seulement le premier problème."""
    cur = FakeCursor([(99,), (12,)])

    results = run_checks(FakeConnection(cur), checks=(MAX_5, MIN_10))

    assert [r.status for r in results] == [FAIL, OK]
    assert len(cur.inserts) == 2


def test_valeur_nulle_traitee_comme_un_echec():
    """Aucune donnée mesurée signifie souvent une table vide : c'est grave."""
    cur = FakeCursor([(None,)])

    results = run_checks(FakeConnection(cur), checks=(MAX_5,))

    assert results[0].status == FAIL
    assert results[0].observed is None


def test_tri_des_resultats():
    results = [
        CheckResult("a", OK, 1, "", ""),
        CheckResult("b", WARN, 2, "", ""),
        CheckResult("c", FAIL, 3, "", ""),
    ]

    assert [r.name for r in failures(results)] == ["c"]
    assert [r.name for r in warnings(results)] == ["b"]


def test_les_controles_reels_sont_bien_declares():
    noms = [c.name for c in CHECKS]

    assert len(noms) == len(set(noms)), "noms de contrôles dupliqués"
    for check in CHECKS:
        assert check.severity in (WARN, FAIL)
        assert check.expected, f"{check.name} doit décrire son seuil"
        assert check.description, f"{check.name} doit être documenté"
        assert check.min_value is not None or check.max_value is not None
        assert "select" in check.sql.lower()


def test_les_anomalies_de_la_source_ne_bloquent_pas_le_pipeline():
    """Décalage Vélib', capacités fausses, stations muettes : warn, pas fail."""
    par_nom = {c.name: c for c in CHECKS}

    for name in ("source_lag_minutes", "capacity_overflow_ratio", "stale_stations"):
        assert par_nom[name].severity == WARN


def test_la_sante_du_pipeline_fait_echouer_le_dag():
    par_nom = {c.name: c for c in CHECKS}

    for name in (
        "pipeline_freshness_minutes",
        "negative_values",
        "dim_stations_multiple_current",
    ):
        assert par_nom[name].severity == FAIL
