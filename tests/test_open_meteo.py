"""Tests du client Open-Meteo : paramètres, validation des séries, erreurs."""

from __future__ import annotations

from datetime import UTC
from urllib.parse import parse_qs, urlparse

import pytest
import responses

from src.clients.open_meteo import FORECAST_URL, OpenMeteoClient, OpenMeteoError

HOURLY_BODY = {
    "latitude": 48.86,
    "longitude": 2.36,
    "hourly_units": {"temperature_2m": "°C", "precipitation": "mm"},
    "hourly": {
        "time": [1790121600, 1790125200],
        "temperature_2m": [16.3, 17.1],
        "precipitation": [0.0, 0.4],
        "wind_speed_10m": [4.3, 5.1],
        "relative_humidity_2m": [67, 65],
    },
}


@pytest.fixture
def client():
    delays: list[float] = []
    client = OpenMeteoClient(timeout=1.0, max_attempts=3, backoff_factor=1.0, sleep=delays.append)
    client.delays = delays 
    return client


@responses.activate
def test_fetch_hourly_ok(client):
    responses.add(responses.GET, FORECAST_URL, json=HOURLY_BODY, status=200)

    snapshot = client.fetch_hourly()

    assert snapshot.source == "open_meteo_hourly"
    assert snapshot.payload == HOURLY_BODY
    assert snapshot.fetched_at.tzinfo is UTC
    assert snapshot.source_updated_at is None


@responses.activate
def test_parametres_de_la_requete(client):
    responses.add(responses.GET, FORECAST_URL, json=HOURLY_BODY, status=200)

    client.fetch_hourly(past_days=2, forecast_days=1)

    query = parse_qs(urlparse(responses.calls[0].request.url).query)
    assert query["timezone"] == ["UTC"]
    assert query["timeformat"] == ["unixtime"] 
    assert query["past_days"] == ["2"]
    assert query["latitude"] == ["48.8566"]
    assert "precipitation" in query["hourly"][0]


@responses.activate
def test_serie_horaire_vide_rejetee(client):
    responses.add(responses.GET, FORECAST_URL, json={"hourly": {"time": []}}, status=200)

    with pytest.raises(OpenMeteoError, match="sans série horaire"):
        client.fetch_hourly()


@responses.activate
def test_series_de_longueurs_differentes_rejetees(client):
    """Tableaux parallèles : une variable plus courte fausserait le recollage."""
    body = {
        "hourly": {
            "time": [1790121600, 1790125200],
            "temperature_2m": [16.3],
            "precipitation": [0.0, 0.4],
            "wind_speed_10m": [4.3, 5.1],
            "relative_humidity_2m": [67, 65],
        }
    }
    responses.add(responses.GET, FORECAST_URL, json=body, status=200)

    with pytest.raises(OpenMeteoError, match="temperature_2m.*incohérente"):
        client.fetch_hourly()


@responses.activate
def test_variable_absente_rejetee(client):
    body = {"hourly": {"time": [1790121600], "temperature_2m": [16.3]}}
    responses.add(responses.GET, FORECAST_URL, json=body, status=200)

    with pytest.raises(OpenMeteoError, match="incohérente"):
        client.fetch_hourly()


@responses.activate
def test_retry_puis_succes(client):
    responses.add(responses.GET, FORECAST_URL, json={}, status=503)
    responses.add(responses.GET, FORECAST_URL, json=HOURLY_BODY, status=200)

    assert client.fetch_hourly().payload == HOURLY_BODY
    assert client.delays == [1.0]


@responses.activate
def test_echec_definitif(client):
    for _ in range(3):
        responses.add(responses.GET, FORECAST_URL, json={}, status=500)

    with pytest.raises(OpenMeteoError, match="après 3 tentatives"):
        client.fetch_hourly()
