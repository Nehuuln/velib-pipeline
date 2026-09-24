"""Tests du client Vélib' : découverte, retries, backoff, erreurs, horodatages."""

from __future__ import annotations

from datetime import UTC

import pytest
import requests
import responses

from src.clients.velib import GBFS_DISCOVERY_URL, VelibApiError, VelibClient

STATUS_URL = "https://example.test/station_status.json"
INFO_URL = "https://example.test/station_information.json"

DISCOVERY_BODY = {
    "data": {
        "en": {
            "feeds": [
                {"name": "station_status", "url": STATUS_URL},
                {"name": "station_information", "url": INFO_URL},
            ]
        }
    }
}

STATUS_BODY = {
    "lastUpdatedOther": 1789997237,
    "ttl": 3600,
    "data": {
        "stations": [
            {
                "station_id": 213688169,
                "stationCode": "16107",
                "num_bikes_available": 26,
                "num_bikes_available_types": [{"mechanical": 12}, {"ebike": 14}],
                "num_docks_available": 8,
                "is_installed": 1,
                "is_renting": 1,
                "is_returning": 1,
                "last_reported": 1789994841,
            }
        ]
    },
}


@pytest.fixture
def client():
    """Client sans attente réelle : le sleep est remplacé par un enregistreur."""
    delays: list[float] = []
    client = VelibClient(
        timeout=1.0,
        max_attempts=4,
        backoff_factor=1.0,
        sleep=delays.append,
    )
    client.delays = delays 
    return client


def add_discovery():
    responses.add(responses.GET, GBFS_DISCOVERY_URL, json=DISCOVERY_BODY, status=200)


@responses.activate
def test_fetch_station_status_ok(client):
    add_discovery()
    responses.add(responses.GET, STATUS_URL, json=STATUS_BODY, status=200)

    snapshot = client.fetch_station_status()

    assert snapshot.source == "velib_station_status"
    assert len(snapshot.stations) == 1
    assert snapshot.payload == STATUS_BODY
    assert snapshot.fetched_at.tzinfo is UTC
    assert snapshot.source_updated_at.tzinfo is UTC
    assert snapshot.source_updated_at.isoformat() == "2026-09-21T13:27:17+00:00"


@responses.activate
def test_discovery_appelee_une_seule_fois(client):
    add_discovery()
    responses.add(responses.GET, STATUS_URL, json=STATUS_BODY, status=200)
    responses.add(responses.GET, INFO_URL, json=STATUS_BODY, status=200)

    client.fetch_station_status()
    client.fetch_station_information()

    discovery_calls = [c for c in responses.calls if c.request.url == GBFS_DISCOVERY_URL]
    assert len(discovery_calls) == 1


@responses.activate
def test_retry_puis_succes_avec_backoff_exponentiel(client):
    add_discovery()
    responses.add(responses.GET, STATUS_URL, json={}, status=503)
    responses.add(responses.GET, STATUS_URL, body=requests.ConnectionError("coupure"))
    responses.add(responses.GET, STATUS_URL, json=STATUS_BODY, status=200)

    snapshot = client.fetch_station_status()

    assert len(snapshot.stations) == 1
    assert client.delays == [1.0, 2.0]


@responses.activate
def test_echec_definitif_apres_max_attempts(client):
    add_discovery()
    for _ in range(4):
        responses.add(responses.GET, STATUS_URL, json={}, status=503)

    with pytest.raises(VelibApiError, match="après 4 tentatives"):
        client.fetch_station_status()

    assert len(client.delays) == 3 


@responses.activate
def test_timeout_est_retente(client):
    add_discovery()
    responses.add(responses.GET, STATUS_URL, body=requests.Timeout("trop lent"))
    responses.add(responses.GET, STATUS_URL, json=STATUS_BODY, status=200)

    assert client.fetch_station_status().stations
    assert client.delays == [1.0]


@responses.activate
def test_erreur_404_non_retentee(client):
    add_discovery()
    responses.add(responses.GET, STATUS_URL, json={}, status=404)

    with pytest.raises(VelibApiError, match="HTTP définitif"):
        client.fetch_station_status()

    assert client.delays == []


@responses.activate
def test_flux_vide_rejete(client):
    add_discovery()
    responses.add(responses.GET, STATUS_URL, json={"data": {"stations": []}}, status=200)

    with pytest.raises(VelibApiError, match="sans station"):
        client.fetch_station_status()


@responses.activate
def test_flux_inconnu_dans_la_decouverte(client):
    add_discovery()

    with pytest.raises(VelibApiError, match="absent de gbfs.json"):
        client.feed_url("free_bike_status")


@responses.activate
def test_json_invalide_est_retente(client):
    add_discovery()
    responses.add(responses.GET, STATUS_URL, body="<html>maintenance</html>", status=200)
    responses.add(responses.GET, STATUS_URL, json=STATUS_BODY, status=200)

    assert client.fetch_station_status().stations
    assert client.delays == [1.0]


@responses.activate
def test_horodatage_source_absent(client):
    add_discovery()
    body = {"data": STATUS_BODY["data"]} 
    responses.add(responses.GET, STATUS_URL, json=body, status=200)

    assert client.fetch_station_status().source_updated_at is None
