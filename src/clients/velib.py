"""Client de l'API GBFS Vélib' Métropole.

Le point d'entrée est le fichier gbfs.json.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)

GBFS_DISCOVERY_URL = (
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/gbfs.json"
)

# Codes HTTP qui valent la peine d'être retentés (surcharge côté serveur)
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class VelibApiError(RuntimeError):
    """Appel à l'API Vélib' en échec."""


@dataclass(frozen=True)
class ApiSnapshot:
    """Réponse API, prête à être insérée dans raw.api_snapshots."""

    source: str
    fetched_at: datetime
    source_updated_at: datetime | None
    payload: dict[str, Any]

    @property
    def stations(self) -> list[dict[str, Any]]:
        return self.payload["data"]["stations"]


class VelibClient:
    def __init__(
        self,
        discovery_url: str = GBFS_DISCOVERY_URL,
        timeout: float = 10.0,
        max_attempts: int = 4,
        backoff_factor: float = 1.0,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts doit valoir au moins 1")
        self.discovery_url = discovery_url
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.backoff_factor = backoff_factor
        self.session = session or requests.Session()
        self._sleep = sleep
        self._feed_urls: dict[str, str] | None = None

    # Appels HTTP

    def _get_json(self, url: str) -> dict[str, Any]:
        """GET avec timeout, retries et backoff exponentiel.

        Les erreurs 4xx (hors 429) ne sont pas retentées : réessayer une
        requête invalide ne la rendra pas valide.
        """
        last_error: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                if response.status_code in RETRYABLE_STATUS:
                    raise VelibApiError(
                        f"HTTP {response.status_code} sur {url}"
                    )
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, VelibApiError, ValueError) as exc:
                if isinstance(exc, requests.HTTPError):
                    raise VelibApiError(f"HTTP définitif sur {url} : {exc}") from exc
                last_error = exc
                if attempt == self.max_attempts:
                    break
                delay = self.backoff_factor * 2 ** (attempt - 1)
                logger.warning(
                    "Tentative %s/%s échouée sur %s (%s), nouvel essai dans %.1fs",
                    attempt, self.max_attempts, url, exc, delay,
                )
                self._sleep(delay)

        raise VelibApiError(
            f"Échec après {self.max_attempts} tentatives sur {url} : {last_error}"
        ) from last_error

    # Découverte des flux

    def feed_url(self, feed_name: str) -> str:
        """URL d'un flux, lue dans gbfs.json."""
        if self._feed_urls is None:
            payload = self._get_json(self.discovery_url)
            try:
                feeds = payload["data"]["en"]["feeds"]
                self._feed_urls = {feed["name"]: feed["url"] for feed in feeds}
            except (KeyError, TypeError) as exc:
                raise VelibApiError(f"gbfs.json inattendu : {payload!r:.200}") from exc

        try:
            return self._feed_urls[feed_name]
        except KeyError:
            raise VelibApiError(
                f"Flux '{feed_name}' absent de gbfs.json "
                f"(disponibles : {sorted(self._feed_urls)})"
            ) from None

    # Flux

    def _fetch_feed(self, feed_name: str, source: str) -> ApiSnapshot:
        payload = self._get_json(self.feed_url(feed_name))
        stations = payload.get("data", {}).get("stations")
        if not stations:
            raise VelibApiError(f"Flux '{feed_name}' sans station exploitable")

        return ApiSnapshot(
            source=source,
            fetched_at=datetime.now(UTC),
            source_updated_at=_epoch_to_utc(
                payload.get("lastUpdatedOther") or payload.get("last_updated")
            ),
            payload=payload,
        )

    def fetch_station_status(self) -> ApiSnapshot:
        return self._fetch_feed("station_status", "velib_station_status")

    def fetch_station_information(self) -> ApiSnapshot:
        return self._fetch_feed("station_information", "velib_station_information")


def _epoch_to_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=UTC)
    except (TypeError, ValueError, OSError, OverflowError):
        logger.warning("Horodatage source illisible : %r", value)
        return None
