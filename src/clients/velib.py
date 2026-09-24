"""Client de l'API GBFS Vélib' Métropole.

Le point d'entrée est le fichier gbfs.json.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from src.clients.http import ApiError, ApiSnapshot, JsonApiClient, epoch_to_utc

logger = logging.getLogger(__name__)

GBFS_DISCOVERY_URL = (
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/gbfs.json"
)

__all__ = ["GBFS_DISCOVERY_URL", "ApiSnapshot", "VelibApiError", "VelibClient", "stations_of"]


class VelibApiError(ApiError):
    """Appel à l'API Vélib' en échec."""


def stations_of(snapshot: ApiSnapshot) -> list[dict[str, Any]]:
    """Liste des stations d'un snapshot Vélib'."""
    return snapshot.payload["data"]["stations"]


class VelibClient(JsonApiClient):
    error_class = VelibApiError

    def __init__(self, discovery_url: str = GBFS_DISCOVERY_URL, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.discovery_url = discovery_url
        self._feed_urls: dict[str, str] | None = None

    def feed_url(self, feed_name: str) -> str:
        """URL d'un flux, lue dans gbfs.json (mise en cache par instance)."""
        if self._feed_urls is None:
            payload = self.get_json(self.discovery_url)
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

    def _fetch_feed(self, feed_name: str, source: str) -> ApiSnapshot:
        payload = self.get_json(self.feed_url(feed_name))
        if not payload.get("data", {}).get("stations"):
            raise VelibApiError(f"Flux '{feed_name}' sans station exploitable")

        return ApiSnapshot(
            source=source,
            fetched_at=datetime.now(UTC),
            source_updated_at=epoch_to_utc(
                # Vélib' expose lastUpdatedOther là où GBFS attend last_updated
                payload.get("lastUpdatedOther") or payload.get("last_updated")
            ),
            payload=payload,
        )

    def fetch_station_status(self) -> ApiSnapshot:
        return self._fetch_feed("station_status", "velib_station_status")

    def fetch_station_information(self) -> ApiSnapshot:
        return self._fetch_feed("station_information", "velib_station_information")
