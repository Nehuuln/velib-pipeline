"""Socle HTTP commun aux clients d'API : timeouts, retries, backoff.

Chaque source (Vélib', Open-Meteo, PRIM) hérite de JsonApiClient et se
contente de décrire ses endpoints.
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

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class ApiError(RuntimeError):
    """Appel à une API en échec définitif."""


@dataclass(frozen=True)
class ApiSnapshot:
    """Réponse API, prête à être insérée dans raw.api_snapshots."""

    source: str
    fetched_at: datetime
    source_updated_at: datetime | None
    payload: dict[str, Any]


class JsonApiClient:
    """Client HTTP JSON avec retries et backoff exponentiel."""

    error_class: type[ApiError] = ApiError

    def __init__(
        self,
        timeout: float = 10.0,
        max_attempts: int = 4,
        backoff_factor: float = 1.0,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts doit valoir au moins 1")
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.backoff_factor = backoff_factor
        self.session = session or requests.Session()
        self._sleep = sleep

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET avec timeout, retries et backoff exponentiel.

        Les erreurs 4xx (hors 429) ne sont pas retentées : réessayer une
        requête invalide ne la rendra pas valide.
        """
        last_error: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                if response.status_code in RETRYABLE_STATUS:
                    raise self.error_class(f"HTTP {response.status_code} sur {url}")
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ApiError, ValueError) as exc:
                if isinstance(exc, requests.HTTPError):
                    raise self.error_class(f"HTTP définitif sur {url} : {exc}") from exc
                last_error = exc
                if attempt == self.max_attempts:
                    break
                delay = self.backoff_factor * 2 ** (attempt - 1)
                logger.warning(
                    "Tentative %s/%s échouée sur %s (%s), nouvel essai dans %.1fs",
                    attempt, self.max_attempts, url, exc, delay,
                )
                self._sleep(delay)

        raise self.error_class(
            f"Échec après {self.max_attempts} tentatives sur {url} : {last_error}"
        ) from last_error


def epoch_to_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=UTC)
    except (TypeError, ValueError, OSError, OverflowError):
        logger.warning("Horodatage source illisible : %r", value)
        return None
