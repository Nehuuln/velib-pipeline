"""Client Open-Meteo : météo horaire de Paris. API publique, sans clé.

On demande les heures passées ET à venir.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from src.clients.http import ApiError, ApiSnapshot, JsonApiClient

logger = logging.getLogger(__name__)

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

PARIS_LAT = 48.8566
PARIS_LON = 2.3522

HOURLY_VARIABLES = (
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "relative_humidity_2m",
)


class OpenMeteoError(ApiError):
    """Appel à Open-Meteo en échec."""


class OpenMeteoClient(JsonApiClient):
    error_class = OpenMeteoError

    def __init__(
        self,
        latitude: float = PARIS_LAT,
        longitude: float = PARIS_LON,
        url: str = FORECAST_URL,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.latitude = latitude
        self.longitude = longitude
        self.url = url

    def fetch_hourly(self, past_days: int = 1, forecast_days: int = 1) -> ApiSnapshot:
        payload = self.get_json(
            self.url,
            params={
                "latitude": self.latitude,
                "longitude": self.longitude,
                "hourly": ",".join(HOURLY_VARIABLES),
                "timezone": "UTC",
                "timeformat": "unixtime", 
                "past_days": past_days,
                "forecast_days": forecast_days,
            },
        )

        hourly = payload.get("hourly") or {}
        times = hourly.get("time") or []
        if not times:
            raise OpenMeteoError("Réponse Open-Meteo sans série horaire")

        for variable in HOURLY_VARIABLES:
            values = hourly.get(variable)
            if values is None or len(values) != len(times):
                raise OpenMeteoError(
                    f"Série '{variable}' incohérente : "
                    f"{0 if values is None else len(values)} valeurs pour {len(times)} heures"
                )

        logger.info("Météo récupérée : %s heures", len(times))
        return ApiSnapshot(
            source="open_meteo_hourly",
            fetched_at=datetime.now(UTC),
            source_updated_at=None,
            payload=payload,
        )
