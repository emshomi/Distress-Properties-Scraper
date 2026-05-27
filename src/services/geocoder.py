"""
Geocoder service.

Resolves a street address into (lat, lng) coordinates. Uses Mapbox as
the preferred provider (better accuracy) with Nominatim as a free
fallback.

Nominatim has a strict 1 req/sec rate limit (OpenStreetMap policy);
we enforce it with an in-process lock. Mapbox has much higher limits
(600/min default) and doesn't require rate limiting at our scale.

Results are cached in core.parcels via the lat/lng fields — once geocoded,
the parcel keeps its coordinates. Re-geocoding only happens after
GEOCODING_CACHE_DAYS or if the address changes.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from src.config import settings
from src.utils.logger import logger


# Nominatim is rate-limited to 1 req/sec. We use 1.1s to be safe.
_NOMINATIM_MIN_INTERVAL_SECONDS: float = 1.1

# In-process lock + last-request-time for Nominatim
_NOMINATIM_LOCK = asyncio.Lock()
_NOMINATIM_LAST_REQUEST_TIME: float = 0.0


async def _geocode_mapbox(address: str) -> tuple[float, float] | None:
    """
    Geocode via Mapbox Geocoding API v6.

    Returns (lat, lng) or None if geocoding failed or no match.
    """
    if settings.mapbox_token is None:
        return None

    token = settings.mapbox_token.get_secret_value()
    url = "https://api.mapbox.com/search/geocode/v6/forward"
    params = {
        "q": address,
        "access_token": token,
        "limit": 1,
        "country": "us",
        "proximity": "-93.265,44.977",  # Minneapolis center
    }

    try:
        async with httpx.AsyncClient(
            timeout=settings.scraper_request_timeout_seconds
        ) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPError as e:
        logger.warning("Mapbox geocode failed", address=address, error=str(e))
        return None

    features = data.get("features") or []
    if not features:
        return None

    # GeoJSON returns [lng, lat] — we want (lat, lng)
    coords = features[0].get("geometry", {}).get("coordinates")
    if not coords or len(coords) < 2:
        return None
    lng, lat = coords[0], coords[1]
    return (float(lat), float(lng))


async def _geocode_nominatim(address: str) -> tuple[float, float] | None:
    """
    Geocode via Nominatim (OpenStreetMap).

    Enforces 1.1s minimum between requests (process-wide).
    Returns (lat, lng) or None if geocoding failed or no match.
    """
    global _NOMINATIM_LAST_REQUEST_TIME

    async with _NOMINATIM_LOCK:
        elapsed = time.monotonic() - _NOMINATIM_LAST_REQUEST_TIME
        if elapsed < _NOMINATIM_MIN_INTERVAL_SECONDS:
            sleep_for = _NOMINATIM_MIN_INTERVAL_SECONDS - elapsed
            await asyncio.sleep(sleep_for)

        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": address,
            "format": "json",
            "limit": 1,
            "countrycodes": "us",
            "addressdetails": 0,
        }
        headers = {
            "User-Agent": settings.nominatim_user_agent,
            "Accept": "application/json",
        }

        try:
            async with httpx.AsyncClient(
                timeout=settings.scraper_request_timeout_seconds
            ) as client:
                response = await client.get(url, params=params, headers=headers)
                _NOMINATIM_LAST_REQUEST_TIME = time.monotonic()
                response.raise_for_status()
                data: list[dict[str, Any]] = response.json()
        except httpx.HTTPError as e:
            logger.warning("Nominatim geocode failed", address=address, error=str(e))
            return None

        if not data:
            return None

        try:
            lat = float(data[0]["lat"])
            lng = float(data[0]["lon"])
            return (lat, lng)
        except (KeyError, ValueError, TypeError):
            return None


async def geocode_address(address: str) -> tuple[float, float] | None:
    """
    Resolve an address to (lat, lng). Tries Mapbox first, then Nominatim.

    Returns None if both providers fail or geocoding is disabled.
    """
    if not settings.geocoding_enabled:
        return None
    if not address or not address.strip():
        return None

    # Prefer Mapbox if configured
    if settings.mapbox_token is not None:
        coords = await _geocode_mapbox(address)
        if coords is not None:
            return coords

    # Fallback to Nominatim
    coords = await _geocode_nominatim(address)
    return coords


__all__ = ["geocode_address"]
