"""
ArcGIS Feature Service base scraper.

Minnesota cities and counties have standardized on ArcGIS Hub for their open
data portals. This base class handles the common ArcGIS REST API patterns:

  - Pagination via resultOffset / resultRecordCount
  - Geometry extraction (lat/lng from feature.geometry)
  - Attribute extraction (the actual record fields are under feature.attributes)
  - Server-side max-record-count discovery
  - Date field handling (ArcGIS returns dates as milliseconds since epoch)
  - Retry logic for transient HTTP failures

Subclasses must:
  - Set source_name (e.g., 'saint_paul_vacant')
  - Set feature_service_url (the /FeatureServer/{layer_id} URL — no /query suffix)
  - Set county_code (used by parcel_id normalizer)
  - Implement parse_feature(attributes, geometry) → typed signal model
  - Implement write(signals) → (new, updated, failed)

The base class handles fetch() and the run() lifecycle.

For large datasets (e.g., Hennepin's 448K parcels), subclasses can override:
  - page_size:         records per HTTP request (default 1000)
  - max_pages:         hard cap on total pages (default 100 = 100K records)
  - progress_log_every: log progress every N records (default 5000)
  - max_records_override: optional runtime cap (set via run metadata)
"""

from __future__ import annotations

from abc import abstractmethod
from datetime import datetime, timezone
from typing import Any, ClassVar, Generic, TypeVar

import httpx

from src.config import settings
from src.scrapers.base_scraper import BaseScraper
from src.utils.errors import ParseError, SourceUnavailableError
from src.utils.logger import logger
from src.utils.retry import retry_on_transient


# Type variable for the typed signal that subclasses produce
SIGNAL = TypeVar("SIGNAL")


# ArcGIS standard query params we always send
_DEFAULT_PAGE_SIZE: int = 1000
_DEFAULT_MAX_PAGES: int = 100  # Safety cap — 100k records max per run by default
_DEFAULT_PROGRESS_LOG_EVERY: int = 5000


def arcgis_date_to_iso(value: Any) -> str | None:
    """
    Convert an ArcGIS date value (milliseconds since epoch) to ISO 8601 string.

    ArcGIS REST APIs return dates as integers representing milliseconds since
    the Unix epoch. We convert to UTC ISO 8601 for storage.

    Returns None for null/invalid values.
    """
    if value is None:
        return None
    try:
        # Some endpoints return strings, some return ints
        ms = int(value)
        if ms <= 0:
            return None
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def arcgis_date_to_date_only(value: Any) -> str | None:
    """
    Convert an ArcGIS date value to a date-only string (YYYY-MM-DD).

    Useful when the source date doesn't carry meaningful time info (e.g., a
    "vacant_as_of" date that's logically a calendar date).
    """
    iso = arcgis_date_to_iso(value)
    if iso is None:
        return None
    return iso[:10]  # YYYY-MM-DD


class BaseArcGISScraper(BaseScraper[dict[str, Any], Any], Generic[SIGNAL]):
    """
    Abstract base class for ArcGIS Feature Service scrapers.

    Subclasses set feature_service_url and implement parse_feature() + write().
    """

    # ---- Class-level config (subclasses override) ----

    # Full URL to the FeatureServer layer, NOT including '/query'
    # Example: 'https://services1.arcgis.com/.../FeatureServer/0'
    feature_service_url: ClassVar[str] = ""

    # County code for parcel_id normalization (e.g., 'ramsey', 'hennepin')
    county_code: ClassVar[str] = ""

    # WHERE clause for the ArcGIS query (default: all records)
    # Subclasses can override to filter (e.g., "STATUS = 'Active'")
    where_clause: ClassVar[str] = "1=1"

    # Whether to fetch geometry (lat/lng). Most signal scrapers want this.
    return_geometry: ClassVar[bool] = True

    # Specific fields to fetch. Default '*' = all fields.
    # Subclasses can narrow this if a service has dozens of irrelevant fields.
    out_fields: ClassVar[str] = "*"

    # Records per HTTP request. ArcGIS services typically cap at 1000-2000.
    page_size: ClassVar[int] = _DEFAULT_PAGE_SIZE

    # Maximum number of pages to fetch. Safety cap.
    # Override in subclasses for large datasets (e.g., 500 for ~500K records).
    max_pages: ClassVar[int] = _DEFAULT_MAX_PAGES

    # Log progress every N records during fetch.
    progress_log_every: ClassVar[int] = _DEFAULT_PROGRESS_LOG_EVERY

    # ---- Runtime override (set per-run via metadata) ----
    # If set to a positive int, the scraper stops fetching after this many records.
    # Useful for test runs (e.g., max_records=100 for initial validation).
    # Set on the instance, not the class.
    _max_records_override: int | None = None

    # ---- Abstract methods subclasses must implement ----

    @abstractmethod
    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> SIGNAL | None:
        """
        Convert one ArcGIS feature into the typed signal model.

        Args:
            attributes: dict of field_name → value from feature.attributes
            geometry:   dict from feature.geometry (or None if not requested)

        Return None to skip this row (e.g., missing required field).
        Raise ParseError to log it but continue with other rows.
        """

    @abstractmethod
    async def write(self, signals: list[SIGNAL]) -> tuple[int, int, int]:
        """
        Write the parsed signals to Supabase.

        Returns: (records_new, records_updated, records_failed)
        """

    # ---- Fetch (provided by base class) ----

    @retry_on_transient(source="arcgis_base")
    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        offset: int,
        page_size: int,
    ) -> dict[str, Any]:
        """Fetch one page of features from the ArcGIS service."""
        if not self.feature_service_url:
            raise ParseError(
                f"feature_service_url not configured for {self.source_name}",
                source=self.source_name,
            )

        url = f"{self.feature_service_url}/query"
        params: dict[str, Any] = {
            "where": self.where_clause,
            "outFields": self.out_fields,
            "returnGeometry": str(self.return_geometry).lower(),
            "outSR": 4326,  # WGS84 lat/lng
            "resultOffset": offset,
            "resultRecordCount": page_size,
            "f": "json",  # ArcGIS REST JSON format
        }

        try:
            response = await client.get(url, params=params)
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"ArcGIS request failed: {e}",
                source=self.source_name,
                context={"url": url, "offset": offset},
            ) from e

        if response.status_code >= 500:
            raise SourceUnavailableError(
                f"ArcGIS returned {response.status_code}",
                source=self.source_name,
                context={"offset": offset},
            )
        if response.status_code != 200:
            raise SourceUnavailableError(
                f"ArcGIS returned unexpected status {response.status_code}",
                source=self.source_name,
                context={"offset": offset, "body": response.text[:500]},
            )

        try:
            data = response.json()
        except ValueError as e:
            raise ParseError(
                f"ArcGIS returned non-JSON: {e}",
                source=self.source_name,
            ) from e

        # ArcGIS returns errors as 200 OK with an 'error' field — catch that
        if isinstance(data, dict) and "error" in data:
            err = data["error"]
            raise ParseError(
                f"ArcGIS API error: {err.get('message', 'unknown')}",
                source=self.source_name,
                context={"arcgis_error": err},
            )

        return data

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """
        Fetch features from the ArcGIS service via pagination.

        Honors:
          - self.page_size            (records per HTTP request)
          - self.max_pages            (hard cap on pages)
          - self._max_records_override (runtime cap, e.g. for test runs)
          - self.progress_log_every   (logging cadence)

        Returns a list of raw feature dicts, each with 'attributes' and
        (optionally) 'geometry' keys.
        """
        page_size = self.page_size
        max_pages = self.max_pages
        record_cap = self._max_records_override

        logger.info(
            "ArcGIS fetch starting",
            source=self.source_name,
            url=self.feature_service_url,
            where=self.where_clause,
            page_size=page_size,
            max_pages=max_pages,
            max_records_override=record_cap,
        )

        all_features: list[dict[str, Any]] = []
        next_progress_threshold = self.progress_log_every

        async with httpx.AsyncClient(
            timeout=settings.scraper_request_timeout_seconds,
            headers={"User-Agent": "DistressProperties/1.0"},
        ) as client:
            for page in range(max_pages):
                offset = page * page_size

                # If a record cap is set and we've already reached it, stop.
                if record_cap is not None and len(all_features) >= record_cap:
                    logger.info(
                        "ArcGIS fetch stopping early — record cap reached",
                        source=self.source_name,
                        record_cap=record_cap,
                        fetched=len(all_features),
                    )
                    break

                # If a record cap is set, narrow the next page size if needed
                effective_page_size = page_size
                if record_cap is not None:
                    remaining = record_cap - len(all_features)
                    effective_page_size = min(page_size, remaining)

                data = await self._fetch_page(
                    client, offset, effective_page_size
                )
                features = data.get("features") or []
                all_features.extend(features)

                # Progress logging at human-readable thresholds
                if len(all_features) >= next_progress_threshold:
                    logger.info(
                        "ArcGIS fetch progress",
                        source=self.source_name,
                        cumulative=len(all_features),
                        page=page + 1,
                    )
                    while next_progress_threshold <= len(all_features):
                        next_progress_threshold += self.progress_log_every

                # If we got fewer than a full page, we're done
                if len(features) < effective_page_size:
                    break

                # If the service signals exceededTransferLimit=False AND we
                # got 0 features, that's also a stop signal
                if (
                    not data.get("exceededTransferLimit", False)
                    and len(features) == 0
                ):
                    break

        logger.info(
            "ArcGIS fetch complete",
            source=self.source_name,
            total_features=len(all_features),
        )
        return all_features

    async def parse(self, raw_records: list[dict[str, Any]]) -> list[SIGNAL]:
        """
        Convert raw ArcGIS features into typed signals.

        Delegates to subclass's parse_feature() for each row.
        Skips None results; logs parse failures as errors but continues.
        """
        from src.services.audit_logger import log_error

        signals: list[SIGNAL] = []
        for feature in raw_records:
            attributes = feature.get("attributes") or {}
            geometry = feature.get("geometry")

            try:
                signal = await self.parse_feature(attributes, geometry)
                if signal is not None:
                    signals.append(signal)
            except ParseError as e:
                log_error(
                    run_id=None,
                    error_type="parse_error",
                    error_message=str(e),
                    raw_record={"attributes": attributes},
                )
            except Exception as e:
                log_error(
                    run_id=None,
                    error_type="parse_error",
                    error_message=f"{type(e).__name__}: {e}",
                    raw_record={"attributes": attributes},
                )

        return signals


__all__ = [
    "BaseArcGISScraper",
    "arcgis_date_to_iso",
    "arcgis_date_to_date_only",
]
