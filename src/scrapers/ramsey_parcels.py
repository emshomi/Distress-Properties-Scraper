"""
Ramsey County Parcels foundation scraper (STREAMING version).

Source: Ramsey County GIS ArcGIS Server (county-operated, public)
API:    https://maps.co.ramsey.mn.us/arcgis/rest/services
        /ParcelData/AttributedData/MapServer/3

License: Ramsey County open data. The layer description states the attribute
set mirrors MetroGIS's Regionally Endorsed Parcel Dataset (field names differ
but content is the same). Public, free.

This is the FOUNDATION layer — loads ALL Ramsey parcels (~167K) into
core.parcels. It is the property-identification spine that Ramsey distress
signals (tax-forfeit, future delinquent, etc.) join to for owner / address /
mailing / value / tax enrichment — exactly as hennepin_parcels backs the
Hennepin tax-roll miner.

=== WHY THIS LAYER (and not the OpenData server) ===
Ramsey publishes TWO parcel services:
  - maps.co.ramsey.mn.us/.../OpenData/OpenData/FeatureServer  — geometry +
    legal description ONLY (no owner / value / tax). Not useful for distress.
  - maps.co.ramsey.mn.us/.../ParcelData/AttributedData/MapServer  — the
    ATTRIBUTED layer: owner, taxpayer mailing, site address, EMV, total tax,
    dwelling info. THIS is the one we load.

=== WHAT THIS LAYER DOES AND DOES NOT CARRY ===
Carries: ParcelID, OwnerName, OwnerAddress1/2, TaxName1/2 + TaxAddress1/2
         (mailing — drives the absentee signal), SiteAddress, SiteCityName,
         SiteZIP5, EMVTotal (market value), TotalTax, SpecialAssessmentDue,
         DwellingType, YearBuilt, LandUseCode, Latitude/Longitude.
Does NOT carry: a tax-delinquency-year field or a tax-forfeit flag (unlike
         Hennepin's parcel feed). Those Ramsey distress signals come from
         OTHER sources (DOR / county auditor / TFL listing) and are joined to
         these parcels later. This loader is the spine, not the signal.

=== STREAMING DESIGN ===
Identical approach to hennepin_parcels: override run() to stream
fetch-page -> parse-page -> write-page -> discard, so we never hold the whole
dataset in memory and each page is persisted as it is written.

What it writes:
  - core.parcels rows + raw_data JSONB (all attributes preserved for mining)
What it does NOT write:
  - signals.distress_events (parcel existence isn't a distress signal)
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

import httpx

from src.config import settings
from src.db.supabase_client import core_table
from src.models.parcel import ParcelUpsert
from src.scrapers.base_arcgis_scraper import BaseArcGISScraper
from src.scrapers.base_scraper import RunResult
from src.services import audit_logger, source_health_tracker
from src.utils.errors import (
    ParseError,
    ScraperAlreadyRunningError,
    ScraperDisabledError,
)
from src.utils.logger import logger
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id


_FEATURE_SERVICE_URL = (
    "https://maps.co.ramsey.mn.us/arcgis/rest/services"
    "/ParcelData/AttributedData/MapServer/3"
)

# Ramsey LandUseCode -> internal property_type. Ramsey uses numeric land-use
# codes; the layer's own renderer documents the groupings we mirror here.
# Residential 1-unit: 510, 511. Residential 2+ units: a block in the 495-578
# range. Apartments: 401-408, 517, 686. Commercial/Industrial: the large
# 100-880 commercial block. Codes not mapped -> property_type NULL (better
# than misclassifying). We map the high-confidence residential ones; the rest
# stay NULL and can be refined later from real data.
_RAMSEY_LANDUSE_TO_INTERNAL: dict[str, str] = {
    "510": "single_family",
    "511": "single_family",
    # Residential 2+ units (documented in the renderer's "Residential 2+ Units")
    "495": "multifamily", "505": "multifamily", "515": "multifamily",
    "520": "multifamily", "521": "multifamily", "530": "multifamily",
    "531": "multifamily", "540": "multifamily", "541": "multifamily",
    "545": "multifamily", "546": "multifamily", "550": "multifamily",
    "551": "multifamily", "552": "multifamily", "553": "multifamily",
    "570": "multifamily", "573": "multifamily", "574": "multifamily",
    "575": "multifamily", "576": "multifamily", "578": "multifamily",
    # Apartments
    "401": "multifamily", "402": "multifamily", "403": "multifamily",
    "404": "multifamily", "408": "multifamily", "517": "multifamily",
    "686": "multifamily",
}

_DB_BATCH_SIZE: int = 500


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        d = Decimal(str(value))
        return d if d >= 0 else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _title_case_city(city: str | None) -> str | None:
    return city.title() if city else None


def _map_property_type(land_use_code: Any) -> str | None:
    code = _safe_str(land_use_code)
    if not code:
        return None
    return _RAMSEY_LANDUSE_TO_INTERNAL.get(code)


def _normalize_ramsey_pid(raw_pid: str) -> str | None:
    """
    Normalize a Ramsey ParcelID.

    We try the shared normalizer first. If it does not yet understand the
    'ramsey' county format (returns an error), we fall back to a sanitized
    raw ParcelID rather than dropping the parcel entirely — a first load
    should not silently produce zero rows because the normalizer lacks a
    Ramsey rule. The fallback keeps the ID usable; it can be upgraded later
    once the normalizer gains a verified Ramsey rule.
    """
    pid, err = safe_normalize_parcel_id("ramsey", raw_pid)
    if pid is not None:
        return pid
    # Fallback: sanitize the raw value (strip, drop internal whitespace).
    sanitized = "".join(raw_pid.split())
    return sanitized or None


def _clean_raw_data(attributes: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in attributes.items():
        if value is None:
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                cleaned[key] = stripped
        elif isinstance(value, (int, float, bool)):
            cleaned[key] = value
        else:
            try:
                cleaned[key] = str(value)
            except Exception:
                continue
    return cleaned


class RamseyParcelsScraper(BaseArcGISScraper[dict[str, Any]]):
    """Ramsey County parcels — streaming foundation loader."""

    source_name: ClassVar[str] = "ramsey_parcels"
    signal_type: ClassVar[str] = "parcel_foundation"
    county_code: ClassVar[str] = "ramsey"
    feature_service_url: ClassVar[str] = _FEATURE_SERVICE_URL

    where_clause: ClassVar[str] = "1=1"
    # We DO want geometry: the base requests outSR=4326 so geometry.y/x give
    # WGS84 lat/lng. The attribute Latitude/Longitude fields are a fallback.
    return_geometry: ClassVar[bool] = True
    page_size: ClassVar[int] = 1000          # layer MaxRecordCount is 1000
    max_pages: ClassVar[int] = 250           # ~167K parcels -> ~167 pages; headroom
    progress_log_every: ClassVar[int] = 10000

    # ---- parse_feature: convert one ArcGIS feature into a parcel dict ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        raw_pid = attributes.get("ParcelID")
        if not raw_pid:
            return None

        pid = _normalize_ramsey_pid(str(raw_pid))
        if pid is None:
            raise ParseError(
                f"Could not normalize Ramsey ParcelID {raw_pid!r}",
                source=self.source_name,
            )

        address = _safe_str(attributes.get("SiteAddress"))
        city = _title_case_city(_safe_str(attributes.get("SiteCityName")))
        zip_cd = _safe_str(attributes.get("SiteZIP5"))

        # Prefer reprojected geometry (outSR=4326); fall back to the layer's
        # own Latitude/Longitude attribute fields.
        lat = None
        lng = None
        if geometry:
            lat = _safe_float(geometry.get("y"))
            lng = _safe_float(geometry.get("x"))
        if lat is None:
            lat = _safe_float(attributes.get("Latitude"))
        if lng is None:
            lng = _safe_float(attributes.get("Longitude"))
        # Sanity-bound to Minnesota; discard nonsense coordinates.
        if lat is not None and not (43.0 <= lat <= 50.0):
            lat = None
        if lng is not None and not (-97.5 <= lng <= -89.0):
            lng = None

        year_built = _safe_int(attributes.get("YearBuilt"))
        if year_built is not None and year_built < 1700:
            year_built = None

        property_type = _map_property_type(attributes.get("LandUseCode"))
        mkt_val = _safe_decimal(attributes.get("EMVTotal"))
        cleaned_raw = _clean_raw_data(attributes)

        return {
            "parcel_id": pid,
            "address": address,
            "city": city,
            "zip": zip_cd,
            "lat": lat,
            "lng": lng,
            "year_built": year_built,
            "property_type": property_type,
            "estimated_market_value": mkt_val,
            "raw_data": cleaned_raw,
        }

    # ---- write: one page at a time (called by streaming run) ----

    async def write(
        self,
        signals: list[dict[str, Any]],
    ) -> tuple[int, int, int]:
        """
        Write a batch of parcel dicts to core.parcels.

        Used by the streaming run() one page at a time. Returns
        (records_new, records_updated, records_failed).
        """
        if not signals:
            return 0, 0, 0

        now_iso = datetime.now(timezone.utc).isoformat()
        records_new = 0
        records_failed = 0
        batch: list[dict[str, Any]] = []

        for sig in signals:
            try:
                payload = ParcelUpsert(
                    parcel_id=sig["parcel_id"],
                    county_code=self.county_code,
                    state="MN",
                    address=sig.get("address"),
                    city=sig.get("city"),
                    zip=sig.get("zip"),
                    lat=sig.get("lat"),
                    lng=sig.get("lng"),
                    year_built=sig.get("year_built"),
                    property_type=sig.get("property_type"),  # type: ignore[arg-type]
                    estimated_market_value=sig.get("estimated_market_value"),
                    raw_data=sig.get("raw_data"),
                    data_sources=[self.source_name],
                    last_observed_at=datetime.now(timezone.utc),
                )
            except Exception as e:
                records_failed += 1
                if records_failed <= 5:
                    logger.warning(
                        "Parcel validation failed",
                        parcel_id=sig.get("parcel_id"),
                        error=str(e)[:200],
                    )
                continue

            row = payload.model_dump(mode="json", exclude_none=True)
            row["last_observed_at"] = now_iso
            batch.append(row)

            if len(batch) >= _DB_BATCH_SIZE:
                n, f = self._upsert_batch(batch)
                records_new += n
                records_failed += f
                batch = []

        if batch:
            n, f = self._upsert_batch(batch)
            records_new += n
            records_failed += f

        return records_new, 0, records_failed

    def _upsert_batch(self, batch: list[dict[str, Any]]) -> tuple[int, int]:
        if not batch:
            return 0, 0
        try:
            result = (
                core_table("parcels")
                .upsert(batch, on_conflict="parcel_id")
                .execute()
            )
            written = len(result.data) if result.data else len(batch)
            return written, 0
        except Exception as e:
            logger.warning(
                "Batch upsert to core.parcels failed",
                source=self.source_name,
                batch_size=len(batch),
                error=str(e)[:500],
            )
            return 0, len(batch)

    # ---- STREAMING run() override ----

    async def run(
        self,
        *,
        trigger: str = "scheduler",
        metadata: dict[str, Any] | None = None,
    ) -> RunResult:
        """
        Streaming run: fetch a page, parse it, write it, repeat.

        Overrides the base fetch()->parse()->write() lifecycle so we never
        hold the whole dataset in memory and each page is persisted as it is
        processed. Mirrors hennepin_parcels exactly.
        """
        start_time = time.monotonic()

        if not settings.scraper_enabled(self.source_name):
            if trigger == "manual":
                raise ScraperDisabledError(
                    f"Scraper '{self.source_name}' is disabled in settings",
                    source=self.source_name,
                )
            return RunResult(
                scraper_name=self.source_name,
                run_id=None,
                status="skipped",
                duration_seconds=0.0,
                error_message="Scraper disabled in settings",
            )

        if self._class_lock.locked():
            raise ScraperAlreadyRunningError(
                f"Scraper '{self.source_name}' is already running",
                source=self.source_name,
                context={"scraper_name": self.source_name},
            )

        async with self._class_lock:
            return await self._run_streaming(trigger, metadata, start_time)

    async def _run_streaming(
        self,
        trigger: str,
        metadata: dict[str, Any] | None,
        start_time: float,
    ) -> RunResult:
        run_metadata = dict(metadata or {})
        run_metadata["trigger"] = trigger
        run_metadata["mode"] = "streaming"
        run_id = audit_logger.start_run(self.source_name, metadata=run_metadata)

        page_size = self.page_size
        max_pages = self.max_pages
        record_cap = self._max_records_override

        logger.info(
            "Ramsey streaming run starting",
            scraper=self.source_name,
            trigger=trigger,
            run_id=run_id,
            page_size=page_size,
            max_pages=max_pages,
            max_records_override=record_cap,
        )

        total_fetched = 0
        total_new = 0
        total_failed = 0
        seen_pids: set[str] = set()
        error_message: str | None = None
        status: str = "success"
        next_progress = self.progress_log_every

        try:
            async with httpx.AsyncClient(
                timeout=settings.scraper_request_timeout_seconds,
                headers={"User-Agent": "DistressProperties/1.0"},
            ) as client:
                for page in range(max_pages):
                    offset = page * page_size

                    if record_cap is not None and total_fetched >= record_cap:
                        break

                    effective_page_size = page_size
                    if record_cap is not None:
                        remaining = record_cap - total_fetched
                        effective_page_size = min(page_size, remaining)

                    # --- FETCH one page ---
                    data = await self._fetch_page(
                        client, offset, effective_page_size
                    )
                    features = data.get("features") or []
                    if not features:
                        break

                    total_fetched += len(features)

                    # --- PARSE this page ---
                    page_signals: list[dict[str, Any]] = []
                    for feature in features:
                        attributes = feature.get("attributes") or {}
                        geometry = feature.get("geometry")
                        try:
                            sig = await self.parse_feature(attributes, geometry)
                        except ParseError:
                            continue
                        except Exception:
                            continue
                        if sig is None:
                            continue
                        pid = sig["parcel_id"]
                        if pid in seen_pids:
                            continue
                        seen_pids.add(pid)
                        page_signals.append(sig)

                    # --- WRITE this page immediately ---
                    if page_signals:
                        n, _u, f = await self.write(page_signals)
                        total_new += n
                        total_failed += f

                    # --- Progress logging ---
                    if total_fetched >= next_progress:
                        logger.info(
                            "Ramsey streaming progress",
                            scraper=self.source_name,
                            fetched=total_fetched,
                            written=total_new,
                            failed=total_failed,
                            page=page + 1,
                        )
                        while next_progress <= total_fetched:
                            next_progress += self.progress_log_every

                    # --- Stop conditions ---
                    if len(features) < effective_page_size:
                        break
                    if (
                        not data.get("exceededTransferLimit", False)
                        and len(features) == 0
                    ):
                        break

            if total_failed > 0 and total_new == 0:
                status = "failed"
                error_message = f"All {total_failed} record writes failed"
            elif total_failed > 0:
                status = "partial"
                error_message = (
                    f"{total_failed} of {total_new + total_failed} records failed"
                )

        except Exception as e:
            status = "failed"
            error_message = f"{type(e).__name__}: {e}"
            logger.exception(
                "Ramsey streaming run failed",
                scraper=self.source_name,
                error_type=type(e).__name__,
                fetched_so_far=total_fetched,
                written_so_far=total_new,
            )

        duration = time.monotonic() - start_time

        if run_id is not None:
            audit_logger.finish_run(
                run_id,
                status=status,  # type: ignore[arg-type]
                records_fetched=total_fetched,
                records_new=total_new,
                records_updated=0,
                records_failed=total_failed,
                error_message=error_message,
                duration_seconds=duration,
            )

        if status == "success":
            source_health_tracker.record_success(self.source_name)
        elif status == "partial":
            source_health_tracker.record_partial(
                self.source_name, notes=error_message
            )
        else:
            source_health_tracker.record_failure(
                self.source_name, notes=error_message
            )

        logger.info(
            "Ramsey streaming run complete",
            scraper=self.source_name,
            status=status,
            duration_seconds=round(duration, 2),
            records_fetched=total_fetched,
            records_new=total_new,
            records_failed=total_failed,
        )

        return RunResult(
            scraper_name=self.source_name,
            run_id=run_id,
            status=status,
            duration_seconds=duration,
            records_fetched=total_fetched,
            records_new=total_new,
            records_updated=0,
            records_failed=total_failed,
            error_message=error_message,
        )


__all__ = ["RamseyParcelsScraper"]
