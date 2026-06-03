"""
Dakota County Parcels foundation scraper (STREAMING version).

Source: Dakota County GIS ArcGIS Server (county-operated, public, open data)
API:    http://gis2.co.dakota.mn.us/arcgis/rest/services
        /DCGIS_OL_PropertyInformation/MapServer/71   (Tax Parcels)

License: Dakota County adopted a free-and-open GIS data policy (with the other
six Twin Cities metro counties, per MetroGIS) under the Minnesota Government
Data Practices Act (Minn. Stat. ch. 13). Public, free, GREEN per the data-source
audit.

This is the FOUNDATION layer — loads ALL Dakota tax parcels (~150K) into
core.parcels. It is the property-identification spine that Dakota distress
signals join to for owner / mailing / market-value / homestead enrichment —
exactly as ramsey_parcels backs Ramsey signals and the Hennepin roll backs the
Hennepin tax-roll miner. The immediate consumer is the Dakota foreclosure
enrichment job (dakota_foreclosure_enrichment), which address-joins the Dakota
sheriff sales to these parcels.

=== WHY THIS LAYER ===
The DCGIS_OL_PropertyInformation MapServer is the single Dakota service that
carries BOTH the foreclosure-sale layers (the source of dakota_sheriff) AND the
attributed Tax Parcels layer (71). Layer 71 is the only one of the parcel layers
that carries owner + mailing + value + homestead together, so it is the one we
load. (Layer 59 "Parcels - Market Value" carries value only; 71 is the superset.)

=== WHAT THIS LAYER CARRIES ===
Carries: PIN (+ TAXPIN, OLDPIN), SITEADDRESS, FULLNAME_PUBLIC / JOINT_OWNER_PUBLIC
         (the county's public-display owner names — preferred over the raw
         FULLNAME/JOINT_OWNER for republication), OWN_ADD_L1/L2/L3 (owner mailing
         address, 3 lines), TOTALVAL (estimated market value; also LANDVAL/BLDGVAL),
         HOMESTEAD ("FULL HOMESTEAD" / "NON HOMESTEAD" / blank), MUNICIPALITY,
         DWELL_TYPE, YEAR_BUILT, plus tax fields (TAX, TOTAL_TAX,
         SPECIAL_ASSESSMENTS) preserved in raw_data for future signals.
Note: many parcels (vacant land, common areas, some condos) have an EMPTY
         SITEADDRESS. Those simply will not address-match a foreclosure row —
         that is expected and honest, not an error.

=== JOIN KEY NOTE ===
The Dakota foreclosure feed (dakota_sheriff) carries NO PIN — only GeoAddress.
So enrichment joins on normalized SITEADDRESS <-> GeoAddress. We still store the
real PIN here as parcel_id so the roll is keyed correctly and reusable.

=== STREAMING DESIGN ===
Identical approach to ramsey_parcels / hennepin_parcels: override run() to stream
fetch-page -> parse-page -> write-page -> discard, so we never hold the whole
dataset in memory and each page is persisted as it is written.

=== PAYLOAD NOTE (why no geometry, trimmed fields) ===
Layer 71 is a POLYGON layer with 70+ fields (incl. a 2000-char Legal field).
A query of returnGeometry=true + outFields=* + a large page failed server-side
with "Error performing query operation" — the per-record payload (all fields +
full parcel polygons) was too heavy to assemble. The enrichment join needs only
owner / mailing / value / homestead / address, and needs NO geometry, so we
request geometry=false and an explicit trimmed field list. This makes each
record tiny and the query reliable. Consequence: parcels load with lat/lng=None
(the geometry parse below is now inert but harmless). Map coordinates for Dakota,
if ever wanted, are a separate future task.

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


_FEATURE_SERVICE_URL = (
    "http://gis2.co.dakota.mn.us/arcgis/rest/services"
    "/DCGIS_OL_PropertyInformation/MapServer/71"
)

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


def _normalize_dakota_pin(raw_pin: Any) -> str | None:
    """Dakota PIN is a 13-char string. We don't run it through the shared
    parcel-id normalizer (which has county-specific rules we haven't verified
    for Dakota); instead we sanitize it directly — strip and remove internal
    whitespace — which keeps it stable and collision-free as a primary key.
    The real PIN is also preserved verbatim in raw_data."""
    s = _safe_str(raw_pin)
    if not s:
        return None
    sanitized = "".join(s.split())
    return sanitized or None


class DakotaParcelsScraper(BaseArcGISScraper[dict[str, Any]]):
    """Dakota County tax parcels — streaming foundation loader."""

    source_name: ClassVar[str] = "dakota_parcels"
    signal_type: ClassVar[str] = "parcel_foundation"
    county_code: ClassVar[str] = "dakota"
    feature_service_url: ClassVar[str] = _FEATURE_SERVICE_URL

    where_clause: ClassVar[str] = "1=1"
    # Explicit trimmed field list — only what enrichment needs. Avoids the
    # outFields=* payload that (with geometry) overwhelmed the server. Every
    # name here is verified present in the layer-71 schema.
    out_fields: ClassVar[str] = (  # CHANGED (added; was inherited "*")
        "PIN,SITEADDRESS,FULLNAME_PUBLIC,JOINT_OWNER_PUBLIC,"
        "OWN_ADD_L1,OWN_ADD_L2,OWN_ADD_L3,TOTALVAL,HOMESTEAD,"
        "MUNICIPALITY,YEAR_BUILT"
    )
    # Geometry OFF: the address join needs no coordinates, and polygon geometry
    # was the main cause of the too-heavy payload. lat/lng will be None.
    return_geometry: ClassVar[bool] = False   # CHANGED (was True)
    page_size: ClassVar[int] = 2000   # CHANGED (was 10000); layer max is 10000
    max_pages: ClassVar[int] = 90     # CHANGED (was 30); ~150K / 2000 = 75 pages + headroom
    progress_log_every: ClassVar[int] = 20000

    # ---- parse_feature: convert one ArcGIS feature into a parcel dict ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        raw_pin = attributes.get("PIN")
        pid = _normalize_dakota_pin(raw_pin)
        if pid is None:
            # No usable PIN — skip (can't key the parcel). Rare.
            return None

        address = _safe_str(attributes.get("SITEADDRESS"))
        city = _title_case_city(_safe_str(attributes.get("MUNICIPALITY")))

        # Geometry: with return_geometry=False this is always None now. Kept
        # inert/harmless so re-enabling geometry later needs no code change.
        lat = None
        lng = None
        if geometry:
            lat = _safe_float(geometry.get("y"))
            lng = _safe_float(geometry.get("x"))
        if lat is not None and not (43.0 <= lat <= 50.0):
            lat = None
        if lng is not None and not (-97.5 <= lng <= -89.0):
            lng = None

        year_built = _safe_int(attributes.get("YEAR_BUILT"))
        # ParcelUpsert validates year_built as 1700..2100; null anything outside
        # that range so one bad value can't fail the whole row. (Dakota has some
        # 0 / null YEAR_BUILT values — those become None here.)
        if year_built is not None and not (1700 <= year_built <= 2100):
            year_built = None

        # Estimated market value: TOTALVAL (land + building total).
        mkt_val = _safe_decimal(attributes.get("TOTALVAL"))

        cleaned_raw = _clean_raw_data(attributes)

        return {
            "parcel_id": pid,
            "address": address,
            "city": city,
            "zip": None,  # Dakota layer has no clean site-ZIP field
            "lat": lat,
            "lng": lng,
            "year_built": year_built,
            "property_type": None,  # Dakota USE*_DESC is free text; not mapped yet
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
        processed. Mirrors ramsey_parcels / hennepin_parcels exactly.
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
            "Dakota streaming run starting",
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
                            "Dakota streaming progress",
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
                "Dakota streaming run failed",
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
            "Dakota streaming run complete",
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


__all__ = ["DakotaParcelsScraper"]
