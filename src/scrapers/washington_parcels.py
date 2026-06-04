"""
Washington County Parcels foundation scraper (STREAMING version).

Source: Washington County, MN hosted ArcGIS Online Feature Service (county
open-data portal, public).
API:    https://services1.arcgis.com/3fjYPqJf7qa1QM1b/arcgis/rest/services
        /TaxParcel/FeatureServer/0   (Tax Parcels)

License: Washington County publishes its GIS layers as free + open data under
the Minnesota Government Data Practices Act (Minn. Stat. ch. 13), alongside the
other Twin Cities metro counties (MetroGIS). Public, free, GREEN per the
data-source audit.

This is the FOUNDATION layer — loads ALL Washington tax parcels (~118K) into
core.parcels. It is the property-identification spine that Washington distress
signals join to for owner / mailing / market-value / homestead enrichment —
exactly as dakota_parcels backs Dakota signals. The immediate consumer is the
Washington foreclosure enrichment job (washington_foreclosure_enrichment), which
PID-joins the Washington sheriff sales (parcel_id "WASHINGTON-FC-{pid}") to these
parcels.

=== WHAT THIS LAYER CARRIES ===
Carries: PIN (the join key — matches the sheriff file's unformatted PID),
         SITUS_ADDRESS (site address), CITY / CITY_USPS, ZIP, OWNER_NAME,
         OWN_ADD_L1/L2/L3 (owner mailing address, 3 lines), HOMESTEAD,
         EMV_TOTAL (estimated market value; also EMV_LAND / EMV_BLDG),
         DWELL_TYPE, YEAR_BUILT, plus tax fields (TOTAL_TAX, TAX_CAPAC)
         preserved in raw_data for future signals.
Note: SPEC_ASSES is present but was found to be all-zero in the prior
         investigation — kept in raw_data but not used as a signal.

=== JOIN KEY NOTE ===
The Washington sheriff feed carries the unformatted PID (col A of the monthly
XLS, e.g. "2103020330102"). The TaxParcel PIN field is the same unformatted
number. So enrichment joins on PIN <-> the sheriff PID directly (no address
fuzzy-match needed, unlike Dakota). We store the real PIN as parcel_id here so
the roll is keyed correctly; the foreclosure stub rows use "WASHINGTON-FC-{pid}"
and the enrichment step bridges the two.

=== STREAMING DESIGN ===
Identical approach to dakota_parcels: override run() to stream
fetch-page -> parse-page -> write-page -> discard, so we never hold the whole
dataset in memory and each page is persisted as it is written.

=== PAYLOAD NOTE (no geometry, trimmed fields) ===
This is a hosted ArcGIS Online feature service (services1.arcgis.com), which
caps pages at 2000 records. As with Dakota we request geometry=false and an
explicit trimmed field list — the enrichment join needs only
owner / mailing / value / homestead / address and NO geometry, which keeps each
record tiny and the query reliable. Parcels load with lat/lng=None.

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
    "https://services1.arcgis.com/3fjYPqJf7qalQMlb/arcgis/rest/services"
    "/TaxParcel/FeatureServer/0"
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


def _normalize_washington_pin(raw_pin: Any) -> str | None:
    """Washington PIN is a numeric parcel-id string. We sanitize it directly —
    strip and remove internal whitespace — which keeps it stable and
    collision-free as a primary key, and (crucially) identical to the
    unformatted PID the sheriff file carries so the two join. openpyxl/JSON may
    hand us an int for an all-digits cell, so coerce to str first. The real PIN
    is also preserved verbatim in raw_data."""
    if raw_pin is None:
        return None
    if isinstance(raw_pin, float) and raw_pin.is_integer():
        raw_pin = int(raw_pin)
    s = _safe_str(raw_pin)
    if not s:
        return None
    sanitized = "".join(s.split())
    return sanitized or None


class WashingtonParcelsScraper(BaseArcGISScraper[dict[str, Any]]):
    """Washington County tax parcels — streaming foundation loader."""

    source_name: ClassVar[str] = "washington_parcels"
    signal_type: ClassVar[str] = "parcel_foundation"
    county_code: ClassVar[str] = "washington"
    feature_service_url: ClassVar[str] = _FEATURE_SERVICE_URL

    where_clause: ClassVar[str] = "1=1"
    # Explicit trimmed field list — only what enrichment needs. Every name here
    # is verified present in the TaxParcel layer-0 schema (from the API Explorer).
    out_fields: ClassVar[str] = (
        "PIN,SITUS_ADDRESS,CITY,ZIP,OWNER_NAME,"
        "OWN_ADD_L1,OWN_ADD_L2,OWN_ADD_L3,EMV_TOTAL,HOMESTEAD,"
        "DWELL_TYPE,YEAR_BUILT"
    )
    # Geometry OFF: the PID join needs no coordinates. lat/lng will be None.
    return_geometry: ClassVar[bool] = False
    # Hosted ArcGIS Online feature services cap pages at 2000.
    page_size: ClassVar[int] = 2000
    max_pages: ClassVar[int] = 90     # ~118K / 2000 = 60 pages + headroom
    progress_log_every: ClassVar[int] = 20000

    # ---- parse_feature: convert one ArcGIS feature into a parcel dict ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        raw_pin = attributes.get("PIN")
        pid = _normalize_washington_pin(raw_pin)
        if pid is None:
            # No usable PIN — skip (can't key the parcel). Rare.
            return None

        address = _safe_str(attributes.get("SITUS_ADDRESS"))
        city = _title_case_city(_safe_str(attributes.get("CITY")))
        zip_code = _safe_str(attributes.get("ZIP"))

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
        # that range so one bad value can't fail the whole row.
        if year_built is not None and not (1700 <= year_built <= 2100):
            year_built = None

        # Estimated market value: EMV_TOTAL (land + building total).
        mkt_val = _safe_decimal(attributes.get("EMV_TOTAL"))

        cleaned_raw = _clean_raw_data(attributes)

        return {
            "parcel_id": pid,
            "address": address,
            "city": city,
            "zip": zip_code,
            "lat": lat,
            "lng": lng,
            "year_built": year_built,
            "property_type": None,  # DWELL_TYPE is free text; not mapped yet
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
            err = str(e)
            # 57014 = Postgres "canceling statement due to statement timeout".
            # Retry the same rows in smaller sub-batches on timeout only.
            is_timeout = "57014" in err or "statement timeout" in err.lower()
            if not is_timeout or len(batch) <= 100:
                logger.warning(
                    "Batch upsert to core.parcels failed",
                    source=self.source_name,
                    batch_size=len(batch),
                    error=err[:500],
                )
                return 0, len(batch)

            logger.info(
                "Batch upsert timed out; retrying in smaller chunks",
                source=self.source_name,
                batch_size=len(batch),
                chunk_size=100,
            )
            sub_written = 0
            sub_failed = 0
            for i in range(0, len(batch), 100):
                chunk = batch[i : i + 100]
                try:
                    result = (
                        core_table("parcels")
                        .upsert(chunk, on_conflict="parcel_id")
                        .execute()
                    )
                    sub_written += len(result.data) if result.data else len(chunk)
                except Exception as e2:
                    logger.warning(
                        "Retry chunk upsert failed",
                        source=self.source_name,
                        chunk_size=len(chunk),
                        error=str(e2)[:300],
                    )
                    sub_failed += len(chunk)
            return sub_written, sub_failed

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
        processed. Mirrors dakota_parcels exactly.
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
            "Washington streaming run starting",
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
                            "Washington streaming progress",
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
                "Washington streaming run failed",
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
            "Washington streaming run complete",
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


__all__ = ["WashingtonParcelsScraper"]
