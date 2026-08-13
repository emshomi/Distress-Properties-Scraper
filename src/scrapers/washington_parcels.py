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

=== PAYLOAD NOTE (centroids, widened fields) — REVISED 2026-08-13 ===
This is a hosted ArcGIS Online feature service (services1.arcgis.com). Layer
metadata reports maxRecordCount 1000 WITH geometry against
standardMaxRecordCountNoGeometry 32000 — a 32x difference — which is why this
loader and dakota_parcels originally requested geometry=false. For a PID join
that was correct.

The cost only became visible when imagery shipped: ALL 182 Washington distress
parcels had no lat/lng, so no map, no Street View, no aerial and no geometry —
and 172 of them HAD matched a real parcel. The coordinates were never missing,
they were never requested.

The layer advertises supportsReturningGeometryCentroid=true, so we now ask for
the CENTROID rather than the polygon: one x/y pair per feature instead of a
ring of hundreds. Payload stays light, and a centroid is the RIGHT point here —
Street View needs somewhere to stand and look, not a boundary.

The layer is projected in WCCS (Transverse Mercator, US survey feet), so the
base class's outSR=4326 is load-bearing: without it centroids arrive in feet
and the Minnesota bounding-box guard below nulls every one of them silently.

Fields were widened at the same time. The trimmed list was chosen when only the
join mattered, and left a dozen ParcelUpsert columns null across all 118,591
parcels while the layer carried them all along.

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
        "OBJECTID,PIN,SITUS_ADDRESS,CITY,ZIP,ZIP4,OWNER_NAME,"
        "OWN_ADD_L1,OWN_ADD_L2,OWN_ADD_L3,HOMESTEAD,"
        "DWELL_TYPE,YEAR_BUILT,"
        # WIDENED 2026-08-13. Every name below is a ParcelUpsert field that
        # was null on all 118,591 Washington parcels while this layer carried
        # it. Verified against the layer-0 schema.
        "EMV_TOTAL,EMV_LAND,EMV_BLDG,FIN_SQ_FT,NUM_UNITS,ACRES_DEED,"
        "SCHOOL_DST,GARAGE,GARAGESQFT,BASEMENT,HEATING,COOLING,"
        "SALE_DATE,SALE_VALUE,USE1_DESC"
    )
    # Geometry ON as CENTROIDS — see the payload note above. returnGeometry
    # must also be true or the service ignores returnCentroid.
    return_geometry: ClassVar[bool] = True
    return_centroid: ClassVar[bool] = True
    # OBJECTID is required in out_fields for keyset paging (see run()).
    objectid_field: ClassVar[str] = "OBJECTID"
    # Hosted ArcGIS Online feature services cap pages at 2000; this layer
    # reports maxRecordCount 1000 WITH geometry, so 1000 is the ceiling now.
    page_size: ClassVar[int] = 1000
    max_pages: ClassVar[int] = 150    # ~118K / 1000 = 118 pages + headroom
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

        # Geometry: the CENTROID, passed in by run()'s parse loop which reads
        # feature["centroid"] as well as feature["geometry"]. Both shapes are
        # {x, y}, so this code is unchanged from the geometry-off era — the
        # original author kept it inert precisely so re-enabling needed none.
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
        #
        # FIXED 2026-08-13: this was written ONLY to estimated_market_value —
        # the parallel LEGACY column. src/models/parcel.py records that
        # emv_total is "the typed column the distress_with_parcel view and the
        # UI actually read". So Washington's assessed values were fetched,
        # parsed and stored where nothing displays them, which is why its
        # distress rows showed no valuation and no deal math. Not missing
        # data — misrouted data. Both are written now: emv_total for the
        # view, the legacy column for anything still reading it.
        mkt_val = _safe_decimal(attributes.get("EMV_TOTAL"))

        # ACRES_DEED is deeded acreage; core.parcels.lot_sqft is square feet.
        acres = _safe_float(attributes.get("ACRES_DEED"))
        lot_sqft = int(round(acres * 43560)) if acres and acres > 0 else None

        cleaned_raw = _clean_raw_data(attributes)

        return {
            "parcel_id": pid,
            "address": address,
            "city": city,
            "zip": zip_code,
            "zip_plus_four": _safe_str(attributes.get("ZIP4")),
            "lat": lat,
            "lng": lng,
            "year_built": year_built,
            "property_type": None,  # DWELL_TYPE is free text; not mapped yet
            "estimated_market_value": mkt_val,
            # WIDENED 2026-08-13 — all previously null for every Washington
            # parcel while the layer carried them.
            "emv_total": mkt_val,
            "emv_land": _safe_decimal(attributes.get("EMV_LAND")),
            "emv_building": _safe_decimal(attributes.get("EMV_BLDG")),
            "sqft": _safe_int(attributes.get("FIN_SQ_FT")),
            "lot_sqft": lot_sqft,
            "num_units": _safe_int(attributes.get("NUM_UNITS")),
            "use_class": _safe_str(attributes.get("USE1_DESC")),
            "school_district": _safe_str(attributes.get("SCHOOL_DST")),
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
                    zip_plus_four=sig.get("zip_plus_four"),
                    year_built=sig.get("year_built"),
                    property_type=sig.get("property_type"),  # type: ignore[arg-type]
                    estimated_market_value=sig.get("estimated_market_value"),
                    emv_total=sig.get("emv_total"),
                    emv_land=sig.get("emv_land"),
                    emv_building=sig.get("emv_building"),
                    sqft=sig.get("sqft"),
                    lot_sqft=sig.get("lot_sqft"),
                    num_units=sig.get("num_units"),
                    use_class=sig.get("use_class"),
                    school_district=sig.get("school_district"),
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

            # dump_owned(), NOT model_dump(exclude_none=True). This is a
            # FULL-REFRESH loader: it is the source of record for these rows,
            # so a field the layer does not publish is ABSENT, not unchanged.
            # exclude_none drops the key, PostgREST omits the column from the
            # UPDATE, and whatever was there survives — which is how 6,268
            # rows came to hold another county's property_type after the
            # 2026-08-06 incident. See src/models/parcel.py.
            row = payload.dump_owned()
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
                # (county_code, parcel_id) — PK became composite 2026-08-06.
                # Minnesota PINs are NOT globally unique: 51,662 nine-char
                # PINs are shared across counties. parcel_id alone no longer
                # matches a unique constraint, and before the key change it
                # silently overwrote other counties' rows.
                .upsert(batch, on_conflict="county_code,parcel_id")
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
                        .upsert(chunk, on_conflict="county_code,parcel_id")
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

        # `with`, not `async with`. FIXED 2026-08-06. _class_lock became a
        # threading.Lock on 2026-08-02; BaseScraper.run() was updated but
        # every scraper OVERRIDING run() was missed, raising TypeError:
        # '_thread.lock' object does not support the asynchronous context
        # manager protocol. This was the last of the six parcel loaders.
        with self._class_lock:
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
                # KEYSET paging, not offset — changed 2026-08-13 alongside
                # centroids. _fetch_page's docstring records why: with
                # resultOffset the server scans past every skipped row, so
                # deep pages get linearly slower and eventually time out
                # (hennepin_parcels died at page ~224 of 448 as pages
                # degraded from ~7s to ~21s). Washington's 118 pages survived
                # that at geometry-off weight; centroids make every page
                # heavier and there is no reason to find the new limit the
                # hard way. Keyset is constant-time at any depth.
                after_oid = 0

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
                        client, offset, effective_page_size,
                        after_object_id=after_oid,
                    )
                    features = data.get("features") or []
                    if not features:
                        break

                    total_fetched += len(features)

                    # --- PARSE this page ---
                    page_signals: list[dict[str, Any]] = []
                    max_oid_this_page = after_oid
                    for feature in features:
                        attributes = feature.get("attributes") or {}
                        # CENTROID FIRST. The service returns BOTH when
                        # returnCentroid=true: "geometry" with polygon rings
                        # AND "centroid" with {x, y}. Reading geometry first
                        # short-circuits on the truthy rings dict, so
                        # parse_feature gets a rings array, calls .get("y"),
                        # and writes lat=None — which is exactly what happened
                        # on the 2026-08-13 load: 118,418 rows written, 0
                        # failed, 0 coordinates, status "success".
                        #
                        # This loader overrides run() with its own parse loop
                        # and never calls the base class's parse(), so the
                        # same fix has to exist in both places.
                        geometry = feature.get("centroid") or feature.get("geometry")
                        oid = attributes.get(self.objectid_field)
                        if isinstance(oid, int) and oid > max_oid_this_page:
                            max_oid_this_page = oid
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

                    # --- Advance the keyset cursor ---
                    # No forward progress means the OBJECTIDs did not increase,
                    # which would loop forever on the same page. Stop instead.
                    if max_oid_this_page <= after_oid:
                        logger.warning(
                            "Washington keyset cursor did not advance — stopping",
                            scraper=self.source_name,
                            after_oid=after_oid,
                            page=page + 1,
                        )
                        break
                    after_oid = max_oid_this_page

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
