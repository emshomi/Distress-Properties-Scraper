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


def _arcgis_date_to_iso(value: Any) -> str | None:
    """ArcGIS epoch-milliseconds -> 'YYYY-MM-DD', or None.

    ArcGIS esriFieldTypeDate fields come back as integer milliseconds since
    the Unix epoch, UTC. core.parcels.last_sale_date is a DATE column, so the
    time component is dropped rather than carried as a fiction — a county
    sale record has a day, not a moment.

    Returns None for 0 and for negative values: 0 is 1970-01-01, which is what
    this layer uses for "no sale on record", and a 1970 sale date on a
    Minnesota parcel would silently become the comparison an investor prices
    against.
    """
    if value is None:
        return None
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    if ms <= 0:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()
    except (OverflowError, OSError, ValueError):
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


def _polygon_centroid(
    geometry: dict[str, Any] | None,
) -> tuple[float | None, float | None]:
    """Area-weighted centroid of an ArcGIS polygon, as (lat, lng).

    ADDED 2026-08-13. Dakota's service is an ArcGIS SERVER MapServer (11.2)
    and its advancedQueryCapabilities does NOT advertise
    supportsReturningGeometryCentroid — unlike Washington's hosted
    FeatureServer, which does. Sending returnCentroid here would be silently
    IGNORED: polygons come back, no centroid key appears, and every row lands
    with lat=None while the run reports success. That exact failure cost a
    full 118,418-row Washington load earlier the same day, so it is checked
    rather than assumed.

    So we compute it ourselves from the rings.

    WHY AREA-WEIGHTED AND NOT A VERTEX MEAN:
    averaging vertices pulls the result toward whichever edge happens to carry
    the most points — and these rings are dense (a single residential lot in
    the sample response has ~40 vertices, unevenly spaced). On a rectangular
    suburban lot the two agree closely; on an irregular rural parcel they can
    differ by tens of metres, which matters directly because the imagery
    resolver's too_far threshold is 60m for a structure. The shoelace formula
    gives the true centroid in a dozen lines with no dependency.

    Falls back to the vertex mean when the ring has zero signed area
    (collinear or degenerate points), where the shoelace centroid is undefined
    rather than merely imprecise.

    Uses the FIRST ring only. Later rings in an ArcGIS polygon are holes or
    disjoint parts; for "where is this parcel" the outer ring is the answer.
    """
    if not geometry:
        return None, None
    rings = geometry.get("rings")
    if not rings or not isinstance(rings, list):
        return None, None
    ring = rings[0]
    if not isinstance(ring, list) or len(ring) < 3:
        return None, None

    pts: list[tuple[float, float]] = []
    for pt in ring:
        if isinstance(pt, (list, tuple)) and len(pt) >= 2:
            x, y = _safe_float(pt[0]), _safe_float(pt[1])
            if x is not None and y is not None:
                pts.append((x, y))
    if len(pts) < 3:
        return None, None

    # Shoelace: signed area and area-weighted centroid.
    area2 = 0.0
    cx = 0.0
    cy = 0.0
    for i in range(len(pts)):
        x0, y0 = pts[i]
        x1, y1 = pts[(i + 1) % len(pts)]
        cross = (x0 * y1) - (x1 * y0)
        area2 += cross
        cx += (x0 + x1) * cross
        cy += (y0 + y1) * cross

    if abs(area2) < 1e-12:
        # Degenerate ring — no area to weight by. Vertex mean is the only
        # meaningful answer left, and is correct for a point-like parcel.
        return (
            sum(p[1] for p in pts) / len(pts),
            sum(p[0] for p in pts) / len(pts),
        )

    factor = 1.0 / (3.0 * area2)
    return cy * factor, cx * factor   # (lat, lng) — ArcGIS is (x=lng, y=lat)


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
        "OBJECTID,PIN,SITEADDRESS,FULLNAME_PUBLIC,JOINT_OWNER_PUBLIC,"
        "OWN_ADD_L1,OWN_ADD_L2,OWN_ADD_L3,HOMESTEAD,"
        "MUNICIPALITY,YEAR_BUILT,TAXPIN,OLDPIN,"
        # WIDENED 2026-08-13. Every name below is a ParcelUpsert field that
        # was null on all ~150K Dakota parcels while this layer carried it.
        # BEDS and BATH are notable: no other Minnesota loader populates
        # them, because no other county's layer publishes them.
        "TOTALVAL,LANDVAL,BLDGVAL,FNSHD_SF,TOTAL_SF,BEDS,BATH,"
        "GAR_SF,TOTAL_ACRES,SCHOOL_DST,USE1_DESC,SALE_DATE,SALE_VALUE,"
        # ADDED after ParcelUpsert was widened on 2026-08-13. These columns
        # exist on core.parcels and this layer publishes them, but the model
        # had no field for them, so nothing could be written. GAR_SF was
        # already requested and had to be dropped mid-change when it raised
        # extra_forbidden and failed all 500 writes of a capped test run.
        "GARAGE,BASEMENT,HEATING,AIR_COND,TOTAL_TAX,SPEC_ASSESS,Legal"
    )
    # Geometry ON as POLYGONS — the layer does NOT support returnCentroid
    # (checked 2026-08-13: no supportsReturningGeometryCentroid in
    # advancedQueryCapabilities), so _polygon_centroid() computes it from the
    # rings. Verified the server serves geometry at 2000/page without
    # complaint; the "too-heavy payload" this comment used to describe was
    # outFields=*, which is a different request entirely.
    return_geometry: ClassVar[bool] = True    # CHANGED 2026-08-13 (was False)
    # OBJECTID is required in out_fields for keyset paging (see _run_streaming).
    objectid_field: ClassVar[str] = "OBJECTID"
    page_size: ClassVar[int] = 2000   # layer maxRecordCount is 10000
    max_pages: ClassVar[int] = 90     # ~150K / 2000 = 75 pages + headroom
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

        # Geometry: POLYGON rings. This layer cannot return a centroid, so we
        # derive one. The geometry.get("y")/("x") branch is kept for the point
        # case — if the service ever starts returning centroids, or the layer
        # is swapped for a point layer, that path takes over unchanged.
        lat = None
        lng = None
        if geometry and geometry.get("rings"):
            lat, lng = _polygon_centroid(geometry)
        elif geometry:
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
        #
        # FIXED 2026-08-13: written ONLY to estimated_market_value, the
        # parallel LEGACY column. src/models/parcel.py records that emv_total
        # is "the typed column the distress_with_parcel view and the UI
        # actually read", so Dakota's assessed values were fetched, parsed and
        # stored where nothing displays them — which is why its distress rows
        # showed no valuation and no deal math. Not missing data, misrouted
        # data. Same defect found in washington_parcels the same day.
        mkt_val = _safe_decimal(attributes.get("TOTALVAL"))

        # TOTAL_ACRES is deeded acreage; core.parcels.lot_sqft is square feet.
        acres = _safe_float(attributes.get("TOTAL_ACRES"))
        lot_sqft = int(round(acres * 43560)) if acres and acres > 0 else None

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
            # WIDENED 2026-08-13 — all previously null for every Dakota parcel
            # while the layer carried them.
            "emv_total": mkt_val,
            "emv_land": _safe_decimal(attributes.get("LANDVAL")),
            "emv_building": _safe_decimal(attributes.get("BLDGVAL")),
            "sqft": _safe_int(attributes.get("FNSHD_SF")),
            "lot_sqft": lot_sqft,
            "beds": _safe_int(attributes.get("BEDS")),
            "baths": _safe_float(attributes.get("BATH")),
            "use_class": _safe_str(attributes.get("USE1_DESC")),
            "school_district": _safe_str(attributes.get("SCHOOL_DST")),
            # Structure characteristics, kept as the county writes them
            # ("Attached 2 stall", "Full", "Forced air"). Not normalised —
            # a wrong mapping across 87 counties' vocabularies is worse than
            # the county's own words.
            "garage": _safe_str(attributes.get("GARAGE")),
            "garage_sqft": _safe_int(attributes.get("GAR_SF")),
            "basement": _safe_str(attributes.get("BASEMENT")),
            "heating": _safe_str(attributes.get("HEATING")),
            "cooling": _safe_str(attributes.get("AIR_COND")),
            # Tax and legal
            "annual_tax": _safe_decimal(attributes.get("TOTAL_TAX")),
            "special_assessments": _safe_decimal(attributes.get("SPEC_ASSESS")),
            "homestead_status": _safe_str(attributes.get("HOMESTEAD")),
            "legal_description": _safe_str(attributes.get("Legal")),
            # Prior arm's-length sale — NOT the distress event. This is what an
            # investor compares an asking price against. SALE_DATE and
            # SALE_VALUE were already being requested and thrown away, because
            # the model had nowhere to put them.
            "last_sale_price": _safe_decimal(attributes.get("SALE_VALUE")),
            "last_sale_date": _arcgis_date_to_iso(attributes.get("SALE_DATE")),
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
                    emv_total=sig.get("emv_total"),
                    emv_land=sig.get("emv_land"),
                    emv_building=sig.get("emv_building"),
                    sqft=sig.get("sqft"),
                    lot_sqft=sig.get("lot_sqft"),
                    beds=sig.get("beds"),
                    baths=sig.get("baths"),
                    use_class=sig.get("use_class"),
                    school_district=sig.get("school_district"),
                    garage=sig.get("garage"),
                    garage_sqft=sig.get("garage_sqft"),
                    basement=sig.get("basement"),
                    heating=sig.get("heating"),
                    cooling=sig.get("cooling"),
                    annual_tax=sig.get("annual_tax"),
                    special_assessments=sig.get("special_assessments"),
                    homestead_status=sig.get("homestead_status"),
                    legal_description=sig.get("legal_description"),
                    last_sale_price=sig.get("last_sale_price"),
                    last_sale_date=sig.get("last_sale_date"),
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
            # FULL-REFRESH loader — the source of record for these rows — so a
            # field the layer does not publish is ABSENT, not unchanged.
            # exclude_none drops the key, PostgREST omits the column from the
            # UPDATE, and whatever was there survives. See src/models/parcel.py.
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
                # (county_code, parcel_id) — core.parcels PK became composite
                # 2026-08-06. Minnesota PINs are NOT globally unique: 51,662
                # nine-char PINs are shared across counties. parcel_id alone
                # no longer matches a unique constraint, and before the key
                # change it silently overwrote other counties' rows.
                .upsert(batch, on_conflict="county_code,parcel_id")
                .execute()
            )
            written = len(result.data) if result.data else len(batch)
            return written, 0
        except Exception as e:
            err = str(e)
            # 57014 = Postgres "canceling statement due to statement timeout".
            # The first upsert into the 150K-row table is the slowest (cold
            # cache / index warmup) and can exceed the timeout. Retry the same
            # rows in smaller sub-batches — each statement does less work and
            # almost always clears. Only retry on timeout; other errors fail.
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

        # `with`, not `async with`. FIXED 2026-08-06.
        # _class_lock became a threading.Lock on 2026-08-02; BaseScraper.run()
        # was updated but every scraper OVERRIDING run() was missed, raising
        # TypeError: '_thread.lock' object does not support the asynchronous
        # context manager protocol. Unnoticed because these loaders are
        # monthly and none had fired since.
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
                # KEYSET paging, not offset — changed 2026-08-13 alongside
                # geometry. With resultOffset the server scans past every
                # skipped row, so deep pages get linearly slower and eventually
                # time out (hennepin_parcels died at page ~224 of 448 as pages
                # degraded from ~7s to ~21s). Dakota's 75 pages survived that
                # at geometry-off weight; polygon rings make every page far
                # heavier, and this is the county's own server rather than
                # ArcGIS Online. Keyset is constant-time at any depth.
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
                        # Polygon rings — parse_feature computes the centroid.
                        # No "centroid" key to prefer here: this layer does not
                        # support returnCentroid (unlike Washington's).
                        geometry = feature.get("geometry")
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
                            "Dakota streaming progress",
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
                    # which would re-fetch the same page forever. Stop instead.
                    if max_oid_this_page <= after_oid:
                        logger.warning(
                            "Dakota keyset cursor did not advance — stopping",
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
