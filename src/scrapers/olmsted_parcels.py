"""
Olmsted County Parcels foundation scraper (STREAMING, keyset-paged).

Source: Olmsted County GIS ArcGIS Server (county-operated, public)
API:    https://gweb01.co.olmsted.mn.us/arcgis/rest/services
        /General_Land_Info_OlmstedCountyMN/MapServer/3   ("Parcels - Composite")

THE ROCHESTER / MAYO PILOT (2026-07-09): the first non-metro county.
Olmsted = Rochester = Mayo Clinic; this spine powers land search for the
Rochester buying trip and proves the loader pattern transfers beyond the
metro. Inspected live before writing (75,593 rows; 75,308 carry the
primary-address+primary-owner flags — near one-row-per-parcel; the first
sample feature the server returned was, fittingly, MAYO CLINIC's Stabile
Building, EMV $53.4M, TaxYear 2027).

=== VERIFIED layer facts (live inspection 2026-07-09) ===
  - PARID        12-digit string parcel id (e.g. "640212077128");
                 length-30 field, so the normalizer keeps a sanitize
                 fallback for any non-conforming ids
  - OBJECTID     esriFieldTypeOID -> keyset pagination
  - Site address COMPONENTIZED: SiteAddrNo/SiteAddSuf/SitePreDir/
                 SiteStName/SiteStType/SitePostDir/SiteUnitNo + SiteCity/
                 SiteState/SiteZip5
  - Owners       OwnerName1/2, OwnTypCode, MultOwnFlg/OwnSeqNbr/PrimOwner,
                 mailing componentized (OwnerAddNo/OwnAdNoSuf/OwnStName/
                 OwnStType/OwnStDir + OwnerCity/OwnerState/OwnerZIP5) plus
                 composite OwnerAddr1-3 lines
  - Values       EMVLand / EMVBldg / EMVTotal (integers)
  - Character    Class ("3a COMMERCIAL PREFERENTIAL"), LandUseDes,
                 DeedAcres/DeededSqFt, NeighDesc, SchoolDist, LivngUnits
  - NO YearBuilt / finished sqft in this layer (honest nulls)
  - NO delinquency/forfeit flags (spine, not signal — same as Ramsey)
  - MaxRecordCount 2000; JSON; geometry = POLYGONS (centroid derived)

=== COMPOSITE-LAYER DEDUP ===
"Parcels - Composite" can carry >1 row per parcel (extra owner rows /
child addresses): 75,593 rows vs 75,308 primary-flagged. We keep
where=1=1 (never lose a parcel to a flag filter) and dedupe with the
seen_pids first-wins set — DETERMINISTIC here because keyset paging
orders by OBJECTID.

=== STREAMING + KEYSET ===
Same page-at-a-time streaming as ramsey/hennepin_parcels, but pages via
the base class's KEYSET mode (WHERE OBJECTID > last ORDER BY OBJECTID) —
constant-time at any depth, the documented fix for the offset-paging
slowdown that killed deep hennepin_parcels pages (2026-07-07).

What it writes:
  - core.parcels rows + raw_data JSONB (county_code 'olmsted')
  - core.owners projection (source 'olmsted_parcels') — same ride-along
    as ramsey_parcels; failures never block parcels
What it does NOT write:
  - signals.distress_events (spine, not signal)
"""

from __future__ import annotations

import re
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
    "https://gweb01.co.olmsted.mn.us/arcgis/rest/services"
    "/General_Land_Info_OlmstedCountyMN/MapServer/3"
)

# MN property-class prefix -> internal property_type. Olmsted publishes the
# statutory class STRING ("1a RESIDENTIAL", "3a COMMERCIAL PREFERENTIAL").
# We map only the unambiguous prefixes; everything else stays NULL (better
# than misclassifying). Class + LandUseDes are preserved in raw_data for
# later refinement against real data.
_CLASS_PREFIX_TO_INTERNAL: dict[str, str] = {
    "1a": "single_family",   # residential homestead
    "1b": "single_family",   # blind/disabled homestead
    "4bb": "single_family",  # residential non-homestead single unit
    "4a": "multifamily",     # apartment (4+ units)
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


def _normalize_olmsted_pid(raw_pid: str) -> str | None:
    """Normalize an Olmsted PARID. The shared normalizer's 'olmsted' rule
    (generic: strip non-alphanumeric, lowercase) passes the verified
    12-digit PARIDs through unchanged; the sanitize fallback keeps any
    non-conforming id usable rather than dropping the parcel (PARID is a
    30-char field, so oddballs are possible)."""
    pid, err = safe_normalize_parcel_id("olmsted", raw_pid)
    if pid is not None:
        return pid
    sanitized = "".join(raw_pid.split())
    return sanitized or None


def _compose_address(*parts: Any) -> str | None:
    """Join address components with single spaces; None if nothing."""
    joined = " ".join(p for p in (_safe_str(x) for x in parts) if p)
    return joined or None


def _map_property_type(class_str: Any) -> str | None:
    s = _safe_str(class_str)
    if not s:
        return None
    prefix = s.split()[0].lower()
    return _CLASS_PREFIX_TO_INTERNAL.get(prefix)


def _polygon_centroid(geometry: dict[str, Any] | None) -> tuple[float | None, float | None]:
    """Approximate centroid (lat, lng) of an ArcGIS polygon in WGS84
    (outSR=4326: coordinates arrive as [lng, lat]). Vertex average of the
    outer ring — plenty for a map pin. Returns (None, None) for missing/
    malformed geometry; also handles point geometry ({x, y}) defensively."""
    if not geometry:
        return None, None
    # Point shape (defensive — this layer is polygons).
    y = _safe_float(geometry.get("y"))
    x = _safe_float(geometry.get("x"))
    if y is not None and x is not None:
        return y, x
    rings = geometry.get("rings")
    if not rings or not isinstance(rings, list) or not rings[0]:
        return None, None
    ring = rings[0]
    xs: list[float] = []
    ys: list[float] = []
    for vertex in ring:
        if not isinstance(vertex, (list, tuple)) or len(vertex) < 2:
            continue
        vx = _safe_float(vertex[0])
        vy = _safe_float(vertex[1])
        if vx is not None and vy is not None:
            xs.append(vx)
            ys.append(vy)
    if not xs:
        return None, None
    return sum(ys) / len(ys), sum(xs) / len(xs)


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


# ============================================================
# OWNER PROJECTION — same vocabulary + patterns as ramsey_parcels /
# signals.owner_distress_summary: government / bank_lender /
# llc_business / individual.
# ============================================================

_OWNER_TYPE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("government", re.compile(
        r"(SECRETARY OF|VETERANS AFFAIRS|\bHUD\b|HOUSING & URBAN|"
        r"HOUSING AND URBAN|COUNTY OF|STATE OF MINNESOTA|CITY OF)")),
    ("bank_lender", re.compile(
        r"(BANK|MORTGAGE|\bMTGE\b|\bMTG\b|LENDING|FINANCIAL|"
        r"CREDIT UNION|NATIONSTAR|FREDDIE|FANNIE|MIDFIRST|BANKUNITED|"
        r"FEDERAL HOME LOAN|FEDERAL NAT|SERVBANK|CITIMORTGAGE)")),
    ("bank_lender", re.compile(
        r"(\bLOAN\b|NATIONAL ASSOC|\bNA\b|\bN A\b|\bN\.A\.|TRUSTEE)")),
    ("llc_business", re.compile(
        r"(\bLLC\b|L\.?L\.?C|\bINC\b|\bLTD\b|HOLDINGS|VENTURES|"
        r"PROPERTIES|RENOVATION|REALTY|GROUP|COMPANY|\bCO\b)")),
]
_LENDER_TRUST = re.compile(
    r"(MORTGAGE|\bMTG\b|\bLOAN\b|PARTIC|POINT|FUNDING|CAPITAL|MASTER|"
    r"TITLE TRUST|TRUST [0-9])")


def _classify_owner(name: str) -> str:
    up = name.upper()
    for otype, pat in _OWNER_TYPE_PATTERNS:
        if pat.search(up):
            return otype
    if "TRUST" in up and _LENDER_TRUST.search(up):
        return "bank_lender"
    return "individual"


def _compose_owner_mailing(attrs: dict[str, Any]) -> str | None:
    """Owner mailing street. Olmsted publishes BOTH composite lines
    (OwnerAddr1-3, often null) and componentized fields; prefer the
    composite when present, else compose (verified sample: Mayo's mailing
    arrives componentized as 200 / 1 / ST / SW = '200 1 ST SW')."""
    line1 = _safe_str(attrs.get("OwnerAddr1"))
    if line1:
        return line1
    no = _safe_str(attrs.get("OwnerAddNo"))
    no_suf = _safe_str(attrs.get("OwnAdNoSuf"))
    number = f"{no}{no_suf}" if (no and no_suf) else no
    return _compose_address(
        number,
        attrs.get("OwnStName"),
        attrs.get("OwnStType"),
        attrs.get("OwnStDir"),
        attrs.get("OwnUnitDes"),
        attrs.get("OwnUnitNo"),
    )


def _compose_site_address(attrs: dict[str, Any]) -> str | None:
    no = _safe_str(attrs.get("SiteAddrNo"))
    no_suf = _safe_str(attrs.get("SiteAddSuf"))
    number = f"{no}{no_suf}" if (no and no_suf) else no
    unit = _safe_str(attrs.get("SiteUnitNo"))
    return _compose_address(
        number,
        attrs.get("SitePreDir"),
        attrs.get("SiteStName"),
        attrs.get("SiteStType"),
        attrs.get("SitePostDir"),
        f"#{unit}" if unit else None,
    )


def _build_owner_row(
    parcel_id: str, attrs: dict[str, Any], now_iso: str
) -> dict[str, Any] | None:
    """Project one Olmsted feature's owner fields into a core.owners row.
    OwnerName1 is the owner-of-record; OwnerName2 (a continuation line,
    e.g. 'MAYO CLINIC' / 'MAYO CLINIC ROCHESTER') stays in raw_data.
    Returns None when the feature carries no owner (honest absence)."""
    owner_name = _safe_str(attrs.get("OwnerName1"))
    if not owner_name:
        return None
    mailing_address = _compose_owner_mailing(attrs)
    mailing_city = _safe_str(attrs.get("OwnerCity"))
    mailing_state = _safe_str(attrs.get("OwnerState"))
    mailing_zip = _safe_str(attrs.get("OwnerZIP5"))
    site_address = _compose_site_address(attrs)
    is_absentee: bool | None = None
    if mailing_address and site_address:
        is_absentee = (
            mailing_address.strip().upper() != site_address.strip().upper()
        )
    is_out_of_state: bool | None = (
        (mailing_state != "MN") if mailing_state else None
    )
    return {
        "parcel_id": parcel_id,
        "owner_name": owner_name,
        "owner_type": _classify_owner(owner_name),
        "mailing_address": mailing_address,
        "mailing_city": mailing_city,
        "mailing_state": mailing_state,
        "mailing_zip": mailing_zip,
        "is_absentee": is_absentee,
        "is_out_of_state": is_out_of_state,
        "is_current": True,
        "source": "olmsted_parcels",
        "observed_at": now_iso,
    }


class OlmstedParcelsScraper(BaseArcGISScraper[dict[str, Any]]):
    """Olmsted County parcels — streaming foundation loader (keyset)."""

    source_name: ClassVar[str] = "olmsted_parcels"
    signal_type: ClassVar[str] = "parcel_foundation"
    county_code: ClassVar[str] = "olmsted"
    feature_service_url: ClassVar[str] = _FEATURE_SERVICE_URL

    where_clause: ClassVar[str] = "1=1"
    return_geometry: ClassVar[bool] = True  # polygons -> centroid map pins
    page_size: ClassVar[int] = 1000          # layer MaxRecordCount is 2000
    max_pages: ClassVar[int] = 120           # 75,593 rows -> ~76 pages; headroom
    progress_log_every: ClassVar[int] = 10000

    # ---- parse_feature: one ArcGIS feature -> parcel dict ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        raw_pid = attributes.get("PARID")
        if not raw_pid:
            return None

        pid = _normalize_olmsted_pid(str(raw_pid))
        if pid is None:
            raise ParseError(
                f"Could not normalize Olmsted PARID {raw_pid!r}",
                source=self.source_name,
            )

        address = _compose_site_address(attributes)
        city = _title_case_city(_safe_str(attributes.get("SiteCity")))
        zip_cd = _safe_str(attributes.get("SiteZip5"))

        lat, lng = _polygon_centroid(geometry)
        # Sanity-bound to Minnesota; discard nonsense coordinates.
        if lat is not None and not (43.0 <= lat <= 50.0):
            lat = None
        if lng is not None and not (-97.5 <= lng <= -89.0):
            lng = None

        property_type = _map_property_type(attributes.get("Class"))
        mkt_val = _safe_decimal(attributes.get("EMVTotal"))
        cleaned_raw = _clean_raw_data(attributes)

        return {
            "parcel_id": pid,
            "address": address,
            "city": city,
            "zip": zip_cd,
            "lat": lat,
            "lng": lng,
            # This layer carries no YearBuilt — honest null, never guessed.
            "year_built": None,
            "property_type": property_type,
            "estimated_market_value": mkt_val,
            "raw_data": cleaned_raw,
        }

    # ---- write: one page at a time (called by streaming run) ----

    async def write(
        self,
        signals: list[dict[str, Any]],
    ) -> tuple[int, int, int]:
        if not signals:
            return 0, 0, 0

        now_iso = datetime.now(timezone.utc).isoformat()
        records_new = 0
        records_failed = 0
        batch: list[dict[str, Any]] = []
        owner_batch: list[dict[str, Any]] = []

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

            owner_row = _build_owner_row(
                sig["parcel_id"], sig.get("raw_data") or {}, now_iso
            )
            if owner_row is not None:
                owner_batch.append(owner_row)

            if len(batch) >= _DB_BATCH_SIZE:
                n, f = self._upsert_batch(batch)
                records_new += n
                records_failed += f
                batch = []
            if len(owner_batch) >= _DB_BATCH_SIZE:
                self._upsert_owner_batch(owner_batch)
                owner_batch = []

        if batch:
            n, f = self._upsert_batch(batch)
            records_new += n
            records_failed += f
        if owner_batch:
            self._upsert_owner_batch(owner_batch)

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

    def _upsert_owner_batch(self, batch: list[dict[str, Any]]) -> None:
        """Upsert owner rows (one current owner per parcel per source).
        Failures are logged but NEVER fail the run — owners are enrichment;
        the parcel write is the source of truth for run status."""
        if not batch:
            return
        try:
            (
                core_table("owners")
                .upsert(batch, on_conflict="parcel_id,source")
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Owner batch upsert failed (parcels unaffected)",
                source=self.source_name,
                batch_size=len(batch),
                error=str(e)[:500],
            )

    # ---- STREAMING run() override (keyset-paged) ----

    async def run(
        self,
        *,
        trigger: str = "scheduler",
        metadata: dict[str, Any] | None = None,
    ) -> RunResult:
        """
        Streaming run: fetch a page, parse it, write it, repeat.

        Mirrors ramsey_parcels exactly, except pages are fetched in the
        base class's KEYSET mode (OBJECTID > cursor, ordered) instead of
        resultOffset — constant-time at any depth.
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
        run_metadata["mode"] = "streaming_keyset"
        run_id = audit_logger.start_run(self.source_name, metadata=run_metadata)

        page_size = self.page_size
        max_pages = self.max_pages
        record_cap = self._max_records_override

        logger.info(
            "Olmsted streaming run starting",
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
        last_object_id = 0  # keyset cursor (OBJECTID > last, ordered ASC)

        try:
            async with httpx.AsyncClient(
                timeout=settings.scraper_request_timeout_seconds,
                headers={"User-Agent": "DistressProperties/1.0"},
            ) as client:
                for page in range(max_pages):
                    if record_cap is not None and total_fetched >= record_cap:
                        break

                    effective_page_size = page_size
                    if record_cap is not None:
                        remaining = record_cap - total_fetched
                        effective_page_size = min(page_size, remaining)

                    # --- FETCH one page (KEYSET mode) ---
                    data = await self._fetch_page(
                        client, 0, effective_page_size,
                        after_object_id=last_object_id,
                    )
                    features = data.get("features") or []
                    if not features:
                        break

                    total_fetched += len(features)

                    # Advance the keyset cursor to the page's max OBJECTID.
                    for feature in features:
                        oid = (feature.get("attributes") or {}).get(
                            self.objectid_field
                        )
                        if isinstance(oid, int) and oid > last_object_id:
                            last_object_id = oid

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
                            continue  # composite duplicate — first row wins
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
                            "Olmsted streaming progress",
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
                "Olmsted streaming run failed",
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
            "Olmsted streaming run complete",
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


__all__ = ["OlmstedParcelsScraper"]
