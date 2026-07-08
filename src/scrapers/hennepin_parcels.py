"""
Hennepin County Parcels foundation scraper (STREAMING version).

Source: Hennepin County GIS ArcGIS Server (county-operated, public)
API:    https://gis.hennepin.us/arcgis/rest/services/HennepinData/LAND_PROPERTY/MapServer/1

License: "Furnished AS IS with no warranty" — public, free, no restrictions.

This is the FOUNDATION layer — loads ALL ~448,000 Hennepin parcels into
core.parcels.

=== STREAMING DESIGN ===
Unlike the base fetch()->parse()->write() lifecycle (which holds all records
in memory before writing), this scraper OVERRIDES run() to stream:

    for each page of 1000 records:
        fetch page  ->  parse page  ->  write page  ->  discard

Benefits:
  - Never holds more than ~1000 records in RAM (vs ~1.3GB for 448K at once)
  - Each page is persisted as it's written — a mid-scrape interruption keeps
    all progress so far (the parcels already written stay in the DB)
  - Progress is visible in real-time via /status (parcel_count climbs)

This fixes two problems we hit with the non-streaming version:
  1. 18-minute single HTTP request dropped on Windows TLS (SEC_E_DECRYPT_FAILURE)
  2. Risk of out-of-memory on Railway holding 448K records

The streaming run still works fine for small test runs (max_records=100/5000).

What it writes:
  - core.parcels rows + raw_data JSONB (all 80+ Hennepin attributes preserved)
  - core.owners rows (owner projection, 2026-07-09): assessor
    owner-of-record + mailing + absentee/out-of-state flags, upserted on
    (parcel_id, source) so each quarterly run refreshes the owner set
What it does NOT write:
  - signals.distress_events (parcel existence isn't a distress signal;
    distress mining happens later via raw_data queries)
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
    "https://gis.hennepin.us/arcgis/rest/services"
    "/HennepinData/LAND_PROPERTY/MapServer/1"
)

# Hennepin PR_TYP_CD1 mapping. Expanded with codes found in test data.
# Codes not in this map produce property_type=NULL (better than misclassifying).
_HENNEPIN_PR_TYP_TO_INTERNAL: dict[str, str] = {
    "R": "single_family",
    "A": "agricultural",
    "C": "commercial",
    "I": "industrial",
    "M": "multifamily",
    "T": "townhouse",
    "Y": "townhouse",          # observed: "TOWNHOUSE"
    "S": "land",
    "LC": "commercial",        # observed: "LAND-COMMERCIAL"
    "LI": "industrial",        # observed: "LAND - INDUSTRIAL"
    "LA": "multifamily",       # observed: "VACANT LAND-APARTMENT"
    "HL": "multifamily",       # observed: "LOW INCOME RENTAL"
    "XM": "condo",             # observed: "CONDO GARAGE/MISCELLANEOUS"
    "U": "unknown",
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


def _compose_address(attributes: dict[str, Any]) -> str | None:
    house_no = _safe_str(attributes.get("HOUSE_NO"))
    frac = _safe_str(attributes.get("FRAC_HOUSE_NO"))
    street = _safe_str(attributes.get("STREET_NM"))
    if not house_no and not street:
        return None
    parts = [p for p in (house_no, frac, street) if p]
    return " ".join(parts) if parts else None


def _map_property_type(pr_typ_cd1: Any) -> str | None:
    code = _safe_str(pr_typ_cd1)
    if not code:
        return None
    return _HENNEPIN_PR_TYP_TO_INTERNAL.get(code.upper())


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


# ============================================================
# OWNER PROJECTION (2026-07-09)
# ============================================================
# The Hennepin roll carries the county assessor's owner-of-record on
# every feature (OWNER_NM / TAXPAYER_NM + TAXPAYER_NM_1..3 mailing
# lines). This projection writes core.owners alongside core.parcels on
# every quarterly run, keeping the 2026-07-09 backfill (443,603 owners)
# permanently fresh. Classification uses the SAME vocabulary + patterns
# as ramsey_parcels._classify_owner / signals.owner_distress_summary:
# government / bank_lender / llc_business / individual.
#
# Hennepin mailing-line semantics (verified against live raw_data):
# TAXPAYER_NM_1..3 are ADDRESS LINES, positional FROM THE END — the
# last line matching "CITY ST 55435[-1234]" is city/state/zip; the line
# immediately before it is the mailing street. When three lines exist,
# line 1 is typically a C/O or second-name line. No CSZ line anywhere
# -> honest NULL mailing (never guessed).
#
# is_absentee: unlike Ramsey (exact compare of two same-format county
# fields), Hennepin's mailing line carries the unit ("#127") while the
# composed site address never does — an exact compare would false-flag
# every owner-occupied condo. Both sides are therefore normalized
# (strip trailing unit token, collapse whitespace) before comparing.
# Matches MIGRATION_hennepin_owners_2026-07-09.sql exactly.

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
_CSZ_RE = re.compile(r"^(?P<city>.*?)\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5})(?:-\d{4})?\s*$")
_UNIT_HASH_RE = re.compile(r"\s*#\s*\S+$")
_UNIT_WORD_RE = re.compile(r"\s+(UNIT|APT|STE|SUITE)\s+\S+$")
_MULTI_WS_RE = re.compile(r"\s+")


def _classify_owner(name: str) -> str:
    up = name.upper()
    for otype, pat in _OWNER_TYPE_PATTERNS:
        if pat.search(up):
            return otype
    if "TRUST" in up and _LENDER_TRUST.search(up):
        return "bank_lender"
    return "individual"


def _norm_street(value: str | None) -> str | None:
    """Normalize a street line for the absentee comparison: upper,
    strip a trailing unit token (#127 / UNIT 4 / APT B / STE 200),
    collapse whitespace. Mirrors the migration's mail_norm/site_norm."""
    if not value:
        return None
    s = value.strip().upper()
    s = _UNIT_HASH_RE.sub("", s)
    s = _UNIT_WORD_RE.sub("", s)
    s = _MULTI_WS_RE.sub(" ", s).strip()
    return s or None


def _build_owner_row(
    parcel_id: str,
    attrs: dict[str, Any],
    site_address: str | None,
    now_iso: str,
) -> dict[str, Any] | None:
    """Project one Hennepin feature's owner fields into a core.owners row.
    Returns None when the source publishes no owner (honest absence)."""
    owner_name = _safe_str(attrs.get("OWNER_NM")) or _safe_str(attrs.get("TAXPAYER_NM"))
    if not owner_name:
        return None
    l1 = _safe_str(attrs.get("TAXPAYER_NM_1"))
    l2 = _safe_str(attrs.get("TAXPAYER_NM_2"))
    l3 = _safe_str(attrs.get("TAXPAYER_NM_3"))
    m3 = _CSZ_RE.match(l3) if l3 else None
    m2 = _CSZ_RE.match(l2) if l2 else None
    if m3 is not None:
        mailing_address, m = l2, m3
    elif m2 is not None:
        mailing_address, m = l1, m2
    else:
        mailing_address, m = None, None
    mailing_city = m.group("city").strip() if m else None
    mailing_state = m.group("state") if m else None
    mailing_zip = m.group("zip") if m else None
    mail_norm = _norm_street(mailing_address)
    site_norm = _norm_street(site_address)
    # Absentee: mailing differs from the property itself. NULL when
    # either side is missing — unknown is unknown, never guessed.
    is_absentee: bool | None = None
    if mail_norm and site_norm:
        is_absentee = mail_norm != site_norm
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
        "source": "hennepin_parcels",
        "observed_at": now_iso,
    }


class HennepinParcelsScraper(BaseArcGISScraper[dict[str, Any]]):
    """Hennepin County parcels — streaming foundation loader."""

    source_name: ClassVar[str] = "hennepin_parcels"
    signal_type: ClassVar[str] = "parcel_foundation"
    county_code: ClassVar[str] = "hennepin"
    feature_service_url: ClassVar[str] = _FEATURE_SERVICE_URL

    where_clause: ClassVar[str] = "1=1"
    return_geometry: ClassVar[bool] = False
    page_size: ClassVar[int] = 1000
    max_pages: ClassVar[int] = 500
    progress_log_every: ClassVar[int] = 10000

    # ---- parse_feature: convert one ArcGIS feature into a parcel dict ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        raw_pid = attributes.get("PID")
        if not raw_pid:
            return None

        pid, err = safe_normalize_parcel_id("hennepin", str(raw_pid))
        if pid is None:
            raise ParseError(
                f"Could not normalize Hennepin PID {raw_pid!r}: {err}",
                source=self.source_name,
            )

        address = _compose_address(attributes)
        city = _title_case_city(_safe_str(attributes.get("MUNIC_NM")))
        zip_cd = _safe_str(attributes.get("ZIP_CD"))
        lat = _safe_float(attributes.get("LAT"))
        lng = _safe_float(attributes.get("LON"))
        if lat is not None and not (43.0 <= lat <= 50.0):
            lat = None
        if lng is not None and not (-97.5 <= lng <= -89.0):
            lng = None

        year_built = _safe_int(attributes.get("BUILD_YR"))
        if year_built is not None and year_built < 1700:
            year_built = None

        property_type = _map_property_type(attributes.get("PR_TYP_CD1"))
        mkt_val = _safe_decimal(attributes.get("MKT_VAL_TOT"))
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

    # ---- write: not used directly in streaming mode, but required by base ----

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

            # Owner projection: rides alongside, never blocks parcels.
            owner_row = _build_owner_row(
                sig["parcel_id"],
                sig.get("raw_data") or {},
                sig.get("address"),
                now_iso,
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
        hold the whole 448K dataset in memory and each page is persisted as
        it's processed.
        """
        start_time = time.monotonic()

        # 1. Enabled check
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

        # 2. Lock
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
            "Hennepin streaming run starting",
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
                # KEYSET pagination (2026-07-07): WHERE OBJECTID > last
                # ORDER BY OBJECTID — constant-time pages at any depth.
                # The old offset paging degraded linearly on the county's
                # server (~7s/page at the start, ~21s/page by page 220) and
                # died with connection errors at page ~224 of 448.
                last_oid = 0
                for page in range(max_pages):
                    # Stop if record cap reached
                    if record_cap is not None and total_fetched >= record_cap:
                        break

                    effective_page_size = page_size
                    if record_cap is not None:
                        remaining = record_cap - total_fetched
                        effective_page_size = min(page_size, remaining)

                    # --- FETCH one page (keyset) ---
                    data = await self._fetch_page(
                        client, 0, effective_page_size,
                        after_object_id=last_oid,
                    )
                    features = data.get("features") or []
                    if not features:
                        break

                    total_fetched += len(features)

                    # Advance the keyset cursor. Fail LOUD if the layer
                    # stops returning the object id — silently reusing the
                    # old cursor would loop on the same page forever.
                    oid_field = self.objectid_field
                    page_oids = [
                        f.get("attributes", {}).get(oid_field)
                        for f in features
                    ]
                    page_oids = [o for o in page_oids if isinstance(o, int)]
                    if not page_oids:
                        raise ParseError(
                            f"keyset pagination: no {oid_field} values in "
                            "page — cannot advance cursor",
                            source=self.source_name,
                        )
                    new_last = max(page_oids)
                    if new_last <= last_oid:
                        # Cursor didn't move — server misbehaving; stop
                        # rather than spin.
                        break
                    last_oid = new_last

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
                        # Dedup within the whole run
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
                            "Hennepin streaming progress",
                            scraper=self.source_name,
                            fetched=total_fetched,
                            written=total_new,
                            failed=total_failed,
                            page=page + 1,
                        )
                        while next_progress <= total_fetched:
                            next_progress += self.progress_log_every

                    # --- Stop conditions ---
                    # With keyset ordering, a short page is the definitive
                    # end signal (exceededTransferLimit is an offset-paging
                    # concept and no longer applies).
                    if len(features) < effective_page_size:
                        break

            # Determine final status
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
                "Hennepin streaming run failed",
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
            "Hennepin streaming run complete",
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


__all__ = ["HennepinParcelsScraper"]
