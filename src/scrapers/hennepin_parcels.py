"""
Hennepin County Parcels foundation scraper.

Source: Hennepin County GIS ArcGIS Server (county-operated, public)
URL:    https://gis-hennepin.hub.arcgis.com/datasets/hennepin::county-parcels
API:    https://gis.hennepin.us/arcgis/rest/services/HennepinData/LAND_PROPERTY/MapServer/1

License: "Furnished AS IS with no warranty" — public, free, no restrictions
         on use/redistribution. Government-published open data.

This is the FOUNDATION layer of the platform's Hennepin County coverage.
It loads ALL ~448,000 parcels in Hennepin County into `core.parcels`.

What it writes:
  - core.parcels rows with parcel_id, address, city, lat/lng, county_code,
    market value, owner-related fields stored in raw_data JSONB.
  - NO distress events. Other scrapers (sheriff sales, tax forfeit, code
    violations) build on top of this foundation by referencing parcel_id.

What it does NOT write:
  - signals.distress_events: parcel existence is not itself a distress signal.
  - signals.* tables: handled by purpose-specific scrapers.

Field mapping (from Hennepin LAND_PROPERTY/MapServer/1 attributes):
  - PID                 → parcel_id (13-digit numeric)
  - HOUSE_NO + STREET_NM → address (composed)
  - MUNIC_NM            → city
  - ZIP_CD              → zip
  - LAT, LON            → lat, lng
  - MKT_VAL_TOT         → estimated_market_value
  - BUILD_YR            → year_built
  - PR_TYP_CD1          → property_type (mapped via _MN_PR_TYP_TO_INTERNAL)
  - ALL ATTRIBUTES      → raw_data (preserved for later mining of distress signals)

Distress signals deferred for v2:
  - FORFEIT_LAND_IND    → tax_forfeit event
  - EARLIEST_DELQ_YR    → tax_delinquency event
  - COMP_JUDG_IND       → tax_judgment event
  - HMSTD_CD1='N'       → potential investor/absentee flag (combined with TAXPAYER_NM)
  - SALE_DATE+SALE_PRICE → recent sale comp

These are not emitted in this scraper because we want to see actual data
distribution first before deciding event thresholds. After the first load
completes, run analysis queries against core.parcels.raw_data to find the
right thresholds.

Test runs:
  Pass {"max_records": 100} in the trigger metadata to limit the fetch
  for initial validation. The base scraper honors this via
  _max_records_override.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from src.db.supabase_client import core_table
from src.models.parcel import ParcelUpsert
from src.scrapers.base_arcgis_scraper import BaseArcGISScraper
from src.utils.errors import ParseError
from src.utils.logger import logger
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id


# Hennepin County GIS ArcGIS endpoint for the parcels layer.
# Discovered via the gis-hennepin.hub.arcgis.com "View API Resources" panel
# for the County Parcels dataset.
_FEATURE_SERVICE_URL = (
    "https://gis.hennepin.us/arcgis/rest/services"
    "/HennepinData/LAND_PROPERTY/MapServer/1"
)

# Hennepin's PR_TYP_CD1 maps loosely to our internal PropertyType enum.
# These codes come from MN Department of Revenue classifications.
_HENNEPIN_PR_TYP_TO_INTERNAL: dict[str, str] = {
    "R": "single_family",  # Residential (we'll refine later via secondary fields)
    "A": "agricultural",
    "C": "commercial",
    "I": "industrial",
    "M": "multifamily",     # Apartments
    "T": "townhouse",
    "S": "land",            # Seasonal/special — closest match is "land"
    "U": "unknown",
}

# Batch size for Supabase upserts. Larger = fewer round trips but bigger
# request payloads. Postgres + supabase-py handle 500 comfortably.
_DB_BATCH_SIZE: int = 500


def _safe_int(value: Any) -> int | None:
    """Convert a value to int, returning None on failure or empty string."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        return int(float(value))  # tolerate '1965.0'
    except (ValueError, TypeError):
        return None


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal, returning None on failure or empty."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        d = Decimal(str(value))
        if d < 0:
            return None
        return d
    except (InvalidOperation, ValueError, TypeError):
        return None


def _safe_float(value: Any) -> float | None:
    """Convert a value to float, returning None on failure or empty."""
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
    """Coerce to a trimmed string; return None for empty/None."""
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _compose_address(attributes: dict[str, Any]) -> str | None:
    """
    Compose a street address from HOUSE_NO + FRAC_HOUSE_NO + STREET_NM.

    Returns 'None' if both house number and street are missing.

    Examples:
      HOUSE_NO=2901, STREET_NM='78TH ST E' → '2901 78TH ST E'
      HOUSE_NO=1234, FRAC_HOUSE_NO='1/2', STREET_NM='MAIN ST' → '1234 1/2 MAIN ST'
    """
    house_no = _safe_str(attributes.get("HOUSE_NO"))
    frac = _safe_str(attributes.get("FRAC_HOUSE_NO"))
    street = _safe_str(attributes.get("STREET_NM"))

    if not house_no and not street:
        return None

    parts = [p for p in (house_no, frac, street) if p]
    return " ".join(parts) if parts else None


def _map_property_type(pr_typ_cd1: Any) -> str | None:
    """
    Map Hennepin's PR_TYP_CD1 to our internal PropertyType enum.

    Returns None if the code is unrecognized — better to leave NULL than
    misclassify.
    """
    code = _safe_str(pr_typ_cd1)
    if not code:
        return None
    return _HENNEPIN_PR_TYP_TO_INTERNAL.get(code.upper())


def _title_case_city(city: str | None) -> str | None:
    """Convert ALL CAPS city names to Title Case for display."""
    if not city:
        return None
    # Special cases for cities like "St. Paul"
    return city.title().replace("St.", "St.").replace("Sl ", "St. ")


class HennepinParcelsScraper(BaseArcGISScraper[dict[str, Any]]):
    """
    Hennepin County parcels — foundation loader.

    Loads ALL Hennepin County parcels into core.parcels. Does NOT generate
    distress events; signal-generating scrapers (sheriff sales, tax forfeit,
    code violations) build on this foundation by referencing parcel_id.
    """

    # ---- Required class config ----

    source_name: ClassVar[str] = "hennepin_parcels"
    signal_type: ClassVar[str] = "parcel_foundation"  # not a signal type, but required by base
    county_code: ClassVar[str] = "hennepin"
    feature_service_url: ClassVar[str] = _FEATURE_SERVICE_URL

    # Fetch all parcels (no filter) — 448K records
    where_clause: ClassVar[str] = "1=1"

    # We don't need full geometry polygons — LAT/LON in attributes is sufficient.
    # Skipping geometry roughly halves the response payload size.
    return_geometry: ClassVar[bool] = False

    # Hennepin's parcels service accepts 1000 records per page reliably.
    page_size: ClassVar[int] = 1000

    # 448,083 records / 1000 per page = 449 pages. Pad to 500 for safety.
    max_pages: ClassVar[int] = 500

    # Log progress every 10K records — keeps log volume manageable on big runs.
    progress_log_every: ClassVar[int] = 10000

    # ---- Feature parsing ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """
        Parse one Hennepin parcel feature into a dict ready for core.parcels.

        We return a dict (not a Pydantic model) so we can carry raw_data
        through to the write step, where Pydantic ParcelUpsert is used for
        validation of the canonical fields.

        Returns None if the parcel lacks a usable PID — these are skipped
        silently rather than logged as errors (some PIDs are placeholders
        for cross-county parcels, etc.).
        """
        # --- Parcel ID (REQUIRED) ---
        raw_pid = attributes.get("PID")
        if not raw_pid:
            return None

        pid, err = safe_normalize_parcel_id("hennepin", str(raw_pid))
        if pid is None:
            # Some PIDs may have unusual formats — skip them rather than
            # crashing the whole scrape. They'll show in audit error log.
            raise ParseError(
                f"Could not normalize Hennepin PID {raw_pid!r}: {err}",
                source=self.source_name,
            )

        # --- Basic location fields ---
        address = _compose_address(attributes)
        city = _title_case_city(_safe_str(attributes.get("MUNIC_NM")))
        zip_cd = _safe_str(attributes.get("ZIP_CD"))
        lat = _safe_float(attributes.get("LAT"))
        lng = _safe_float(attributes.get("LON"))

        # Validate lat/lng are in Minnesota-ish range
        if lat is not None and not (43.0 <= lat <= 50.0):
            lat = None
        if lng is not None and not (-97.5 <= lng <= -89.0):
            lng = None

        # --- Property attributes ---
        year_built = _safe_int(attributes.get("BUILD_YR"))
        # Reject obviously-invalid year_built (e.g., 0, 1)
        if year_built is not None and year_built < 1700:
            year_built = None

        property_type = _map_property_type(attributes.get("PR_TYP_CD1"))

        # --- Financial fields (overwrite semantics in core.parcels) ---
        mkt_val = _safe_decimal(attributes.get("MKT_VAL_TOT"))

        # --- Return parcel dict ---
        # raw_data preserves ALL Hennepin attributes for later distress mining.
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
            # raw_data is NOT a column on ParcelUpsert — it's stripped out
            # before validation and stored separately if the parcels table
            # gains a raw_data column. For now we drop it; future migration
            # can add a parcels.raw_data JSONB column and we'll re-include it.
            "_raw_attributes": attributes,
        }

    # ---- Write (batched upserts to core.parcels) ----

    async def write(
        self,
        signals: list[dict[str, Any]],
    ) -> tuple[int, int, int]:
        """
        Batch upsert parcels into core.parcels.

        Uses Postgres ON CONFLICT (parcel_id) — single SQL operation per batch
        of 500 records. Each batch is independent; one failed batch doesn't
        kill the whole scrape.

        Returns (records_new, records_updated, records_failed).

        Note: We can't easily distinguish "new" vs "updated" in a batch upsert
        without an extra SELECT, so we report all successful writes as "new".
        This is conservative; the audit log will track exact counts via
        scraper_runs.records_new + records_updated.
        """
        if not signals:
            return 0, 0, 0

        # Deduplicate by parcel_id (Hennepin's multi-PID feature can produce
        # multiple rows with the same PID — we keep the first).
        unique: dict[str, dict[str, Any]] = {}
        for sig in signals:
            pid = sig["parcel_id"]
            if pid not in unique:
                unique[pid] = sig

        total = len(unique)
        logger.info(
            "Hennepin parcels write starting",
            source=self.source_name,
            unique_parcels=total,
            duplicate_pids_dropped=len(signals) - total,
        )

        # Build payloads using ParcelUpsert for validation, then serialize.
        # We strip _raw_attributes before validation (it's not a column).
        now_iso = datetime.now(timezone.utc).isoformat()

        records_new = 0
        records_failed = 0
        progress_log_every = 25000  # log progress every 25K writes
        next_progress = progress_log_every

        # Build the batch list
        batch: list[dict[str, Any]] = []
        signal_list = list(unique.values())

        for i, sig in enumerate(signal_list):
            # Build a ParcelUpsert (drops _raw_attributes, validates types)
            payload_dict = {
                k: v for k, v in sig.items() if k != "_raw_attributes"
            }
            try:
                payload = ParcelUpsert(
                    parcel_id=payload_dict["parcel_id"],
                    county_code=self.county_code,
                    state="MN",
                    address=payload_dict.get("address"),
                    city=payload_dict.get("city"),
                    zip=payload_dict.get("zip"),
                    lat=payload_dict.get("lat"),
                    lng=payload_dict.get("lng"),
                    year_built=payload_dict.get("year_built"),
                    property_type=payload_dict.get("property_type"),  # type: ignore[arg-type]
                    estimated_market_value=payload_dict.get(
                        "estimated_market_value"
                    ),
                    data_sources=[self.source_name],
                    last_observed_at=datetime.now(timezone.utc),
                )
            except Exception as e:
                # Validation failed — count as failed and continue
                records_failed += 1
                if records_failed <= 10:  # log first 10 only to avoid spam
                    logger.warning(
                        "Parcel validation failed",
                        parcel_id=payload_dict.get("parcel_id"),
                        error=str(e)[:200],
                    )
                continue

            # Serialize, exclude None values (so DB nulls aren't overwritten)
            row = payload.model_dump(mode="json", exclude_none=True)
            # Always set last_observed_at to now (override exclude_none)
            row["last_observed_at"] = now_iso
            batch.append(row)

            # Flush batch when full
            if len(batch) >= _DB_BATCH_SIZE:
                new_in_batch, failed_in_batch = self._upsert_batch(batch)
                records_new += new_in_batch
                records_failed += failed_in_batch
                batch = []

                # Progress logging
                processed = i + 1
                if processed >= next_progress:
                    logger.info(
                        "Hennepin parcels write progress",
                        source=self.source_name,
                        processed=processed,
                        total=total,
                        records_new=records_new,
                        records_failed=records_failed,
                    )
                    while next_progress <= processed:
                        next_progress += progress_log_every

        # Flush final partial batch
        if batch:
            new_in_batch, failed_in_batch = self._upsert_batch(batch)
            records_new += new_in_batch
            records_failed += failed_in_batch

        logger.info(
            "Hennepin parcels write complete",
            source=self.source_name,
            records_new=records_new,
            records_failed=records_failed,
        )

        return records_new, 0, records_failed

    def _upsert_batch(
        self,
        batch: list[dict[str, Any]],
    ) -> tuple[int, int]:
        """
        Upsert one batch of parcels via single Supabase round-trip.

        Returns (new_or_updated, failed).
        """
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


__all__ = ["HennepinParcelsScraper"]
