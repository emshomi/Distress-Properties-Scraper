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
    market value, year_built, property_type
  - core.parcels.raw_data JSONB: ALL Hennepin attributes preserved verbatim
    (80+ fields including FORFEIT_LAND_IND, EARLIEST_DELQ_YR, COMP_JUDG_IND,
    TAXPAYER_NM, HMSTD_CD1, etc.) for later distress-signal mining

What it does NOT write (deferred to v2):
  - signals.distress_events: parcel existence is not itself a distress signal
  - signals.* tables: handled by purpose-specific scrapers

After this scraper completes, distress-signal mining queries can run against
core.parcels.raw_data to surface flags like:

    SELECT parcel_id, raw_data->>'EARLIEST_DELQ_YR' AS first_delq
    FROM core.parcels
    WHERE county_code = 'hennepin'
      AND raw_data->>'FORFEIT_LAND_IND' = 'Y';

Field mapping (from Hennepin LAND_PROPERTY/MapServer/1 attributes):
  - PID                 -> parcel_id (13-digit numeric)
  - HOUSE_NO + STREET_NM -> address (composed)
  - MUNIC_NM            -> city
  - ZIP_CD              -> zip
  - LAT, LON            -> lat, lng
  - MKT_VAL_TOT         -> estimated_market_value
  - BUILD_YR            -> year_built
  - PR_TYP_CD1          -> property_type (mapped via _HENNEPIN_PR_TYP_TO_INTERNAL)
  - ALL ATTRIBUTES      -> raw_data JSONB

Test runs:
  Pass `?max_records=100` to the trigger endpoint to limit fetch for
  initial validation. The base scraper honors this via _max_records_override.
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
_FEATURE_SERVICE_URL = (
    "https://gis.hennepin.us/arcgis/rest/services"
    "/HennepinData/LAND_PROPERTY/MapServer/1"
)

# Hennepin PR_TYP_CD1 mapping. Codes not in this map produce property_type=NULL.
# After first full scrape, group by raw_data->>'PR_TYP_CD1' to find uncommon
# codes and expand this map.
_HENNEPIN_PR_TYP_TO_INTERNAL: dict[str, str] = {
    "R": "single_family",
    "A": "agricultural",
    "C": "commercial",
    "I": "industrial",
    "M": "multifamily",
    "T": "townhouse",
    "S": "land",
    "U": "unknown",
}

# Batch size for Supabase upserts. With raw_data JSONB included, each row is
# ~2-4KB, so 500 per batch = ~1-2MB per HTTP request (well under limits).
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
        return int(float(value))
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

    Returns None if both house number and street are missing.
    """
    house_no = _safe_str(attributes.get("HOUSE_NO"))
    frac = _safe_str(attributes.get("FRAC_HOUSE_NO"))
    street = _safe_str(attributes.get("STREET_NM"))

    if not house_no and not street:
        return None

    parts = [p for p in (house_no, frac, street) if p]
    return " ".join(parts) if parts else None


def _map_property_type(pr_typ_cd1: Any) -> str | None:
    """Map Hennepin's PR_TYP_CD1 to our internal PropertyType enum."""
    code = _safe_str(pr_typ_cd1)
    if not code:
        return None
    return _HENNEPIN_PR_TYP_TO_INTERNAL.get(code.upper())


def _title_case_city(city: str | None) -> str | None:
    """Convert ALL CAPS city names to Title Case for display."""
    if not city:
        return None
    return city.title()


def _clean_raw_data(attributes: dict[str, Any]) -> dict[str, Any]:
    """
    Clean Hennepin attributes for JSONB storage.

    Strips empty strings (so they're absent rather than ""), trims string
    values, and ensures all values are JSON-serializable. Numeric and bool
    types pass through unchanged.

    Returns a copy — does not mutate the input.
    """
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


class HennepinParcelsScraper(BaseArcGISScraper[dict[str, Any]]):
    """
    Hennepin County parcels — foundation loader.

    Loads ALL Hennepin County parcels into core.parcels. Preserves source
    attributes in raw_data JSONB for later distress-signal mining. Does NOT
    generate distress events directly.
    """

    source_name: ClassVar[str] = "hennepin_parcels"
    signal_type: ClassVar[str] = "parcel_foundation"
    county_code: ClassVar[str] = "hennepin"
    feature_service_url: ClassVar[str] = _FEATURE_SERVICE_URL

    where_clause: ClassVar[str] = "1=1"
    return_geometry: ClassVar[bool] = False
    page_size: ClassVar[int] = 1000
    max_pages: ClassVar[int] = 500
    progress_log_every: ClassVar[int] = 10000

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Parse one Hennepin parcel feature into a dict ready for core.parcels."""
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

        # Validate lat/lng are in Minnesota-ish range
        if lat is not None and not (43.0 <= lat <= 50.0):
            lat = None
        if lng is not None and not (-97.5 <= lng <= -89.0):
            lng = None

        year_built = _safe_int(attributes.get("BUILD_YR"))
        if year_built is not None and year_built < 1700:
            year_built = None

        property_type = _map_property_type(attributes.get("PR_TYP_CD1"))
        mkt_val = _safe_decimal(attributes.get("MKT_VAL_TOT"))

        # Preserve ALL Hennepin attributes for later mining
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

    async def write(
        self,
        signals: list[dict[str, Any]],
    ) -> tuple[int, int, int]:
        """
        Batch upsert parcels into core.parcels.

        Returns (records_new, records_updated, records_failed).
        """
        if not signals:
            return 0, 0, 0

        # Deduplicate by parcel_id
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

        now_iso = datetime.now(timezone.utc).isoformat()
        records_new = 0
        records_failed = 0
        progress_log_every = 25000
        next_progress = progress_log_every

        batch: list[dict[str, Any]] = []
        signal_list = list(unique.values())

        for i, sig in enumerate(signal_list):
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
                if records_failed <= 10:
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
                new_in_batch, failed_in_batch = self._upsert_batch(batch)
                records_new += new_in_batch
                records_failed += failed_in_batch
                batch = []

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
        """Upsert one batch via single Supabase round-trip."""
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
