"""
Minneapolis Vacant Building Registration (VBR) + Condemned scraper.

Source: VBR_MPLS feature service on ArcGIS Online (hosted by GreenInfoNetwork),
the only public, queryable copy of the City of Minneapolis vacant/condemned
building registry that we could locate.

    https://services1.arcgis.com/4ZKi1B1zTblbwgWB/arcgis/rest/services/VBR_MPLS/FeatureServer/0

IMPORTANT — DATA FRESHNESS (honest note):
    This dataset is essentially a COMPLETE SNAPSHOT of the Minneapolis vacant /
    condemned registry as of roughly early-to-mid 2023. As of May 2026 it holds
    309 records; only ~11 carry a 2024+ registry date. It is NOT a continuously
    updated live feed.

    Why we load it anyway:
      - 309 real Minneapolis distressed properties WITH owner names, status,
        dates, and coordinates — data no national competitor surfaces.
      - Vacant buildings are sticky: the City has reported 130+ properties on
        the list for 3+ years, so a 2023 snapshot is largely still accurate.
      - It establishes Minneapolis coverage alongside Saint Paul and Dakota.

    The LIVE current registry lives behind the City's own ArcGIS dashboard
    (subscription-locked) — pursue via an open-data / MGDPA request to the City
    as a follow-up. This scraper re-polls on schedule, so if GreenInfoNetwork
    refreshes the snapshot, we pick it up automatically.

Real fields on the service (DBF-truncated to 10 chars from a shapefile export):
    Address     (str)    property street address
    Address_2   (str)    secondary address line (often blank)
    APN_Txt     (str)    assessor parcel number (Hennepin PID, may have separators)
    Property_s  (str)    property status, e.g. "Vacant+Restoration Agreement"
    Property_O  (str)    property owner name
    Owner_Addr  (str)    owner mailing address (absentee-owner signal)
    Day_of_VBR  (str)    date entered VBR registry, e.g. "August 17, 2015"
    Day_of_CON  (str)    date condemned, e.g. "July 20, 2015" (often blank)
    Day_of_C_1  (str)    secondary condemnation-related date
    Day_of_RA   (str)    date of raze / demolition order (often blank)
    Neighborho  (str)    neighborhood name
    Ward        (int)    city council ward
    City, State, Zip
    Latitude    (float)  decimal latitude
    Longitude   (float)  decimal longitude
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, ClassVar

from src.models.parcel import ParcelUpsert
from src.models.signal import VbrListingInsert
from src.scrapers.base_arcgis_scraper import BaseArcGISScraper
from src.services.event_writer import (
    write_events_dedup,
    write_typed_signals_dedup,
)
from src.services.parcel_resolver import resolve_parcel
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id
from src.utils.logger import logger


# ----- VBR_MPLS ArcGIS Feature Service (layer 0) -----
_FEATURE_SERVICE_URL = (
    "https://services1.arcgis.com/4ZKi1B1zTblbwgWB"
    "/arcgis/rest/services/VBR_MPLS/FeatureServer/0"
)

# Minneapolis VBR annual fee (2024+ schedule). Applied to active registrations.
_VBR_ANNUAL_FEE = Decimal("7228.70")
# Prolonged Vacancy Enforcement monthly citation (post-2-year vacancy).
_PVE_MONTHLY_FINE = Decimal("2000.00")

# Long-form month-name dates seen in the source, e.g. "August 17, 2015".
_DATE_FORMATS = ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d")


def _parse_text_date(raw: Any) -> date | None:
    """Parse the source's string dates ("August 17, 2015") into a date.

    Returns None for blanks, whitespace-only, or unparseable values.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _classify_status(status: str | None, condemned_date: date | None) -> tuple[bool, bool, str]:
    """Map the Property_s text + condemnation date to (boarded, condemned, label).

    The source's Property_s values are free text like:
      "Vacant+Restoration Agreement", "Condemned", "Boarded", etc.
    We classify conservatively:
      - any mention of "condemn" OR a real Day_of_CON date  -> condemned
      - any mention of "board"                              -> boarded
      - otherwise                                           -> registered vacant
    """
    text = (status or "").strip()
    low = text.lower()

    is_condemned = ("condemn" in low) or (condemned_date is not None)
    is_boarded = "board" in low

    label = text if text else "Registered Vacant"
    return is_boarded, is_condemned, label


class MplsVacantBuildingScraper(BaseArcGISScraper[VbrListingInsert]):
    """Minneapolis VBR + condemned buildings — VBR_MPLS ArcGIS source."""

    # ---- Required class config ----
    source_name: ClassVar[str] = "mpls_vbr"
    signal_type: ClassVar[str] = "vbr_listing"
    county_code: ClassVar[str] = "hennepin"  # Minneapolis is in Hennepin County
    feature_service_url: ClassVar[str] = _FEATURE_SERVICE_URL

    # All ~309 records are relevant.
    where_clause: ClassVar[str] = "1=1"

    # The layer has explicit Latitude/Longitude fields, so geometry is optional;
    # we still request it as a fallback.
    return_geometry: ClassVar[bool] = True

    # ---- Feature parsing ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> VbrListingInsert | None:
        """Convert one VBR_MPLS feature into a VbrListingInsert signal."""
        address = attributes.get("Address")
        if not address or not str(address).strip():
            # No address → not actionable as a property lead; skip silently.
            return None

        # --- Parcel ID ---
        # Prefer the real Hennepin PID from APN_Txt so these link to the
        # 448K Hennepin parcels we already loaded. If the APN is missing or
        # not a valid 13-digit Hennepin PID, synthesize a stable id from the
        # source OID so the record is still loadable and dedupes cleanly.
        parcel_id: str | None = None
        raw_apn = attributes.get("APN_Txt")
        if raw_apn and str(raw_apn).strip():
            pid, _err = safe_normalize_parcel_id("hennepin", str(raw_apn))
            if pid is not None:
                parcel_id = pid

        if parcel_id is None:
            fid = attributes.get("FID")
            parcel_id = f"MPLS-VBR-{fid}" if fid is not None else None
            if parcel_id is None:
                return None

        # --- Dates ---
        vbr_date = _parse_text_date(attributes.get("Day_of_VBR"))
        con_date = _parse_text_date(attributes.get("Day_of_CON"))

        # --- Status classification ---
        status_text = attributes.get("Property_s")
        boarded, condemned, label = _classify_status(status_text, con_date)

        # --- Years on registry + PVE eligibility (≥2 yrs vacant) ---
        years_on_registry: float | None = None
        monthly_pve: Decimal | None = None
        if vbr_date is not None:
            days = (date.today() - vbr_date).days
            if days >= 0:
                years_on_registry = round(days / 365.25, 1)
                if years_on_registry >= 2.0:
                    monthly_pve = _PVE_MONTHLY_FINE

        return VbrListingInsert(
            parcel_id=parcel_id,
            city="Minneapolis",
            registry_type=label,
            date_entered_registry=vbr_date,
            years_on_registry=years_on_registry,
            annual_fee=_VBR_ANNUAL_FEE,
            monthly_pve_fine=monthly_pve,
            is_active=True,
            raw_data={
                "attributes": attributes,
                "geometry": geometry,
                "owner_name": str(attributes.get("Property_O") or "").strip() or None,
                "owner_address": str(attributes.get("Owner_Addr") or "").strip() or None,
                "neighborhood": str(attributes.get("Neighborho") or "").strip() or None,
                "condemned_date": con_date.isoformat() if con_date else None,
                "_source": self.source_name,
                "_data_vintage": "2023_snapshot",
            },
            observed_at=datetime.now(timezone.utc),
            source=self.source_name,
            # Stable identity: the parcel id (or the MPLS-VBR-{fid} synthetic
            # when no APN exists). FID is a layer ROW INDEX — using it as
            # source_id gave 12 ids shared across 480 duplicate rows.
            registration_number=parcel_id,
            boarded=boarded,
            condemned=condemned,
            condemned_date=con_date,
        )

    # ---- Write ----

    async def write(
        self,
        signals: list[VbrListingInsert],
    ) -> tuple[int, int, int]:
        """Persist signals: resolve parcels, write typed rows + unified events."""
        if not signals:
            return 0, 0, 0

        # --- Step 1: Resolve each unique parcel ---
        unique_parcels: dict[str, ParcelUpsert] = {}
        for sig in signals:
            if sig.parcel_id in unique_parcels:
                continue

            raw_attributes = (sig.raw_data or {}).get("attributes") or {}
            raw_geometry = (sig.raw_data or {}).get("geometry") or {}

            # Prefer explicit Latitude/Longitude fields; fall back to geometry.
            lat = raw_attributes.get("Latitude")
            lng = raw_attributes.get("Longitude")
            if (lat is None or lng is None) and raw_geometry:
                # VBR_MPLS geometry is Web Mercator (x/y) — only usable as a
                # fallback if we requested outSR=4326. The base scraper requests
                # lat/lng geometry, but the explicit fields are authoritative.
                lng = lng if lng is not None else raw_geometry.get("x")
                lat = lat if lat is not None else raw_geometry.get("y")

            try:
                lat_f = float(lat) if lat is not None else None
                lng_f = float(lng) if lng is not None else None
            except (ValueError, TypeError):
                lat_f, lng_f = None, None

            # Sanity-bound to Minnesota-ish lat/lng; discard obvious Mercator
            # meters that slipped through (e.g. -10386504) so we don't store junk.
            if lat_f is not None and not (43.0 <= lat_f <= 49.5):
                lat_f = None
            if lng_f is not None and not (-97.5 <= lng_f <= -89.0):
                lng_f = None

            address = raw_attributes.get("Address")
            zip_code = raw_attributes.get("Zip")

            unique_parcels[sig.parcel_id] = ParcelUpsert(
                parcel_id=sig.parcel_id,
                county_code=self.county_code,
                state="MN",
                address=str(address).strip() if address else None,
                city="Minneapolis",
                zip=str(zip_code).strip() if zip_code else None,
                lat=lat_f,
                lng=lng_f,
                vacancy_status="vacant",
                data_sources=[self.source_name],
                last_observed_at=datetime.now(timezone.utc),
            )

        for parcel_payload in unique_parcels.values():
            resolve_parcel(parcel_payload)

        # --- Step 2: Write typed signals.vacant_registrations rows ---
        _IN_MEMORY_ONLY = {
            "source", "boarded", "condemned", "registration_number",
            "condemned_date",  # in-memory projection field (2026-07-07)
        }
        signal_rows = []
        for sig in signals:
            row = sig.model_dump(mode="json", exclude_none=True)
            for k in _IN_MEMORY_ONLY:
                row.pop(k, None)
            signal_rows.append(row)

        new_typed, failed_typed = write_typed_signals_dedup(
            "vacant_registrations",
            signal_rows,
            on_conflict="parcel_id,date_entered_registry",
        )

        # --- Step 3: Write unified distress_events ---
        events = [sig.to_event() for sig in signals]
        new_events, failed_events = write_events_dedup(events)

        logger.info(
            "Minneapolis VBR write complete",
            parcels=len(unique_parcels),
            typed_new=new_typed,
            events_new=new_events,
            failed=failed_typed + failed_events,
        )

        return (new_typed, 0, failed_typed + failed_events)


__all__ = ["MplsVacantBuildingScraper"]
