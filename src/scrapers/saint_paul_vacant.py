"""
Saint Paul DSI (Department of Safety and Inspections) Vacant Buildings scraper.

Source: Saint Paul Open Information portal (ArcGIS Hub)
URL: https://information.stpaul.gov/datasets/vacant-buildings-3

Real fields available from the ArcGIS Feature Service:
  - ADDRESS         (text)      property street address
  - VACANT_AS_OF    (date ms)   date the property was first registered as vacant
  - DWELLING_TYPE   (text)      'Single Family Residential', 'Duplex', 'Commercial', etc.
  - VB_CATEGORY     (int 1-3)   1=registered vacant, 2=boarded, 3=condemned
  - WARD            (int)       city council ward
  - DISTRICT        (int)       planning district number
  - CENSUS_TRACT    (text)      census tract code
  - PIN             (text 12)   Ramsey County 12-digit parcel ID
  - LATITUDE        (float)     decimal latitude
  - LONGITUDE       (float)     decimal longitude

Saint Paul publishes ~384 vacant buildings (as of May 2026). Category 1 is
the largest bucket — these are properties registered with DSI but not yet
boarded or condemned. Categories 2 and 3 are higher-severity distress
signals worth surfacing prominently.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, ClassVar

from src.models.parcel import ParcelUpsert
from src.models.signal import VbrListingInsert
from src.scrapers.base_arcgis_scraper import (
    BaseArcGISScraper,
    arcgis_date_to_date_only,
)
from src.services.event_writer import (
    write_events_dedup,
    write_typed_signals_dedup,
)
from src.services.parcel_resolver import resolve_parcel
from src.utils.errors import ParseError
from src.utils.logger import logger
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id


# ----- Saint Paul ArcGIS Feature Service URL -----
# Discovered via the ArcGIS Hub "View API Resources" panel for the
# Vacant Buildings dataset. The /query endpoint is appended by the base class.
_FEATURE_SERVICE_URL = (
    "https://services1.arcgis.com/9meaaHE3uiba0zr8"
    "/arcgis/rest/services/VacantBuildings/FeatureServer/0"
)


def _category_to_flags(category: Any) -> tuple[bool, bool, str]:
    """
    Map Saint Paul VB_CATEGORY (1, 2, 3) to (boarded, condemned, category_label).

    Saint Paul DSI uses three categories:
      1 = Registered vacant (no sale without compliance)
      2 = Boarded (no sale without city approval + code compliance report)
      3 = Condemned (no sale without certificate of occupancy)
    """
    try:
        cat_int = int(category) if category is not None else 0
    except (ValueError, TypeError):
        cat_int = 0

    if cat_int == 3:
        return False, True, "Category 3 - Condemned"
    if cat_int == 2:
        return True, False, "Category 2 - Boarded"
    if cat_int == 1:
        return False, False, "Category 1 - Registered Vacant"
    return False, False, f"Unknown (raw={category!r})"


class SaintPaulVacantBuildingScraper(BaseArcGISScraper[VbrListingInsert]):
    """Saint Paul DSI vacant buildings — ArcGIS Hub source."""

    # ---- Required class config ----

    source_name: ClassVar[str] = "saint_paul_vacant"
    signal_type: ClassVar[str] = "vbr_listing"
    county_code: ClassVar[str] = "ramsey"
    feature_service_url: ClassVar[str] = _FEATURE_SERVICE_URL

    # Fetch all records (no filter) — there are ~384 total, all relevant
    where_clause: ClassVar[str] = "1=1"

    # We need lat/lng from geometry
    return_geometry: ClassVar[bool] = True

    # ---- Feature parsing ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> VbrListingInsert | None:
        """
        Convert one ArcGIS feature into a VbrListingInsert signal.

        Fields we use (per the actual Saint Paul service):
          ADDRESS, VACANT_AS_OF, DWELLING_TYPE, VB_CATEGORY,
          PIN, LATITUDE, LONGITUDE, WARD, DISTRICT, CENSUS_TRACT
        """
        # --- Parcel ID (required) ---
        raw_pin = attributes.get("PIN")
        if not raw_pin:
            # Some rows may legitimately not have a PIN (e.g., new construction)
            # — skip silently rather than logging an error.
            return None

        pid, err = safe_normalize_parcel_id("ramsey", str(raw_pin))
        if pid is None:
            raise ParseError(
                f"Could not normalize Saint Paul PIN {raw_pin!r}: {err}",
                source=self.source_name,
            )

        # --- VBR category (1, 2, or 3) ---
        raw_category = attributes.get("VB_CATEGORY")
        boarded, condemned, category_label = _category_to_flags(raw_category)

        # --- Registration date ---
        # VACANT_AS_OF arrives as a STRING like "05/02/2024" on this service
        # (not ArcGIS epoch-ms). The old epoch-only parse returned None for
        # every row, and the event projection then fabricated scrape-day
        # dates — the root of the ~38x Saint Paul duplication. Handle both.
        raw_vacant_as_of = attributes.get("VACANT_AS_OF")
        registered_date: date | None = None
        registered_date_str = arcgis_date_to_date_only(raw_vacant_as_of)
        if registered_date_str:
            try:
                registered_date = date.fromisoformat(registered_date_str)
            except ValueError:
                registered_date = None
        if registered_date is None and raw_vacant_as_of:
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                try:
                    registered_date = datetime.strptime(
                        str(raw_vacant_as_of).strip(), fmt
                    ).date()
                    break
                except ValueError:
                    continue

        # --- Build the signal ---
        # Field names match signals.vacant_registrations columns directly.
        # `source`, `boarded`, `condemned`, `registration_number` are in-memory
        # only — they are stripped before write to vacant_registrations and
        # used to project the unified distress_events row.
        return VbrListingInsert(
            parcel_id=pid,
            # ADDED 2026-08-10. The write() step below already patches
            # county_code onto the TYPED vacant_registrations rows, and its
            # comment records why: a NULL-county row dedups only against
            # other NULL-county rows, "precisely how 1,451 duplicate
            # distress_events accumulated in 24 hours on 2026-08-07".
            #
            # The EVENT projection was never given the same treatment.
            # to_event() now carries county_code through, so setting it here
            # fixes the same class of bug on signals.distress_events, where
            # the composite FK (county_code, parcel_id) -> core.parcels and
            # distress_events_dedup_key both depend on it.
            county_code=self.county_code,
            city="Saint Paul",
            registry_type=category_label,
            date_entered_registry=registered_date,
            is_active=True,
            raw_data={
                "attributes": attributes,
                "geometry": geometry,
                "dwelling_type": str(attributes.get("DWELLING_TYPE") or "unknown"),
                "_source": self.source_name,
            },
            observed_at=datetime.now(timezone.utc),
            source=self.source_name,
            # Saint Paul publishes no per-record ID; the PIN is the stable
            # identity (one active registration per parcel). NULL source_ids
            # left 14,592 rows with no usable key before 2026-07-07.
            registration_number=pid,
            boarded=boarded,
            condemned=condemned,
        )

    # ---- Write ----

    async def write(
        self,
        signals: list[VbrListingInsert],
    ) -> tuple[int, int, int]:
        """
        Persist signals into Supabase.

        Three writes happen per scraper run:
          1. resolve_parcel() for each unique parcel
          2. signals.vacant_registrations  (typed table)
          3. signals.distress_events (unified feed)
        """
        if not signals:
            return 0, 0, 0

        # --- Step 1: Resolve each unique parcel ---
        unique_parcels: dict[str, ParcelUpsert] = {}
        for sig in signals:
            if sig.parcel_id not in unique_parcels:
                # Extract lat/lng from the raw geometry (set by parse_feature)
                raw_geometry = (sig.raw_data or {}).get("geometry") or {}
                raw_attributes = (sig.raw_data or {}).get("attributes") or {}

                lat = raw_attributes.get("LATITUDE")
                lng = raw_attributes.get("LONGITUDE")

                # Prefer attributes lat/lng; fall back to geometry
                if (lat is None or lng is None) and raw_geometry:
                    lng = raw_geometry.get("x") or lng
                    lat = raw_geometry.get("y") or lat

                try:
                    lat_f = float(lat) if lat is not None else None
                    lng_f = float(lng) if lng is not None else None
                except (ValueError, TypeError):
                    lat_f, lng_f = None, None

                # Build address string for the parcel record
                address = raw_attributes.get("ADDRESS")

                unique_parcels[sig.parcel_id] = ParcelUpsert(
                    parcel_id=sig.parcel_id,
                    county_code=self.county_code,
                    state="MN",
                    address=str(address) if address else None,
                    city="Saint Paul",
                    lat=lat_f,
                    lng=lng_f,
                    vacancy_status="vacant",
                    data_sources=[self.source_name],
                    last_observed_at=datetime.now(timezone.utc),
                )

        # resolve_parcel returns None on failure. Its result MUST be counted
        # and folded into the run's failure total.
        #
        # FIXED 2026-08-07. This loop previously discarded the return value, and
        # this method logged NOTHING — so a fully-failing parcel leg was
        # invisible in both the run counters and the logs. Measured live: the
        # 15:47 run on 2026-08-07 reported records_failed=0 and status=success
        # while every one of its 384 parcel upserts was failing with 42P10
        # (parcel_resolver.py carried a stale single-column on_conflict after
        # core.parcels moved to PRIMARY KEY (county_code, parcel_id)). The
        # failure was only found by querying core.parcels.last_observed_at,
        # which had not moved since 13:34.
        #
        # dakota_sheriff.py has always done this correctly and was therefore
        # the only affected scraper whose logs showed the failure directly.
        # Match that pattern; never discard this return value.
        parcels_ok = 0
        parcels_failed = 0
        for parcel_payload in unique_parcels.values():
            if resolve_parcel(parcel_payload) is not None:
                parcels_ok += 1
            else:
                parcels_failed += 1

        # --- Step 2: Write typed signals.vacant_registrations rows ---
        # Strip in-memory-only fields that don't exist as columns in
        # signals.vacant_registrations: source, boarded, condemned,
        # registration_number. They live in raw_data already (under _source)
        # and drive the unified event_type / severity via to_event().
        _IN_MEMORY_ONLY = {
            "source", "boarded", "condemned", "registration_number",
            "condemned_date",  # in-memory projection field (2026-07-07)
        }
        signal_rows = []
        for sig in signals:
            row = sig.model_dump(mode="json", exclude_none=True)
            for k in _IN_MEMORY_ONLY:
                row.pop(k, None)
            # county_code is set EXPLICITLY, not inherited from the model.
            # `county_code` on this class is a ClassVar, so it is not a
            # pydantic field and never appears in model_dump — every row
            # reached PostgREST without it. That was survivable while the
            # dedup index was (parcel_id, date_entered_registry); it is not
            # survivable now that county_code is the FIRST column of
            # vacant_registrations_dedup. The index is NULLS NOT DISTINCT,
            # so a NULL-county row dedups against other NULL-county rows and
            # looks healthy, while never matching the correctly-labelled row
            # for the same property. That is precisely how 1,451 duplicate
            # distress_events accumulated in 24 hours on 2026-08-07.
            row["county_code"] = self.county_code
            signal_rows.append(row)

        # Conflict target must match signals.vacant_registrations_dedup, rebuilt
        # in Phase 5 of the composite-key migration as
        #   (county_code, parcel_id, date_entered_registry) NULLS NOT DISTINCT
        # because Minnesota county PINs are not globally unique (51,662 nine-char
        # PINs are shared across counties). Columns are listed in index order.
        # write_typed_signals_dedup passes ignore_duplicates=False, so this is a
        # real upsert-update: a target matching no unique index raises 42P10 and
        # PostgREST rejects the ENTIRE batch. event_writer catches that into a
        # logger.warning and the run still reports counts, so the scraper looks
        # like it completed. Never change this without re-reading the index from
        # pg_catalog.
        new_typed, failed_typed = write_typed_signals_dedup(
            "vacant_registrations",
            signal_rows,
            on_conflict="county_code,parcel_id,date_entered_registry",
        )

        # --- Step 3: Write unified distress_events ---
        events = [sig.to_event() for sig in signals]
        new_events, failed_events = write_events_dedup(events)

        logger.info(
            "Saint Paul vacant write complete",
            source=self.source_name,
            parcels_ok=parcels_ok,
            parcels_failed=parcels_failed,
            typed_new=new_typed,
            events_new=new_events,
            failed=failed_typed + failed_events + parcels_failed,
        )

        return (
            new_typed,
            0,
            failed_typed + failed_events + parcels_failed,
        )


__all__ = ["SaintPaulVacantBuildingScraper"]
