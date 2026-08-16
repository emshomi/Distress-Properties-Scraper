"""
Minneapolis Vacant Building Registration (VBR) + Condemned scraper.

Source: the City of Minneapolis's own Tableau Server, the same list the City
publishes to residents on the Vacant & Condemned Property Dashboard.

    https://tableau.minneapolismn.gov/views/MinneapolisVacantCondemnedPropertyInventory/DailyVBRcondemnedpropertylist.csv

=== SOURCE CHANGED 2026-08-16 — READ THIS BEFORE TOUCHING THE URL ===
This scraper previously read the City ArcGIS service
`services.arcgis.com/afSMGVsC7QlRK1kZ/.../VBR_October2025/FeatureServer/0`,
adopted 2026-08-07 on the strength of `hasStaticData: false` and a
`dataLastEditDate` of 2025-10-22.

**That service reports itself live and is not.** Measured 2026-08-16 from
signals.vacant_registrations: 311 rows, newest `date_entered_registry`
2024-10-31 — twenty-one months stale. The City's own published list on the
same day held **414 rows, newest 2026-08-12**. The 08-07 change moved from a
2023 snapshot to a 2024 one. `hasStaticData` is a claim about the service,
not a measurement of the data in it; check the newest RECORD, not the flag.

Endpoint stability was verified before adopting it: two pulls an hour apart
returned byte-identical files (md5 ede44fdf82a53e95f7b3dbb1f23f88c7, 33,881
bytes, 415 lines) with no browser session in between.

=== WHY A CSV AND NOT AN API ===
Minneapolis retired its machine-readable publication of this registry. The
data now lives only behind Tableau, and the platform moved to MinneapolisData
(data.minneapolismn.gov) on 2026-08-13. The catalog there lists the vacant
registry as a Hub Page wrapping this dashboard — Feature Service (0), Map
Service (0). Tableau Server's `.csv` view export is the only machine-readable
door, and it is unauthenticated.

The City's Regulatory Services VIOLATIONS feed is behind the same wall and is
NOT solved: `/views/OpenDataRegulatoryServices-Violations/ViolationDetails.csv`
returns a single aggregate (`Number of Records`), `/vudcsv/` 404s server-wide,
and `PropertySearch.csv?Address=<addr>` returns a per-address COUNT only
(verified: 167 for 2309 HAYES ST NE, matching the dashboard). Row-level
violations need a request to MapIT Minneapolis, not another URL variant.

=== TWO FIELD MISREADINGS FIXED HERE ===
The City's own legend, printed on the VBR Properties view:

    VBR  - Date entered in the Vacant Building Registration program.
           If no condemned date the building is vacant but not condemned.
    CONB - Date the building was condemned for BEING BOARDED.
    CON1 - Date the building was condemned for LACK OF MAINTENANCE.
    RA   - Date the RESTORATION AGREEMENT was signed.

1. **RA is a Restoration Agreement, not a raze order.** The previous
   docstring called `USER_Day_of_RA_Date` the "raze / demolition order date"
   and classified those properties as `condemned=True`, registry_type
   "Raze Order Issued". A Restoration Agreement is the owner COMMITTING TO
   REPAIR — the opposite signal — and because raze outranked condemnation in
   the old severity ladder it overwrote the property's real status. Two
   Hennepin rows carried that label on 2026-08-16. This CSV does not publish
   RA at all, so the misreading has no input here; it is recorded so nobody
   reintroduces it from another source.

2. **`boarded` IS determinable — it is CONB.** The previous docstring said
   "boarded can no longer be determined and is always False. Do not infer
   it." CONB is the City's own field for condemned-for-being-boarded. 117 of
   311 rows carried a CONB date while every one of them reported
   `boarded = false`, against a boarded_building event type holding 9,990
   rows from other sources and zero from Minneapolis.

=== COLUMNS ON THE CSV ===
    Address           property street address
    APN               Hennepin PID — 12 OR 13 chars (see below)
    blank             junk column, trailing space in the header, ignored
    CON1              condemned for lack of maintenance, "" when absent
    CONB              condemned for being boarded, "" when absent
    Neighborhood      neighborhood name
    Property Owner    owner name
    VBR Date          date entered the registry, populated on all 414 rows
    Ward              city council ward

Dates are `%m/%d/%y` — TWO-DIGIT years ("08/12/26"). Python maps 00-68 to
2000-2068, which covers the observed range (2006-12-12 .. 2026-08-12).

**PARSE BY COLUMN NAME, NEVER BY POSITION.** The CSV alphabetises its columns
and the dashboard does not, so the two disagree on order. The header also
carries a junk `blank ` column with a trailing space.

**THE CSV DROPS LEADING ZEROS ON THE APN.** 155 of 414 come back 12 chars
against 13 in the dashboard's own Excel export. Zero-padding the CSV values
reconciles the two sets exactly — 0 differences in either direction. The
normalizer's 12-to-13 left-pad (fixed 2026-07-08) is what makes this endpoint
safe to use; without it 155 properties would take synthetic ids.

=== NO COORDINATES ===
The CSV carries no geometry. That is acceptable because core.parcels holds
448,087 Hennepin parcels with coordinates and parcel_resolver fills in rather
than overwrites: 20 of 20 sampled APNs from this CSV were already present
WITH coordinates on 2026-08-16. A property genuinely absent from the spine
gets a parcel row without lat/lng rather than a wrong one.
"""

from __future__ import annotations

import csv
import io
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, ClassVar

import httpx

from src.config import settings
from src.models.parcel import ParcelUpsert
from src.models.signal import VbrListingInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import (
    write_events_dedup,
    write_typed_signals_dedup,
)
from src.services.parcel_resolver import resolve_parcel
from src.utils.errors import ParseError, SourceUnavailableError
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id
from src.utils.logger import logger


# ----- City of Minneapolis Tableau view, CSV export -----
_CSV_URL = (
    "https://tableau.minneapolismn.gov/views"
    "/MinneapolisVacantCondemnedPropertyInventory"
    "/DailyVBRcondemnedpropertylist.csv"
)

# Minneapolis VBR annual fee (2024+ schedule). Applied to active registrations.
_VBR_ANNUAL_FEE = Decimal("7228.70")
# Prolonged Vacancy Enforcement monthly citation (post-2-year vacancy).
_PVE_MONTHLY_FINE = Decimal("2000.00")

# The CSV uses two-digit years. The long forms are kept as a cheap hedge
# against the City changing the view's date formatting.
_DATE_FORMATS = (
    "%m/%d/%y", "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y",
)

# Header names as the CSV emits them. Read by NAME; see the docstring.
_COL_ADDRESS = "Address"
_COL_APN = "APN"
_COL_CON1 = "CON1"
_COL_CONB = "CONB"
_COL_NEIGHBORHOOD = "Neighborhood"
_COL_OWNER = "Property Owner"
_COL_VBR_DATE = "VBR Date"
_COL_WARD = "Ward"

# Every column the parse depends on. A missing one means the City changed the
# view, and continuing would write hundreds of half-empty rows that look like
# real records. Fail the run instead.
_REQUIRED_COLUMNS = (
    _COL_ADDRESS, _COL_APN, _COL_CON1, _COL_CONB, _COL_VBR_DATE,
)


def _clean(raw: Any) -> str | None:
    """Trim to a non-empty string, or None. The CSV uses "" for absent."""
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


def _parse_text_date(raw: Any) -> date | None:
    """Parse the CSV's string dates ("08/12/26") into a date.

    Returns None for blanks, whitespace-only, or unparseable values.
    """
    s = _clean(raw)
    if s is None:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


class MplsVacantBuildingScraper(BaseScraper[dict[str, Any], VbrListingInsert]):
    """Minneapolis VBR + condemned buildings — City of Minneapolis CSV source."""

    source_name: ClassVar[str] = "mpls_vbr"
    signal_type: ClassVar[str] = "vbr_listing"
    county_code: ClassVar[str] = "hennepin"  # Minneapolis is in Hennepin County

    # ---- Fetch ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """Download the City's VBR list and return it as dict rows."""
        try:
            async with httpx.AsyncClient(
                timeout=settings.scraper_request_timeout_seconds,
                follow_redirects=True,
                headers={"User-Agent": "DistressProperties/1.0"},
            ) as client:
                resp = await client.get(_CSV_URL)
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"Minneapolis VBR CSV request failed: {e}",
                source=self.source_name,
            ) from e

        if resp.status_code != 200:
            raise SourceUnavailableError(
                f"Minneapolis VBR CSV returned {resp.status_code}",
                source=self.source_name,
                context={"body": resp.text[:300]},
            )

        # utf-8-sig: Tableau prefixes a BOM, which would otherwise become part
        # of the first header name and break the Address lookup silently.
        text = resp.content.decode("utf-8-sig", errors="replace")
        rows = list(csv.DictReader(io.StringIO(text)))

        if not rows:
            raise ParseError(
                "Minneapolis VBR CSV contained no rows",
                source=self.source_name,
            )

        missing = [c for c in _REQUIRED_COLUMNS if c not in (rows[0] or {})]
        if missing:
            raise ParseError(
                f"Minneapolis VBR CSV is missing required columns: {missing}",
                source=self.source_name,
                context={"headers": list((rows[0] or {}).keys())},
            )

        logger.info(
            "Minneapolis VBR fetch complete",
            source=self.source_name,
            rows=len(rows),
            headers=list((rows[0] or {}).keys()),
        )
        return rows

    # ---- Parse ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[VbrListingInsert]:
        """Convert CSV rows into VbrListingInsert signals."""
        signals: list[VbrListingInsert] = []
        skipped_no_address = 0
        skipped_no_apn = 0
        skipped_no_vbr_date = 0

        for row in raw_records:
            sig = self._parse_row(row)
            if sig is not None:
                signals.append(sig)
                continue
            # Cheap re-check purely for the counters; _parse_row logs nothing
            # per row because 414 warnings would drown the run log.
            if _clean(row.get(_COL_ADDRESS)) is None:
                skipped_no_address += 1
            elif _clean(row.get(_COL_APN)) is None:
                skipped_no_apn += 1
            else:
                skipped_no_vbr_date += 1

        logger.info(
            "Minneapolis VBR parsed",
            source=self.source_name,
            signals=len(signals),
            skipped_no_address=skipped_no_address,
            skipped_no_apn=skipped_no_apn,
            skipped_no_vbr_date=skipped_no_vbr_date,
        )
        return signals

    def _parse_row(self, row: dict[str, Any]) -> VbrListingInsert | None:
        """One CSV row to one signal, or None when it is not usable."""
        address = _clean(row.get(_COL_ADDRESS))
        if address is None:
            return None

        # --- Parcel ID ---
        # The CSV drops leading zeros on 155 of 414 APNs; the normalizer
        # left-pads 12 to 13. NO SYNTHETIC FALLBACK: the 2026-07-09 session
        # spent itself deleting 118 MPLS-VBR-* synthetic ids and the rows they
        # spawned across three tables. A row without a usable APN is skipped.
        raw_apn = _clean(row.get(_COL_APN))
        if raw_apn is None:
            return None
        parcel_id, _err = safe_normalize_parcel_id("hennepin", raw_apn)
        if parcel_id is None:
            return None

        # --- Dates ---
        vbr_date = _parse_text_date(row.get(_COL_VBR_DATE))
        con1_date = _parse_text_date(row.get(_COL_CON1))
        conb_date = _parse_text_date(row.get(_COL_CONB))

        # date_entered_registry is the third column of the dedup index
        # (county_code, parcel_id, date_entered_registry). It is NULLS NOT
        # DISTINCT, so a NULL row dedups only against other NULL rows and
        # never matches the real row for the same property. All 414 rows
        # carry a VBR Date; a row without one is a shape change, so skip it.
        if vbr_date is None:
            return None

        # Either condemnation counts; take the earlier, which is when the
        # property actually became condemned.
        con_candidates = [d for d in (con1_date, conb_date) if d is not None]
        con_date = min(con_candidates) if con_candidates else None

        # CONB is the City's own "condemned for being boarded" — see the
        # docstring. This is the field the previous source read as absent.
        boarded = conb_date is not None
        condemned = con_date is not None
        label = "Condemned" if condemned else "Registered Vacant"

        # --- Years on registry + PVE eligibility (>= 2 yrs vacant) ---
        years_on_registry: float | None = None
        monthly_pve: Decimal | None = None
        days = (date.today() - vbr_date).days
        if days >= 0:
            years_on_registry = round(days / 365.25, 1)
            if years_on_registry >= 2.0:
                monthly_pve = _PVE_MONTHLY_FINE

        return VbrListingInsert(
            parcel_id=parcel_id,
            county_code=self.county_code,
            city="Minneapolis",
            registry_type=label,
            date_entered_registry=vbr_date,
            years_on_registry=years_on_registry,
            annual_fee=_VBR_ANNUAL_FEE,
            monthly_pve_fine=monthly_pve,
            is_active=True,
            raw_data={
                "attributes": dict(row),
                "geometry": None,  # CSV carries none; see the docstring
                "owner_name": _clean(row.get(_COL_OWNER)),
                "neighborhood": _clean(row.get(_COL_NEIGHBORHOOD)),
                "ward": _clean(row.get(_COL_WARD)),
                "condemned_date": con_date.isoformat() if con_date else None,
                "condemned_date_con1": con1_date.isoformat() if con1_date else None,
                "condemned_date_conb": conb_date.isoformat() if conb_date else None,
                # raze_order_date is deliberately absent. The field the old
                # source called a raze order was the Restoration Agreement.
                "_source": self.source_name,
                "_data_vintage": "city_tableau_daily_vbr",
            },
            observed_at=datetime.now(timezone.utc),
            source=self.source_name,
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

            # This service has NO Latitude/Longitude attribute fields — the
            # only source of coordinates is the geometry, which the base class
            # requests with outSR=4326 (x=lng, y=lat).
            lng = raw_geometry.get("x")
            lat = raw_geometry.get("y")

            try:
                lat_f = float(lat) if lat is not None else None
                lng_f = float(lng) if lng is not None else None
            except (ValueError, TypeError):
                lat_f, lng_f = None, None

            # Sanity-bound to Minnesota. The layer's NATIVE spatial reference
            # is wkid 103734 — county coords in feet, where an "x" is ~500000.
            # If outSR handling ever regresses, these bounds turn that into
            # NULL rather than storing 500000 as a latitude.
            if lat_f is not None and not (43.0 <= lat_f <= 49.5):
                lat_f = None
            if lng_f is not None and not (-97.5 <= lng_f <= -89.0):
                lng_f = None

            address = (
                raw_attributes.get("USER_Display")
                or raw_attributes.get("Match_addr")
                or raw_attributes.get("StAddr")
            )
            # NOTE: do NOT write Zip from this feed without checking whose zip
            # it is. On the PREVIOUS (GreenInfoNetwork) feed the Zip/City/State
            # fields were the OWNER's mailing address, not the property's
            # (e.g. Atlanta GA 30312 on a Logan Ave N property). The City feed
            # has a ZIP field from the geocoder which is probably the
            # property's, but that is unverified — hennepin_parcels is the
            # authority for property zips either way.

            unique_parcels[sig.parcel_id] = ParcelUpsert(
                parcel_id=sig.parcel_id,
                county_code=self.county_code,
                state="MN",
                address=str(address).strip() if address else None,
                city="Minneapolis",
                lat=lat_f,
                lng=lng_f,
                vacancy_status="vacant",
                data_sources=[self.source_name],
                last_observed_at=datetime.now(timezone.utc),
            )

        # resolve_parcel returns None on failure. Its result MUST be counted
        # and folded into the run's failure total.
        #
        # FIXED 2026-08-07. This loop previously discarded the return value and
        # the log line below reported `parcels=len(unique_parcels)` — the count
        # ATTEMPTED, not the count written. Measured live: on 2026-08-07 the
        # 15:45 run logged `parcels=309 failed=0` and reported status=success
        # while EVERY ONE of those 309 parcel upserts was failing with 42P10
        # (parcel_resolver.py carried a stale single-column on_conflict after
        # core.parcels moved to PRIMARY KEY (county_code, parcel_id)). The
        # scraper asserted success for two hours; the defect was only found by
        # querying core.parcels.last_observed_at directly.
        #
        # dakota_sheriff.py has always done this correctly and was therefore
        # the only one of the affected scrapers whose logs showed the failure.
        # Match that pattern; never discard this return value.
        parcels_ok = 0
        parcels_failed = 0
        for parcel_payload in unique_parcels.values():
            if resolve_parcel(parcel_payload) is not None:
                parcels_ok += 1
            else:
                parcels_failed += 1

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

        # Escalation counts are logged so a drop is visible immediately. 49 of
        # 311 carried a condemnation or raze date when this source was adopted
        # on 2026-08-07; a sudden 0 means the City changed the field names.
        condemned_count = sum(1 for s in signals if s.condemned)
        raze_count = sum(
            1
            for s in signals
            if (s.raw_data or {}).get("raze_order_date") is not None
        )

        logger.info(
            "Minneapolis VBR write complete",
            parcels_ok=parcels_ok,
            parcels_failed=parcels_failed,
            typed_new=new_typed,
            events_new=new_events,
            condemned=condemned_count,
            raze_orders=raze_count,
            failed=failed_typed + failed_events + parcels_failed,
        )

        return (new_typed, 0, failed_typed + failed_events + parcels_failed)


__all__ = ["MplsVacantBuildingScraper"]
