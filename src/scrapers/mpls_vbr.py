"""
Minneapolis Vacant Building Registration (VBR) + Condemned scraper.

Source: the CITY OF MINNEAPOLIS's own ArcGIS org (`afSMGVsC7QlRK1kZ`),
service `VBR_October2025`, layer 0 (`VBR_Oct2025_Geocoded`).

    https://services.arcgis.com/afSMGVsC7QlRK1kZ/ArcGIS/rest/services/VBR_October2025/FeatureServer/0

=== SOURCE CHANGED 2026-08-07 — READ THIS BEFORE TOUCHING THE URL ===
This scraper previously read a GreenInfoNetwork-hosted copy
(`services1.arcgis.com/4ZKi1B1zTblbwgWB/.../VBR_MPLS/FeatureServer/0`)
described in its own docstring as "a COMPLETE SNAPSHOT ... as of roughly
early-to-mid 2023," with only ~11 of 309 records carrying a 2024+ date. That
docstring also claimed the live registry was "subscription-locked — pursue
via an open-data / MGDPA request to the City."

**It was not locked.** The City publishes it directly. Verified 2026-08-07:
`hasStaticData: false`, `dataLastEditDate` 2025-10-22, 311 records.

**The reason for the switch is NOT the record count** — 311 vs 309 is
essentially the same population. It is these two fields:

    USER_Day_of_Con1_Date   condemnation date
    USER_Day_of_RA_Date     RESTORATION AGREEMENT date -- NOT a raze order.
                            The docstring said "raze / demolition order" from
                            2026-08-07 until 2026-08-16 and the classifier
                            acted on it. See _classify_from_dates.

**49 of 311 records carry one or both** (measured 2026-08-07). Govire had
ZERO condemnation and ZERO demolition signals before this change. The old
feed exposed `Day_of_RA` and this scraper never read it — the field appeared
only in the docstring.

Two sibling services promised the same content and are BOTH DEAD, do not
revisit them:
  * `Condemned_by_Boarding`      hasStaticData: true, last edit 2016-04-08
  * `PropertiesDueForWrecking`   a 2020 geocoding artifact, no parcel id

**Check `hasStaticData` and `dataLastEditDate` before trusting ANY service on
this org.** Most of its ~700 services are one-off analyst uploads, not
maintained feeds. Three were rejected on that basis in one session.

=== WHAT WAS LOST IN THE SWITCH (accept knowingly) ===
The old feed had a free-text `Property_s` status ("Vacant+Restoration
Agreement", "Boarded", "Condemned") which drove a `boarded` flag. The new
service has NO status field.

CORRECTED 2026-08-16: this section used to claim `boarded` "can no longer be
determined and is always False". It can. **CONB is the City's own field for
condemned-FOR-BEING-BOARDED** — not an inference, a definition. 117 of 311
rows carried a CONB date while every one reported boarded=false. Condemnation
is still derived from a DATE being present, which remains more reliable than
string-matching free text.

That old `Property_s` vocabulary is also the proof that RA means Restoration
Agreement: one event with subtype "Vacant+Restoration Agreement" is still in
signals.distress_events from that feed.

The old feed also carried `Owner_Addr` (owner mailing address, an
absentee-owner signal). The new service has no equivalent. Owner NAME is
still present as `USER_Full_Name`.

=== FIELDS ON THE NEW SERVICE ===
    USER_APN                 12-digit Hennepin PID (normalizer pads to 13)
    USER_Display             property street address
    USER_Full_Name           owner name
    USER_Day_of_VBR_Date     date entered VBR registry, e.g. "12/8/2023"
    USER_Day_of_Con1_Date    condemnation date (often "")
    USER_Day_of_Conb_Date    secondary condemnation date (often "")
    USER_Day_of_RA_Date      Restoration Agreement date (often "") -- the
                             owner has COMMITTED TO REPAIR. A de-escalation,
                             carried in raw_data, never used to classify.
    USER_Wards               city council ward
    USER_Neighborhoods_Desc  neighborhood name
    Match_addr / StAddr      geocoder output (fallbacks for address)

All USER_* date fields are STRINGS in `%m/%d/%Y` form, and are `""` rather
than null when absent. `_parse_text_date` handles both.

There are NO Latitude/Longitude attribute fields. The layer's native spatial
reference is `wkid 103734` (Minnesota county coords, in FEET) — but
`BaseArcGISScraper._fetch_page` always sends `outSR=4326`, so
`feature.geometry` comes back as WGS84 lat/lng. Coordinates therefore come
from geometry ONLY. The Minnesota sanity bounds below are the backstop: if
outSR handling ever changes, coords become NULL rather than storing 500000ft
as a latitude.
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
from src.services.snapshot_reconciler import (
    build_key,
    reconcile_snapshot,
    snapshot_is_complete,
)
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id
from src.utils.logger import logger


# ----- City of Minneapolis VBR feature service (layer 0) -----
_FEATURE_SERVICE_URL = (
    "https://services.arcgis.com/afSMGVsC7QlRK1kZ"
    "/ArcGIS/rest/services/VBR_October2025/FeatureServer/0"
)

# Minneapolis VBR annual fee (2024+ schedule). Applied to active registrations.
_VBR_ANNUAL_FEE = Decimal("7228.70")
# Prolonged Vacancy Enforcement monthly citation (post-2-year vacancy).
_PVE_MONTHLY_FINE = Decimal("2000.00")

# The USER_* date fields arrive as "12/8/2023". The long-form month-name
# formats are retained from the old feed in case the City changes shape.
_DATE_FORMATS = ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y")


def _parse_text_date(raw: Any) -> date | None:
    """Parse the source's string dates ("12/8/2023") into a date.

    Returns None for blanks, whitespace-only, or unparseable values. The new
    service uses EMPTY STRINGS rather than nulls for absent dates, which this
    handles via the `if not s` guard.
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


def _classify_from_dates(
    con1_date: date | None,
    conb_date: date | None,
) -> tuple[bool, bool, str]:
    """Derive (boarded, condemned, label) from the presence of dates.

    The City's service has NO status text field, so classification is by date
    presence. This is MORE reliable than the old free-text matching: a date
    is unambiguous, whereas "Vacant+Restoration Agreement" required guessing.

    === TWO CORRECTIONS, 2026-08-16 — READ BEFORE CHANGING THIS ===

    The City's own legend, printed on the Vacant & Condemned dashboard:

        VBR  - Date entered in the Vacant Building Registration program.
               If no condemned date the building is vacant but not condemned.
        CONB - Date the building was condemned for BEING BOARDED.
        CON1 - Date the building was condemned for LACK OF MAINTENANCE.
        RA   - Date the RESTORATION AGREEMENT was signed.

    1. **RA IS NOT A RAZE ORDER.** This function used to take a `raze_date`
       argument sourced from `USER_Day_of_RA_Date`, return
       ("Raze Order Issued", condemned=True), and let it SUPERSEDE a real
       condemnation on the grounds that demolition outranks it.

       A Restoration Agreement is the owner COMMITTING TO REPAIR. It is a
       de-escalation — the opposite signal — and it was overwriting the
       property's true status.

       Our own history proves the reading: signals.distress_events still
       holds one event with event_subtype "Vacant+Restoration Agreement"
       from the OLD GreenInfo feed's free-text status field, where the City
       spelled it out. The same field was later renamed RA and misread.

       Measured 2026-08-16: 1 event labelled "Raze Order Issued", and 172
       events carrying a `raze_order_date` key in raw_data — including 62
       labelled "Registered Vacant", which by the old ladder could not have
       had one. The key was far more widespread than the label.

       RA is no longer read here at all. It is carried in raw_data under its
       true name (see parse_feature) for anyone who wants it, and it does
       NOT affect classification. An owner agreeing to restore a property is
       not a reason to change its condemnation status either way.

    2. **`boarded` IS DETERMINABLE — IT IS CONB.** This function used to
       return False unconditionally, with a docstring saying "the new
       service carries nothing that identifies a boarded-but-not-condemned
       property. Do not infer it."

       CONB is the City's own field for condemned-FOR-BEING-BOARDED. It is
       not an inference; it is the definition. Measured 2026-08-16: 117 of
       311 rows carried a CONB date and every one reported boarded=false,
       against a boarded_building event type holding 9,990 rows from other
       sources and ZERO from Minneapolis.

    Both condemnation reasons still count toward `condemned`, and the label
    stays "Condemned" for either, because the subtype vocabulary in
    signals.distress_events is shared across sources and a Minneapolis-only
    value would fragment it. The reason lives in raw_data.
    """
    boarded = conb_date is not None
    condemned = (con1_date is not None) or boarded
    label = "Condemned" if condemned else "Registered Vacant"
    return boarded, condemned, label


# ----- Retirement threshold for this full-snapshot source -----
# A registration absent from THIS many consecutive complete fetches is
# retired (is_active = False). Present rows reset the counter to zero.
#
# 3, matching saint_paul_vacant, and for the same reasons. Absent-once is not
# evidence: THIS scraper took a bare 403 from the City on 2026-08-16 and
# fetched zero rows, which under a one-miss rule retires all 311 Minneapolis
# registrations in a single morning. 7 was rejected in the other direction —
# the source runs daily, so a week of publishing a condemned building the
# City has released is too long to be wrong.
#
# The mechanics live in src/services/snapshot_reconciler.py, proven against
# the Saint Paul registry on 2026-08-22 across three live runs.
_RETIREMENT_THRESHOLD: int = 3


class MplsVacantBuildingScraper(BaseArcGISScraper[VbrListingInsert]):
    """Minneapolis VBR + condemned buildings — City of Minneapolis source."""

    # ---- Required class config ----
    source_name: ClassVar[str] = "mpls_vbr"
    signal_type: ClassVar[str] = "vbr_listing"
    county_code: ClassVar[str] = "hennepin"  # Minneapolis is in Hennepin County
    feature_service_url: ClassVar[str] = _FEATURE_SERVICE_URL

    # This layer's object-id field is "ObjectID", not the "OBJECTID" default.
    # Only consulted by keyset pagination (unused here at 311 records), but
    # wrong values are a trap for whoever enables it later.
    objectid_field: ClassVar[str] = "ObjectID"

    # All ~311 records are relevant.
    where_clause: ClassVar[str] = "1=1"

    # Coordinates come from geometry ONLY — this service has no lat/lng
    # attribute fields. The base class requests outSR=4326.
    return_geometry: ClassVar[bool] = True

    # ---- Fetch (records the row count for the retirement guard) ----

    # Rows the last fetch returned, or None if it has not run. write()
    # compares this against the PARSED signal count: parse() logs and skips a
    # ParseError and parse_feature returns None on a missing PIN, so the two
    # differ whenever rows are dropped — and a dropped row is indistinguishable
    # from a removed one unless the fetched count is known.
    _fetched_count: int | None = None

    # Matches signals.vacant_registrations_dedup:
    #   (county_code, parcel_id, date_entered_registry) NULLS NOT DISTINCT
    # county_code rides in the scope rather than the key, since every row this
    # scraper owns carries the same one.
    _KEY_COLUMNS: ClassVar[tuple[str, ...]] = (
        "parcel_id",
        "date_entered_registry",
    )

    async def fetch(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        """Fetch the registry, remembering how many rows came back."""
        features = await super().fetch(*args, **kwargs)
        self._fetched_count = len(features)
        return features

    def _seen_keys(self, signals: list[VbrListingInsert]) -> set[tuple]:
        """Identity of every registration this run actually saw."""
        return {
            build_key((sig.parcel_id, sig.date_entered_registry))
            for sig in signals
        }

    # ---- Feature parsing ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> VbrListingInsert | None:
        """Convert one VBR feature into a VbrListingInsert signal."""
        address = (
            attributes.get("USER_Display")
            or attributes.get("Match_addr")
            or attributes.get("StAddr")
        )
        if not address or not str(address).strip():
            # No address → not actionable as a property lead; skip silently.
            return None

        # --- Parcel ID ---
        # USER_APN is a 12-digit Hennepin PID (e.g. "102824110109"). The
        # normalizer left-pads to 13 — Hennepin PIDs legitimately begin with 0
        # for section numbers 01-09, so a 12-digit input is unambiguous.
        # Verified against 117 of 118 rows on the previous feed.
        parcel_id: str | None = None
        raw_apn = attributes.get("USER_APN")
        if raw_apn and str(raw_apn).strip():
            pid, _err = safe_normalize_parcel_id("hennepin", str(raw_apn))
            if pid is not None:
                parcel_id = pid

        if parcel_id is None:
            oid = attributes.get("ObjectID")
            parcel_id = f"MPLS-VBR-{oid}" if oid is not None else None
            if parcel_id is None:
                return None

        # --- Dates ---
        vbr_date = _parse_text_date(attributes.get("USER_Day_of_VBR_Date"))
        con1_date = _parse_text_date(attributes.get("USER_Day_of_Con1_Date"))
        conb_date = _parse_text_date(attributes.get("USER_Day_of_Conb_Date"))
        # RA = Restoration Agreement, NOT a raze order. Carried for the
        # record, never used to classify. See _classify_from_dates.
        restoration_agreement_date = _parse_text_date(
            attributes.get("USER_Day_of_RA_Date")
        )

        # Either condemnation field counts; take the earlier one when both are
        # present, since that is when the property actually became condemned.
        con_candidates = [d for d in (con1_date, conb_date) if d is not None]
        con_date = min(con_candidates) if con_candidates else None

        # --- Status classification (by date presence — see docstring) ---
        # Passed the two condemnation dates SEPARATELY: CONB drives `boarded`
        # and cannot be recovered from the min() above.
        boarded, condemned, label = _classify_from_dates(con1_date, conb_date)

        # --- Years on registry + PVE eligibility (>= 2 yrs vacant) ---
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
            # ADDED 2026-08-10. VbrListingInsert gained an optional
            # county_code field the same day; to_event() now projects it into
            # signals.distress_events, where the composite FK
            # (county_code, parcel_id) -> core.parcels and the dedup key
            # (county_code, parcel_id, event_type, event_date, source) BOTH
            # need it. A NULL leaves both unenforced -- NULL is never equal
            # to anything -- so the event points at no parcel and cannot
            # collide with a duplicate.
            #
            # Note the write() step below already sets county_code on the
            # TYPED signals.vbr_listings rows (see the ClassVar comment
            # there); this is the same value on the EVENT projection, which
            # was missed.
            county_code=self.county_code,
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
                "owner_name": str(attributes.get("USER_Full_Name") or "").strip()
                or None,
                "neighborhood": str(
                    attributes.get("USER_Neighborhoods_Desc") or ""
                ).strip()
                or None,
                "ward": str(attributes.get("USER_Wards") or "").strip() or None,
                # Explicit escalation dates. These are the whole reason this
                # scraper moved to the City's own service — 49 of 311 records
                # carry at least one of them.
                "condemned_date": con_date.isoformat() if con_date else None,
                "condemned_date_con1": con1_date.isoformat() if con1_date else None,
                "condemned_date_conb": conb_date.isoformat() if conb_date else None,
                # RENAMED 2026-08-16 from "raze_order_date", which was wrong:
                # this is the Restoration Agreement date. A rename rather than
                # an alias is deliberate — 172 events carried the old key, and
                # anything still reading it should get None and fail visibly
                # rather than keep reading a demolition date that never was.
                "restoration_agreement_date": (
                    restoration_agreement_date.isoformat()
                    if restoration_agreement_date else None
                ),
                "_source": self.source_name,
                "_data_vintage": "city_vbr_october2025",
            },
            observed_at=datetime.now(timezone.utc),
            source=self.source_name,
            # Stable identity: the parcel id (or the MPLS-VBR-{oid} synthetic
            # when no APN exists). Never use a row index as source_id — on the
            # old feed FID was a layer ROW INDEX and gave 12 ids shared across
            # 480 duplicate rows.
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
        # 311 carried a condemnation or restoration-agreement date when this
        # source was adopted on 2026-08-07; a sudden 0 means the City changed
        # the field names. boarded_count added 2026-08-16 — it was structurally
        # 0 before CONB was read, so a 0 here now is a real signal, not the
        # constant it used to be.
        condemned_count = sum(1 for s in signals if s.condemned)
        boarded_count = sum(1 for s in signals if s.boarded)
        restoration_agreement_count = sum(
            1
            for s in signals
            if (s.raw_data or {}).get("restoration_agreement_date") is not None
        )

        # --- Step 4: Retire rows absent from this complete fetch ---
        # Only meaningful because where_clause is "1=1". A partial fetch must
        # never reach this: retiring on incomplete data marks live buildings
        # inactive, which is worse than carrying a stale row — a stale row is
        # visibly wrong, a missing row is invisible.
        #
        # A failure here must NOT fail the run. The registry data is already
        # written and correct by this point; only reconciliation is lost.
        rows_reset = rows_missed = rows_retired = 0
        retire_failed = 0
        ok, skip_reason = snapshot_is_complete(
            fetched=self._fetched_count,
            parsed=len(signals),
            record_cap=self._max_records_override,
        )
        if not ok:
            logger.warning(
                "Minneapolis retirement pass SKIPPED — fetch not a complete "
                "snapshot; registry rows written normally",
                source=self.source_name,
                reason=skip_reason,
                fetched=self._fetched_count,
                parsed=len(signals),
                max_records_override=self._max_records_override,
            )
        else:
            try:
                rows_reset, rows_missed, rows_retired = reconcile_snapshot(
                    "signals",
                    "vacant_registrations",
                    # Scope must exclude Saint Paul, which shares this table.
                    scope={
                        "county_code": self.county_code,
                        "city": "Minneapolis",
                    },
                    key_columns=self._KEY_COLUMNS,
                    seen_keys=self._seen_keys(signals),
                    threshold=_RETIREMENT_THRESHOLD,
                )
            except Exception as exc:
                retire_failed = 1
                logger.warning(
                    "Minneapolis retirement pass failed; registry rows unaffected",
                    source=self.source_name,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )

        logger.info(
            "Minneapolis VBR write complete",
            parcels_ok=parcels_ok,
            parcels_failed=parcels_failed,
            typed_upserted=new_typed,
            events_new=new_events,
            condemned=condemned_count,
            boarded=boarded_count,
            restoration_agreements=restoration_agreement_count,
            rows_reset=rows_reset,
            rows_missed=rows_missed,
            rows_retired=rows_retired,
            failed=failed_typed + failed_events + parcels_failed + retire_failed,
        )

        # COUNTER SEMANTICS CORRECTED 2026-08-22, matching saint_paul_vacant.
        # This returned (new_typed, 0, failures). new_typed is the LENGTH of
        # the upserted batch — write_typed_signals_dedup passes
        # ignore_duplicates=False and PostgREST returns every row it touched,
        # inserted or updated — so records_new read 311 of 311 on EVERY run.
        # A registry that gains a handful of buildings a month reported three
        # hundred new records a day, and nothing could be alerted on it.
        #
        # The middle slot is records_updated and was a hardcoded literal 0,
        # which is why records_updated is 0 for every source in
        # audit.scraper_runs. new_events — the only genuinely-new count, since
        # write_events_dedup uses ON CONFLICT DO NOTHING — was computed above
        # and then discarded.
        return (
            new_events,
            new_typed,
            failed_typed + failed_events + parcels_failed + retire_failed,
        )


__all__ = ["MplsVacantBuildingScraper"]
