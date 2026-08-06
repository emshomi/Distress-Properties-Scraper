"""
MNGAC statewide open-parcels foundation scraper (STREAMING, keyset-paged).

Source: MnGeo ArcGIS Enterprise — Minnesota Geospatial Commons aggregate
API:    https://enterprise.gisdata.mn.gov/aghost/rest/services
        /us_mn_state_mngeo/plan_parcels_open/MapServer/1   ("Plan Parcels Open")
Index:  .../plan_parcels_open/MapServer/0  ("Plan Parcels Open Metadata")

THE MNGAC PIVOT (2026-07-27): every county that publishes open parcels to
the Commons lands in ONE statewide layer under ONE standardized schema (the
MNGAC Parcel Data Standard v1.1.3). Layer 0 is a machine-readable roster of
all 87 MN counties with per-county acquisition dates. 59 of the 87 carry
gac_open_approval = 'true'.

So this file is a GENERIC loader: the base class holds all the logic and a
per-county subclass supplies three lines (co_code, county_code,
source_name). Onboarding an approved county becomes a subclass + a config
flag + a core.counties row + a source_county_map row — not a build session.

=== WHY THIS DOES NOT REPLACE THE COUNTY-DIRECT LOADERS ===
Coverage in this layer says NOTHING about currency. acqdate is set per
county at MnGeo's discretion, not on a schedule (verified live 2026-07-27
against the 2026-04-28 compile):

    Houston / Hennepin  acq 2026-04-27   (1 day before compile)
    Wabasha             acq 2026-04-09   (19 days)
    Olmsted             acq 2026-03-30   (29 days)
    Winona              acq 2025-12-17   (132 days)
    Fillmore            acq 2025-04-14   (379 days — FIFTEEN MONTHS)

Fillmore's county-direct loader is vastly fresher than the aggregate. The
existing six county-direct spines stay authoritative. THIS loader is the
EXPANSION path for counties we have no direct door into.

MANDATORY before onboarding any new county here: check its acqdate on
layer 0. If it is badly stale, build a county-direct loader instead.

=== VERIFIED layer facts (live inspection 2026-07-27, Wabasha slice) ===
  - Field names are LOWERCASE (objectid, not OBJECTID) -> objectid_field
    is overridden below; the base default would hang the keyset cursor
  - Filter on co_code (5-digit state FIPS, '27157'). co_name is Title Case
    ("Wabasha") and is NOT used — casing/spelling drift is a real risk
  - county_pin  hyphenated alphanumeric ("R08-00241-00") -> normalizer
                'wabasha' entry; canonical form "r080024100"
  - state_pin   is co_code + county_pin ("27157-R08-00241-00") — NEVER use
                it as the parcel id; it normalizes to a DIFFERENT value
  - Site addr   PARSED into components (anumber / st_pre_dir / st_name /
                st_pos_typ / ...) — must be reassembled, not read from one
                field. 64.0% populated in Wabasha (11,084 / 17,323)
  - Locality    ctu_name = civil division ("Lake Township");
                postcomm  = postal city ("Lake City"). city <- postcomm,
                ctu_name preserved in raw_data (township search runs off
                raw_data->>'ctu_name')
  - Owners      owner_name is NATURAL ORDER, MIXED CASE ("Daniel J Siewert")
                — every other county publishes LAST-FIRST uppercase. We
                UPPERCASE ONLY and never reorder: "Daniel J & Mary Siewert"
                and "Sahaj Hotel Group" would both corrupt under a
                reordering rule. The owner classifier and the probate
                word-boundary matcher are keyword-based and order-agnostic.
  - Mailing     own_add_l1 (+ l2) = street; own_add_l3 is a COMBINED
                "Lake City, MN 55041-2739" -> parsed here
  - Values      emv_land / emv_bldg / emv_total. 100% populated in Wabasha
  - Acreage     acres_poly (100%) vs acres_deed (0.0 sentinel) -> prefer
                acres_poly. NOTE this INVERTS the Fillmore rule, where
                DEEDEDACRE was preferred over the GIS-computed figure
  - Character   dwell_type / home_style / useclass1-4 / year_built /
                fin_sq_ft / num_units / sale_date / sale_value / homestead
                / green_acre / ag_preserv / abb_legal / school_dst /
                section-township-range
  - MaxRecordCount 2000; JSON; POLYGONS in EPSG:26915 (base requests
    outSR=4326, centroid derived here)

=== SENTINEL VALUES — THE TRAP IN THIS LAYER ===
Absence is encoded as zeros and placeholder strings, NOT as nulls. Loaded
raw they would poison every numeric filter (a year_built sort would rank
thousands of phantom parcels at year zero). Coerced to honest None here:

    year_built / fin_sq_ft / num_units / garagesqft / sale_value /
    acres_deed  == 0
    dwell_type  == 'N/A'      ownership == 'No Value'

Verified cross-check (Wabasha, 17,323 parcels):
    dwell_type 'N/A' = 7,033 + null = 499  ->  7,532 with no structure
    year_built > 0                          ->  9,791
    17,323 - 7,532 = 9,791 EXACTLY. The two fields agree to the row, which
    confirms 'N/A' means BARE LAND, not "unclassified building" (every
    commercial and institutional type is separately enumerated: Store,
    Industrial Wrhse, Medical, School, Courthouse, Shed/Barn).
Raw sentinels are PRESERVED in raw_data — the bare-land query is
raw_data->>'dwell_type' = 'N/A'.

=== DEDUP ===
seen_pids first-wins, as in olmsted_parcels / fillmore_parcels. Also
guards the hyphen-stripping collision case: verify after run #1 that
COUNT(DISTINCT parcel_id) == COUNT(*) for the county.

=== STREAMING + KEYSET ===
Identical to fillmore_parcels / olmsted_parcels: page-at-a-time streaming
via the base class's KEYSET mode (WHERE (co_code=...) AND objectid > last
ORDER BY objectid ASC).

What it writes:
  - core.parcels rows + raw_data JSONB (county_code from the subclass)
  - core.owners projection (source = the subclass's source_name) —
    ride-along; failures never block parcels
What it does NOT write:
  - signals.distress_events (spine, not signal)
"""

from __future__ import annotations

import math
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

import httpx

from src.config import settings
from src.db.supabase_client import core_table
from src.models.parcel import ParcelUpsert
from src.scrapers.base_arcgis_scraper import BaseArcGISScraper, arcgis_date_to_iso
from src.scrapers.base_scraper import RunResult
from src.services import audit_logger, source_health_tracker
from src.utils.errors import (
    ParseError,
    ScraperAlreadyRunningError,
    ScraperDisabledError,
)
from src.utils.logger import logger
from src.utils.owner_classifier import classify_owner
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id


_MNGAC_BASE = (
    "https://enterprise.gisdata.mn.gov/aghost/rest/services"
    "/us_mn_state_mngeo/plan_parcels_open/MapServer"
)
_PARCEL_LAYER_URL = f"{_MNGAC_BASE}/1"
_METADATA_LAYER_URL = f"{_MNGAC_BASE}/0"

_DB_BATCH_SIZE: int = 500

# Numeric fields where 0 means ABSENT, not zero (see module docstring).
_ZERO_IS_NULL_INT = (
    "year_built",
    "fin_sq_ft",
    "num_units",
    "garagesqft",
    "sale_value",
)

# String placeholders that mean ABSENT.
_PLACEHOLDER_STRINGS = {"N/A", "NO VALUE", "NONE", "UNKNOWN"}

# Date fields worth surfacing in raw_data as readable ISO alongside the
# raw epoch-millis the service returns.
_RAW_DATE_FIELDS = ("sale_date", "agpre_enrd", "agpre_expd", "edit_date", "exp_date")

# dwell_type -> internal property_type.
#
# DELIBERATELY MINIMAL. property_type is a constrained field on
# ParcelUpsert and only two internal values are proven safe in this repo
# (single_family / multifamily, per fillmore_parcels + olmsted_parcels).
# Guessing 'commercial' / 'mobile_home' / 'vacant_land' risks validation
# failures that would silently increment records_failed on tens of
# thousands of rows. Everything else stays None until src/models/parcel.py
# is read and the allowed set is confirmed — then extend this table
# (Store / Industrial Wrhse / Medical / Hotel-Club / Motel-Lodge ->
# commercial; Mobile Home; Courthouse / School -> institutional;
# dwell_type 'N/A' + agreeing useclass1 -> vacant land).
_DWELL_TO_INTERNAL: dict[str, str] = {
    "SINGLE FAMILY": "single_family",
    "MULTI-RESIDENCE": "multifamily",
}

# useclass1 is a clean plain-text class in this layer ("Residential"),
# unlike Olmsted's / Fillmore's compound statutory strings. Used only to
# CONFIRM a dwell_type mapping, never to invent one.
_USECLASS_CONFIRMS: dict[str, tuple[str, ...]] = {
    "single_family": ("RESIDENTIAL",),
    "multifamily": ("RESIDENTIAL", "APARTMENT"),
}


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


def _collapse_ws(value: Any) -> str | None:
    """Strip AND collapse interior whitespace runs to single spaces."""
    s = _safe_str(value)
    if s is None:
        return None
    collapsed = " ".join(s.split())
    return collapsed or None


def _clean_str(value: Any) -> str | None:
    """Collapse whitespace, then treat known placeholders as absent."""
    s = _collapse_ws(value)
    if s is None:
        return None
    if s.upper() in _PLACEHOLDER_STRINGS:
        return None
    return s


def _nonzero_int(value: Any) -> int | None:
    """Int, with 0 coerced to None (0 is this layer's 'absent' sentinel)."""
    n = _safe_int(value)
    if n is None or n == 0:
        return None
    return n


def _title_case_city(city: str | None) -> str | None:
    return city.title() if city else None


def _map_property_type(dwell_type: Any, useclass1: Any) -> str | None:
    """dwell_type is the primary signal; useclass1 must not contradict it.

    Conservative by design (the olmsted 2026-07-14 lesson): an unmapped or
    contradicted value returns None rather than a guess.
    """
    dwell = _clean_str(dwell_type)
    if not dwell:
        return None
    internal = _DWELL_TO_INTERNAL.get(dwell.upper())
    if internal is None:
        return None
    confirms = _USECLASS_CONFIRMS.get(internal)
    if confirms:
        use = _clean_str(useclass1)
        if use and not any(c in use.upper() for c in confirms):
            # dwell_type and useclass1 disagree — trust neither.
            return None
    return internal


def _compose_site_address(attrs: dict[str, Any]) -> str | None:
    """Reassemble the parsed MNGAC address components into one line.

    Component order follows the MNGAC / NENA convention:
      [anumberpre] anumber [anumbersuf] [st_pre_mod] [st_pre_dir]
      [st_pre_typ] [st_pre_sep] st_name [st_pos_typ] [st_pos_dir]
      [st_pos_mod] [sub_type1 sub_id1] [sub_type2 sub_id2]

    Returns None when there is no street name — 36% of Wabasha parcels are
    genuinely unaddressed bare land, and an honest None beats a fragment.
    """
    st_name = _clean_str(attrs.get("st_name"))
    if not st_name:
        return None

    number_bits = [
        _clean_str(attrs.get("anumberpre")),
        str(_nonzero_int(attrs.get("anumber")) or "") or None,
        _clean_str(attrs.get("anumbersuf")),
    ]
    number = "".join(b for b in number_bits if b) or None

    parts: list[str | None] = [
        number,
        _clean_str(attrs.get("st_pre_mod")),
        _clean_str(attrs.get("st_pre_dir")),
        _clean_str(attrs.get("st_pre_typ")),
        _clean_str(attrs.get("st_pre_sep")),
        st_name,
        _clean_str(attrs.get("st_pos_typ")),
        _clean_str(attrs.get("st_pos_dir")),
        _clean_str(attrs.get("st_pos_mod")),
    ]

    for type_key, id_key in (("sub_type1", "sub_id1"), ("sub_type2", "sub_id2")):
        sub_type = _clean_str(attrs.get(type_key))
        sub_id = _clean_str(attrs.get(id_key))
        if sub_type or sub_id:
            parts.append(" ".join(p for p in (sub_type, sub_id) if p))

    line = " ".join(p for p in parts if p)
    return line or None


def _polygon_centroid(
    geometry: dict[str, Any] | None,
) -> tuple[float | None, float | None]:
    """Approximate centroid (lat, lng) of an ArcGIS polygon in WGS84
    (outSR=4326: coordinates arrive as [lng, lat]). Vertex average of the
    outer ring — plenty for a map pin. Returns (None, None) for missing/
    malformed geometry; also handles point geometry ({x, y}) defensively."""
    if not geometry:
        return None, None
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
    """Raw means raw: sentinels ('N/A', 0) are PRESERVED here on purpose —
    the bare-land query is raw_data->>'dwell_type' = 'N/A'. Only nulls and
    empty strings are dropped. Epoch-millis dates get readable *_iso
    siblings for query convenience."""
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
    for date_key in _RAW_DATE_FIELDS:
        iso = arcgis_date_to_iso(attributes.get(date_key))
        if iso:
            cleaned[f"{date_key}_iso"] = iso
    return cleaned


# ============================================================
# OWNER PROJECTION — same vocabulary + patterns as fillmore_parcels /
# olmsted_parcels / ramsey_parcels / signals.owner_distress_summary:
# government / bank_lender / llc_business / individual.
# ============================================================

# Owner classification moved to src/utils/owner_classifier.py (2026-07-28).
# This block was duplicated identically across five loaders; the shared
# version also fixes ~1,255 government parcels misfiled as individual.

# "Lake City, MN 55041-2739"  ->  ("Lake City", "MN", "55041")
_CITY_STATE_ZIP = re.compile(
    r"^(?P<city>.+?),\s*(?P<state>[A-Za-z]{2})\.?\s+(?P<zip>\d{5})(?:-\d{4})?$"
)


_classify_owner = classify_owner


def _parse_city_state_zip(
    value: Any,
) -> tuple[str | None, str | None, str | None]:
    """own_add_l3 arrives COMBINED ('Lake City, MN 55041-2739')."""
    s = _clean_str(value)
    if not s:
        return None, None, None
    m = _CITY_STATE_ZIP.match(s)
    if not m:
        return None, None, None
    return (
        _title_case_city(m.group("city")),
        m.group("state").upper(),
        m.group("zip"),
    )


def _compose_owner_mailing(attrs: dict[str, Any]) -> str | None:
    """Owner mailing street: own_add_l1 plus continuation own_add_l2
    (own_add_l3 is city/state/zip and is parsed separately)."""
    parts = [
        _clean_str(attrs.get("own_add_l1")),
        _clean_str(attrs.get("own_add_l2")),
    ]
    joined = " ".join(p for p in parts if p)
    return joined or None


def _compose_owner_name(attrs: dict[str, Any]) -> str | None:
    """UPPERCASE ONLY — never reorder. See module docstring."""
    primary = _clean_str(attrs.get("owner_name"))
    if not primary:
        return None
    extra = _clean_str(attrs.get("owner_more"))
    full = f"{primary} & {extra}" if extra else primary
    return full.upper()


def _build_owner_row(
    parcel_id: str,
    attrs: dict[str, Any],
    site_address: str | None,
    source_name: str,
    now_iso: str,
) -> dict[str, Any] | None:
    """Project one MNGAC feature's owner fields into a core.owners row.
    Returns None when the feature carries no owner (honest absence — 499
    Wabasha parcels, i.e. right-of-way / water / exempt land)."""
    owner_name = _compose_owner_name(attrs)
    if not owner_name:
        return None

    mailing_address = _compose_owner_mailing(attrs)
    mailing_city, mailing_state, mailing_zip = _parse_city_state_zip(
        attrs.get("own_add_l3")
    )
    if mailing_state is None:
        # own_add_l4 occasionally carries the city/state/zip instead.
        alt_city, alt_state, alt_zip = _parse_city_state_zip(
            attrs.get("own_add_l4")
        )
        mailing_city = mailing_city or alt_city
        mailing_state = alt_state
        mailing_zip = mailing_zip or alt_zip

    # ABSENTEE (2026-07-27, run #1 lesson): the mailing-vs-site string
    # comparison the other counties use flagged 91% of Wabasha owners as
    # absentee (10,049 of 11,047 addressed parcels) — real rates run
    # 20-35%. Our site address is REASSEMBLED from MNGAC components
    # ("73422 N 319th Ave") while own_add_l1 is the county's free-text
    # mailing line ("73422 319th Ave"), so owner-occupied parcels fail
    # equality on formatting alone.
    #
    # This layer publishes the ASSESSOR'S OWN owner-occupancy
    # determination in `homestead` — authoritative where a string compare
    # is a guess. Wabasha uses "Yes"/"No"; other MNGAC counties may use
    # the "FULL HOMESTEAD"/"NON HOMESTEAD" form (cf. dakota_parcels), so
    # both are handled. Honest None when the flag is absent — never fall
    # back to the string compare that produced the bad number.
    homestead = _clean_str(attrs.get("homestead"))
    is_absentee: bool | None = None
    if homestead:
        up = homestead.upper()
        if up.startswith("N"):          # "No" / "Non-homestead"
            is_absentee = True
        # "Yes" / "Fractional" / "Partial" / "FULL HOMESTEAD" — all mean
        # the owner occupies the parcel (fractional = partial homestead,
        # i.e. split ownership or mixed use, still owner-occupied).
        elif up.startswith(("Y", "F", "P")) or "HOMESTEAD" in up:
            is_absentee = False
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
        "source": source_name,
        "observed_at": now_iso,
    }


class MNGACParcelsScraper(BaseArcGISScraper[dict[str, Any]]):
    """Generic MNGAC open-parcels loader. Subclass per county."""

    # ---- Per-county config: subclasses set these THREE ----
    co_code: ClassVar[str] = ""          # 5-digit state FIPS, e.g. '27157'
    county_code: ClassVar[str] = ""      # Govire slug, e.g. 'wabasha'
    source_name: ClassVar[str] = ""      # e.g. 'wabasha_parcels'

    signal_type: ClassVar[str] = "parcel_foundation"
    feature_service_url: ClassVar[str] = _PARCEL_LAYER_URL
    metadata_service_url: ClassVar[str] = _METADATA_LAYER_URL

    # This layer's fields are LOWERCASE. The base default ("OBJECTID")
    # would still query fine (ArcGIS SQL is case-insensitive) but the
    # cursor advance is a plain dict lookup on the returned attributes —
    # it would silently never advance. Verified live 2026-07-27.
    objectid_field: ClassVar[str] = "objectid"

    return_geometry: ClassVar[bool] = True   # polygons -> centroid map pins
    page_size: ClassVar[int] = 1000          # layer MaxRecordCount is 2000
    max_pages: ClassVar[int] = 100
    progress_log_every: ClassVar[int] = 5000

    # Config key consulted by settings.scraper_enabled(). Empty = fall back to
    # source_name, which is what all six hand-written subclasses do — they are
    # unaffected by this attribute existing.
    #
    # The config-table instances set it to 'mngeo_parcels' so FIFTY-ONE
    # counties share ONE toggle. source_name stays per-county because
    # core.parcels.data_sources and core.owners.source key on it and owner
    # provenance would collapse into a single statewide blob otherwise.
    # Per-county control lives in core.mngeo_county_load.enabled — an UPDATE,
    # not a redeploy.
    enable_key: ClassVar[str] = ""

    def __init__(
        self,
        *,
        co_code: str | None = None,
        county_code: str | None = None,
        source_name: str | None = None,
        max_pages: int | None = None,
        enable_key: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Optionally configure this instance at RUNTIME instead of by subclass.

        ADDED 2026-08-05 for the statewide load. Called with no arguments —
        which is how every existing subclass and runner constructs one — this
        is a pass-through and behaviour is byte-identical. The six subclasses
        set ClassVars and never pass these, so they are untouched.

        When arguments ARE passed, they are set as INSTANCE attributes, which
        shadow the ClassVars on every `self.` lookup in this file. That is why
        __init_subclass__ below did not need to change: where_clause is simply
        re-derived here from the same co_code, by the same rule, so the two
        paths cannot drift.

        Subclassing per county was right when counties arrived one at a time.
        At 51 it inverts: five artefacts each (subclass, config flag,
        core.counties row, runner, workflow), 51 cron schedules to offset and
        51 audit.source_health rows. This is the same move already made twice
        for the same reason — source_county_map replaced a hardcoded CASE,
        ecrv_county_map replaced a hardcoded county list.
        """
        super().__init__(**kwargs)
        if co_code:
            self.co_code = co_code
            # SAME derivation as __init_subclass__. Filtering on co_code and
            # never co_name is deliberate: co_name is Title Case in this layer
            # and casing drift across compiles would silently return 0 rows.
            self.where_clause = f"co_code = '{co_code}'"
        if county_code:
            self.county_code = county_code
        if source_name:
            self.source_name = source_name
        if max_pages is not None:
            self.max_pages = max_pages
        if enable_key is not None:
            self.enable_key = enable_key

    @classmethod
    def from_config_row(cls, row: dict[str, Any]) -> "MNGACParcelsScraper":
        """Build a loader for one core.mngeo_county_load row.

        The row's max_pages is a SEED. _run_streaming re-derives it from a
        live returnCountOnly before paging, so a county that grows past its
        seeded figure cannot silently truncate.
        """
        return cls(
            co_code=str(row["co_code"]),
            county_code=str(row["county_code"]),
            source_name=str(row["source_name"]),
            max_pages=row.get("max_pages"),
            enable_key="mngeo_parcels",
        )

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Derive the county filter from co_code so the two can never drift.

        Filtering on co_code (not co_name) is deliberate: co_name is Title
        Case in this layer and spelling/casing drift across compiles would
        silently return zero rows.
        """
        super().__init_subclass__(**kwargs)
        if cls.co_code:
            cls.where_clause = f"co_code = '{cls.co_code}'"

    # ---- Freshness preflight (layer 0) ----

    async def _fetch_freshness(
        self, client: httpx.AsyncClient
    ) -> dict[str, Any]:
        """Read this county's acqdate/rundate from the metadata layer.

        Advisory only — logged and stamped into run metadata so a stale
        compile is visible in the audit trail. NEVER blocks the run.

        A future v2 can short-circuit when rundate has not advanced since
        the last successful run; deliberately not built yet because the
        compile cadence is unknown (all 87 counties shared a single
        rundate of 2026-04-28 at inspection time — one data point).
        """
        info: dict[str, Any] = {}
        try:
            fips3 = self.co_code[-3:] if self.co_code else ""
            response = await client.get(
                f"{self.metadata_service_url}/query",
                params={
                    "where": f"countyfips = '{fips3}'",
                    "outFields": "countyname,countyfips,acqdate,rundate,"
                                 "gac_open_approval,notes",
                    "returnGeometry": "false",
                    "f": "json",
                },
            )
            data = response.json()
            features = data.get("features") or []
            if features:
                attrs = features[0].get("attributes") or {}
                info = {
                    "countyname": attrs.get("countyname"),
                    "gac_open_approval": attrs.get("gac_open_approval"),
                    "acqdate": arcgis_date_to_iso(attrs.get("acqdate")),
                    "rundate": arcgis_date_to_iso(attrs.get("rundate")),
                    "notes": attrs.get("notes"),
                }
                logger.info(
                    "MNGAC source freshness",
                    scraper=self.source_name,
                    **{k: v for k, v in info.items() if v is not None},
                )
        except Exception as e:
            logger.warning(
                "MNGAC freshness preflight failed (run continues)",
                scraper=self.source_name,
                error=str(e)[:200],
            )
        return info

    async def _fetch_live_count(
        self, client: httpx.AsyncClient
    ) -> int | None:
        """This county's CURRENT row count, for deriving max_pages.

        ADDED 2026-08-05. max_pages defaults to 100 (= 100,000 rows) and every
        hand-written subclass overrides it by hand — Wabasha 40, Anoka 200.
        That does not survive 51 counties: St. Louis is 186,455 rows and needs
        290 pages. A run capped at 100 would stop at 100,000 and report
        SUCCESS, which is the silent-failure class the runbook names as the
        dangerous one — no error, no short-page signal, just a county that is
        quietly 46% loaded.

        Deriving from a live count rather than the seeded core.mngeo_county_load
        figure means a county that grows between compiles self-corrects. The
        stored figure is a seed and a sanity check, never the cap.

        Advisory: on failure this returns None and the caller keeps the
        configured max_pages. Never blocks the run.
        """
        try:
            response = await client.get(
                f"{self.feature_service_url}/query",
                params={
                    "where": self.where_clause,
                    "returnCountOnly": "true",
                    "f": "json",
                },
            )
            data = response.json()
            count = data.get("count")
            if isinstance(count, int) and count >= 0:
                return count
        except Exception as e:
            logger.warning(
                "MNGAC live count preflight failed (using configured "
                "max_pages)",
                scraper=self.source_name,
                error=str(e)[:200],
            )
        return None

    # ---- parse_feature: one ArcGIS feature -> parcel dict ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        raw_pin = attributes.get("county_pin")
        if not raw_pin:
            return None

        # county_pin, NEVER state_pin: '27157-R08-00241-00' would normalize
        # to a different id than 'R08-00241-00'.
        pid, _err = safe_normalize_parcel_id(self.county_code, str(raw_pin))
        if pid is None:
            sanitized = "".join(str(raw_pin).split())
            pid = sanitized or None
        if pid is None:
            raise ParseError(
                f"Could not normalize {self.county_code} PIN {raw_pin!r}",
                source=self.source_name,
            )

        address = _compose_site_address(attributes)
        # postcomm = postal city ('Lake City'); ctu_name = civil division
        # ('Lake Township'). City takes the postal form; ctu_name stays in
        # raw_data for township-level filtering.
        city = _title_case_city(
            _clean_str(attributes.get("postcomm"))
            or _clean_str(attributes.get("ctu_name"))
        )
        zip_cd = _clean_str(attributes.get("zip"))

        lat, lng = _polygon_centroid(geometry)
        # Sanity-bound to Minnesota; discard nonsense coordinates.
        if lat is not None and not (43.0 <= lat <= 50.0):
            lat = None
        if lng is not None and not (-97.5 <= lng <= -89.0):
            lng = None

        property_type = _map_property_type(
            attributes.get("dwell_type"), attributes.get("useclass1")
        )

        # 0 = unassessed/exempt, not "worth nothing" — a $0 EMV would
        # poison equity-spread math. Component zeros below stay: bare land
        # truly has emv_bldg 0.
        emv_total = _safe_decimal(attributes.get("emv_total"))
        if emv_total is not None and emv_total == 0:
            emv_total = None
        emv_land = _safe_decimal(attributes.get("emv_land"))
        emv_building = _safe_decimal(attributes.get("emv_bldg"))

        # Acreage: acres_poly is the populated figure in this layer (100%
        # in Wabasha); acres_deed is a 0.0 sentinel. This INVERTS the
        # fillmore_parcels rule — verified per county, never assumed.
        #
        # SANITY CAP (fillmore run #1 lesson): a garbage acreage overflowed
        # int4 (2.6e12 sqft) and killed its whole 500-row batch. No MN
        # county exceeds ~2M acres.
        #
        # CAP LOWERED 100,000 -> 49,000 ACRES, 2026-08-05 (Stearns run #1).
        # 100,000 acres = 4.36e9 sqft, but core.parcels.lot_sqft is int4 and
        # tops out at 2,147,483,647 = 49,299 ACRES. The cap sat at more than
        # twice the column's real ceiling, leaving a 50,701-acre window where
        # a bad row passes the filter and then overflows the column — killing
        # its entire 500-row batch. Cost two batches (1,000 rows) on the
        # first Stearns run.
        #
        # These are NOT large legitimate parcels. Verified live against the
        # layer: all three Stearns rows over 40,000 acres_deed have
        # acres_poly NULL, owner_name NULL, ctu_name NULL and useclass1
        # '3a Commercial Land And Building' — 49,355 and 49,440 acres (5.5%
        # of the county each) and one at 180,998 acres, larger than Stearns
        # County's entire 889,600. Source garbage.
        #
        # For scale: the largest lot_sqft across all 1.2M rows already loaded
        # is 10,256 acres. 49,000 leaves nearly five times that headroom and
        # still sits safely under int4. Widening the column to bigint was
        # considered and REJECTED — a 5.6 GB table rewrite to preserve
        # garbage, and it would have hidden this defect rather than fixing it.
        acres = _safe_float(attributes.get("acres_poly"))
        if not acres or acres <= 0:
            acres = _safe_float(attributes.get("acres_deed"))
        if acres and 0 < acres <= 49000:
            lot_sqft = int(acres * 43560)
        else:
            lot_sqft = None

        use_class = _clean_str(attributes.get("useclass1"))
        school_district = _clean_str(attributes.get("school_dst"))

        cleaned_raw = _clean_raw_data(attributes)

        return {
            "parcel_id": pid,
            "address": address,
            "city": city,
            "zip": zip_cd,
            "lat": lat,
            "lng": lng,
            # Sentinel coercion: 0 means ABSENT in this layer.
            "year_built": _nonzero_int(attributes.get("year_built")),
            "property_type": property_type,
            "estimated_market_value": emv_total,
            "emv_total": emv_total,
            "emv_land": emv_land,
            "emv_building": emv_building,
            "lot_sqft": lot_sqft,
            "num_units": _nonzero_int(attributes.get("num_units")),
            "use_class": use_class,
            "school_district": school_district,
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
                    emv_total=sig.get("emv_total"),
                    emv_land=sig.get("emv_land"),
                    emv_building=sig.get("emv_building"),
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

            row = payload.model_dump(mode="json", exclude_none=True)
            row["last_observed_at"] = now_iso
            batch.append(row)

            owner_row = _build_owner_row(
                sig["parcel_id"],
                sig.get("raw_data") or {},
                sig.get("address"),
                self.source_name,
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
                # RACE FIX (fillmore run #1 lesson): owner batches fill
                # slower than parcel batches when some parcels lack an
                # owner_name, so an owner flush can reference parcels still
                # sitting in the unflushed parcel batch -> FK violation
                # kills the whole owner batch. Flush parcels FIRST, always.
                if batch:
                    n, f = self._upsert_batch(batch)
                    records_new += n
                    records_failed += f
                    batch = []
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

        Mirrors fillmore_parcels / olmsted_parcels exactly: pages fetched in
        the base class's KEYSET mode (objectid > cursor, ordered) —
        constant-time at any depth.
        """
        start_time = time.monotonic()

        # enable_key, not source_name: the config-table instances all gate on
        # a single 'mngeo_parcels' toggle while keeping a per-county
        # source_name for provenance. Empty enable_key => source_name, which
        # is exactly what the six hand-written subclasses do today.
        enable_name = self.enable_key or self.source_name

        if not settings.scraper_enabled(enable_name):
            if trigger == "manual":
                raise ScraperDisabledError(
                    f"Scraper '{enable_name}' is disabled in settings",
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

        # `with`, not `async with`. FIXED 2026-08-05.
        #
        # _class_lock became a threading.Lock on 2026-08-02 (runs dispatched
        # to worker threads; an asyncio.Lock guards nothing across threads —
        # see base_scraper.py). BaseScraper.run() was updated to a plain
        # `with` in that change. This method OVERRIDES run(), and its copy was
        # missed, leaving:
        #     TypeError: '_thread.lock' object does not support the
        #     asynchronous context manager protocol
        #
        # That broke EVERY MNGAC subclass, not just this file's newer
        # config-driven path — it went unnoticed for three days only because
        # Wabasha is quarterly (next due October) and no MNGAC job happened to
        # fire in between. Surfaced on the first Stearns run, 2026-08-05.
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
        run_metadata["mode"] = "streaming_keyset"
        run_metadata["co_code"] = self.co_code
        run_id = audit_logger.start_run(self.source_name, metadata=run_metadata)

        page_size = self.page_size
        max_pages = self.max_pages
        record_cap = self._max_records_override

        logger.info(
            "MNGAC streaming run starting",
            scraper=self.source_name,
            county_code=self.county_code,
            co_code=self.co_code,
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
        last_object_id = 0  # keyset cursor (objectid > last, ordered ASC)

        try:
            async with httpx.AsyncClient(
                timeout=settings.scraper_request_timeout_seconds,
                headers={"User-Agent": "DistressProperties/1.0"},
            ) as client:
                await self._fetch_freshness(client)

                # Derive the page cap from a LIVE count, not the configured
                # figure. See _fetch_live_count: a 100-page default against
                # St. Louis's 186,455 rows stops at 100,000 and reports
                # success. 1.5x headroom + 10 absorbs growth and short pages.
                live_count = await self._fetch_live_count(client)
                if live_count:
                    derived = math.ceil(live_count / page_size * 1.5) + 10
                    if derived > max_pages:
                        logger.info(
                            "MNGAC max_pages raised from live count",
                            scraper=self.source_name,
                            live_count=live_count,
                            configured_max_pages=max_pages,
                            derived_max_pages=derived,
                        )
                        max_pages = derived
                    run_metadata["live_count"] = live_count

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

                    # Advance the keyset cursor to the page's max objectid.
                    # GUARD: if the cursor fails to advance the loop would
                    # refetch page 1 forever (masked by seen_pids) — bail
                    # loudly instead.
                    previous_object_id = last_object_id
                    for feature in features:
                        oid = (feature.get("attributes") or {}).get(
                            self.objectid_field
                        )
                        if isinstance(oid, int) and oid > last_object_id:
                            last_object_id = oid
                    if last_object_id <= previous_object_id:
                        raise ParseError(
                            f"Keyset cursor did not advance past "
                            f"{previous_object_id} — check objectid_field "
                            f"({self.objectid_field!r}) against the layer",
                            source=self.source_name,
                        )

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
                            continue  # duplicate — first row wins
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
                            "MNGAC streaming progress",
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
                "MNGAC streaming run failed",
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
            "MNGAC streaming run complete",
            scraper=self.source_name,
            county_code=self.county_code,
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


class WabashaParcelsScraper(MNGACParcelsScraper):
    """Wabasha County parcels via the MNGAC statewide open layer.

    17,323 parcels verified live 2026-07-27 (acqdate 2026-04-09).
    The eighth county, and the first onboarded through the aggregate.
    """

    source_name: ClassVar[str] = "wabasha_parcels"
    county_code: ClassVar[str] = "wabasha"
    co_code: ClassVar[str] = "27157"

    max_pages: ClassVar[int] = 40   # 17,323 rows -> ~18 pages; headroom


class AnokaParcelsScraper(MNGACParcelsScraper):
    """Anoka County parcels via the MNGAC statewide open layer.

    139,930 parcels verified live 2026-07-28. acqdate 2026-04-27 — one day
    before the compile, the freshest tier in the whole aggregate (tied with
    Hennepin and Houston), so the aggregate is a legitimate primary source
    here rather than an expansion compromise.

    THE NINTH COUNTY, and the reason the generic loader was worth building:
    before this, core.parcels held 192 Anoka rows, every one a synthetic
    ANOKA-FC-* placeholder created when a foreclosure signal could not
    resolve to a real parcel. A top-five metro foreclosure county had no
    spine at all — no EMV, no equity math, no eCRV outcome confirmation.
    """

    source_name: ClassVar[str] = "anoka_parcels"
    county_code: ClassVar[str] = "anoka"
    co_code: ClassVar[str] = "27003"

    max_pages: ClassVar[int] = 200   # 139,930 rows -> 140 pages; headroom
    progress_log_every: ClassVar[int] = 20000


__all__ = ["MNGACParcelsScraper", "WabashaParcelsScraper", "AnokaParcelsScraper"]
