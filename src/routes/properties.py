"""
Public properties + stats endpoints for the govire.com frontend.

Returns live data from signals.distress_events and core.parcels —
NO hardcoded numbers.

Each upstream scraper writes raw_data in its own shape (we built them
in different sessions before standardizing), so a per-source extractor
maps each shape to a common output payload. The /properties endpoint
exposes a `category` filter so the frontend table can render the right
columns for the selected signal type.

Routes:
    GET /stats                              — live signal counts
    GET /properties                         — paginated property list
    GET /properties/{source}/{source_id}    — single property detail
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status as http_status

from src.db.supabase_client import core_table, signals_table, outcomes_table, scoring_table
from src.utils.errors import success_envelope
from src.utils.logger import logger
from src.middleware.tier import TierResolved, TierContext
from src.utils.redaction import (
    redact_property,
    redact_detail_extras,
    owner_browse_allowed,
    gate_filters_for_tier,
)


# ============================================================
# CATEGORY + COUNTY MAPPINGS
# ============================================================
# Categories pair (source[, event_type]) tuples — same shape used in
# /stats counts. Sharing one source of truth means a count and a
# table query for the same category always reference the same rows.

_CATEGORY_FILTERS: dict[str, list[dict[str, str]]] = {
    "foreclosure": [
        {"source": "anoka_sheriff"},
        {"source": "dakota_sheriff"},
        {"source": "hennepin_sheriff"},
        {"source": "ramsey_sheriff"},
        {"source": "washington_sheriff"},
        {"source": "scott_sheriff"},
        {"source": "carver_sheriff"},
        # Extracted legal-notice foreclosures (statewide, from published
        # Notice-of-Sale documents via the LLM extraction pipeline).
        {"source": "startribune_legal"},
        # Rochester Post Bulletin foreclosure notices (Column API) — the
        # Olmsted pilot's first signal source (2026-07-09).
        {"source": "postbulletin_legal"},
        # Fillmore County Journal foreclosure notices (WordPress REST) —
        # the Chatfield-corridor expansion's first signal source
        # (2026-07-23).
        {"source": "fillmore_legal"},
    ],
    "tax_forfeit": [
        {"source": "hennepin_tax_roll", "event_type": "tax_forfeit"},
        {"source": "ramsey_tax_roll", "event_type": "tax_forfeit"},
        {"source": "ramsey_tfl"},
        {"source": "mn_dor_red_book"},
    ],
    "vacant": [
        {"source": "mpls_vbr"},
        {"source": "saint_paul_vacant"},
        {"source": "saint_paul_dsi"},
    ],
    "tax_delinquent": [
        {"source": "hennepin_tax_roll", "event_type": "tax_delinquent"},
        # Olmsted 2026 statutory Delinquent Tax List (bulk-loaded
        # 2026-07-10 from the Post Bulletin publication, OCR + spine-
        # validated; see MIGRATION_olmsted_delq_list_2026-07-10.sql).
        {"source": "olmsted_delq_list"},
        {"source": "ramsey_tax_roll", "event_type": "tax_delinquent"},
    ],
    "tax_assessment": [
        {"source": "ramsey_tax_roll", "event_type": "tax_assessment"},
    ],
}

# Fixed source -> county for sources whose county is one-to-one with the
# source name. Statewide/extracted sources (see _PER_ROW_COUNTY_SOURCES)
# are NOT listed here — their county is resolved per-row from the data.
_SOURCE_TO_COUNTY: dict[str, str] = {
    "anoka_sheriff": "Anoka",
    "dakota_sheriff": "Dakota",
    "hennepin_sheriff": "Hennepin",
    "ramsey_sheriff": "Ramsey",
    "washington_sheriff": "Washington",
    "scott_sheriff": "Scott",
    "carver_sheriff": "Carver",
    "mpls_vbr": "Hennepin",
    "saint_paul_vacant": "Ramsey",
    "saint_paul_dsi": "Ramsey",
    "hennepin_tax_roll": "Hennepin",
    "ramsey_tax_roll": "Ramsey",
    "ramsey_tfl": "Ramsey",
    "postbulletin_legal": "Olmsted",
    "fillmore_legal": "Fillmore",
    "olmsted_delq_list": "Olmsted",
    "mn_dor_red_book": "Statewide",
}

# Reverse of the county_code slug -> display name, covering the counties
# seeded in core.counties. Used to render a statewide/extracted row's
# county (stored as a slug in raw_data.detail.county) back to a name.
_SLUG_TO_COUNTY_NAME: dict[str, str] = {
    "anoka": "Anoka",
    "carver": "Carver",
    "cass": "Cass",
    "chisago": "Chisago",
    "dakota": "Dakota",
    "fillmore": "Fillmore",
    "hennepin": "Hennepin",
    "olmsted": "Olmsted",
    "otter_tail": "Otter Tail",
    "ramsey": "Ramsey",
    "scott": "Scott",
    "st_louis": "St. Louis",
    "stearns": "Stearns",
    "washington": "Washington",
    "wright": "Wright",
}

# Sources whose county is NOT one-to-one with the source name (statewide
# feeds). For these we resolve county per-row from raw_data.detail.county
# (a lowercase slug), mapped back to the display name. Everything else uses
# the fixed _SOURCE_TO_COUNTY map.
_PER_ROW_COUNTY_SOURCES = {"startribune_legal"}


def _resolve_county(source: str, raw: dict) -> Optional[str]:
    """County display name for a row.

    Statewide/extracted sources carry their own county in
    raw_data.detail.county (slug, e.g. 'scott'); we map that back to a display
    name. All other sources use the fixed source->county map. Returns None
    when no county is resolvable (honest em-dash)."""
    if source in _PER_ROW_COUNTY_SOURCES:
        detail = raw.get("detail") or {}
        slug = (detail.get("county") or "").strip().lower()
        if not slug:
            return None
        return _SLUG_TO_COUNTY_NAME.get(slug, slug.replace("_", " ").title())
    return _SOURCE_TO_COUNTY.get(source)


def _sources_for_county(county: str) -> list[str]:
    """All fixed-mapping sources for a county name. Does NOT include
    per-row-county sources (startribune_legal) — those can't be filtered by a
    simple source IN-list, so the county filter handles them separately."""
    return [src for src, c in _SOURCE_TO_COUNTY.items() if c == county]


router = APIRouter(tags=["properties"])

async def require_access_key(
    x_access_key: Optional[str] = Header(default=None, alias="X-Access-Key"),
) -> str:
    """FastAPI dependency that gates an endpoint behind a valid access key.

    The frontend sends the visitor's key in the 'X-Access-Key' header. We
    look it up in access.access_requests; the key is valid only if it exists
    AND its row status is 'approved'. Missing/unknown/blocked -> 401.
    On success, stamps last_seen_at (best-effort) so the owner can see
    activity."""
    from src.db.supabase_client import access_table

    if not x_access_key:
        raise HTTPException(
            status_code=401,
            detail="Access key required. Request access at govire.com/data.",
        )

    try:
        result = (
            access_table("access_requests")
            .select("id, status")
            .eq("access_key", x_access_key)
            .limit(1)
            .execute()
        )
        rows = result.data or []
    except Exception as e:
        logger.exception(
            "access key validation query failed",
            error_type=type(e).__name__,
        )
        raise HTTPException(status_code=503, detail="Access check unavailable.")

    if not rows or rows[0].get("status") != "approved":
        raise HTTPException(
            status_code=401,
            detail="Invalid or unapproved access key.",
        )

    try:
        from datetime import datetime, timezone
        access_table("access_requests").update(
            {"last_seen_at": datetime.now(timezone.utc).isoformat()}
        ).eq("id", rows[0]["id"]).execute()
    except Exception as e:
        logger.warning("last_seen_at stamp failed", error_type=type(e).__name__)

    return x_access_key


# ============================================================
# PER-SOURCE RAW_DATA EXTRACTORS
# ============================================================
# Each scraper persisted raw_data with its own conventions. These
# extractors map each known shape into a common dict the frontend can
# render. Unknown sources fall through to a generic extractor that
# does best-effort with common keys.


def _extract_anoka(raw: dict, row: dict) -> dict[str, Any]:
    """anoka_sheriff — {list, detail} shape. Enriched (2026-05-31) with
    owner / market value / homestead / absentee from Anoka's attributed
    parcel layer via a verified PIN2 join. Those gis_* fields are present
    only on rows whose tax_parcel_no matched a parcel (~66%); the rest stay
    null and render as em-dash. We prefer the assessor owner-of-record
    (gis_owner) over the notice mortgagor when available."""
    list_ = raw.get("list") or {}
    detail = raw.get("detail") or {}

    gis_market = detail.get("gis_market_value")
    try:
        market_value = float(gis_market) if gis_market is not None else None
    except (TypeError, ValueError):
        market_value = None

    return {
        "address": list_.get("address") or detail.get("detail_address"),
        "city": list_.get("city"),
        "zip": list_.get("zip"),
        "owner": detail.get("gis_owner") or detail.get("owner_name"),
        "sale_date": list_.get("scheduled_date") or row.get("event_date"),
        "sale_time": detail.get("sale_time"),
        "amount": row.get("event_value"),
        "status": detail.get("status") or "Active",
        "tax_parcel_no": detail.get("tax_parcel_no"),
        "original_principal": detail.get("original_principal"),
        "municipality": list_.get("city"),
        "lat": None,
        "lng": None,
        "neighborhood": None,
        "registered_date": None,
        "market_value": market_value,
        "earliest_delq_year": None,
        "dwelling_type": None,
        "ward": None,
        # Generic enrichment fields (any foreclosure source MAY populate;
        # Anoka does now, Hennepin/Dakota can later).
        "owner_mailing": detail.get("gis_owner_mailing"),
        "is_absentee": detail.get("gis_is_absentee"),
        "homestead": detail.get("gis_homestead"),
    }


def _extract_dakota(raw: dict, row: dict) -> dict[str, Any]:
    """dakota_sheriff — ArcGIS feature service shape: attributes + geometry.
    Enriched (2026-06) with owner / market value / mailing / homestead from
    Dakota's Tax Parcels layer (71) via a unique suffix-normalized address
    match. Those gis_* fields live under raw_data.detail (same keys as Anoka /
    Hennepin) and are present only on rows that matched exactly one parcel;
    the rest stay null and render as em-dash. We prefer the assessor
    owner-of-record (gis_owner) over the inconsistent GIS Mortgagor field."""
    attrs = raw.get("attributes") or {}
    geom = raw.get("geometry") or {}
    detail = raw.get("detail") or {}

    gis_market = detail.get("gis_market_value")
    try:
        market_value = float(gis_market) if gis_market is not None else None
    except (TypeError, ValueError):
        market_value = None

    return {
        "address": attrs.get("GeoAddress"),
        "city": attrs.get("GeoCity") or attrs.get("CITYNAME"),
        "zip": None,
        # Prefer the assessor owner-of-record (gis_owner) from enrichment;
        # fall back to Dakota's inconsistently-populated Mortgagor field.
        "owner": detail.get("gis_owner") or (attrs.get("Mortgagor") or "").strip() or None,
        "sale_date": row.get("event_date"),
        "sale_time": None,
        "amount": attrs.get("SaleAmount") or row.get("event_value"),
        # Dakota records are completed sales (already happened).
        "status": "Sold",
        "tax_parcel_no": detail.get("gis_pid"),
        "original_principal": None,
        "municipality": attrs.get("GeoCity"),
        "lat": geom.get("y"),
        "lng": geom.get("x"),
        "neighborhood": None,
        "registered_date": None,
        "market_value": market_value,
        "earliest_delq_year": None,
        "dwelling_type": None,
        "ward": None,
        # Generic foreclosure enrichment fields — populated by the Dakota
        # foreclosure enrichment job; null (em-dash) on unmatched rows.
        "owner_mailing": detail.get("gis_owner_mailing"),
        "is_absentee": detail.get("gis_is_absentee"),
        "homestead": detail.get("gis_homestead"),
    }


def _extract_mpls_vbr(raw: dict, row: dict) -> dict[str, Any]:
    """mpls_vbr — VBR_MPLS feature service: attributes + top-level owner_name.

    2026-07-09 (owner freshness): the feed is a 2023 snapshot
    (raw_data._data_vintage) and its owner_name is stale — 129 of 187
    numeric-PIN rows differ from the CURRENT assessor owner. Owner is
    deliberately left None here so _apply_assessor_owners() fills the
    current owner-of-record (same mechanism that fixed Saint Paul); the
    2023 owner stays in raw_data as provenance. Likewise the feed's
    City/State/Zip attributes are the OWNER's mailing address, not the
    property's (Atlanta GA on a Logan Ave N row) — every property in
    this registry is in Minneapolis, so city/municipality are fixed and
    zip is an honest None (assessor value patch does not cover zip)."""
    attrs = raw.get("attributes") or {}
    return {
        "address": attrs.get("Address"),
        "city": "Minneapolis",
        "zip": None,
        "owner": None,
        "sale_date": None,
        "sale_time": None,
        # event_value here is the VBR annual fee, not a sale price.
        "amount": row.get("event_value"),
        "status": attrs.get("Property_s"),
        "tax_parcel_no": attrs.get("APN_Txt"),
        "original_principal": None,
        "municipality": "Minneapolis",
        "lat": attrs.get("Latitude"),
        "lng": attrs.get("Longitude"),
        "neighborhood": raw.get("neighborhood"),
        "registered_date": (
            raw.get("condemned_date") or row.get("event_date")
        ),
        "market_value": None,
        "earliest_delq_year": None,
        "dwelling_type": None,
        "ward": None,
    }


def _extract_saint_paul_vacant(raw: dict, row: dict) -> dict[str, Any]:
    """saint_paul_vacant — Saint Paul DSI ArcGIS feed. ALL-CAPS keys
    (ADDRESS, PIN, VACANT_AS_OF, VB_CATEGORY, DWELLING_TYPE). Note
    that LONGGITUDE is misspelled in the source data — we read it
    as-is. Saint Paul DSI does NOT publish owner names, so the owner
    field stays null and the table renders an em-dash for it."""
    attrs = raw.get("attributes") or {}

    # Parse VACANT_AS_OF (MM/DD/YYYY) to ISO so the frontend's date
    # formatter renders it consistently with other sources.
    vacant_as_of = attrs.get("VACANT_AS_OF")
    registered_iso: Optional[str] = None
    if vacant_as_of and isinstance(vacant_as_of, str):
        parts = vacant_as_of.split("/")
        if len(parts) == 3:
            try:
                month, day, year = (int(p) for p in parts)
                registered_iso = (
                    f"{year:04d}-{month:02d}-{day:02d}"
                )
            except ValueError:
                registered_iso = vacant_as_of

    # Category 1/2/3 → human-readable label. Saint Paul's three-tier
    # vacant-building classification maps to escalating risk:
    #   1 = sound + secured (lowest risk)
    #   2 = boarded (moderate)
    #   3 = nuisance / hazardous (highest risk, condemnable)
    vb_cat = str(attrs.get("VB_CATEGORY") or "").strip()
    status_label = {
        "1": "Category 1 (sound)",
        "2": "Category 2 (boarded)",
        "3": "Category 3 (nuisance)",
    }.get(vb_cat, row.get("title") or "Vacant")

    # Latitude / longitude are strings in this feed and longitude is
    # misspelled. Convert to floats so the future map view can plot.
    def _to_float(v: Any) -> Optional[float]:
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    return {
        "address": attrs.get("ADDRESS"),
        # The feed has no City field; this is the Saint Paul DSI feed
        # by definition, so we set it explicitly.
        "city": "Saint Paul",
        "zip": None,
        # Saint Paul DSI does not publish owner names in its public
        # feed (unlike Minneapolis VBR, which does).
        "owner": None,
        "sale_date": None,
        "sale_time": None,
        "amount": row.get("event_value"),
        "status": status_label,
        "tax_parcel_no": attrs.get("PIN"),
        "original_principal": None,
        "municipality": "Saint Paul",
        "lat": _to_float(attrs.get("LATITUDE")),
        # Sic — source data misspells "longitude".
        "lng": _to_float(attrs.get("LONGGITUDE")),
        # Saint Paul tracks by ward/district + census tract rather
        # than by neighborhood; surface ward as the closest analog.
        "neighborhood": (
            f"Ward {attrs.get('WARD')}"
            if attrs.get("WARD")
            else None
        ),
        "registered_date": registered_iso or row.get("event_date"),
        "market_value": None,
        "earliest_delq_year": None,
        # Saint-Paul-specific extras (the frontend may render these
        # when the vacant tab is active).
        "dwelling_type": (
            raw.get("dwelling_type") or attrs.get("DWELLING_TYPE")
        ),
        "ward": attrs.get("WARD"),
    }


def _extract_hennepin_tax(raw: dict, row: dict) -> dict[str, Any]:
    """hennepin_tax_roll — mined from core.parcels. Now enriched with
    property address, owner name, owner mailing address, absentee flag,
    real market value, and annual tax. Tax delinquent vs forfeit is
    distinguished by event_type."""
    market_raw = raw.get("market_value")
    market_value: Optional[float]
    try:
        market_value = float(market_raw) if market_raw is not None else None
    except (TypeError, ValueError):
        market_value = None

    tax_raw = raw.get("annual_tax")
    annual_tax: Optional[float]
    try:
        annual_tax = float(tax_raw) if tax_raw is not None else None
    except (TypeError, ValueError):
        annual_tax = None

    # Property address: the miner composes it from HOUSE_NO + STREET_NM and
    # leaves it null for genuinely unassigned (vacant) parcels.
    prop_addr = raw.get("property_address")

    return {
        "address": prop_addr,
        "city": raw.get("property_city") or raw.get("municipality"),
        "zip": raw.get("property_zip"),
        "owner": raw.get("owner_name"),
        "sale_date": None,
        "sale_time": None,
        # event_value on tax rows is the market value — surfaced as amount
        # so a single column can show it.
        "amount": market_value if market_value is not None else row.get("event_value"),
        "status": (
            "Tax-forfeited"
            if row.get("event_type") == "tax_forfeit"
            else "Special assessment"
            if row.get("event_type") == "tax_assessment"
            else "Tax-delinquent"
        ),
        "tax_parcel_no": row.get("parcel_id"),
        "original_principal": None,
        "municipality": raw.get("municipality"),
        "lat": None,
        "lng": None,
        "neighborhood": None,
        "registered_date": None,
        "market_value": market_value,
        "earliest_delq_year": raw.get("earliest_delq_year"),
        "dwelling_type": None,
        "ward": None,
        # --- Enriched tax-roll fields (property identification + owner) ---
        "owner_mailing": raw.get("owner_mailing"),
        "is_absentee": raw.get("is_absentee"),
        "annual_tax": annual_tax,
        "special_assessment_due": raw.get("special_assessment_due"),
    }


def _extract_hennepin_sheriff(raw: dict, row: dict) -> dict[str, Any]:
    """hennepin_sheriff — clean JSON API. raw_data holds the full detail
    record at the top level (not nested under list/detail). Mortgagors are
    a list of {display} objects; redemptionExpirationDate is server-computed
    by Hennepin so we surface it directly rather than recomputing it.

    Enriched (2026-05-31) by the hennepin_foreclosure_enrichment job, which
    matches each row's address to the Hennepin parcel roll in core.parcels and
    writes owner / market value / mailing / homestead under raw_data.detail.gis_*
    (same shape as Anoka). ~332 of 465 rows match uniquely; the rest stay blank
    and render as em-dash. We prefer the assessor owner-of-record (gis_owner)
    over the notice mortgagor when available."""
    mortgagors = raw.get("mortgagors") or []
    owner = None
    if isinstance(mortgagors, list):
        names = [
            (m.get("display") or "").strip()
            for m in mortgagors
            if isinstance(m, dict) and (m.get("display") or "").strip()
        ]
        owner = "; ".join(n for n in names if n) or None

    # Enrichment lives under raw_data.detail.gis_* (written by the
    # hennepin_foreclosure_enrichment job). Absent on unmatched rows.
    detail = raw.get("detail") or {}

    gis_market = detail.get("gis_market_value")
    try:
        market_value = float(gis_market) if gis_market is not None else None
    except (TypeError, ValueError):
        market_value = None

    return {
        "address": raw.get("address"),
        "city": raw.get("city"),
        "zip": None,
        # Prefer the assessor owner-of-record where the enrichment matched;
        # fall back to the foreclosure-notice mortgagor otherwise.
        "owner": detail.get("gis_owner") or owner,
        "sale_date": raw.get("dateOfSale") or row.get("event_date"),
        "sale_time": None,
        "amount": raw.get("finalBidAmount") or row.get("event_value"),
        # Completed sheriff sales; the actionable state is the redemption
        # window, which the redemption-window UI will derive from
        # redemption_ends_at below.
        "status": "Sold",
        "tax_parcel_no": None,
        "original_principal": None,
        "municipality": raw.get("city"),
        "lat": None,
        "lng": None,
        "neighborhood": None,
        "registered_date": None,
        "market_value": market_value,
        "earliest_delq_year": None,
        "dwelling_type": None,
        "ward": None,
        # Enrichment fields (populated on rows that matched a parcel).
        "owner_mailing": detail.get("gis_owner_mailing"),
        "is_absentee": detail.get("gis_is_absentee"),
        "homestead": detail.get("gis_homestead"),
        # Hennepin publishes this; preserved for the redemption-window work.
        "redemption_ends_at": raw.get("redemptionExpirationDate"),
        "mortgagee": raw.get("mortgagee"),
        "law_firm": raw.get("lawFirm"),
        "type_of_sale": raw.get("typeOfSale"),
    }


def _extract_washington(raw: dict, row: dict) -> dict[str, Any]:
    """washington_sheriff — monthly Report-of-Sheriff's-Sales XLS shape:
    raw_data holds a 'sale' sub-object (pid, sale_date, sale_amount, purchaser,
    instrument, original_lender, notice owner). Enriched (2026-06) by the
    washington_foreclosure_enrichment job, which PID-joins each row to the
    Washington TaxParcel roll in core.parcels and writes owner / market value /
    mailing / homestead / site address under raw_data.detail.gis_* (same keys as
    Anoka / Dakota / Hennepin). 115 of 116 rows matched by exact PID; the one
    unmatched row stays blank and renders as em-dash. We prefer the assessor
    owner-of-record (gis_owner) over the foreclosure-notice owner when present.
    Completed sales (post-auction) → status 'Sold'; the redemption window is
    derived downstream from event_date (sale + ~6 months)."""
    sale = raw.get("sale") or {}
    detail = raw.get("detail") or {}

    gis_market = detail.get("gis_market_value")
    try:
        market_value = float(gis_market) if gis_market is not None else None
    except (TypeError, ValueError):
        market_value = None

    return {
        "address": detail.get("gis_site_address"),
        "city": detail.get("gis_city"),
        "zip": detail.get("gis_zip"),
        # Prefer the assessor owner-of-record from enrichment; fall back to the
        # foreclosure-notice grantor/owner from the sheriff file.
        "owner": detail.get("gis_owner") or sale.get("owner"),
        "sale_date": sale.get("sale_date") or row.get("event_date"),
        "sale_time": None,
        "amount": row.get("event_value"),
        # Washington publishes completed sales (post-auction).
        "status": "Sold",
        "tax_parcel_no": detail.get("gis_pid") or sale.get("pid"),
        "original_principal": None,
        "municipality": detail.get("gis_city"),
        "lat": None,
        "lng": None,
        "neighborhood": None,
        "registered_date": None,
        "market_value": market_value,
        "earliest_delq_year": None,
        "dwelling_type": None,
        "ward": None,
        # Generic foreclosure enrichment fields — populated by the Washington
        # foreclosure enrichment job; null (em-dash) on the unmatched row.
        "owner_mailing": detail.get("gis_owner_mailing"),
        "is_absentee": detail.get("gis_is_absentee"),
        "homestead": detail.get("gis_homestead"),
    }


def _extract_startribune_legal(raw: dict, row: dict) -> dict[str, Any]:
    """startribune_legal — extracted legal foreclosure notices (Feature #5).
    raw_data is the shape written by the promotion module: address/city at the
    top level, mortgagee, mortgagors:[{display}], amount_due, redemption_period,
    and a detail block carrying the real PID (gis_pid) + county slug. These are
    SCHEDULED (future) sheriff sales, so status reflects that — never 'Sold'.
    The real parcel PID is surfaced as tax_parcel_no; the owner is the notice
    mortgagor (no assessor enrichment on these yet)."""
    detail = raw.get("detail") or {}

    mortgagors = raw.get("mortgagors") or []
    owner = None
    if isinstance(mortgagors, list):
        names = [
            (m.get("display") or "").strip()
            for m in mortgagors
            if isinstance(m, dict) and (m.get("display") or "").strip()
        ]
        owner = "; ".join(n for n in names if n) or None

    return {
        "address": raw.get("address"),
        "city": raw.get("city"),
        "zip": None,
        "owner": owner,
        "sale_date": raw.get("dateOfSale") or row.get("event_date"),
        "sale_time": None,
        "amount": raw.get("amount_due") or row.get("event_value"),
        # Scheduled (upcoming) sale — not completed.
        "status": "Scheduled sale",
        "tax_parcel_no": detail.get("gis_pid"),
        "original_principal": None,
        "municipality": raw.get("city"),
        "lat": None,
        "lng": None,
        "neighborhood": None,
        "registered_date": None,
        "market_value": None,
        "earliest_delq_year": None,
        "dwelling_type": None,
        "ward": None,
        "owner_mailing": None,
        "is_absentee": None,
        "homestead": None,
        "mortgagee": raw.get("mortgagee"),
        "law_firm": raw.get("lawFirm"),
    }


def _extract_generic(raw: dict, row: dict) -> dict[str, Any]:
    """Fallback extractor for unknown sources. Tries common keys."""
    return {
        "address": (
            raw.get("address")
            or (raw.get("attributes") or {}).get("Address")
        ),
        "city": (
            raw.get("city")
            or (raw.get("attributes") or {}).get("City")
        ),
        "zip": raw.get("zip"),
        "owner": raw.get("owner_name") or raw.get("owner"),
        "sale_date": row.get("event_date"),
        "sale_time": None,
        "amount": row.get("event_value"),
        "status": None,
        "tax_parcel_no": row.get("parcel_id"),
        "original_principal": None,
        "municipality": raw.get("municipality") or raw.get("city"),
        "lat": None,
        "lng": None,
        "neighborhood": None,
        "registered_date": row.get("event_date"),
        "market_value": None,
        "earliest_delq_year": None,
        "dwelling_type": None,
        "ward": None,
        # Generic foreclosure enrichment fields — null until wired;
        # keeps the foreclosure row shape uniform across all sources.
        "owner_mailing": None,
        "is_absentee": None,
        "homestead": None,
    }


def _extract_ramsey_tfl(raw: dict, row: dict) -> dict[str, Any]:
    """ramsey_tfl — county tax-forfeited land auction/OTC lists. raw_data
    is flat (written by the scraper 2026-07-09). The county's APPRAISED
    value (the minimum bid) is surfaced as `amount`; market_value stays
    None so the assessor patch fills the EMV — showing both lets a buyer
    see minimum bid vs assessed worth side by side. Status carries the
    sale channel (auction list vs available over the counter)."""
    appraised = raw.get("appraised_value")
    try:
        appraised_f = float(appraised) if appraised is not None else None
    except (TypeError, ValueError):
        appraised_f = None
    status = (
        "Available over the counter"
        if raw.get("sale_status") == "otc_available"
        else "On auction list"
    )
    return {
        "address": raw.get("property_address"),
        "city": raw.get("property_city"),
        "zip": None,
        "owner": None,  # forfeited: assessor patch fills the state/county owner
        "sale_date": None,
        "sale_time": None,
        "amount": appraised_f,
        "status": status,
        "tax_parcel_no": row.get("parcel_id"),
        "original_principal": None,
        "municipality": raw.get("property_city"),
        "lat": None,
        "lng": None,
        "neighborhood": None,
        "registered_date": raw.get("list_date"),
        "market_value": None,
        "earliest_delq_year": None,
        "dwelling_type": raw.get("property_type"),
        "ward": None,
        "owner_mailing": None,
        "is_absentee": None,
        "annual_tax": None,
        "special_assessment_due": None,
    }


def _extract_postbulletin_legal(raw: dict, row: dict) -> dict[str, Any]:
    """postbulletin_legal — Rochester Post Bulletin foreclosure notices
    (Column API, 2026-07-09). Flat raw_data written by the scraper. These
    are SCHEDULED (future) sheriff sales — status says so, never 'Sold'.
    The notice's own TAX PARCEL NO. is the parcel id (direct Olmsted spine
    join), so owner/EMV/coords/lot arrive via the generic assessor patch —
    owner is left None here on purpose (the CURRENT owner beats the notice
    mortgagor; the mortgagor is preserved in raw_data and the drawer)."""
    def _f(key: str) -> float | None:
        v = raw.get(key)
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None
    return {
        "address": raw.get("property_address"),
        "city": raw.get("property_city"),
        "zip": raw.get("property_zip"),
        "owner": None,  # assessor patch fills the current owner
        "sale_date": raw.get("sale_date") or row.get("event_date"),
        "sale_time": raw.get("sale_time"),
        "amount": _f("amount_due"),
        "status": ("Scheduled sale (postponed)" if raw.get("postponed")
                   else "Scheduled sale"),
        "tax_parcel_no": row.get("parcel_id"),
        "original_principal": _f("original_principal"),
        "municipality": raw.get("property_city"),
        "lat": None,
        "lng": None,
        "neighborhood": None,
        "registered_date": None,
        "market_value": None,
        "earliest_delq_year": None,
        "dwelling_type": None,
        "ward": None,
        "owner_mailing": None,
        "is_absentee": None,
        "annual_tax": None,
        "special_assessment_due": None,
        # Redemption clock written by the scraper (2026-07-10/11 session):
        # basis is 'stated' (period read from the notice text) or 'computed'
        # (statutory default applied). The caveat flags the MN abandonment
        # exception (5-week window if abandoned). Passed through verbatim so
        # the frontend can show the clock WITH its provenance — the date
        # never ships without its basis.
        "redemption_basis": raw.get("redemption_basis"),
        "redemption_abandonment_caveat": raw.get("redemption_abandonment_caveat"),
        "redemption_months": raw.get("redemption_months"),
    }


def _extract_olmsted_delq(raw: dict, row: dict) -> dict[str, Any]:
    """olmsted_delq_list — the county's statutory Delinquent Tax List
    (bulk migration, 2026-07-10). The list publishes legal descriptions,
    not situs addresses, so address/city/zip are left None for the
    assessor patch to fill from the Olmsted spine — along with owner,
    EMV, coords, and lot size. Amount = total tax + penalties to clear
    the list; earliest_delq_year = the delinquent pay year."""
    amt = raw.get("total_tax_penalties")
    try:
        amt_f = float(amt) if amt is not None else None
    except (TypeError, ValueError):
        amt_f = None
    return {
        "address": None,   # spine fills (list has legal descriptions only)
        "city": None,
        "zip": None,
        "owner": None,     # assessor patch fills the current owner
        "sale_date": None,
        "sale_time": None,
        "amount": amt_f,
        "status": "On the delinquent tax list",
        "tax_parcel_no": row.get("parcel_id"),
        "original_principal": None,
        "municipality": raw.get("city_section"),
        "lat": None,
        "lng": None,
        "neighborhood": None,
        "registered_date": raw.get("published"),
        "market_value": None,
        "earliest_delq_year": raw.get("pay_year"),
        "dwelling_type": None,
        "ward": None,
        "owner_mailing": None,
        "is_absentee": None,
        "annual_tax": None,
        "special_assessment_due": None,
    }


_EXTRACTORS: dict[str, Any] = {
    "anoka_sheriff": _extract_anoka,
    "hennepin_sheriff": _extract_hennepin_sheriff,
    "dakota_sheriff": _extract_dakota,
    "washington_sheriff": _extract_washington,
    "startribune_legal": _extract_startribune_legal,
    "mpls_vbr": _extract_mpls_vbr,
    "saint_paul_vacant": _extract_saint_paul_vacant,
    "saint_paul_dsi": _extract_saint_paul_vacant,
    "hennepin_tax_roll": _extract_hennepin_tax,
    "ramsey_tax_roll": _extract_hennepin_tax,
    "ramsey_tfl": _extract_ramsey_tfl,
    "postbulletin_legal": _extract_postbulletin_legal,
    # fillmore_legal writes the SAME raw_data schema by design
    # (2026-07-23) — the extractor is shared, like ramsey_tax_roll.
    "fillmore_legal": _extract_postbulletin_legal,
    "olmsted_delq_list": _extract_olmsted_delq,
}

# ============================================================
# MULTI-SIGNAL OVERLAY (signals.parcel_distress_overlay)
# ============================================================
# The overlay view rolls every distress event up to the parcel level and
# computes cross-signal flags (triple-distress, etc). It is keyed by
# (county, effective_parcel_id) — where sheriff rows use the real gis_pid
# pulled out of raw_data, not their synthetic case-number parcel_id.
# We mirror that same effective-id logic here so a property row can find
# its own overlay entry.


def _effective_parcel_id(source: str, raw: dict, row: dict) -> Optional[str]:
    """Compute the SAME parcel key the overlay view groups on.

    Sheriff rows store a synthetic parcel_id (case number); their real
    parcel id lives in raw_data.detail.gis_pid (present only on enriched
    rows). Every other source's stored parcel_id is already the real one.
    Returns None when no real parcel id is resolvable (honest em-dash).
    """
    if source in _FORECLOSURE_SOURCES:
        detail = raw.get("detail") or {}
        return detail.get("gis_pid")
    return row.get("parcel_id")

def _owner_key(raw: dict) -> Optional[str]:
    """Compute the SAME owner key the owner-summary view groups on.

    The view keys on upper(trim(gis_owner)). Only rows with an enriched
    gis_owner are in the view, so we normalize that exact field — NOT the
    display 'owner' (which may fall back to the mortgagor and wouldn't match).
    Returns None when there's no gis_owner (no portfolio lookup possible).
    """
    detail = raw.get("detail") or {}
    gis_owner = detail.get("gis_owner")
    if not gis_owner or not str(gis_owner).strip():
        return None
    return str(gis_owner).strip().upper()


def _fetch_all_rows(table_name: str, columns: str) -> list[dict[str, Any]]:
    """Fetch EVERY row of a table/view, paging past PostgREST's per-response
    row cap (default 1000). A single .range(0, 9999) does NOT override that
    cap — PostgREST still returns only its configured maximum — so any view
    larger than ~1000 rows was being silently truncated. We page in chunks
    until a short page signals the end. Returns [] on failure.
    """
    _PAGE = 1000
    _MAX_PAGES = 1000  # safety stop (= up to 1M rows)
    all_rows: list[dict[str, Any]] = []
    page_idx = 0
    while page_idx < _MAX_PAGES:
        start = page_idx * _PAGE
        end = start + _PAGE - 1
        try:
            result = (
                signals_table(table_name)
                .select(columns)
                .range(start, end)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "paged fetch failed",
                table=table_name,
                page=page_idx,
                error_type=type(e).__name__,
            )
            break
        page_rows = result.data or []
        all_rows.extend(page_rows)
        if len(page_rows) < _PAGE:
            break  # last page reached
        page_idx += 1
    else:
        logger.warning(
            "paged fetch hit max pages — result may be incomplete",
            table=table_name,
            fetched=len(all_rows),
        )
    return all_rows


def _load_overlay_map() -> dict[tuple[str, str], dict[str, Any]]:
    """Fetch the whole overlay view and index it by (county, parcel_id).

    Pages through the ENTIRE view (via _fetch_all_rows) — the previous single
    .range(0, 9999) was silently capped at PostgREST's 1000-row maximum, so
    every parcel past row 1000 lost its overlay and never matched (that's why
    bare multi_signal searches under-counted). County is lowercased on both
    sides to avoid a case mismatch (the view emits 'hennepin'; _SOURCE_TO_COUNTY
    emits 'Hennepin'). Returns an empty map on failure so property listing
    still works without the badge rather than 500-ing.
    """
    rows = _fetch_all_rows(
        "parcel_distress_overlay",
        "county, parcel_id, distinct_signal_count, is_triple_distress, "
        "signal_families, max_severity, has_foreclosure, "
        "has_vacant_condemned, has_tax_delinquent, has_tax_forfeit, "
        "has_special_assessment",
    )

    overlay_map: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        county = (r.get("county") or "").lower()
        pid = r.get("parcel_id")
        if not pid:
            continue
        overlay_map[(county, pid)] = {
            "distinct_signal_count": r.get("distinct_signal_count"),
            "is_triple_distress": r.get("is_triple_distress"),
            "signal_families": r.get("signal_families"),
            "max_severity": r.get("max_severity"),
            "has_foreclosure": r.get("has_foreclosure"),
            "has_vacant_condemned": r.get("has_vacant_condemned"),
            "has_tax_delinquent": r.get("has_tax_delinquent"),
            "has_tax_forfeit": r.get("has_tax_forfeit"),
            "has_special_assessment": r.get("has_special_assessment"),
        }
    return overlay_map


def _load_owner_map() -> dict[str, dict[str, Any]]:
    """Fetch signals.owner_distress_summary once and index it by owner_norm.

    Mirrors _load_overlay_map: one fetch per request, indexed for O(1) lookup.
    The key is the normalized owner name — upper(trim(gis_owner)) — the SAME
    expression the view groups on, so a property row finds its owner's
    portfolio by normalizing its own gis_owner identically.

    Returns an empty map on failure so property listing still works (just
    without the owner-portfolio badge) rather than 500-ing.
    """
    rows = _fetch_all_rows(
        "owner_distress_summary",
        "owner_norm, owner_type, parcel_count, event_count, "
        "max_severity, any_absentee, owner_mailing",
    )
    

    owner_map: dict[str, dict[str, Any]] = {}
    for r in rows:
        key = r.get("owner_norm")
        if not key:
            continue
        owner_map[key] = {
            "owner_type": r.get("owner_type"),
            "parcel_count": r.get("parcel_count"),
            "event_count": r.get("event_count"),
            "max_severity": r.get("max_severity"),
            "any_absentee": r.get("any_absentee"),
            "owner_mailing": r.get("owner_mailing"),
        }
    return owner_map

def _load_parcel_enrichment(county_code: str, parcel_id: str) -> Optional[dict[str, Any]]:
    """Fetch the enriched property characteristics for ONE parcel from
    core.parcels, keyed by (county_code, parcel_id). Returns only the fields
    that are actually populated (null fields are omitted, so the frontend shows
    nothing for them rather than 'Unknown'/blank). The literal string 'Unknown'
    (a MnGeo non-value) is treated as null. Returns None if the parcel isn't
    found or has no enrichment.

    The caller passes the EFFECTIVE parcel id (the real gis_pid for sheriff
    rows, recovered via _effective_parcel_id) and the lowercase county slug.
    """
    if not county_code or not parcel_id:
        return None
    try:
        result = (
            core_table("parcels")
            .select(
                "year_built, sqft, lot_sqft, last_sale_price, last_sale_date, "
                "emv_land, emv_building, emv_total, annual_tax, "
                "special_assessments, num_units, use_class, school_district, "
                "homestead_status, garage, garage_sqft, basement, heating, "
                "cooling, legal_description, property_type, "
                "estimated_market_value"
            )
            .eq("county_code", county_code)
            .eq("parcel_id", parcel_id)
            .limit(1)
            .execute()
        )
        rows = result.data or []
    except Exception as e:
        logger.warning(
            "parcel enrichment fetch failed",
            county=county_code, parcel_id=parcel_id,
            error_type=type(e).__name__,
        )
        return None

    if not rows:
        return None

    raw = rows[0]
    # Keep only populated fields; treat 'Unknown' (MnGeo non-value) as absent.
    enrichment: dict[str, Any] = {}
    for k, v in raw.items():
        if v is None:
            continue
        if isinstance(v, str) and v.strip().lower() in ("", "unknown"):
            continue
        enrichment[k] = v

    return enrichment or None


# ============================================================
# REDEMPTION OUTCOME TRACKER (outcomes.redemption_tracker)
# ============================================================
# The outcome-capture system (see BUILDLOG_outcome-capture.md) tracks every
# sheriff sale's statutory redemption clock and — after expiry — its actual
# ENDING: redeemed by the owner, taken by the lender (REO), or resold, with
# the confirmed resale price from county/eCRV records. When a tracker row
# exists it is the authoritative source for the redemption fields, replacing
# the published-date / sale+182d guesswork below.
#
# JOIN KEY: (county_lower, effective_parcel_id, anchor_date). This is how the
# tracker was seeded (eff_parcel_id + event/sale date) and it works on EVERY
# endpoint path — the list endpoint selects from distress_with_parcel WITHOUT
# the distress_events id, so a PK join would silently fail there. A secondary
# (county, parcel)-keyed map catches rows whose event_date drifted from the
# tracker's anchor (e.g. postponements), preferring the latest anchor.
#
# NOTE: requires the `outcomes` schema in Supabase's exposed schemas
# (Settings -> API). If not exposed, the fetch fails and we degrade
# gracefully to the estimate path (empty map).

_OUTCOME_LABELS = {
    "redeemed_by_owner": "Redeemed by owner",
    "redeemed_by_junior": "Redeemed by junior lienholder",
    "foreclosed": "Bank-owned (REO)",
    "foreclosed_sold": "Sold after foreclosure",
    "deed_in_lieu": "Deed in lieu of foreclosure",
    "sale_cancelled": "Sale cancelled",
    "unknown": "Outcome pending confirmation",
}

# Outcomes that mean the case is finished (countdown no longer meaningful).
_RESOLVED_OUTCOMES = {
    "redeemed_by_owner", "redeemed_by_junior", "foreclosed",
    "foreclosed_sold", "deed_in_lieu", "sale_cancelled",
}

import re as _re

# detection_notes formats written by the outcome checker / eCRV matcher:
#   'eCRV: WARRNTY 2026-03-10 amt 310000.0 seller ...'
#   'County last-sale 2026-03-17 (value 299900, ...) is after ...'
_NOTES_PRICE_RE = _re.compile(r"(?:amt|\(value)\s+([0-9][0-9_.,]*)")
_NOTES_DATE_RE = _re.compile(r"(?:eCRV:\s+\S+|last-sale)\s+(\d{4}-\d{2}-\d{2})")


def _parse_resale_from_notes(notes: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    """Best-effort extraction of (resale_price, resale_date) from the
    tracker's detection_notes. Returns (None, None) when absent/unparseable —
    honest gaps, never guesses."""
    if not notes:
        return None, None
    price = None
    m = _NOTES_PRICE_RE.search(notes)
    if m:
        try:
            price = float(m.group(1).replace(",", "").replace("_", ""))
        except ValueError:
            price = None
    date_s = None
    m = _NOTES_DATE_RE.search(notes)
    if m:
        date_s = m.group(1)
    return price, date_s


def _fetch_all_rows_in_schema(table_fn, table_name: str, columns: str) -> list[dict[str, Any]]:
    """Schema-generic twin of _fetch_all_rows (which is signals-bound):
    page through an entire table using the given schema accessor."""
    _PAGE = 1000
    _MAX_PAGES = 100
    all_rows: list[dict[str, Any]] = []
    page_idx = 0
    while page_idx < _MAX_PAGES:
        start = page_idx * _PAGE
        end = start + _PAGE - 1
        try:
            result = (
                table_fn(table_name)
                .select(columns)
                .range(start, end)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "paged schema fetch failed",
                table=table_name,
                page=page_idx,
                error_type=type(e).__name__,
            )
            break
        page_rows = result.data or []
        all_rows.extend(page_rows)
        if len(page_rows) < _PAGE:
            break
        page_idx += 1
    return all_rows


def _load_redemption_tracker_map() -> dict[str, dict[Any, dict[str, Any]]]:
    """Fetch outcomes.redemption_tracker and index it two ways:

      'exact'  : (county_lower, parcel_id, anchor_date_iso) -> tracker row
      'parcel' : (county_lower, parcel_id) -> tracker row with LATEST anchor

    Returns {'exact': {}, 'parcel': {}} on any failure (graceful degrade to
    the estimate path — same philosophy as overlay/owner maps)."""
    empty: dict[str, dict[Any, dict[str, Any]]] = {"exact": {}, "parcel": {}}
    try:
        rows = _fetch_all_rows_in_schema(
            outcomes_table,
            "redemption_tracker",
            "county_code, parcel_id, anchor_date, redemption_expiry_date, "
            "period_source, outcome, ambiguous, detection_source, detection_notes",
        )
    except Exception as e:
        logger.warning(
            "redemption tracker load failed (degrading to estimates)",
            error_type=type(e).__name__,
        )
        return empty
    if not rows:
        return empty

    exact: dict[Any, dict[str, Any]] = {}
    by_parcel: dict[Any, dict[str, Any]] = {}
    for r in rows:
        county = (r.get("county_code") or "").lower()
        pid = r.get("parcel_id")
        anchor = r.get("anchor_date")
        if not county or not pid or not anchor:
            continue
        exact[(county, pid, str(anchor))] = r
        pk = (county, pid)
        prev = by_parcel.get(pk)
        if prev is None or str(anchor) > str(prev.get("anchor_date") or ""):
            by_parcel[pk] = r
    return {"exact": exact, "parcel": by_parcel}


# ------------------------------------------------------------------
# Tyler-portal tax-delinquency status (signals.tax_delinquency_status,
# written weekly by olmsted_tax_detail v2.2 — 2026-07-12).
# One row per delinquent-list parcel: the current verdict (254/502 REDEEMED
# since the annual list published vs 248 true delinquents), the forfeiture
# clock (ALWAYS a computed estimate — ships with forfeiture_basis), and the
# county owner-of-record mailing (premium skip-trace value; gated in
# redaction, never here). Attached to olmsted_delq_list rows as a nested
# `tax_status` block, same convention as overlay / owner_portfolio.
# ------------------------------------------------------------------

# The exact keys the API exposes from a status row — raw_data stays behind
# (heavy, and the detail `raw` field is premium-gated separately anyway).
_TAX_STATUS_KEYS = (
    "redeemed_since_list",
    "first_delinquent_year", "years_delinquent",
    "total_delinquent_due", "current_year_due",
    "estimated_judgment_date", "estimated_forfeiture_date", "forfeiture_basis",
    "in_forfeiture", "coj", "in_bankruptcy", "homestead",
    "owner_name", "owner_name_2",
    "owner_mailing_address", "owner_mailing_city_state_zip",
)


def _load_delq_status_map() -> dict[tuple[str, str], dict[str, Any]]:
    """Fetch signals.tax_delinquency_status and index by
    (county_slug_lower, parcel_id) — the table's natural PK.

    Small table (502 rows for the Olmsted pilot), one paged fetch per
    request, mirroring the overlay/owner/tracker map convention. Returns {}
    on any failure — graceful degrade: rows simply carry tax_status=None,
    the frontend shows no badge/clock (honest gap, never a guess)."""
    try:
        rows = _fetch_all_rows(
            "tax_delinquency_status",
            "parcel_id, county_slug, " + ", ".join(_TAX_STATUS_KEYS),
        )
    except Exception as e:
        logger.warning(
            "tax delinquency status load failed (rows degrade to no block)",
            error_type=type(e).__name__,
        )
        return {}
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        pid = r.get("parcel_id")
        county = (r.get("county_slug") or "").lower()
        if pid and county:
            out[(county, pid)] = r
    return out


# ============================================================
# DEAL MATH (scoring.comp_ratios + scoring.distress_multipliers)
# ============================================================
# The negotiation-math engine: for a foreclosure whose redemption window is
# still OPEN, compute the payoff floor, the locally calibrated market value,
# and the empirically observed in-window sale band — every number carrying
# its sample size and scope, never a naked point prediction.
#
# Calibration sources (materialized views, refreshed Mondays by pg_cron):
#   scoring.comp_ratios          — median sale/assessed ratio per city (12mo
#                                  WARRNTY comps, outliers trimmed), with
#                                  county and metro fallbacks
#   scoring.distress_multipliers — what confirmed in-window sales
#                                  (redeemed_by_owner) and REO resales
#                                  (foreclosed_sold) actually closed at as a
#                                  fraction of assessed value (p25/median/p75)
#
# The multipliers were measured against RAW assessed value, so the band is
# raw_emv x quartiles (same basis); the local ratio calibrates the market-
# value CONTEXT number. Loaded lazily with a 10-minute in-process cache —
# graceful degrade: no calibration -> no deal_math (None), never a guess.

import time as _time_mod

_DEAL_CALIBRATION_CACHE: dict[str, Any] = {"at": 0.0, "data": None}
_DEAL_CALIBRATION_TTL_S = 600


# ============================================================
# ASSESSOR OWNERS (core.owners — backfilled 2026-07-08, 163,880 Ramsey
# owners from the county assessor roll; kept fresh by the weekly
# ramsey_parcels run)
# ============================================================
# Some sources publish no owner (Saint Paul DSI most notably: all 384
# vacant buildings showed em-dash). When a shaped row lacks an owner but
# its parcel exists in core.owners, patch in the ASSESSOR owner-of-record
# + mailing address. Applied BEFORE redaction, so the existing tier rules
# (owner locked below basic+, mailing below standard+) govern it
# automatically. Honest sourcing: this is the county assessor's
# owner-of-record, same provenance as every other owner on the platform.

def _apply_assessor_owners(shaped_rows: list[dict[str, Any]]) -> None:
    """Fill owner/owner_mailing (from core.owners) AND parcel foundation
    fields (from core.parcels) on shaped rows whose SOURCE published
    none — batched fetches, strictly additive, never overwrites a
    source-published value. No-ops on failure (honest em-dash stays).

    2026-07-08: values half added — the vacant tab (SP DSI publishes no
    values) gains est. market value the same way it gained owners.
    2026-07-09: forfeit-land surfacing — the same single parcels query
    now also fills lat/lng, lot_sqft, and the assessor's property-type
    name (PR_TYP_NM1), so unaddressed forfeit land can render as
    "a 2.4-acre vacant-land dot on the map" instead of "ADDRESS
    UNASSIGNED / $0". Generic: vacant + foreclosure rows inherit map
    data for free. Tier safety: lat/lng were already _LOCATOR_FIELDS;
    lot_sqft/property_type_name added to redaction (STANDARD+)."""
    # ---- Parcel foundation: values, coords, lot size, type name ----
    def _missing_mv(s: dict[str, Any]) -> bool:
        mv = s.get("market_value")
        return not (isinstance(mv, (int, float)) and mv > 0)

    def _missing_coords(s: dict[str, Any]) -> bool:
        return s.get("lat") is None or s.get("lng") is None

    val_need: dict[str, list[dict[str, Any]]] = {}
    for s in shaped_rows:
        if not (_missing_mv(s) or _missing_coords(s)
                or s.get("lot_sqft") is None
                or s.get("property_type_name") is None
                or s.get("address") is None):
            continue
        pid = s.get("parcel_id")
        if not pid:
            continue
        val_need.setdefault(pid, []).append(s)
    if val_need:
        try:
            vres = (
                core_table("parcels")
                .select(
                    "parcel_id, estimated_market_value, lat, lng, "
                    "lot_sqft, prop_type_name:raw_data->>PR_TYP_NM1, "
                    "address, city, zip"
                )
                .in_("parcel_id", list(val_need.keys()))
                .execute()
            )
            for p in (vres.data or []):
                emv = p.get("estimated_market_value")
                plat, plng = p.get("lat"), p.get("lng")
                plot = p.get("lot_sqft")
                ptype = (p.get("prop_type_name") or "").strip() or None
                paddr = (p.get("address") or "").strip() or None
                pcity = (p.get("city") or "").strip() or None
                pzip = (p.get("zip") or "").strip() or None
                for s in val_need.get(p.get("parcel_id"), []):
                    if s.get("address") is None and paddr:
                        # Source published no address (e.g. the delinquent
                        # tax list carries legal descriptions only) — the
                        # spine supplies the situs address. Additive only.
                        s["address"] = paddr
                        if s.get("city") is None and pcity:
                            s["city"] = pcity
                        if s.get("zip") is None and pzip:
                            s["zip"] = pzip
                    if _missing_mv(s) and isinstance(emv, (int, float)) and emv > 0:
                        s["market_value"] = float(emv)
                    if _missing_coords(s) and plat is not None and plng is not None:
                        s["lat"] = plat
                        s["lng"] = plng
                    if s.get("lot_sqft") is None and isinstance(plot, (int, float)) and plot > 0:
                        s["lot_sqft"] = plot
                    if s.get("property_type_name") is None and ptype:
                        s["property_type_name"] = ptype
        except Exception as e:
            logger.warning(
                "assessor parcel patch failed (rows keep em-dash)",
                error_type=type(e).__name__,
            )

    # ---- Owners ----
    # Two needs, one query:
    #  - need_name : rows whose source published NO owner — get the assessor
    #    name + mailing (fill-only, the original behavior).
    #  - need_attrs: rows lacking owner_type/is_absentee — get the CURRENT-
    #    owner attributes regardless of whose name the source printed
    #    (2026-07-09, owner filters: "is the current owner an LLC /
    #    absentee" is well-defined even when the displayed owner is a
    #    notice mortgagor). Additive: never overwrites a value already set
    #    by the source (tax rolls compute their own is_absentee).
    need_name: dict[str, list[dict[str, Any]]] = {}
    need_attrs: dict[str, list[dict[str, Any]]] = {}
    for s in shaped_rows:
        pid = s.get("parcel_id")
        if not pid:
            continue
        if not s.get("owner"):
            need_name.setdefault(pid, []).append(s)
        if s.get("owner_type") is None or s.get("is_absentee") is None:
            need_attrs.setdefault(pid, []).append(s)
    all_pids = set(need_name) | set(need_attrs)
    if not all_pids:
        return
    try:
        result = (
            core_table("owners")
            .select(
                "parcel_id, owner_name, owner_type, mailing_address, "
                "mailing_city, mailing_state, mailing_zip, is_absentee"
            )
            .in_("parcel_id", list(all_pids))
            .eq("is_current", True)
            .execute()
        )
        rows = result.data or []
    except Exception as e:
        logger.warning(
            "assessor owner patch failed (rows keep em-dash)",
            error_type=type(e).__name__,
        )
        return
    for o in rows:
        pid = o.get("parcel_id")
        name = o.get("owner_name")
        if not pid:
            continue
        mailing_bits = [
            o.get("mailing_address"),
            " ".join(
                b for b in [
                    o.get("mailing_city"),
                    o.get("mailing_state"),
                    o.get("mailing_zip"),
                ] if b
            ) or None,
        ]
        mailing = ", ".join(b for b in mailing_bits if b) or None
        if name:
            for s in need_name.get(pid, []):
                s["owner"] = name
                if not s.get("owner_mailing"):
                    s["owner_mailing"] = mailing
        for s in need_attrs.get(pid, []):
            if s.get("owner_type") is None:
                s["owner_type"] = o.get("owner_type")
            if s.get("is_absentee") is None:
                s["is_absentee"] = o.get("is_absentee")


def _load_deal_calibration() -> Optional[dict[str, Any]]:
    now = _time_mod.monotonic()
    if (
        _DEAL_CALIBRATION_CACHE["data"] is not None
        and now - _DEAL_CALIBRATION_CACHE["at"] < _DEAL_CALIBRATION_TTL_S
    ):
        return _DEAL_CALIBRATION_CACHE["data"]
    try:
        ratio_rows = _fetch_all_rows_in_schema(
            scoring_table, "comp_ratios", "scope, county_code, city_norm, n, ratio"
        )
        mult_rows = _fetch_all_rows_in_schema(
            scoring_table, "distress_multipliers", "outcome, n, p25, median, p75"
        )
    except Exception as e:
        logger.warning(
            "deal calibration load failed (deal_math disabled this request)",
            error_type=type(e).__name__,
        )
        return _DEAL_CALIBRATION_CACHE["data"]  # stale-if-error
    if not ratio_rows or not mult_rows:
        return None
    city = {}
    county = {}
    metro = None
    for r in ratio_rows:
        if r["scope"] == "city":
            city[(r["county_code"], r["city_norm"])] = r
        elif r["scope"] == "county":
            county[r["county_code"]] = r
        elif r["scope"] == "metro":
            metro = r
    mult = {m["outcome"]: m for m in mult_rows}
    if "redeemed_by_owner" not in mult:
        return None
    data = {"city": city, "county": county, "metro": metro, "mult": mult}
    _DEAL_CALIBRATION_CACHE["data"] = data
    _DEAL_CALIBRATION_CACHE["at"] = now
    return data


# ============================================================
# VACANCY ESCALATION CLOCK (mpls_vbr / saint_paul_vacant)
# ============================================================
# Pure read-time date math on the TRUE registry/condemnation dates (real
# since the 2026-07-07 repair — never trust stored countdowns, they go
# stale; same lesson as the redemption clock).
#
# Fee model — Minneapolis ONLY, and explicitly estimated:
#   * VBR annual fee $7,228.70 (2024+ schedule; applied at the current
#     rate — historical rates differed, hence "estimated").
#   * Prolonged Vacancy Enforcement: monthly citations up to $2,000 once
#     a property passes the 2-year VBR cap. PVE began Dec 2024, so
#     exposure accrues from max(2-year anniversary, 2024-12-01).
# Saint Paul publishes no fee schedule in our sources -> time + Category
# only, fees honestly null. Never invent a fee.

from datetime import date as _date  # (re-imported later too; harmless)

_VACANCY_SOURCES = {"mpls_vbr", "saint_paul_vacant"}
_MPLS_VBR_ANNUAL_FEE = 7228.70
_MPLS_PVE_MONTHLY = 2000.0
_MPLS_PVE_PROGRAM_START = _date(2024, 12, 1)
# Saint Paul DSI vacant-building fees — CATEGORY-BASED, confirmed from the
# city's official program page/packet + the May 2025 council rate increase:
#   Category I:  $2,705/yr
#   Category II: $2,705 first year, then $5,410/yr
#   Category III:$5,410/yr
# Historical rates were lower ($2,127 -> $2,459 -> current), so estimates
# are labeled "at current rates". Official packet: outstanding fees must
# be paid before the property can be sold.
_SP_CAT1_ANNUAL = 2705.0
_SP_CAT23_ANNUAL = 5410.0


def _vacancy_fields(source: str, raw: dict, row: dict) -> dict[str, Any]:
    """Compute the vacancy escalation clock for a row.

    Returns all-null fields for non-vacancy sources or undated rows.
    Fields:
      vacancy_years        float, years since registration/condemnation (ALL tiers
                           — derivable from the public date)
      vacancy_pve_active   bool, Minneapolis 2-year cap passed (ALL tiers)
      vacancy_est_fees_paid      PREMIUM — estimated cumulative VBR fees (MPLS only)
      vacancy_est_pve_exposure   PREMIUM — estimated PVE citations to date (MPLS only)
      vacancy_cost_basis         PREMIUM — one honest sentence on the estimate
    """
    null_result = {
        "vacancy_years": None,
        "vacancy_pve_active": None,
        "vacancy_est_fees_paid": None,
        "vacancy_est_pve_exposure": None,
        "vacancy_cost_basis": None,
    }
    if source not in _VACANCY_SOURCES:
        return null_result

    anchor_date = _coerce_date(row.get("event_date"))
    if anchor_date is None:
        return null_result
    today = _date.today()
    days = (today - anchor_date).days
    if days < 0:
        return null_result
    years = round(days / 365.25, 1)

    if source == "saint_paul_vacant":
        # Category from the registry payload ("1"/"2"/"3"). No category ->
        # no fee math (honest null); years still shown.
        cat = str(
            ((raw.get("attributes") or {}).get("VB_CATEGORY")) or ""
        ).strip()
        sp_fees: Optional[int] = None
        sp_basis: Optional[str] = None
        if cat == "1":
            sp_fees = round(years * _SP_CAT1_ANNUAL)
            rate_desc = "$2,705/yr (Category I)"
        elif cat == "2":
            sp_fees = round(
                _SP_CAT1_ANNUAL + max(0.0, years - 1.0) * _SP_CAT23_ANNUAL
            )
            rate_desc = "$2,705 first year + $5,410/yr after (Category II)"
        elif cat == "3":
            sp_fees = round(years * _SP_CAT23_ANNUAL)
            rate_desc = "$5,410/yr (Category III)"
        else:
            rate_desc = ""
        if sp_fees is not None:
            sp_basis = (
                "Estimated at Saint Paul's current registration rates - "
                "%s - over %.1f years. Historical rates were lower; actual "
                "billed amounts may differ. Outstanding vacant-building "
                "fees must be paid before the property can be sold. Not a "
                "statement of account." % (rate_desc, years)
            )
        return {
            "vacancy_years": years,
            "vacancy_pve_active": None,   # PVE is a Minneapolis program
            "vacancy_est_fees_paid": sp_fees,
            "vacancy_est_pve_exposure": None,
            "vacancy_cost_basis": sp_basis,
        }

    # Minneapolis
    pve_active = years >= 2.0
    est_fees = round(years * _MPLS_VBR_ANNUAL_FEE)
    pve_exposure = None
    if pve_active:
        pve_start = max(
            _date(anchor_date.year + 2, anchor_date.month, min(anchor_date.day, 28)),
            _MPLS_PVE_PROGRAM_START,
        )
        pve_months = max(
            0,
            (today.year - pve_start.year) * 12 + (today.month - pve_start.month),
        )
        pve_exposure = round(pve_months * _MPLS_PVE_MONTHLY) if pve_months > 0 else 0
    return {
        "vacancy_years": years,
        "vacancy_pve_active": pve_active,
        "vacancy_est_fees_paid": est_fees,
        "vacancy_est_pve_exposure": pve_exposure,
        "vacancy_cost_basis": (
            "Estimated at the current $7,228.70/yr VBR fee over %.1f years%s. "
            "Actual billed amounts may differ - historical rates varied. "
            "Not a statement of account." % (
                years,
                (
                    "; PVE citations estimated at $2,000/month since the "
                    "2-year cap (program began Dec 2024)"
                ) if pve_active else "",
            )
        ),
    }


def _compute_deal_math(shaped: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Deal math for ONE shaped row, or None when it doesn't apply.

    Applies only to foreclosure rows whose redemption window is OPEN
    (in_redemption / expiring_soon) — a resolved or closed window has no
    negotiation left; the outcome fields tell that story instead. Requires
    both the debt amount and an assessed market value; missing either means
    no math, never a fabricated side."""
    if shaped.get("redemption_state") not in ("in_redemption", "expiring_soon"):
        return None
    amount = shaped.get("amount")
    raw_emv = shaped.get("market_value")
    if not isinstance(amount, (int, float)) or amount <= 0:
        return None
    if not isinstance(raw_emv, (int, float)) or raw_emv <= 0:
        return None

    calib = _load_deal_calibration()
    if calib is None:
        return None

    county_lower = (shaped.get("county") or "").lower()
    city_norm = (shaped.get("city") or "").lower()
    ratio_row = calib["city"].get((county_lower, city_norm))         or calib["county"].get(county_lower)         or calib["metro"]
    if ratio_row is None:
        return None
    scope = "city" if (county_lower, city_norm) in calib["city"] else (
        "county" if county_lower in calib["county"] else "metro"
    )

    inwin = calib["mult"]["redeemed_by_owner"]
    reo = calib["mult"].get("foreclosed_sold")

    est_market = round(raw_emv * float(ratio_row["ratio"]))
    band_low = round(raw_emv * float(inwin["p25"]))
    band_high = round(raw_emv * float(inwin["p75"]))
    seller_net = round(raw_emv * float(inwin["median"]) - amount)
    equity_spread = round(est_market - amount)

    return {
        "payoff_floor": round(amount),
        "payoff_is_partial": True,  # foreclosing debt only; other liens may exist
        "est_market_value": est_market,
        "local_ratio": float(ratio_row["ratio"]),
        "ratio_scope": scope,
        "ratio_n": int(ratio_row["n"]),
        "inwindow_band_low": band_low,
        "inwindow_band_high": band_high,
        "inwindow_n": int(inwin["n"]),
        "seller_net_estimate": seller_net,
        "equity_spread": equity_spread,
        "reo_benchmark": round(raw_emv * float(reo["median"])) if reo else None,
        "reo_n": int(reo["n"]) if reo else None,
        "basis": (
            "Assessed value calibrated by the median of %d recent %s-level "
            "sales; band from %d confirmed in-window foreclosure sales "
            "(25th-75th pct). Floor is the foreclosing debt only - other "
            "liens may apply. Not an appraisal."
            % (int(ratio_row["n"]), scope, int(inwin["n"]))
        ),
    }


# ============================================================
# REDEMPTION-WINDOW COMPUTATION
# ============================================================
# Minnesota sheriff sales carry a redemption period (typically 6 months)
# during which the prior owner can reclaim the property. That window is
# the actionable signal for investors, so we compute it for every
# foreclosure row:
#   * Hennepin publishes redemptionExpirationDate per record — we read it
#     directly (handles 5-week / 2-month / 12-month edge cases exactly).
#   * Anoka / Dakota don't publish it, so we estimate sale_date + 6 months
#     (the 95%-accurate statutory default). We tag these `is_estimated`.
# State buckets (relative to today):
#   in_redemption  — expires in > 90 days
#   expiring_soon  — expires within 90 days (urgent)
#   expired        — expiration already passed
# Non-foreclosure rows get all-null redemption fields.

from datetime import date as _date, datetime as _datetime, timedelta as _timedelta

_REDEMPTION_DEFAULT_DAYS = 182  # ~6 months, MN statutory default
_REDEMPTION_EXPIRING_SOON_DAYS = 90

# Sources that are sheriff foreclosure sales (carry a redemption window).
_FORECLOSURE_SOURCES = {
    "anoka_sheriff",
    "dakota_sheriff",
    "hennepin_sheriff",
    "ramsey_sheriff",
    "washington_sheriff",
    "scott_sheriff",
    "carver_sheriff",
    "startribune_legal",
}

# Sources that carry a redemption window in _redemption_fields. This is
# _FORECLOSURE_SOURCES plus postbulletin_legal — postbulletin is deliberately
# NOT added to _FORECLOSURE_SOURCES itself, because that set also drives
# _effective_parcel_id's gis_pid extraction (sheriff rows store a synthetic
# case-number parcel_id; their real pid lives in raw_data.detail.gis_pid).
# Postbulletin raw_data is FLAT with no detail key, and its stored parcel_id
# is ALREADY the real Olmsted PIN — putting it in _FORECLOSURE_SOURCES would
# null its parcel resolution and silently break overlay/enrichment joins.
_REDEMPTION_SOURCES = _FORECLOSURE_SOURCES | {"postbulletin_legal"}


def _coerce_date(value: Any) -> Optional[_date]:
    """Parse a date or ISO datetime string into a date. Tolerant of the
    several shapes our sources store (ISO datetime, YYYY-MM-DD, MM/DD/YYYY)."""
    if value is None:
        return None
    if isinstance(value, _date) and not isinstance(value, _datetime):
        return value
    if isinstance(value, _datetime):
        return value.date()
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1]
    try:
        return _datetime.fromisoformat(s).date()
    except ValueError:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                return _datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    return None


def _redemption_fields(
    source: str,
    raw: dict,
    row: dict,
    tracker_map: Optional[dict[str, dict[Any, dict[str, Any]]]] = None,
) -> dict[str, Any]:
    """Compute the redemption lifecycle fields for a row.

    Returns all-null fields for non-foreclosure sources.

    THREE sources, in priority order:
      0. The outcome tracker (outcomes.redemption_tracker) — authoritative.
         Carries the statutorily computed expiry AND, once the window closes,
         the confirmed OUTCOME (redeemed / REO / sold + resale price from
         county & eCRV records). This is what replaces the old dead-end
         'expired' state with an answer.
      1. A county-published redemption date (Hennepin / Ramsey expose
         redemptionExpirationDate). Authoritative for the date, silent on
         the outcome.
      2. Otherwise, estimate sale_date + ~6 months — but ONLY for sales that
         have actually COMPLETED (see COMPLETED-SALE GUARD below).

    redemption_state values (safe for ALL tiers — never names the outcome):
      in_redemption   — window open, > 90 days left
      expiring_soon   — window open, <= 90 days left
      outcome_pending — window closed, resolution not yet confirmed
      resolved        — resolution confirmed (specifics are Premium fields)
      expired         — legacy fallback ONLY (no tracker row exists)

    Premium-gated outcome fields (locked in redaction._LEVERAGE_FIELDS):
      redemption_outcome, redemption_outcome_label,
      redemption_outcome_ambiguous, redemption_resale_price,
      redemption_resale_date

    COMPLETED-SALE GUARD (the estimate path only):
      * Anoka publishes its PENDING sales list. Each row carries a status of
        'Sold' (completed), 'Postponed' (rescheduled — did not happen), or
        null (pending). Only 'Sold' has a redemption window; the rest get
        all-null (em-dash), because estimating sale+182d for a sale that never
        occurred invents data.
      * Dakota's feed is completed sales by definition and carries no per-row
        status, so it passes the guard and estimates as before.
      * Hennepin/Ramsey never reach the estimate (they have a published date).
      * startribune_legal notices are SCHEDULED (future) sales — the sale has
        not happened, so no redemption window yet (all-null).
    """
    null_result = {
        "redemption_ends_at": None,
        "redemption_days_left": None,
        "redemption_state": None,
        "redemption_is_estimated": None,
        "redemption_outcome": None,
        "redemption_outcome_label": None,
        "redemption_outcome_ambiguous": None,
        "redemption_resale_price": None,
        "redemption_resale_date": None,
    }
    if source not in _REDEMPTION_SOURCES:
        return null_result

    # ---- 0. Outcome tracker (authoritative when present) ----
    tracker = None
    if tracker_map:
        county_lower = (_resolve_county(source, raw) or "").lower()
        eff_pid = _effective_parcel_id(source, raw, row)
        event_date = _coerce_date(row.get("event_date"))
        if county_lower and eff_pid:
            if event_date is not None:
                tracker = tracker_map.get("exact", {}).get(
                    (county_lower, eff_pid, event_date.isoformat())
                )
            if tracker is None:
                tracker = tracker_map.get("parcel", {}).get(
                    (county_lower, eff_pid)
                )

    if tracker is not None:
        ends_at = _coerce_date(tracker.get("redemption_expiry_date"))
        is_estimated = (tracker.get("period_source") or "") == "default_6mo"
        outcome = tracker.get("outcome") or "pending"
        days_left = (ends_at - _date.today()).days if ends_at else None

        if outcome in _RESOLVED_OUTCOMES:
            state = "resolved"
            price, resale_date = _parse_resale_from_notes(
                tracker.get("detection_notes")
            )
            return {
                "redemption_ends_at": ends_at.isoformat() if ends_at else None,
                "redemption_days_left": None,  # countdown is over
                "redemption_state": state,
                "redemption_is_estimated": is_estimated,
                "redemption_outcome": outcome,
                "redemption_outcome_label": _OUTCOME_LABELS.get(outcome),
                "redemption_outcome_ambiguous": bool(tracker.get("ambiguous")),
                "redemption_resale_price": price,
                "redemption_resale_date": resale_date,
            }

        # pending / unknown: the window is either still open or closed
        # without a confirmed resolution yet.
        if days_left is None:
            state = None
        elif days_left < 0:
            state = "outcome_pending"
            days_left = None  # a negative countdown is noise, not information
        elif days_left <= _REDEMPTION_EXPIRING_SOON_DAYS:
            state = "expiring_soon"
        else:
            state = "in_redemption"
        return {
            "redemption_ends_at": ends_at.isoformat() if ends_at else None,
            "redemption_days_left": days_left,
            "redemption_state": state,
            "redemption_is_estimated": is_estimated,
            "redemption_outcome": None,
            "redemption_outcome_label": None,
            "redemption_outcome_ambiguous": None,
            "redemption_resale_price": None,
            "redemption_resale_date": None,
        }

    # ---- 1./2. No tracker row: original published-date / estimate logic ----
    ends_at: Optional[_date] = None
    is_estimated = False

    # 1. County-published exact date (Hennepin / Ramsey). Authoritative.
    published = raw.get("redemptionExpirationDate")
    if published is None:
        # postbulletin_legal (2026-07-10/11): the scraper writes
        # redemption_expires = scheduled sale date + the redemption period,
        # with redemption_basis 'stated' (period read from the notice text —
        # treated as published/authoritative) or 'computed' (statutory
        # default applied — tagged estimated, same honesty rule as the
        # sale+182d path).
        published = raw.get("redemption_expires")
        if published is not None:
            is_estimated = (raw.get("redemption_basis") or "") == "computed"
    ends_at = _coerce_date(published)

    # 2. Estimate from sale date — only for COMPLETED sales.
    if ends_at is None:
        # Determine whether this sale has actually happened. A status field
        # (Anoka) that is anything other than 'Sold' means it has not — no
        # redemption window. Sources without a per-row status (Dakota) are
        # completed-sale feeds by definition and pass through.
        detail = raw.get("detail") or {}
        status_raw = detail.get("status")
        sale_completed = True
        if status_raw is not None:
            sale_completed = str(status_raw).strip().lower() == "sold"
        elif source == "anoka_sheriff":
            # Anoka pulls a PENDING list; a null status means the sale has
            # not been confirmed completed. No redemption window.
            sale_completed = False
        elif source in ("startribune_legal", "postbulletin_legal"):
            # Extracted notices are SCHEDULED future sales — not completed,
            # so no redemption window until the sale actually occurs.
            # (postbulletin only reaches here when the scraper wrote no
            # redemption_expires; estimating sale+182d for a sale that has
            # not happened would invent data.)
            sale_completed = False

        if sale_completed:
            sale_date = _coerce_date(row.get("event_date"))
            if sale_date is not None:
                ends_at = sale_date + _timedelta(days=_REDEMPTION_DEFAULT_DAYS)
                is_estimated = True

    if ends_at is None:
        return null_result

    days_left = (ends_at - _date.today()).days
    if days_left < 0:
        state = "expired"
    elif days_left <= _REDEMPTION_EXPIRING_SOON_DAYS:
        state = "expiring_soon"
    else:
        state = "in_redemption"

    return {
        "redemption_ends_at": ends_at.isoformat(),
        "redemption_days_left": days_left,
        "redemption_state": state,
        "redemption_is_estimated": is_estimated,
        "redemption_outcome": None,
        "redemption_outcome_label": None,
        "redemption_outcome_ambiguous": None,
        "redemption_resale_price": None,
        "redemption_resale_date": None,
    }

def _shape_property_row(
    row: dict[str, Any],
    overlay_map: Optional[dict[tuple[str, str], dict[str, Any]]] = None,
    owner_map: Optional[dict[str, dict[str, Any]]] = None,
    tracker_map: Optional[dict[str, dict[Any, dict[str, Any]]]] = None,
    delq_map: Optional[dict[tuple[str, str], dict[str, Any]]] = None,
) -> dict[str, Any]:
    
    """Dispatch to the right per-source extractor and merge common fields.

    If an overlay_map is supplied, attach this parcel's cross-signal flags
    under a nested 'overlay' key (None when the parcel has no resolvable id
    or no overlay entry — the frontend shows no badge in that case).
    If a tracker_map is supplied, redemption fields come from the outcome
    tracker where a row exists (authoritative), else fall back to the
    published-date / estimate logic.
    If a delq_map is supplied, olmsted_delq_list rows get a nested
    'tax_status' block (Tyler-portal verdict: redeemed vs true delinquent,
    forfeiture clock + basis, owner mailing) — tier-gated in redaction.
    """
    source = row.get("source") or ""
    raw = row.get("raw_data") or {}
    extractor = _EXTRACTORS.get(source, _extract_generic)
    extracted = extractor(raw, row)

    redemption = _redemption_fields(source, raw, row, tracker_map)

    shaped = {
        "source": source,
        "source_id": row.get("source_id"),
        "parcel_id": row.get("parcel_id"),
        "county": _resolve_county(source, raw),
        "event_type": row.get("event_type"),
        "title": row.get("title"),
        "description": row.get("description"),
        "observed_at": row.get("observed_at"),
        **extracted,
        **redemption,
    }

    # Compute the effective parcel key (county_lower, real_parcel_id) — the
    # SAME key the overlay groups on. Stashed under a private field so the
    # multi_signal path can de-duplicate to one row per parcel (a parcel can
    # have many event rows — e.g. 9 condemned-building notices). Stripped
    # before the response is returned.
    _eff_pid = _effective_parcel_id(source, raw, row)
    _county_lower = (_resolve_county(source, raw) or "").lower()
    shaped["_eff_key"] = (_county_lower, _eff_pid) if _eff_pid else None

    # Deal math: only meaningful while the redemption window is open, and
    # only when a tracker_map was supplied (list/detail paths) — computed
    # from the fields already on the shaped row. Premium-gated in redaction.
    shaped["deal_math"] = (
        _compute_deal_math(shaped) if tracker_map is not None else None
    )

    # Vacancy escalation clock: pure date math on the true registry/
    # condemnation date (fees Premium-gated in redaction).
    shaped.update(_vacancy_fields(source, raw, row))

    overlay = None
    if overlay_map is not None and _eff_pid:
        overlay = overlay_map.get((_county_lower, _eff_pid))
    shaped["overlay"] = overlay

    # Tyler tax-delinquency status: the per-parcel verdict from the weekly
    # portal scrape. Only delq-list rows carry it; keyed on the row's OWN
    # parcel_id (the list's PINs are real — the status table was built from
    # them, verified 502/502). None when absent — no badge, honest gap.
    tax_status = None
    if delq_map is not None and source == "olmsted_delq_list":
        _pid = row.get("parcel_id")
        if _pid and _county_lower:
            s_row = delq_map.get((_county_lower, _pid))
            if s_row:
                tax_status = {k: s_row.get(k) for k in _TAX_STATUS_KEYS}
    shaped["tax_status"] = tax_status

    # Owner portfolio: how many distressed properties this row's owner holds,
    # plus their classified type. Looked up by the normalized gis_owner key,
    # matching signals.owner_distress_summary. None when the row has no
    # gis_owner or no entry — frontend shows no owner badge in that case.
    owner_portfolio = None
    if owner_map is not None:
        okey = _owner_key(raw)
        if okey:
            owner_portfolio = owner_map.get(okey)
    shaped["owner_portfolio"] = owner_portfolio

    return shaped

# ============================================================
# Helpers
# ============================================================


def _count_for_filter(filter_dict: dict[str, str]) -> int:
    """Run one COUNT query against signals.distress_events using
    the given equality filters. All conditions are AND'd."""
    try:
        query = signals_table("distress_events").select(
            "id", count="exact"
        )
        for column, value in filter_dict.items():
            query = query.eq(column, value)
        result = query.limit(1).execute()
        return result.count or 0
    except Exception as e:
        logger.warning(
            "stats: filter count failed",
            filter=filter_dict,
            error_type=type(e).__name__,
        )
        return 0


# ============================================================
# GET /stats
# ============================================================


@router.get(
    "/stats",
    status_code=http_status.HTTP_200_OK,
    summary="Live signal counts (categories + summary + counties).",
)
async def stats_endpoint() -> dict[str, Any]:
    """Live counts for the homepage signal catalog."""
    categories: list[dict[str, Any]] = []
    for cat_id, filters in _CATEGORY_FILTERS.items():
        total = sum(_count_for_filter(f) for f in filters)
        srcs = sorted({f.get("source", "") for f in filters if f.get("source")})
        categories.append({"id": cat_id, "count": total, "sources": srcs})

    try:
        parcels_result = (
            core_table("parcels")
            .select("parcel_id", count="exact")
            .limit(1)
            .execute()
        )
        parcels_count = parcels_result.count or 0
    except Exception as e:
        logger.warning(
            "stats: parcels count failed",
            error_type=type(e).__name__,
        )
        parcels_count = 0
    categories.append({
        "id": "parcels",
        "count": parcels_count,
        "sources": ["core.parcels"],
    })

    total_signals = sum(c["count"] for c in categories if c["id"] != "parcels")

    # Probe each known source for presence (avoids the 1k-row response cap).
    # Include the per-row-county sources too so they count toward data_sources.
    _probe_sources = set(_SOURCE_TO_COUNTY.keys()) | _PER_ROW_COUNTY_SOURCES
    distinct_sources: set[str] = set()
    for src in _probe_sources:
        try:
            r = (
                signals_table("distress_events")
                .select("id", count="exact")
                .eq("source", src)
                .limit(1)
                .execute()
            )
            if (r.count or 0) > 0:
                distinct_sources.add(src)
        except Exception as e:
            logger.warning(
                "stats: source existence probe failed",
                source=src,
                error_type=type(e).__name__,
            )

    distinct_counties = {
        _SOURCE_TO_COUNTY[src]
        for src in distinct_sources
        if src in _SOURCE_TO_COUNTY
    }

    try:
        newest = (
            signals_table("distress_events")
            .select("observed_at")
            .order("observed_at", desc=True)
            .limit(1)
            .execute()
        )
        last_updated = (
            newest.data[0].get("observed_at") if newest.data else None
        )
    except Exception:
        last_updated = None

    counties_breakdown: list[dict[str, Any]] = []
    for county in sorted(distinct_counties):
        county_sources = _sources_for_county(county)
        try:
            cresult = (
                signals_table("distress_events")
                .select("id", count="exact")
                .in_("source", county_sources)
                .limit(1)
                .execute()
            )
            ccount = cresult.count or 0
        except Exception:
            ccount = 0
        counties_breakdown.append({
            "name": county,
            "signals": ccount,
            "parcels": parcels_count if county == "Hennepin" else None,
        })

    return success_envelope({
        "categories": categories,
        "summary": {
            "total_signals": total_signals,
            "parcels_indexed": parcels_count,
            "counties_covered": len(distinct_counties),
            "data_sources": len(distinct_sources),
            "last_updated": last_updated,
        },
        "counties": counties_breakdown,
    })
    
# ============================================================
# GET /counties — the data-driven coverage registry (2026-07-13)
# ============================================================
# Kills the frontend's two hardcoded county arrays (dropdown + coverage
# list). Membership rule, decided explicitly: a county is COVERED iff it
# has a non-null slug in core.source_county_map — the mapping table is the
# single coverage registry (the role it took on 2026-07-11). Row counts
# come from the view; counties with zero current rows are omitted from the
# response (a dropdown entry with nothing behind it is a coverage claim we
# can't back). startribune's ~36 incidental one-notice counties never
# appear: they're not in the registry.
#
# Onboarding a new county after this: scraper + one INSERT into
# source_county_map (+ the properties.py Python maps until the backlogged
# consolidation). ZERO frontend edits.


@router.get(
    "/counties",
    status_code=http_status.HTTP_200_OK,
    summary="Covered counties with live signal counts (drives the county dropdown).",
)
async def covered_counties() -> dict[str, Any]:
    """Counties from the coverage registry (core.source_county_map) with a
    live row count each from distress_with_parcel. Public, no tier gating —
    coverage is marketing-page data. Fails LOUD (503) if the registry is
    unreachable: an empty dropdown would be an honest-looking lie."""
    try:
        map_rows = (
            core_table("source_county_map")
            .select("county_slug")
            .not_.is_("county_slug", "null")
            .execute()
            .data
            or []
        )
    except Exception as e:
        logger.error(
            "county registry load failed", error_type=type(e).__name__
        )
        raise HTTPException(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="County coverage registry unavailable",
        )

    slugs = sorted({r["county_slug"] for r in map_rows if r.get("county_slug")})

    counties: list[dict[str, Any]] = []
    for slug in slugs:
        try:
            result = (
                signals_table("distress_with_parcel")
                .select("id", count="exact")
                .eq("county_slug", slug)
                .limit(1)
                .execute()
            )
            count: Optional[int] = result.count or 0
        except Exception as e:
            # Count failed for this county only: keep the county (it IS
            # registered coverage) with count=None rather than silently
            # dropping it from the dropdown on a transient error.
            logger.warning(
                "county count failed",
                county=slug,
                error_type=type(e).__name__,
            )
            count = None
        if count == 0:
            continue  # registered but currently empty (e.g. a source not yet live)
        counties.append(
            {
                "slug": slug,
                "name": _SLUG_TO_COUNTY_NAME.get(
                    slug, slug.replace("_", " ").title()
                ),
                "count": count,
            }
        )

    counties.sort(key=lambda c: c["name"])
    return success_envelope({"counties": counties})


# ============================================================
# GET /stats/differentiators — live USP numbers for the /about page
# ============================================================


@router.get(
    "/stats/differentiators",
    status_code=http_status.HTTP_200_OK,
    summary="Live cross-signal + owner-portfolio counts (the /about page USPs).",
)
async def differentiator_stats() -> dict[str, Any]:
    """Live counts that prove govire's differentiation — the cross-signal
    overlap and owner-portfolio patterns no single source reveals. All
    queried live from the two views (no hardcoded numbers). Each count
    degrades to None on failure so the page can hide that stat rather than
    show a wrong/zero number."""

    def _count_gte(table: str, column: str, threshold: int) -> Optional[int]:
        try:
            result = (
                signals_table(table)
                .select(column, count="exact")
                .gte(column, threshold)
                .limit(1)
                .execute()
            )
            return result.count or 0
        except Exception as e:
            logger.warning(
                "differentiator count failed",
                table=table,
                column=column,
                error_type=type(e).__name__,
            )
            return None

    multi_signal_parcels = _count_gte(
        "parcel_distress_overlay", "distinct_signal_count", 2
    )
    triple_distress_parcels = _count_gte(
        "parcel_distress_overlay", "distinct_signal_count", 3
    )
    multi_property_owners = _count_gte(
        "owner_distress_summary", "parcel_count", 2
    )

    return success_envelope({
        "multi_signal_parcels": multi_signal_parcels,
        "triple_distress_parcels": triple_distress_parcels,
        "multi_property_owners": multi_property_owners,
    })


# ============================================================
# GET /stats/redemption — outcome-tracker aggregates (public)
# ============================================================
# Free-safe by construction: COUNTS ONLY, no parcels, no addresses, no
# dates, no prices. Powers the homepage proof strip. Cached in-process for
# 10 minutes — the underlying tracker changes at most daily.

_REDEMPTION_STATS_CACHE: dict[str, Any] = {"at": 0.0, "data": None}
_REDEMPTION_STATS_TTL_S = 600


@router.get(
    "/stats/redemption",
    status_code=http_status.HTTP_200_OK,
    summary="Aggregate redemption-lifecycle stats (counts only; public).",
)
async def redemption_stats() -> dict[str, Any]:
    import time as _time

    now = _time.monotonic()
    if (
        _REDEMPTION_STATS_CACHE["data"] is not None
        and now - _REDEMPTION_STATS_CACHE["at"] < _REDEMPTION_STATS_TTL_S
    ):
        return success_envelope(_REDEMPTION_STATS_CACHE["data"])

    try:
        rows = _fetch_all_rows_in_schema(
            outcomes_table,
            "redemption_tracker",
            "redemption_expiry_date, outcome",
        )
    except Exception as e:
        logger.warning(
            "redemption stats fetch failed",
            error_type=type(e).__name__,
        )
        rows = []

    today = _date.today()
    soon = today + _timedelta(days=30)
    in_redemption = 0
    closing_30d = 0
    redeemed = 0
    reo = 0
    sold = 0
    outcome_pending = 0
    for r in rows:
        outcome = r.get("outcome") or "pending"
        expiry = _coerce_date(r.get("redemption_expiry_date"))
        if outcome in ("pending", "unknown"):
            if expiry is not None and expiry >= today:
                in_redemption += 1
                if expiry <= soon:
                    closing_30d += 1
            else:
                outcome_pending += 1
        elif outcome in ("redeemed_by_owner", "redeemed_by_junior"):
            redeemed += 1
        elif outcome == "foreclosed":
            reo += 1
        elif outcome == "foreclosed_sold":
            sold += 1

    resolved_total = redeemed + reo + sold
    pct_sold_during_redemption = (
        round(100 * redeemed / resolved_total) if resolved_total else None
    )

    data = {
        "in_redemption": in_redemption,
        "closing_within_30_days": closing_30d,
        "outcome_pending": outcome_pending,
        "resolved_total": resolved_total,
        "resolved_redeemed": redeemed,
        "resolved_reo": reo,
        "resolved_sold": sold,
        "pct_sold_during_redemption": pct_sold_during_redemption,
    }
    _REDEMPTION_STATS_CACHE["data"] = data
    _REDEMPTION_STATS_CACHE["at"] = now
    return success_envelope(data)


# ============================================================
# GET /properties — paginated property list
# ============================================================


def _apply_category_filter(query: Any, category: str) -> Any:
    """Apply category-specific filter to a Supabase query.

    Categories that need an event_type discriminator (tax_forfeit,
    tax_delinquent) get both the source IN-list and the event_type
    equality applied. Source-only categories (foreclosure, vacant)
    just get the IN-list.
    """
    filters = _CATEGORY_FILTERS.get(category, [])
    sources = sorted({f.get("source", "") for f in filters if f.get("source")})
    if not sources:
        return query
    query = query.in_("source", sources)

    # If every filter in this category specifies the SAME event_type,
    # apply that as an additional constraint. Used by tax_forfeit /
    # tax_delinquent — both share hennepin_tax_roll as a source so we
    # MUST narrow by event_type to avoid mixing them.
    event_types = {f.get("event_type") for f in filters if f.get("event_type")}
    if len(event_types) == 1:
        only = next(iter(event_types))
        query = query.eq("event_type", only)

    return query


# ============================================================
# COMPUTED SORTS (equity / redemption urgency)
# ============================================================
# These order on values that aren't plain DB columns, so they're applied in
# Python after the rows are shaped (see list_properties). Both push rows that
# can't be scored to the END regardless of asc/desc, so missing-data rows never
# masquerade as the best or worst results — they're set aside, not hidden.

def _equity_key(p: dict[str, Any]) -> float | None:
    """Equity = market_value - amount_due. Returns None unless BOTH values are
    present and numeric — a spread is only meaningful with both sides, so a row
    missing either is unscoreable (sorts to the end) rather than faked as 0."""
    mv = p.get("market_value")
    amt = p.get("amount")
    if mv is None or amt is None:
        return None
    try:
        return float(mv) - float(amt)
    except (TypeError, ValueError):
        return None


def _redemption_key(p: dict[str, Any]) -> int | None:
    """Urgency key = days until redemption expires (smaller = more urgent).
    Uses redemption_days_left, already computed per row. Returns None when
    there's no redemption window (non-foreclosure / no date) so those sort to
    the end."""
    d = p.get("redemption_days_left")
    if d is None:
        return None
    try:
        return int(d)
    except (TypeError, ValueError):
        return None


# ============================================================
# PARCEL DE-DUPLICATION (multi-signal path)
# ============================================================
# A multi-signal parcel has multiple event rows by definition (events from
# different sources), and a single source can emit many rows for one parcel
# (e.g. repeated condemned-building notices). When we filter to multi-signal
# parcels we must collapse to ONE representative row per parcel, or the same
# property appears many times and the count balloons. We keep the most
# actionable row as the representative; the parcel's full cross-signal overlay
# badge rides along on whichever row wins.

# Source → actionability rank. Lower number = more actionable / time-sensitive.
_SOURCE_ACTIONABILITY: dict[str, int] = {
    "anoka_sheriff": 1,
    "dakota_sheriff": 1,
    "hennepin_sheriff": 1,
    "ramsey_sheriff": 1,
    "washington_sheriff": 1,
    "scott_sheriff": 1,
    "carver_sheriff": 1,
    "startribune_legal": 1,
    "hennepin_tax_roll": 3,   # tax_forfeit vs delinquent split in _actionability_rank
    "ramsey_tax_roll": 3,
    "mn_dor_red_book": 2,
    "mpls_vbr": 4,
    "saint_paul_vacant": 4,
    "saint_paul_dsi": 4,
}


def _actionability_rank(row: dict[str, Any]) -> int:
    """Lower = more actionable. Foreclosure sales rank highest (a redemption
    clock is ticking); tax-forfeit outranks plain delinquency; vacant/condemned
    sits mid; everything else last. Used to pick the representative row when a
    parcel has many events."""
    source = row.get("source") or ""
    base = _SOURCE_ACTIONABILITY.get(source, 9)
    # Within the tax roll, forfeiture is more actionable than delinquency.
    if row.get("event_type") == "tax_forfeit":
        return 2
    return base


def _dedupe_by_parcel(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse shaped rows to one per effective parcel key, keeping the
    most-actionable representative. Rows without a resolvable parcel key
    (_eff_key is None) are kept as-is (each is its own entry — we can't tell
    if they're the same property). The private _eff_key field is removed from
    every returned row so it never leaks into the API response."""
    best_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    no_key: list[dict[str, Any]] = []

    for r in rows:
        key = r.get("_eff_key")
        if not key:
            no_key.append(r)
            continue
        existing = best_by_key.get(key)
        if existing is None or _actionability_rank(r) < _actionability_rank(existing):
            best_by_key[key] = r

    result = list(best_by_key.values()) + no_key
    # Strip the internal field before returning.
    for r in result:
        r.pop("_eff_key", None)
    return result


def _sort_computed(
    rows: list[dict[str, Any]], sort: str, descending: bool
) -> list[dict[str, Any]]:
    """Sort shaped rows by a computed key, always sending unscoreable rows
    (key is None) to the end. `descending` applies only to the scored rows.

    equity:              higher spread = better deal. Default view wants the
                         biggest deals first, so 'desc' is the natural order.
    redemption_urgency:  fewer days left = more urgent. 'asc' (soonest first)
                         is the natural order; expired rows (negative days)
                         would sort before in-redemption ones, so we also drop
                         already-expired rows to the bottom of the scored set
                         to keep 'act now' rows on top.
    """
    if sort == "equity":
        keyed = [(_equity_key(p), p) for p in rows]
        scored = [(k, p) for (k, p) in keyed if k is not None]
        unscored = [p for (k, p) in keyed if k is None]
        scored.sort(key=lambda kp: kp[0], reverse=descending)
        return [p for (_k, p) in scored] + unscored

    if sort == "redemption_urgency":
        keyed = [(_redemption_key(p), p) for p in rows]
        # Split: still-actionable (days >= 0) vs expired (days < 0) vs no-window.
        actionable = [(k, p) for (k, p) in keyed if k is not None and k >= 0]
        expired = [(k, p) for (k, p) in keyed if k is not None and k < 0]
        no_window = [p for (k, p) in keyed if k is None]
        # Soonest-expiring actionable rows first (ascending days-left). If the
        # caller asked desc, reverse only the actionable ordering.
        actionable.sort(key=lambda kp: kp[0], reverse=descending)
        # Among expired, most-recently-expired first (closest to 0 = least
        # stale), so if a user scrolls they see freshest expired next.
        expired.sort(key=lambda kp: kp[0], reverse=True)
        return (
            [p for (_k, p) in actionable]
            + [p for (_k, p) in expired]
            + no_window
        )

    return rows


@router.get(
    "/properties",
    status_code=http_status.HTTP_200_OK,
    summary="List distressed properties (filterable, paginated).",
)
async def list_properties(
    _ctx: TierContext = TierResolved,
    category: Optional[str] = Query(
        default=None,
        pattern="^(foreclosure|tax_forfeit|vacant|tax_delinquent|tax_assessment)$",
        description=(
            "Restrict to one signal category. The frontend table "
            "renders different columns per category."
        ),
    ),
    source: Optional[str] = Query(
        default=None,
        description="Filter by data source (e.g. 'anoka_sheriff').",
    ),
    county: Optional[str] = Query(
        default=None,
        description="Filter by county name (e.g. 'Anoka').",
    ),
    status_filter: Optional[str] = Query(
        default=None,
        alias="status",
        description="Filter by status: 'active' or 'postponed'.",
    ),

    redemption: Optional[str] = Query(
        default=None,
        pattern="^(in_redemption|expiring_soon|expired|outcome_pending|resolved)$",
        description=(
            "Filter foreclosure rows by redemption lifecycle state. "
            "'in_redemption'/'expiring_soon'/'expired' use DB-level date math "
            "(county-published date where present, else sale+~182d). "
            "'outcome_pending' and 'resolved' come from the outcome tracker, "
            "so they route through the fetch-all + Python-filter path."
        ),
    ),

    outcome: Optional[str] = Query(
        default=None,
        pattern="^(redeemed|reo|sold)$",
        description=(
            "PREMIUM: filter resolved foreclosures by confirmed outcome. "
            "'redeemed' = owner/junior redeemed; 'reo' = lender took title; "
            "'sold' = sold after foreclosure. Silently ignored below premium "
            "(the outcome itself is premium data)."
        ),
    ),

    redeemed: Optional[bool] = Query(
        default=None,
        description=(
            "Tyler tax-status filter (tax_delinquent view). false = hide "
            "parcels that have REDEEMED since the annual list published (the "
            "default list-hygiene view — 254 of Olmsted's 502 had already "
            "cured at first scrape); true = only redeemed parcels; absent = "
            "all rows. Rows without a tax_status block (e.g. Hennepin "
            "roll-mined delinquencies) always pass — the filter never hides "
            "rows it has no verdict for. Navigation-level (not tier-gated): "
            "the redeemed flag itself is free-tier data. Routes through the "
            "fetch-all path because the verdict joins from "
            "signals.tax_delinquency_status during shaping, never DB-side."
        ),
    ),

    multi_signal: Optional[int] = Query(
        default=None,
        ge=2,
        le=5,
        description=(
            "Filter to parcels appearing on at least this many distinct "
            "government signal families (2 = multi-signal, 3 = triple-distress). "
            "Cross-references signals.parcel_distress_overlay, so it routes "
            "through the fetch-all + Python-filter path like computed sorts."
        ),
    ),

   
    min_amount: Optional[float] = Query(
        default=None,
        ge=0,
        description="Minimum event_value (USD) — the debt/bid amount (investor lens).",
    ),
    # --- Buyer-lens filters (backed by signals.distress_with_parcel columns) ---
    year_built_min: Optional[int] = Query(
        default=None, ge=1700, le=2100,
        description="Earliest year built (inclusive). Rows without a known "
                    "year_built are excluded when this is set.",
    ),
    year_built_max: Optional[int] = Query(
        default=None, ge=1700, le=2100,
        description="Latest year built (inclusive).",
    ),
    sqft_min: Optional[int] = Query(
        default=None, ge=0,
        description="Minimum finished interior square footage. Coverage varies "
                    "by county (currently strongest in Ramsey; not yet present "
                    "for Hennepin) — rows without sqft are excluded when set.",
    ),
    lot_sqft_min: Optional[int] = Query(
        default=None, ge=0,
        description="Minimum lot size in square feet.",
    ),
    property_type: Optional[str] = Query(
        default=None,
        description="Exact property type (e.g. 'townhouse', 'single family').",
    ),
    school_district: Optional[str] = Query(
        default=None,
        description="Exact school district code (e.g. '281').",
    ),
    price_min: Optional[float] = Query(
        default=None, ge=0,
        description="Minimum estimated market value (emv_total) — the "
                    "property's worth (buyer lens), distinct from min_amount.",
    ),
    price_max: Optional[float] = Query(
        default=None, ge=0,
        description="Maximum estimated market value (emv_total).",
    ),
    
    sale_date_from: Optional[str] = Query(
        default=None,
        description="Earliest sale date (YYYY-MM-DD).",
    ),
    sale_date_to: Optional[str] = Query(
        default=None,
        description="Latest sale date (YYYY-MM-DD).",
    ),
    owner_type: Optional[str] = Query(
        default=None,
        pattern="^(individual|llc_business|government|bank_lender)$",
        description=(
            "Filter by the CURRENT assessor owner's classification "
            "(individual / llc_business / government / bank_lender). "
            "Premium hunting filter — applied on shaped rows because owner "
            "data joins from core.owners after the DB query."
        ),
    ),
    absentee: Optional[bool] = Query(
        default=None,
        description=(
            "true = absentee-owned only (owner's mailing differs from the "
            "property); false = owner-occupied only. Premium hunting filter."
        ),
    ),
    sort: str = Query(
        default="event_date",
        pattern="^(event_date|event_value|observed_at|equity|redemption_urgency|year_built|sqft|emv_total)$",
        description=(
            "Sort field. event_date/event_value/observed_at sort on real DB "
            "columns (fast path). 'equity' (market value minus amount due, "
            "biggest deal first) and 'redemption_urgency' (soonest redemption "
            "deadline first) are computed per-row, so they take a Python-side "
            "sort path that fetches all matching rows before paginating — fine "
            "at this scale (hundreds of foreclosure rows)."
        ),
    ),
    order: str = Query(
        default="asc",
        pattern="^(asc|desc)$",
        description="Sort order.",
    ),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """Return a paginated list of distress events with optional filters."""
    # ---- Tier filter/sort gating (server-side, non-bypassable) ----
    # Per GOVIRE_FILTER_GATING_SPEC.md: the STANDARD tier can read full property
    # detail but may NOT use the power filters/sorts ("hunting" is premium).
    # free/basic keep filters (their rows are locked → teaser only); premium/
    # admin keep them on full data. gate_filters_for_tier neutralizes the gated
    # levers for standard and forces the default sort. Navigation filters
    # (category/county/status) are always preserved. This runs BEFORE the query
    # is built, so a standard token cannot bypass it from the browser.
    _gated = gate_filters_for_tier(
        _ctx.tier,
        {
            "multi_signal": multi_signal,
            "min_amount": min_amount,
            "year_built_min": year_built_min,
            "year_built_max": year_built_max,
            "sqft_min": sqft_min,
            "lot_sqft_min": lot_sqft_min,
            "property_type": property_type,
            "school_district": school_district,
            "price_min": price_min,
            "price_max": price_max,
            "sale_date_from": sale_date_from,
            "sale_date_to": sale_date_to,
            "redemption": redemption,
            "owner_type": owner_type,
            "absentee": absentee,
            "sort": sort,
        },
    )
    multi_signal = _gated["multi_signal"]
    min_amount = _gated["min_amount"]
    year_built_min = _gated["year_built_min"]
    year_built_max = _gated["year_built_max"]
    sqft_min = _gated["sqft_min"]
    lot_sqft_min = _gated["lot_sqft_min"]
    property_type = _gated["property_type"]
    school_district = _gated["school_district"]
    price_min = _gated["price_min"]
    price_max = _gated["price_max"]
    sale_date_from = _gated["sale_date_from"]
    sale_date_to = _gated["sale_date_to"]
    redemption = _gated["redemption"]
    owner_type = _gated["owner_type"]
    absentee = _gated["absentee"]
    sort = _gated["sort"]

    try:
        # Read from the enrichment-joined view (signals.distress_with_parcel),
        # not distress_events directly. Same rows, plus the parcel
        # characteristics (year_built, sqft, lot_sqft, emv_total, property_type,
        # school_district, ...) exposed as real, filterable/sortable columns.
        # This is what lets the buyer-lens filters (year built, square footage,
        # value, type, school) run at the DB level and scale as enrichment grows.
        query = (
            signals_table("distress_with_parcel")
            .select(
                "source_id, source, parcel_id, event_type, event_date, "
                "event_value, severity, title, description, raw_data, "
                "observed_at, year_built, sqft, lot_sqft, emv_total, "
                "property_type, school_district",
                count="exact",
            )
        )
        
        if category:
            query = _apply_category_filter(query, category)

        if source:
            query = query.eq("source", source)

        if county:
            # Sources whose county is fixed by name (IN-list filterable).
            county_sources = _sources_for_county(county)
            # Per-row-county sources (statewide/extracted) can't be filtered
            # by a source IN-list — their county lives in raw_data.detail.county
            # as a slug. We OR a JSON-path match on that slug so a county query
            # finds them too. Build the slug from the requested county name.
            slug = None
            for s, name in _SLUG_TO_COUNTY_NAME.items():
                if name == county:
                    slug = s
                    break

            if county_sources and slug:
                src_list = ",".join(county_sources)
                # (source in fixed-list) OR (per-row source AND detail.county == slug)
                query = query.or_(
                    f"source.in.({src_list}),"
                    f"and(source.in.({','.join(_PER_ROW_COUNTY_SOURCES)}),"
                    f"raw_data->detail->>county.eq.{slug})"
                )
            elif county_sources:
                query = query.in_("source", county_sources)
            elif slug:
                # Only resolvable via per-row county.
                query = query.in_("source", list(_PER_ROW_COUNTY_SOURCES))
                query = query.eq("raw_data->detail->>county", slug)
            else:
                # Unknown county — no rows.
                return success_envelope({
                    "properties": [],
                    "total": 0,
                    "limit": limit,
                    "offset": offset,
                })

        if status_filter:
            sv = status_filter.strip().lower()
            if sv == "postponed":
                query = query.ilike(
                    "raw_data->detail->>status", "%postpon%"
                )
            elif sv == "active":
                query = query.is_(
                    "raw_data->detail->>status", "null"
                )

        if redemption:
            # Redemption-window filter. Where a source publishes the exact
            # redemption date (Hennepin: raw_data->>redemptionExpirationDate)
            # we filter on that real date so the filter and the displayed
            # pill always agree. Where it's absent (Anoka/Dakota) we fall
            # back to the event_date approximation (sale + ~182 days), which
            # is ~95% accurate. The .or_() combines: "(exact date in range)
            # OR (no exact date AND approximate sale_date in range)".
            from datetime import date as _d, timedelta as _td

            today = _d.today()
            today_s = today.isoformat()
            soon_cutoff = (today + _td(days=90)).isoformat()  # exact-date side
            # Approximation boundaries (sale_date space; redemption≈sale+182):
            soon_start = (today - _td(days=182)).isoformat()
            soon_end = (today - _td(days=92)).isoformat()
            in_redemption_after = (today - _td(days=92)).isoformat()
            expired_before = (today - _td(days=182)).isoformat()

            # JSON path to Hennepin's published date inside raw_data.
            rk = "raw_data->>redemptionExpirationDate"

            if redemption == "in_redemption":
                # Exact: redemption date strictly after the 90-day cutoff.
                # Approx: row has no exact date AND sold < 92 days ago.
                query = query.or_(
                    f"{rk}.gt.{soon_cutoff},"
                    f"and({rk}.is.null,event_date.gt.{in_redemption_after})"
                )
            elif redemption == "expiring_soon":
                # Exact: redemption date between today and today+90.
                # Approx: no exact date AND sold 92–182 days ago.
                query = query.or_(
                    f"and({rk}.gte.{today_s},{rk}.lte.{soon_cutoff}),"
                    f"and({rk}.is.null,event_date.gte.{soon_start},"
                    f"event_date.lte.{soon_end})"
                )
            elif redemption == "expired":
                # Exact: redemption date already passed.
                # Approx: no exact date AND sold > 182 days ago.
                query = query.or_(
                    f"{rk}.lt.{today_s},"
                    f"and({rk}.is.null,event_date.lt.{expired_before})"
                )

        

        if min_amount is not None:
            query = query.gte("event_value", min_amount)

        # --- Buyer-lens filters (real columns on the view) ---
        # Each is a plain DB-level comparison, so it's fast and scales with the
        # data. A characteristic filter naturally excludes rows where that
        # field is null (e.g. an unmatched parcel has no year_built) — which is
        # the honest behavior: "built after 1990" should not return properties
        # whose build year we don't know. The frontend signals when a
        # characteristic filter is narrowing to enriched rows.
        if year_built_min is not None:
            query = query.gte("year_built", year_built_min)
        if year_built_max is not None:
            query = query.lte("year_built", year_built_max)
        if sqft_min is not None:
            query = query.gte("sqft", sqft_min)
        if lot_sqft_min is not None:
            query = query.gte("lot_sqft", lot_sqft_min)
        if property_type:
            query = query.eq("property_type", property_type)
        if school_district:
            query = query.eq("school_district", school_district)
        if price_min is not None:
            query = query.gte("emv_total", price_min)
        if price_max is not None:
            query = query.lte("emv_total", price_max)

        if sale_date_from:
            query = query.gte("event_date", sale_date_from)
        if sale_date_to:
            query = query.lte("event_date", sale_date_to)

        # --- Two sort paths ---
        # Column sorts (event_date / event_value / observed_at) are real DB
        # columns, so PostgREST sorts + paginates them server-side (fast).
        #
        # Computed sorts (equity, redemption_urgency) order on values that
        # aren't plain columns — equity = market_value - amount_due (both
        # nested in raw_data and one of them derived), and redemption urgency
        # = the effective redemption date (published JSON date for Hennepin,
        # estimated sale+182d otherwise). PostgREST can't ORDER BY those, so we
        # fetch ALL matching rows, shape them (which computes those values),
        # sort in Python, then slice the page. This is appropriate at our scale
        # (foreclosure is a few hundred rows), not a workaround to feel bad
        # about — sorting hundreds of dicts is instant.
        computed_sorts = {"equity", "redemption_urgency"}

        # The outcome filter is PREMIUM data (which rows redeemed vs sold is
        # itself the leverage) — silently neutralized below premium, matching
        # the gate_filters_for_tier convention of not erroring on gated input.
        if _ctx.tier not in ("premium", "admin"):
            outcome = None

        # multi_signal requires the overlay map to filter, which only exists
        # after shaping — so it forces the fetch-all path too, exactly like
        # the computed sorts. Pagination then happens in Python on the
        # filtered set. The tracker-backed redemption states (outcome_pending
        # / resolved) and the outcome filter also live on shaped rows only.
        tracker_states = redemption in ("outcome_pending", "resolved")
        needs_fetch_all = (
            sort in computed_sorts
            or multi_signal is not None
            or tracker_states
            or outcome is not None
            or owner_type is not None
            or absentee is not None
            or redeemed is not None
        )

        if needs_fetch_all:

            # STABLE PAGE ORDER (2026-07-12): the paging loop below issues
            # SEPARATE queries per 1000-row page, and Postgres guarantees no
            # row order at all without ORDER BY — rows shuffle between pages,
            # so some duplicate and some vanish (observed live: 40 of 4,614
            # tax_delinquent rows lost/duplicated across 5 pages, deflating
            # redeemed_count 254->214 while inflating total). Order by the
            # requested column when it's a real DB column, then ALWAYS by id
            # as the unique tiebreaker — ties on the primary sort are exactly
            # how rows slip between page boundaries. Computed sorts re-sort
            # in Python afterward, so id-only order is sufficient for them.
            if sort not in computed_sorts:
                query = query.order(sort, desc=(order == "desc"), nullsfirst=False)
            query = query.order("id", desc=False)

            # Fetch EVERY matching row before shaping/filtering — never a
            # single capped slice. The multi_signal filter and computed sorts
            # operate on the shaped rows, so an incomplete fetch silently
            # hides results (e.g. bare multi_signal=2 missing the multi-signal
            # parcels because they fell outside a 2000-row window). We page
            # through the full result set until exhausted, so the answer is
            # complete now and stays complete as the data grows.
            _PAGE = 1000          # rows per page request
            _MAX_PAGES = 1000     # hard safety stop (= up to 1M rows) so a bug
                                  # can never spin forever
            rows: list[dict[str, Any]] = []
            total = 0
            page_idx = 0
            while page_idx < _MAX_PAGES:
                start = page_idx * _PAGE
                end = start + _PAGE - 1
                page_result = query.range(start, end).execute()
                page_rows = page_result.data or []
                # count is the same on every page (exact total of the filtered
                # query); capture it once for the honest "X of Y".
                if page_result.count is not None:
                    total = page_result.count
                rows.extend(page_rows)
                # Last page reached when we got fewer rows than we asked for.
                if len(page_rows) < _PAGE:
                    break
                page_idx += 1
            else:
                # Loop exhausted _MAX_PAGES without a short page — log it so a
                # runaway dataset is visible rather than silently truncated.
                logger.warning(
                    "list_properties: fetch-all hit max pages — result may be "
                    "incomplete",
                    fetched=len(rows),
                    max_pages=_MAX_PAGES,
                )

            overlay_map = _load_overlay_map()
            owner_map = _load_owner_map()
            tracker_map = _load_redemption_tracker_map()
            delq_map = _load_delq_status_map()
            shaped = [
                _shape_property_row(r, overlay_map, owner_map, tracker_map, delq_map)
                for r in rows
            ]
            _apply_assessor_owners(shaped)

            

           # Apply the multi-signal filter (if requested) on the shaped rows,
            # since signal counts come from the overlay attached during shaping.
            if multi_signal is not None:
                shaped = [
                    s for s in shaped
                    if s.get("overlay")
                    and (s["overlay"].get("distinct_signal_count") or 0) >= multi_signal
                ]

                # De-duplicate to ONE row per parcel. A multi-signal parcel has
                # multiple event rows by definition (that's WHY it's
                # multi-signal — events from different sources), plus a single
                # source can emit many rows for one parcel (e.g. repeated
                # condemned-building notices). Without de-dup the same property
                # appears many times and the count balloons (110 parcels -> 550+
                # rows). We collapse by the effective parcel key and keep the
                # most-actionable representative; the parcel's full cross-signal
                # badge is preserved on whichever row represents it.
                shaped = _dedupe_by_parcel(shaped)

            # Tracker-backed lifecycle filters: these states/outcomes exist
            # only on shaped rows (they come from outcomes.redemption_tracker
            # during shaping), so they are applied here, never DB-side.
            if tracker_states:
                shaped = [
                    s for s in shaped
                    if s.get("redemption_state") == redemption
                ]
            if outcome is not None:
                _OUTCOME_BUCKETS = {
                    "redeemed": {"redeemed_by_owner", "redeemed_by_junior"},
                    "reo": {"foreclosed"},
                    "sold": {"foreclosed_sold"},
                }
                wanted = _OUTCOME_BUCKETS.get(outcome, set())
                shaped = [
                    s for s in shaped
                    if s.get("redemption_outcome") in wanted
                ]

            # Owner filters (2026-07-09): owner_type/is_absentee come from
            # core.owners during _apply_assessor_owners, so they exist only
            # on shaped rows — never DB-side. Rows with unknown attributes
            # (no assessor owner) are excluded when a filter is active:
            # "show me LLC-owned" must not include "ownership unknown".
            if owner_type is not None:
                shaped = [
                    s for s in shaped
                    if s.get("owner_type") == owner_type
                ]
            if absentee is not None:
                shaped = [
                    s for s in shaped
                    if s.get("is_absentee") is absentee
                ]

            # Tyler redeemed filter (2026-07-12): the verdict lives in the
            # nested tax_status block attached during shaping — never
            # DB-side. The PRE-filter redeemed count ships in the envelope
            # so the frontend toggle can label itself with the live number
            # ("show redeemed (254)") instead of a hardcode. Rows WITHOUT a
            # tax_status block always pass: no verdict, no hiding.
            redeemed_count: Optional[int] = None
            if redeemed is not None:
                redeemed_count = sum(
                    1 for s in shaped
                    if (s.get("tax_status") or {}).get("redeemed_since_list") is True
                )
                if redeemed is False:
                    shaped = [
                        s for s in shaped
                        if (s.get("tax_status") or {}).get("redeemed_since_list") is not True
                    ]
                else:
                    shaped = [
                        s for s in shaped
                        if (s.get("tax_status") or {}).get("redeemed_since_list") is True
                    ]

            # 'sort' may be a normal column here (when multi_signal forced this
            # path); _sort_computed passes those through unchanged, so the rows
            # keep the DB order they arrived in. Computed sorts still sort.
            descending = (order == "desc")
            shaped = _sort_computed(shaped, sort, descending)

            total = len(shaped)  # filtered count, so pagination + "X of Y" stay honest
            page = shaped[offset:offset + limit]
            
            _envelope: dict[str, Any] = {
                "properties": [redact_property(_r, tier=_ctx.tier) for _r in page],
                "total": total,
                "limit": limit,
                "offset": offset,
            }
            if redeemed_count is not None:
                _envelope["redeemed_count"] = redeemed_count
            return success_envelope(_envelope)

        # Fast DB-level column sort + pagination.
        # nullsfirst=False: Postgres floats NULLs to the top of DESC sorts by
        # default, which put date-less legal notices above every dated sale on
        # "latest first". Nulls belong at the end in both directions.
        # id tiebreaker (2026-07-12): rows tied on the sort column can swap
        # across page boundaries between requests (same instability fixed in
        # the fetch-all path) — a unique secondary key pins them.
        query = query.order(sort, desc=(order == "desc"), nullsfirst=False)
        query = query.order("id", desc=False)
        query = query.range(offset, offset + limit - 1)

        result = query.execute()
        rows = result.data or []
        total = result.count or 0

        overlay_map = _load_overlay_map()
        owner_map = _load_owner_map()
        tracker_map = _load_redemption_tracker_map()
        delq_map = _load_delq_status_map()
        _shaped_page = [
            _shape_property_row(r, overlay_map, owner_map, tracker_map, delq_map)
            for r in rows
        ]
        _apply_assessor_owners(_shaped_page)
        return success_envelope({
            "properties": [
                redact_property(s, tier=_ctx.tier)
                for s in _shaped_page
            ],
            "total": total,
            "limit": limit,
            "offset": offset,
        })

    except Exception as e:
        logger.exception(
            "properties list query failed",

            
            error_type=type(e).__name__,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch properties: {type(e).__name__}",
        )


# ============================================================
# GET /properties/{source}/{source_id} — single property
# ============================================================


@router.get(
    "/properties/{source}/{source_id}",
    status_code=http_status.HTTP_200_OK,
    summary="Fetch a single property by (source, source_id).",
)
async def get_property(
    source: str,
    source_id: str,
    parcel_id: Optional[str] = Query(
        default=None,
        max_length=200,
        description=(
            "Optional disambiguator: (source, source_id) is not unique for "
            "every source (reused counters / page indexes / nulls). Pass the "
            "row's parcel_id to guarantee the right property."
        ),
    ),
    _ctx: TierContext = TierResolved,
) -> dict[str, Any]:
    """Return the full record for one property identified by its
    natural key (source, source_id)."""
    try:
        result = (
            signals_table("distress_events")
            .select("*")
            .eq("source", source)
            .eq("source_id", source_id)
        )
        if parcel_id:
            result = result.eq("parcel_id", parcel_id)
        result = (
            result.order("event_date", desc=True, nullsfirst=False)
            .limit(2)
            .execute()
        )
        rows = result.data or []
        if len(rows) > 1:
            logger.warning(
                "get_property: ambiguous (source, source_id) — returning "
                "the most recent event",
                source=source,
                source_id=source_id,
                parcel_id=parcel_id,
            )
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"Property not found: {source}/{source_id}",
            )

        overlay_map = _load_overlay_map()
        owner_map = _load_owner_map()
        tracker_map = _load_redemption_tracker_map()
        delq_map = _load_delq_status_map()
        shaped = _shape_property_row(rows[0], overlay_map, owner_map, tracker_map, delq_map)
        _apply_assessor_owners([shaped])
        shaped["raw"] = rows[0].get("raw_data") or {}

        # Attach enriched property characteristics from core.parcels, keyed by
        # the EFFECTIVE parcel id (real gis_pid for sheriff rows). This is the
        # detail-view data: year built, lot size, school district, assessor
        # values, garage/basement/heating where available. Only populated
        # fields are returned; the drawer renders whatever exists.
        raw_data = rows[0].get("raw_data") or {}
        src = rows[0].get("source") or ""
        eff_pid = _effective_parcel_id(src, raw_data, rows[0])
        county_slug = (_resolve_county(src, raw_data) or "").lower()
        shaped["enrichment"] = _load_parcel_enrichment(county_slug, eff_pid) if eff_pid else None

        # Drop the internal de-dup key — single-property responses don't need it.
        shaped.pop("_eff_key", None)

        # Tier-aware redaction (admin/premium full; lower tiers locked).
        shaped = redact_property(shaped, tier=_ctx.tier)
        shaped = redact_detail_extras(shaped, tier=_ctx.tier)

        return success_envelope(shaped)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "single property query failed",
            source=source,
            source_id=source_id,
            error_type=type(e).__name__,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch property: {type(e).__name__}",
        )


# ============================================================
# GET /owners — list resolved owners (portfolio browse)
# ============================================================


@router.get(
    "/owners",
    status_code=http_status.HTTP_200_OK,
    summary="List resolved owners with their distressed-property counts.",
)
async def list_owners(
    _ctx: TierContext = TierResolved,
    owner_type: Optional[str] = Query(
        default=None,
        pattern="^(individual|llc_business|bank_lender|government)$",
        description="Filter by owner type.",
    ),
    min_parcels: int = Query(
        default=2,
        ge=1,
        le=100,
        description="Only owners holding at least this many distinct properties.",
    ),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """Browse owners from signals.owner_distress_summary, biggest portfolios
    first. Defaults to 2+ properties (the frequent-flyer signal); pass
    min_parcels=1 to include single-property owners."""
    if not owner_browse_allowed(_ctx.tier):
        raise HTTPException(
            status_code=402,
            detail="Owner portfolio browsing requires a Premium subscription.",
        )
    try:
        query = (
            signals_table("owner_distress_summary")
            .select(
                "owner_norm, owner_type, parcel_count, event_count, "
                "max_severity, any_absentee, owner_mailing, event_types, "
                "sources, addresses",
                count="exact",
            )
            .gte("parcel_count", min_parcels)
        )
        if owner_type:
            query = query.eq("owner_type", owner_type)

        query = query.order("parcel_count", desc=True)
        query = query.range(offset, offset + limit - 1)

        result = query.execute()
        rows = result.data or []
        total = result.count or 0

        return success_envelope({
            "owners": rows,
            "total": total,
            "limit": limit,
            "offset": offset,
        })
    except Exception as e:
        logger.exception(
            "owners list query failed",
            error_type=type(e).__name__,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch owners: {type(e).__name__}",
        )


# ============================================================
# GET /owners/{owner_norm}/properties — one owner's full holdings
# ============================================================


@router.get(
    "/owners/{owner_norm}/properties",
    status_code=http_status.HTTP_200_OK,
    summary="List every distressed property tied to one owner.",
)
async def owner_properties(
    owner_norm: str,
    _ctx: TierContext = TierResolved,
) -> dict[str, Any]:
    """Return all distress events whose normalized gis_owner matches
    owner_norm, shaped like regular property rows so the frontend reuses its
    rendering. owner_norm is the upper(trim(gis_owner)) key (URL-encoded by
    the caller). Cross-county by nature — an owner's holdings can span
    multiple counties, which is the whole point of resolving them."""
    try:
        key = owner_norm.strip().upper()

        # Pull candidate rows. We can't filter on the computed gis_owner key in
        # PostgREST directly, so we fetch foreclosure-source rows that HAVE a
        # gis_owner and match in Python. The enriched set is small (~600 rows).
        result = (
            signals_table("distress_events")
            .select(
                "source_id, source, parcel_id, event_type, event_date, "
                "event_value, severity, title, description, raw_data, "
                "observed_at"
            )
            .not_.is_("raw_data->detail->>gis_owner", "null")
            .range(0, 4999)
            .execute()
        )
        rows = result.data or []

        matched = [
            r for r in rows
            if (str((r.get("raw_data") or {}).get("detail", {}).get("gis_owner") or "")
                .strip().upper()) == key
        ]

        if not matched:
            raise HTTPException(
                status_code=404,
                detail=f"No properties found for owner: {owner_norm}",
            )

        # Owner-portfolio browse is a PREMIUM leverage feature.
        if not owner_browse_allowed(_ctx.tier):
            raise HTTPException(
                status_code=402,
                detail="Owner portfolio browsing requires a Premium subscription.",
            )

        overlay_map = _load_overlay_map()
        owner_map = _load_owner_map()
        tracker_map = _load_redemption_tracker_map()
        delq_map = _load_delq_status_map()
        shaped = [
            _shape_property_row(r, overlay_map, owner_map, tracker_map, delq_map)
            for r in matched
        ]
        # Drop the internal de-dup key before returning.
        for s in shaped:
            s.pop("_eff_key", None)
        # Redact each row to the caller's tier (premium here, but keep
        # the pass so future tier changes are honored).
        shaped = [redact_property(s, tier=_ctx.tier) for s in shaped]

        # The owner summary (type, count) comes from the first shaped row's
        # attached owner_portfolio — identical across the set, so any row's is
        # representative.
        summary = shaped[0].get("owner_portfolio") if shaped else None

        return success_envelope({
            "owner_norm": key,
            "summary": summary,
            "properties": [redact_property(_r, tier=_ctx.tier) for _r in shaped],
            "total": len(shaped),
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "owner properties query failed",
            owner_norm=owner_norm,
            error_type=type(e).__name__,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch owner properties: {type(e).__name__}",
        )

__all__ = ["router"]
