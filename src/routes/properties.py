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

from src.db.supabase_client import core_table, signals_table
from src.utils.errors import success_envelope
from src.utils.logger import logger


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
    ],
    "tax_forfeit": [
        {"source": "hennepin_tax_roll", "event_type": "tax_forfeit"},
        {"source": "ramsey_tax_roll", "event_type": "tax_forfeit"},
        {"source": "mn_dor_red_book"},
    ],
    "vacant": [
        {"source": "mpls_vbr"},
        {"source": "saint_paul_vacant"},
        {"source": "saint_paul_dsi"},
    ],
    "tax_delinquent": [
        {"source": "hennepin_tax_roll", "event_type": "tax_delinquent"},
        {"source": "ramsey_tax_roll", "event_type": "tax_delinquent"},
    ],
    "tax_assessment": [
        {"source": "ramsey_tax_roll", "event_type": "tax_assessment"},
    ],
}
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
    "mn_dor_red_book": "Statewide",
}


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
    """mpls_vbr — VBR_MPLS feature service: attributes + top-level owner_name."""
    attrs = raw.get("attributes") or {}
    return {
        "address": attrs.get("Address"),
        "city": attrs.get("City"),
        "zip": attrs.get("Zip"),
        "owner": raw.get("owner_name") or attrs.get("Property_O"),
        "sale_date": None,
        "sale_time": None,
        # event_value here is the VBR annual fee, not a sale price.
        "amount": row.get("event_value"),
        "status": attrs.get("Property_s"),
        "tax_parcel_no": attrs.get("APN_Txt"),
        "original_principal": None,
        "municipality": attrs.get("City"),
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


_EXTRACTORS: dict[str, Any] = {
    "anoka_sheriff": _extract_anoka,
    "hennepin_sheriff": _extract_hennepin_sheriff,
    "dakota_sheriff": _extract_dakota,
    "washington_sheriff": _extract_washington,
    "mpls_vbr": _extract_mpls_vbr,
    "saint_paul_vacant": _extract_saint_paul_vacant,
    "saint_paul_dsi": _extract_saint_paul_vacant,
    "hennepin_tax_roll": _extract_hennepin_tax,
    "ramsey_tax_roll": _extract_hennepin_tax,
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
}


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


def _redemption_fields(source: str, raw: dict, row: dict) -> dict[str, Any]:
    """Compute redemption_ends_at / days_left / redemption_state for a row.

    Returns all-null fields for non-foreclosure sources.

    Two date sources, in priority order:
      1. A county-published redemption date (Hennepin / Ramsey expose
         redemptionExpirationDate). Authoritative — used as-is, no guard.
      2. Otherwise, estimate sale_date + ~6 months — but ONLY for sales that
         have actually COMPLETED. A redemption clock only starts once the
         sheriff sale happens; an upcoming or postponed sale has none.

    COMPLETED-SALE GUARD (the estimate path only):
      * Anoka publishes its PENDING sales list. Each row carries a status of
        'Sold' (completed), 'Postponed' (rescheduled — did not happen), or
        null (pending). Only 'Sold' has a redemption window; the rest get
        all-null (em-dash), because estimating sale+182d for a sale that never
        occurred invents data.
      * Dakota's feed is completed sales by definition and carries no per-row
        status, so it passes the guard and estimates as before.
      * Hennepin/Ramsey never reach the estimate (they have a published date).
    """
    null_result = {
        "redemption_ends_at": None,
        "redemption_days_left": None,
        "redemption_state": None,
        "redemption_is_estimated": None,
    }
    if source not in _FORECLOSURE_SOURCES:
        return null_result

    ends_at: Optional[_date] = None
    is_estimated = False

    # 1. County-published exact date (Hennepin / Ramsey). Authoritative.
    published = raw.get("redemptionExpirationDate")
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
    }

def _shape_property_row(row: dict[str, Any]) -> dict[str, Any]:
    """Dispatch to the right per-source extractor and merge common fields."""
    source = row.get("source") or ""
    raw = row.get("raw_data") or {}
    extractor = _EXTRACTORS.get(source, _extract_generic)
    extracted = extractor(raw, row)

    redemption = _redemption_fields(source, raw, row)

    return {
        "source": source,
        "source_id": row.get("source_id"),
        "parcel_id": row.get("parcel_id"),
        "county": _SOURCE_TO_COUNTY.get(source),
        "event_type": row.get("event_type"),
        "title": row.get("title"),
        "description": row.get("description"),
        "observed_at": row.get("observed_at"),
        **extracted,
        **redemption,
    }

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
    distinct_sources: set[str] = set()
    for src in _SOURCE_TO_COUNTY.keys():
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
        county_sources = [
            src for src, c in _SOURCE_TO_COUNTY.items() if c == county
        ]
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
    _access_key: str = Depends(require_access_key),
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
        pattern="^(in_redemption|expiring_soon|expired)$",
        description=(
            "Filter foreclosure rows by redemption-window state. "
            "Approximated via event_date (redemption ≈ sale + ~6 months) "
            "so it works uniformly across all sheriff counties."
        ),
    ),

    
    min_amount: Optional[float] = Query(
        default=None,
        ge=0,
        description="Minimum event_value (USD).",
    ),
    sale_date_from: Optional[str] = Query(
        default=None,
        description="Earliest sale date (YYYY-MM-DD).",
    ),
    sale_date_to: Optional[str] = Query(
        default=None,
        description="Latest sale date (YYYY-MM-DD).",
    ),
    sort: str = Query(
        default="event_date",
        pattern="^(event_date|event_value|observed_at|equity|redemption_urgency)$",
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
    try:
        query = (
            signals_table("distress_events")
            .select(
                "source_id, source, parcel_id, event_type, event_date, "
                "event_value, severity, title, description, raw_data, "
                "observed_at",
                count="exact",
            )
        )

        if category:
            query = _apply_category_filter(query, category)

        if source:
            query = query.eq("source", source)

        if county:
            county_sources = [
                src for src, c in _SOURCE_TO_COUNTY.items() if c == county
            ]
            if not county_sources:
                return success_envelope({
                    "properties": [],
                    "total": 0,
                    "limit": limit,
                    "offset": offset,
                })
            query = query.in_("source", county_sources)

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

        if sort in computed_sorts:
            # Fetch all matching rows (generous cap), shape, sort, paginate.
            _ALL_CAP = 2000
            query = query.range(0, _ALL_CAP - 1)
            result = query.execute()
            rows = result.data or []
            total = result.count or 0

            shaped = [_shape_property_row(r) for r in rows]
            descending = (order == "desc")
            shaped = _sort_computed(shaped, sort, descending)

            page = shaped[offset:offset + limit]
            return success_envelope({
                "properties": page,
                "total": total,
                "limit": limit,
                "offset": offset,
            })

        # Fast DB-level column sort + pagination.
        query = query.order(sort, desc=(order == "desc"))
        query = query.range(offset, offset + limit - 1)

        result = query.execute()
        rows = result.data or []
        total = result.count or 0

        return success_envelope({
            "properties": [_shape_property_row(r) for r in rows],
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
async def get_property(source: str, source_id: str) -> dict[str, Any]:
    """Return the full record for one property identified by its
    natural key (source, source_id)."""
    try:
        result = (
            signals_table("distress_events")
            .select("*")
            .eq("source", source)
            .eq("source_id", source_id)
            .limit(1)
            .execute()
        )
        rows = result.data or []
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"Property not found: {source}/{source_id}",
            )

        shaped = _shape_property_row(rows[0])
        shaped["raw"] = rows[0].get("raw_data") or {}
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


__all__ = ["router"]
