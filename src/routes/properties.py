"""
Public properties + stats endpoints for the govire.com frontend.

Returns live data from signals.distress_events and core.parcels —
NO hardcoded numbers. The frontend (src/components/SignalCatalog.tsx,
src/components/PropertiesView.tsx) consumes these endpoints directly.

Routes:
    GET /stats                              — live signal counts
    GET /properties                         — paginated property list
    GET /properties/{source}/{source_id}    — single property detail
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, status as http_status

from src.db.supabase_client import core_table, signals_table
from src.utils.errors import success_envelope
from src.utils.logger import logger


# ============================================================
# DISPLAY-LAYER CONFIG (no data — just classification mappings)
# ============================================================
# Each category is defined by a LIST OF FILTERS. A filter is a dict
# of column->value that must all match (AND). Multiple filters per
# category are OR'd together (sum of independent queries).
#
# This shape handles two real cases:
#   (1) "vacant" — match by source only; one or more sources.
#   (2) "tax_forfeit" / "tax_delinquent" — share ONE source
#       (hennepin_tax_roll) but split by event_type. Filtering by
#       source alone would double-count.
#
# Add new sources here as scrapers go live; the frontend's
# src/data/stats.ts owns the human-readable strings to match.

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
        # Hennepin tax_roll source contains BOTH forfeit and delinquent
        # records — distinguished by event_type. Without this filter
        # we'd double-count 4,251 across both categories.
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
    # "parcels" handled separately — counted from core.parcels.
}

# Map signal source → MN county name. Used for per-county breakdown
# and for the ?county=... filter on /properties.
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


def _shape_property_row(row: dict[str, Any]) -> dict[str, Any]:
    """Flatten a distress_events row into a public-API shape."""
    raw = row.get("raw_data") or {}
    list_data = raw.get("list") or {}
    detail = raw.get("detail") or {}
    source = row.get("source") or ""
    return {
        "source": row.get("source"),
        "source_id": row.get("source_id"),
        "parcel_id": row.get("parcel_id"),
        "county": _SOURCE_TO_COUNTY.get(source),
        "sale_date": (
            list_data.get("scheduled_date") or row.get("event_date")
        ),
        "sale_time": detail.get("sale_time"),
        "address": (
            list_data.get("address") or detail.get("detail_address")
        ),
        "city": list_data.get("city"),
        "zip": list_data.get("zip"),
        "mortgagor": detail.get("owner_name"),
        "amount_due": row.get("event_value"),
        "tax_parcel_no": detail.get("tax_parcel_no"),
        "original_principal": detail.get("original_principal"),
        "status": detail.get("status") or "Active",
        "severity": row.get("severity"),
        "event_type": row.get("event_type"),
        "title": row.get("title"),
        "description": row.get("description"),
        "observed_at": row.get("observed_at"),
    }


# ============================================================
# GET /stats — live counts for the homepage
# ============================================================


@router.get(
    "/stats",
    status_code=http_status.HTTP_200_OK,
    summary="Live signal counts (categories + summary + counties).",
)
async def stats_endpoint() -> dict[str, Any]:
    """
    Compute live counts for the homepage signal catalog.

    Returns three sections — categories (per signal type),
    summary (totals), counties (per-county breakdown). Frontend
    pairs these with static presentation strings.
    """
    # Per-category counts. Each category is a list of filters; we sum
    # their independent counts. This lets a single source (e.g.
    # hennepin_tax_roll) contribute to multiple categories by event_type
    # without double-counting.
    categories: list[dict[str, Any]] = []
    for cat_id, filters in _CATEGORY_FILTERS.items():
        total = sum(_count_for_filter(f) for f in filters)
        # Surface the unique source names that fed this category so
        # the frontend can show "Source: X, Y" labels honestly.
        srcs = sorted({f.get("source", "") for f in filters if f.get("source")})
        categories.append({
            "id": cat_id,
            "count": total,
            "sources": srcs,
        })

    # Parcels: foundational, counted from core.parcels.
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

    # totalSignals = sum of distress signals (parcels excluded — they
    # are a foundation, not an event of distress).
    total_signals = sum(
        c["count"] for c in categories if c["id"] != "parcels"
    )

    # Discover which sources have actually delivered any data.
    try:
        sources_result = (
            signals_table("distress_events")
            .select("source")
            .limit(10000)
            .execute()
        )
        distinct_sources = {
            row["source"]
            for row in (sources_result.data or [])
            if row.get("source")
        }
    except Exception:
        distinct_sources = set()

    distinct_counties = {
        _SOURCE_TO_COUNTY[src]
        for src in distinct_sources
        if src in _SOURCE_TO_COUNTY
    }

    # Newest observed_at — the "live as of" timestamp.
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

    # Per-county breakdown for the counties currently delivering data.
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


@router.get(
    "/properties",
    status_code=http_status.HTTP_200_OK,
    summary="List distressed properties (filterable, paginated).",
)
async def list_properties(
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
        pattern="^(event_date|event_value|observed_at)$",
        description="Sort field.",
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

        if min_amount is not None:
            query = query.gte("event_value", min_amount)

        if sale_date_from:
            query = query.gte("event_date", sale_date_from)

        if sale_date_to:
            query = query.lte("event_date", sale_date_to)

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
