"""
Saved searches — an investor's standing criteria, and what's new since they
last looked.

=== WHY THIS EXISTS ===
An investor does not browse. They run the same search over and over:
"Hennepin, single family, equity over $100k, redemption expiring within 90
days." That is a buy box, not a query, and the valuable question is not
"what matches?" but "what STARTED matching since Tuesday?"

Our scrapers run daily; the competing products ship a monthly export. A
badge that says "4 new" is the one thing they structurally cannot do, and
it is the reason someone opens govire on a weekday instead of remembering
it exists in March.

=== THE COUNT IS COMPUTED BY /properties, NOT BY A SECOND QUERY ===
`_count_matches` below calls list_properties() — the real endpoint, the
real filters, the real tier gating — with the saved filter set and
`observed_since=last_viewed_at`.

The alternative was a SQL function mirroring the filter logic. It was
rejected: /properties applies twenty-odd filters, some at the DB level and
some in Python after fetch, and a second implementation would drift from it
within a month. A badge that disagrees with the page it links to is worse
than no badge. One implementation, called twice.

=== last_viewed_at IS SET ON VIEW, NOT ON ALERT ===
Sending the email must not clear the badge. Someone who gets a digest at
7am and opens the app at noon should still see what was new. Only
POST /saved-searches/{id}/viewed moves the marker.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Body, HTTPException, Path, status as http_status
from pydantic import BaseModel, Field

from src.db.supabase_client import saved_table
from src.middleware.tier import TierContext, TierResolved
from src.routes.properties import list_properties
from src.utils.errors import success_envelope
from src.utils.logger import logger


router = APIRouter(tags=["saved-searches"])


# A buy box spans signal types: "Hennepin under $200k" is as true of a tax
# forfeit as of a foreclosure. Empty list means every category.
_VALID_CATEGORIES = {
    "foreclosure",
    "tax_forfeit",
    "vacant",
    "tax_delinquent",
    "tax_assessment",
}

_VALID_FREQUENCIES = {"daily", "weekly", "never"}

# Filter keys accepted from the client. Anything else is DROPPED rather than
# stored, so a typo or a renamed frontend field cannot silently persist a
# filter that /properties will ignore — the saved search would then return
# different rows than the user configured, forever.
# Taken VERBATIM from the list_properties signature on 2026-08-11. Note
# `status_filter`, not `status` — the endpoint aliases it to ?status= for
# the URL but the Python parameter is status_filter, and **filters passes
# Python names. Getting this wrong raises TypeError at count time, which
# _count_matches logs loudly rather than swallowing.
_ALLOWED_FILTER_KEYS = {
    "source", "county", "status_filter", "redemption", "outcome",
    "redeemed", "multi_signal", "min_amount",
    "year_built_min", "year_built_max", "sqft_min", "lot_sqft_min",
    "property_type", "school_district", "price_min", "price_max",
    "sale_date_from", "sale_date_to", "owner_type", "absentee",
    "sort", "order",
}

_MAX_SEARCHES_PER_USER = 25

# TEMPORARY 2026-08-11: last exception from _count_matches, surfaced in the
# list response so the cause is visible. Remove with the debug fields.
_LAST_COUNT_ERROR: Optional[str] = None


class SavedSearchIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    filters: dict[str, Any] = Field(default_factory=dict)
    categories: list[str] = Field(default_factory=list)
    alert_frequency: str = Field(default="daily")


def _require_user(ctx: TierContext) -> str:
    """The user's uuid, or 401.

    TierContext.user_id is populated only on the JWT path — admin-key and
    access-key callers are not user accounts, and anonymous obviously is
    not. A saved search belongs to a person, so those callers are refused
    rather than silently sharing one bucket.
    """
    if not ctx.user_id:
        raise HTTPException(
            status_code=http_status.HTTP_401_UNAUTHORIZED,
            detail={"message": "Sign in to use saved searches."},
        )
    return ctx.user_id


def _clean_filters(raw: dict[str, Any]) -> dict[str, Any]:
    """Keep only keys /properties understands, dropping empties."""
    return {
        k: v for k, v in (raw or {}).items()
        if k in _ALLOWED_FILTER_KEYS and v not in (None, "", [])
    }


def _clean_categories(raw: list[str]) -> list[str]:
    return [c for c in (raw or []) if c in _VALID_CATEGORIES]


async def _count_matches(
    ctx: TierContext,
    filters: dict[str, Any],
    categories: list[str],
    since: Optional[str],
) -> Optional[int]:
    """How many properties match this buy box, optionally only since a time.

    Calls list_properties() directly. Returns None on any failure — a
    missing badge is a cosmetic loss, while a 500 on the list endpoint would
    make every saved search unreadable.

    A buy box with several categories is counted per category and summed. A
    property carrying two signal types is therefore counted twice, which is
    the honest reading of "new things to look at": two separate events did
    appear, and the user will see two rows.
    """
    global _LAST_COUNT_ERROR
    cats: list[Optional[str]] = list(categories) if categories else [None]
    total = 0
    for cat in cats:
        # EVERY parameter is passed explicitly. list_properties is declared
        # with FastAPI Query(default=None) defaults, and calling it as a
        # plain Python function does NOT resolve those — an omitted
        # parameter arrives as a Query object, which is truthy, so the
        # endpoint would try to filter on it.
        #
        # Verified 2026-08-11: calling such a function with 3 of 4
        # parameters leaves the fourth as `Query(None)`, not None. Passing
        # the full set is the only safe way to reuse the endpoint directly.
        call: dict[str, Any] = {k: None for k in _ALLOWED_FILTER_KEYS}
        # sort and order are the two filter keys whose Query default is NOT
        # None — "event_date" and "asc". Passing None for them made
        # list_properties order by nothing, and PostgREST rejected the
        # query: "Failed to fetch properties: APIError", seen live
        # 2026-08-11 while the same filters over HTTP returned 63 rows.
        #
        # The count does not care about order, but the endpoint does, so
        # they get their real defaults.
        call.update({"sort": "event_date", "order": "asc"})
        call.update(filters)
        try:
            res = await list_properties(
                _ctx=ctx,
                category=cat,
                observed_since=since,
                limit=1,
                offset=0,
                **call,
            )
        except TypeError as e:
            _LAST_COUNT_ERROR = f"TypeError: {str(e)[:300]}"
            # A stored filter key that list_properties no longer accepts.
            # Log it loudly: it means _ALLOWED_FILTER_KEYS has drifted from
            # the endpoint signature and saved searches are now wrong.
            logger.warning(
                "saved search filter rejected by /properties",
                error=str(e)[:200],
            )
            return None
        except Exception as e:
            # TEMPORARY DIAGNOSTIC 2026-08-11, same reason as the list
            # endpoint: logger.warning is not reaching Railway's logs, so the
            # exception is stashed on the module for the next response to
            # report. Remove once the cause is known.
            _LAST_COUNT_ERROR = f"{type(e).__name__}: {str(e)[:300]}"
            logger.warning(
                "saved search count failed",
                error_type=type(e).__name__,
            )
            return None
        total += int(((res or {}).get("data") or {}).get("total") or 0)
    return total


@router.get(
    "/saved-searches",
    status_code=http_status.HTTP_200_OK,
    summary="List the caller's saved searches, with new-match counts.",
)
async def list_saved_searches(_ctx: TierContext = TierResolved) -> dict[str, Any]:
    user_id = _require_user(_ctx)

    try:
        result = (
            saved_table("searches")
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .execute()
        )
        rows = result.data or []
    except Exception as e:
        # TEMPORARY DIAGNOSTIC 2026-08-11: the exception text is returned to
        # the caller because logger.warning is not reaching Railway's log
        # stream, and three rounds of guessing produced nothing. Revert to a
        # generic message once the cause is known — an internal error string
        # is not something to leave in a production response.
        logger.warning(
            "saved search list failed", error_type=type(e).__name__
        )
        raise HTTPException(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "message": "Could not load your saved searches.",
                "debug_type": type(e).__name__,
                "debug_error": str(e)[:500],
            },
        ) from e

    out: list[dict[str, Any]] = []
    for r in rows:
        filters = r.get("filters") or {}
        cats = r.get("categories") or []
        out.append({
            "id": r.get("id"),
            "name": r.get("name"),
            "filters": filters,
            "categories": cats,
            "alert_frequency": r.get("alert_frequency"),
            "last_viewed_at": r.get("last_viewed_at"),
            "created_at": r.get("created_at"),
            "total_matches": await _count_matches(_ctx, filters, cats, None),
            "new_matches": await _count_matches(
                _ctx, filters, cats, r.get("last_viewed_at")
            ),
        })

    return success_envelope({
        "searches": out,
        "count": len(out),
        "debug_count_error": _LAST_COUNT_ERROR,
    })


@router.post(
    "/saved-searches",
    status_code=http_status.HTTP_200_OK,
    summary="Create a saved search, or update the one with this name.",
)
async def create_saved_search(
    body: SavedSearchIn = Body(...),
    _ctx: TierContext = TierResolved,
) -> dict[str, Any]:
    user_id = _require_user(_ctx)

    if body.alert_frequency not in _VALID_FREQUENCIES:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail={"message": "Alert frequency must be daily, weekly or never."},
        )

    name = body.name.strip()
    if not name:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail={"message": "Give the search a name."},
        )

    filters = _clean_filters(body.filters)
    categories = _clean_categories(body.categories)

    # Saving twice under one name is an EDIT, not a second buy box — the
    # table's UNIQUE (user_id, name) enforces it, and this makes the
    # behaviour deliberate rather than a constraint violation the user sees.
    try:
        existing = (
            saved_table("searches")
            .select("id")
            .eq("user_id", user_id)
            .eq("name", name)
            .limit(1)
            .execute()
        )
        prior = (existing.data or [None])[0]

        if prior:
            saved_table("searches").update({
                "filters": filters,
                "categories": categories,
                "alert_frequency": body.alert_frequency,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", prior["id"]).eq("user_id", user_id).execute()
            search_id = prior["id"]
        else:
            count_res = (
                saved_table("searches")
                .select("id", count="exact")
                .eq("user_id", user_id)
                .limit(1)
                .execute()
            )
            if (count_res.count or 0) >= _MAX_SEARCHES_PER_USER:
                raise HTTPException(
                    status_code=http_status.HTTP_400_BAD_REQUEST,
                    detail={"message": (
                        f"You can keep up to {_MAX_SEARCHES_PER_USER} saved "
                        "searches. Delete one to add another."
                    )},
                )
            ins = saved_table("searches").insert({
                "user_id": user_id,
                "name": name,
                "filters": filters,
                "categories": categories,
                "alert_frequency": body.alert_frequency,
            }).execute()
            search_id = ((ins.data or [{}])[0]).get("id")
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(
            "saved search write failed", error_type=type(e).__name__
        )
        raise HTTPException(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Could not save that search."},
        ) from e

    return success_envelope({
        "id": search_id,
        "name": name,
        "filters": filters,
        "categories": categories,
        "alert_frequency": body.alert_frequency,
        "updated": bool(prior),
        "total_matches": await _count_matches(_ctx, filters, categories, None),
    })


@router.post(
    "/saved-searches/{search_id}/viewed",
    status_code=http_status.HTTP_200_OK,
    summary="Mark a saved search as seen, clearing its new-match badge.",
)
async def mark_viewed(
    search_id: int = Path(..., ge=1),
    _ctx: TierContext = TierResolved,
) -> dict[str, Any]:
    user_id = _require_user(_ctx)
    now = datetime.now(timezone.utc).isoformat()

    try:
        # user_id in the WHERE clause, not just the lookup: without it a
        # caller could clear another account's badge by guessing an id.
        res = (
            saved_table("searches")
            .update({"last_viewed_at": now})
            .eq("id", search_id)
            .eq("user_id", user_id)
            .execute()
        )
    except Exception as e:
        logger.warning(
            "saved search view-mark failed", error_type=type(e).__name__
        )
        raise HTTPException(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Could not update that search."},
        ) from e

    if not (res.data or []):
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail={"message": "No such saved search."},
        )

    return success_envelope({"id": search_id, "last_viewed_at": now})


@router.delete(
    "/saved-searches/{search_id}",
    status_code=http_status.HTTP_200_OK,
    summary="Delete a saved search.",
)
async def delete_saved_search(
    search_id: int = Path(..., ge=1),
    _ctx: TierContext = TierResolved,
) -> dict[str, Any]:
    user_id = _require_user(_ctx)

    try:
        res = (
            saved_table("searches")
            .delete()
            .eq("id", search_id)
            .eq("user_id", user_id)
            .execute()
        )
    except Exception as e:
        logger.warning(
            "saved search delete failed", error_type=type(e).__name__
        )
        raise HTTPException(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Could not delete that search."},
        ) from e

    if not (res.data or []):
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail={"message": "No such saved search."},
        )

    return success_envelope({"id": search_id, "deleted": True})


__all__ = ["router"]
