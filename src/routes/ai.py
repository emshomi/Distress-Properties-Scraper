"""
AI / LLM endpoints (Anthropic Claude).

  GET  /ai/ping    — health check for the LLM chain
  POST /ai/search  — natural-language property search (LLM as query compiler)
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, status as http_status
from pydantic import BaseModel, Field

from src.llm.client import call_claude
from src.llm.nl_search import compile_query
from src.llm.property_summary import summarize_property
from src.routes.properties import list_properties, require_access_key
from src.utils.logger import logger

router = APIRouter(prefix="/ai", tags=["ai"])


def _envelope(data: Any) -> dict[str, Any]:
    return {"success": True, "data": data}


# ============================================================
# GET /ai/ping
# ============================================================


@router.get(
    "/ping",
    status_code=http_status.HTTP_200_OK,
    summary="Verify the LLM chain works (trivial Claude call).",
)
async def ai_ping() -> dict[str, Any]:
    """Make a trivial Claude call and report the result. Public, no key — it's
    a diagnostic. Returns whether the call succeeded plus token/cost info so we
    can confirm usage_log is being fed. Safe: call_claude never raises."""
    result = call_claude(
        system="You are a connectivity test. Reply with exactly: OK",
        user="ping",
        feature="ping",
        max_tokens=10,
    )

    if not result.ok:
        logger.error("ai_ping failed", error=result.error)
        return _envelope({
            "llm_ok": False,
            "error": result.error,
            "hint": (
                "Check ANTHROPIC_API_KEY in Railway and that Anthropic "
                "billing is enabled."
            ),
        })

    return _envelope({
        "llm_ok": True,
        "reply": result.text,
        "model": result.model,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "cost_usd": result.cost_usd,
    })


# ============================================================
# POST /ai/search — natural-language property search
# ============================================================


class SearchBody(BaseModel):
    """Request body for NL search."""
    query: str = Field(..., max_length=500, description="Plain-English search.")
    limit: int = Field(default=50, ge=1, le=200)
    offset: int = Field(default=0, ge=0)


@router.post(
    "/search",
    status_code=http_status.HTTP_200_OK,
    summary="Natural-language property search (LLM compiles English -> filters).",
)
async def ai_search(
    body: SearchBody,
    access_key: str = Depends(require_access_key),
) -> dict[str, Any]:
    """Compile the user's English query into validated filters (Claude as a
    query compiler — it can only set values for filters that already exist),
    then run those filters through the SAME list_properties path the manual
    filter UI uses. Returns the results plus a transparent interpretation so
    the user sees exactly how their query was understood.

    Claude never sees data, never writes SQL, never returns results — it only
    picks filter values, which are validated against a strict allowlist before
    anything touches the database."""

    plan = compile_query(body.query)

    if not plan.ok:
        # LLM unavailable or unparseable. Fail honestly — don't silently
        # return everything as if the search worked.
        return _envelope({
            "ok": False,
            "error": plan.error,
            "interpretation": (
                "Couldn't interpret that search right now. Try rephrasing, "
                "or use the filters above."
            ),
            "filters": {},
            "notes": [],
            "properties": [],
            "total": 0,
        })

    f = plan.filters

    # Run the validated filters through the existing properties query path.
    # Every value in `f` has already been checked against the allowlist in
    # nl_search._validate, so these are all safe, known parameters.
    try:
        result = await list_properties(
            _access_key=access_key,
            category=f.get("category"),
            source=None,
            county=f.get("county"),
            status_filter=f.get("status"),
            redemption=f.get("redemption"),
            multi_signal=f.get("multi_signal"),
            min_amount=f.get("min_amount"),
            sale_date_from=f.get("sale_date_from"),
            sale_date_to=f.get("sale_date_to"),
            sort=f.get("sort", "event_date"),
            order=f.get("order", "asc"),
            limit=body.limit,
            offset=body.offset,
        )
    except Exception as e:
        logger.exception(
            "ai_search: properties query failed",
            error_type=type(e).__name__,
        )
        return _envelope({
            "ok": False,
            "error": "query_failed",
            "interpretation": plan.interpretation,
            "filters": f,
            "notes": plan.notes,
            "properties": [],
            "total": 0,
        })

    # list_properties returns a success envelope: {"success": True, "data": {...}}
    inner = result.get("data", {}) if isinstance(result, dict) else {}

    return _envelope({
        "ok": True,
        "interpretation": plan.interpretation,
        "filters": f,
        "notes": plan.notes,
        "properties": inner.get("properties", []),
        "total": inner.get("total", 0),
        "limit": inner.get("limit", body.limit),
        "offset": inner.get("offset", body.offset),
    })


__all__ = ["router"]
