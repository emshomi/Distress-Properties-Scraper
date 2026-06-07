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
from src.llm.foreclosure_extraction import extract_foreclosure_notice
from src.llm.foreclosure_extraction import extract_foreclosure_notice
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

# ============================================================
# POST /ai/summary — plain-English summary of one property
# ============================================================


class SummaryBody(BaseModel):
    """Request body for a single-property summary. Identifies the property by
    its natural key (source, source_id) — the same key /properties uses."""
    source: str = Field(..., max_length=100)
    source_id: str = Field(..., max_length=200)


@router.post(
    "/summary",
    status_code=http_status.HTTP_200_OK,
    summary="Plain-English summary of one property (fact-constrained).",
)
async def ai_summary(
    body: SummaryBody,
    _access_key: str = Depends(require_access_key),
) -> dict[str, Any]:
    """Fetch one property by (source, source_id), shape it through the same
    path the table uses, then produce a plain-English summary that can only
    restate facts present in the row (no fabrication — see property_summary).

    Fails honestly: if the property isn't found we 404; if the LLM is
    unavailable we return ok=False with an explanation rather than inventing
    a summary."""
    from src.routes.properties import (
        _load_overlay_map,
        _load_owner_map,
        _shape_property_row,
    )
    from src.db.supabase_client import signals_table

    # Fetch the single row by its natural key.
    try:
        result = (
            signals_table("distress_events")
            .select(
                "source_id, source, parcel_id, event_type, event_date, "
                "event_value, severity, title, description, raw_data, "
                "observed_at"
            )
            .eq("source", body.source)
            .eq("source_id", body.source_id)
            .limit(1)
            .execute()
        )
        rows = result.data or []
    except Exception as e:
        logger.exception(
            "ai_summary: property fetch failed",
            error_type=type(e).__name__,
        )
        return _envelope({
            "ok": False,
            "error": "fetch_failed",
            "summary": None,
        })

    if not rows:
        return _envelope({
            "ok": False,
            "error": "not_found",
            "summary": None,
        })

    # Shape it (attaches overlay + owner portfolio), then summarize.
    overlay_map = _load_overlay_map()
    owner_map = _load_owner_map()
    shaped = _shape_property_row(rows[0], overlay_map, owner_map)
    shaped.pop("_eff_key", None)

    summary = summarize_property(shaped)

    if not summary.ok:
        return _envelope({
            "ok": False,
            "error": summary.error,
            "summary": None,
        })

    return _envelope({
        "ok": True,
        "summary": summary.summary,
        "source": body.source,
        "source_id": body.source_id,
    })

# ============================================================
# POST /ai/extract — extract one foreclosure notice and store it
# in ai.extracted_foreclosures (pending human review).
# ============================================================


class ExtractBody(BaseModel):
    """A single foreclosure-notice to extract and stage for review."""
    notice_text: str = Field(..., max_length=20000)
    source_url: str = Field(..., max_length=1000)
    source_name: str = Field(default="manual", max_length=100)
    store: bool = Field(
        default=True,
        description="If false, extract and return without writing to the DB "
        "(dry run for testing).",
    )


@router.post("/extract", status_code=http_status.HTTP_200_OK)
async def ai_extract(
    body: ExtractBody,
    _access_key: str = Depends(require_access_key),
) -> dict[str, Any]:
    """Extract structured fields from one foreclosure notice and (by default)
    insert the result into ai.extracted_foreclosures with review_status
    'pending'. Nothing here reaches the live site until a human approves it.

    Idempotent: the table's UNIQUE(source_url) plus ON CONFLICT DO NOTHING
    means re-posting the same notice URL won't create a duplicate.

    Fails honestly: if extraction fails we return ok=False with the raw model
    output for debugging, and we do NOT write a half-baked row."""
    extraction = extract_foreclosure_notice(body.notice_text)

    if not extraction.ok:
        return _envelope({
            "ok": False,
            "error": extraction.error,
            "raw_output": extraction.raw_output,
            "stored": False,
        })

    # Dry-run path: return the parsed result without storing.
    if not body.store:
        return _envelope({
            "ok": True,
            "stored": False,
            "data": extraction.data,
            "model": extraction.model,
        })

    # Build the row: extracted fields + provenance + audit.
    from src.db.supabase_client import ai_table

    row = dict(extraction.data)  # statutory + confidence + extraction_notes
    row["source_url"] = body.source_url
    row["source_name"] = body.source_name
    row["raw_notice_text"] = body.notice_text
    row["model"] = extraction.model
    # review_status defaults to 'pending' in the table; fetched_at defaults now().

    try:
        result = (
            ai_table("extracted_foreclosures")
            .upsert(row, on_conflict="source_url", ignore_duplicates=True)
            .execute()
        )
        inserted = bool(result.data)
        stored_id = result.data[0]["id"] if inserted else None
    except Exception as e:
        logger.exception(
            "ai_extract: insert failed",
            error_type=type(e).__name__,
        )
        # Extraction succeeded; storage failed. Return the data so the work
        # isn't lost, but be honest that it wasn't stored.
        return _envelope({
            "ok": True,
            "stored": False,
            "store_error": "insert_failed",
            "data": extraction.data,
            "model": extraction.model,
        })

    return _envelope({
        "ok": True,
        "stored": inserted,
        "duplicate": not inserted,  # already had this source_url
        "id": stored_id,
        "confidence": extraction.data.get("confidence"),
        "review_status": "pending",
        "data": extraction.data,
        "model": extraction.model,
    })
  
__all__ = ["router"]
