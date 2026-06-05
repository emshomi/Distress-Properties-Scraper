"""
AI / LLM endpoints (Anthropic Claude).

Currently just a health-check ping used to verify the LLM chain end to end
(key -> SDK -> call -> usage_log) before NL search is built on top.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, status as http_status

from src.llm.client import call_claude
from src.utils.logger import logger

router = APIRouter(prefix="/ai", tags=["ai"])


def _envelope(data: Any) -> dict[str, Any]:
    return {"success": True, "data": data}


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


__all__ = ["router"]
