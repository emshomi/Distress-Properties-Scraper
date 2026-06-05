"""
Server-side Anthropic Claude client — the single entry point for every
LLM feature (NL search, summaries, extraction).

Design principles:
- ONE function, call_claude(), that all features share.
- Fails safe: on any error (missing key, timeout, API failure) it returns
  an LLMResult with ok=False rather than raising into the request path, so
  an LLM problem never 500s a user-facing endpoint. Callers check .ok.
- Every successful call logs token usage + estimated cost to ai.usage_log
  (best-effort: a logging failure never breaks the actual call).
- The API key is read from settings (SecretStr) — never logged, never
  returned to the client.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from anthropic import Anthropic

from src.config import settings
from src.db.supabase_client import get_client
from src.utils.logger import logger


# ------------------------------------------------------------
# Pricing (USD per million tokens) for cost estimation.
# Keep in sync with https://www.anthropic.com/pricing — used only for the
# ai.usage_log ledger, not for billing. Unknown models fall back to Haiku.
# ------------------------------------------------------------
_PRICING_PER_MTOK: dict[str, tuple[float, float]] = {
    # model_id: (input_per_mtok, output_per_mtok)
    "claude-haiku-4-5-20251001": (1.00, 5.00),
    "claude-sonnet-4-6": (3.00, 15.00),
}
_DEFAULT_PRICING = _PRICING_PER_MTOK["claude-haiku-4-5-20251001"]


@dataclass
class LLMResult:
    """Result of an LLM call. Check `ok` before using `text`."""
    ok: bool
    text: str = ""
    model: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    error: Optional[str] = None


_client: Optional[Anthropic] = None


def _get_anthropic() -> Optional[Anthropic]:
    """Lazily build the Anthropic client. Returns None if no key is set."""
    global _client
    if _client is not None:
        return _client
    key = settings.anthropic_api_key
    if key is None:
        logger.error("ANTHROPIC_API_KEY is not set — LLM features disabled")
        return None
    _client = Anthropic(
        api_key=key.get_secret_value(),
        timeout=float(settings.llm_timeout_seconds),
    )
    return _client


def _estimate_cost(model: str, in_tok: int, out_tok: int) -> float:
    in_price, out_price = _PRICING_PER_MTOK.get(model, _DEFAULT_PRICING)
    return round((in_tok / 1_000_000) * in_price + (out_tok / 1_000_000) * out_price, 6)


def _log_usage(feature: str, model: str, in_tok: int, out_tok: int, cost: float) -> None:
    """Best-effort write to ai.usage_log. Never raises into the caller."""
    try:
        get_client().schema("ai").table("usage_log").insert({
            "feature": feature,
            "model": model,
            "input_tokens": in_tok,
            "output_tokens": out_tok,
            "cost_usd": cost,
        }).execute()
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "usage_log write failed (call still succeeded)",
            feature=feature,
            error_type=type(e).__name__,
            error_detail=str(e)[:500],
        )


def call_claude(
    *,
    system: str,
    user: str,
    feature: str,
    max_tokens: Optional[int] = None,
    model: Optional[str] = None,
    temperature: float = 0.0,
) -> LLMResult:
    """
    Make a single Claude call. Fails safe — returns LLMResult(ok=False, ...)
    on any error instead of raising.

    Args:
        system: System prompt (the instructions / role).
        user:   User message (the actual input, e.g. the search query).
        feature: Short label for ai.usage_log (e.g. "nl_search").
        max_tokens: Output cap (defaults to settings.llm_max_tokens).
        model: Model id (defaults to settings.llm_model).
        temperature: 0.0 for deterministic structured output (default).
    """
    client = _get_anthropic()
    if client is None:
        return LLMResult(ok=False, error="anthropic_api_key_missing")

    use_model = model or settings.llm_model
    use_max = max_tokens or settings.llm_max_tokens

    try:
        resp = client.messages.create(
            model=use_model,
            max_tokens=use_max,
            temperature=temperature,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
    except Exception as e:  # noqa: BLE001
        logger.error(
            "claude call failed",
            feature=feature,
            model=use_model,
            error_type=type(e).__name__,
        )
        return LLMResult(ok=False, model=use_model, error=type(e).__name__)

    # Extract text from the response content blocks.
    text_parts = [
        block.text for block in resp.content if getattr(block, "type", None) == "text"
    ]
    text = "".join(text_parts).strip()

    in_tok = getattr(resp.usage, "input_tokens", 0) or 0
    out_tok = getattr(resp.usage, "output_tokens", 0) or 0
    cost = _estimate_cost(use_model, in_tok, out_tok)

    _log_usage(feature, use_model, in_tok, out_tok, cost)

    return LLMResult(
        ok=True,
        text=text,
        model=use_model,
        input_tokens=in_tok,
        output_tokens=out_tok,
        cost_usd=cost,
    )
