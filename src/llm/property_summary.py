"""
LLM property summary (Anthropic Claude).

Turns one shaped property row into a short, plain-English paragraph for a
county staff member or other reader — a human-readable digest of the distress
signals instead of reading across a dozen columns.

ANTI-FABRICATION DISCIPLINE (the whole point):
We never hand Claude a blank prompt and hope. We build a "facts block" from
ONLY the non-null fields of the property row, hand Claude exactly those
labeled facts, and instruct it to use only what's provided — never infer,
estimate, or add anything. Null fields are simply omitted from the block, so
Claude never sees them and cannot reference or invent them. The summary can
only ever restate facts that are actually in govire's data.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from src.llm.client import call_claude
from src.utils.logger import logger


@dataclass
class PropertySummary:
    """Result of a property-summary request. Fails safe (ok=False) — never
    raises into the request path."""
    ok: bool
    summary: Optional[str] = None
    error: Optional[str] = None


_SYSTEM_PROMPT = (
    "You write a single short, plain-English paragraph summarizing one "
    "distressed property, for a county staff member or member of the public. "
    "Follow these rules exactly:\n"
    "- Use ONLY the facts provided in the user message. Do not infer, "
    "estimate, assume, or add ANY information that is not explicitly given.\n"
    "- If a fact is not provided, do not mention it and do not speculate "
    "about it. Never write phrases like 'no information available'.\n"
    "- State plainly what the public records show. No marketing language, no "
    "adjectives like 'great deal' or 'opportunity', no advice.\n"
    "- Do not invent owner names, dollar amounts, dates, or addresses. Only "
    "use the exact values given.\n"
    "- Keep it to 2-4 sentences. Write for clarity, not persuasion.\n"
    "- If the facts include that the property appears on multiple government "
    "distress lists, mention that cross-signal overlap plainly, since it is "
    "the most notable fact."
)

# Human-readable labels for signal families (matches the frontend).
_FAMILY_LABEL = {
    "foreclosure": "foreclosure",
    "vacant_condemned": "vacant/condemned",
    "tax_delinquent": "tax-delinquent",
    "tax_forfeit": "tax-forfeit",
    "special_assessment": "special assessment",
}

_OWNER_TYPE_LABEL = {
    "individual": "an individual",
    "llc_business": "an LLC / business",
    "bank_lender": "a bank / lender",
    "government": "a government entity",
}


def _build_facts(p: dict[str, Any]) -> list[str]:
    """Build a list of 'Label: value' fact lines from ONLY the non-null
    fields of the shaped property row. Anything missing is omitted entirely,
    so the model never sees it. This list IS the full universe of things the
    summary may state."""
    facts: list[str] = []

    def add(label: str, value: Any) -> None:
        if value is None:
            return
        s = str(value).strip()
        if not s or s.lower() == "null":
            return
        facts.append(f"{label}: {s}")

    # Location
    add("Address", p.get("address"))
    add("City", p.get("city"))
    add("County", p.get("county"))

    # What kind of distress signal this row is
    add("Signal type", p.get("event_type"))
    add("Status", p.get("status"))

    # Owner + portfolio context
    if p.get("redemption_state") == "resolved":
        add(
            "Current owner of record",
            (p.get("owner") or "")
            + " (post-resolution owner of record — likely the BUYER, not "
            "the foreclosed owner; do not describe them as having redeemed)",
        )
    else:
        add("Owner of record", p.get("owner"))
    portfolio = p.get("owner_portfolio") or {}
    if portfolio:
        otype = _OWNER_TYPE_LABEL.get(portfolio.get("owner_type"))
        if otype:
            add("Owner type", otype)
        pc = portfolio.get("parcel_count")
        if isinstance(pc, int) and pc >= 2:
            add(
                "Owner portfolio",
                f"this owner holds {pc} distressed properties in govire's data",
            )

    add("Owner mailing address", p.get("owner_mailing"))
    if p.get("is_absentee") is True:
        add("Occupancy flag", "non-homestead (not owner-occupied per county flag)")
    add("Homestead code", p.get("homestead"))

    # Money
    add("Sale / bid amount", _money(p.get("amount")))
    add("Market value", _money(p.get("market_value")))
    add("Annual tax", _money(p.get("annual_tax")))
    add("Special assessment due", _money(p.get("special_assessment_due")))

    # Dates
    add("Sale date", p.get("sale_date"))
    add("Registered/observed date", p.get("registered_date"))

    # Redemption lifecycle (foreclosure rows). Resolved cases get the
    # OUTCOME, never a countdown — narrating "days remaining" for a
    # foreclosure that already ended is fabrication by staleness.
    rstate = p.get("redemption_state")
    if rstate == "resolved":
        outcome_label = p.get("redemption_outcome_label")
        add(
            "Redemption status",
            "RESOLVED — this foreclosure has concluded"
            + (f": {outcome_label}" if outcome_label else ""),
        )
        add("Redemption window end date", p.get("redemption_ends_at"))
        price = p.get("redemption_resale_price")
        if price is not None:
            add("Confirmed sale price", _money(price) or str(price))
        add("Confirmed sale date", p.get("redemption_resale_date"))
        add(
            "Outcome note",
            "the outcome is confirmed from county records and state deed "
            "filings — describe the property as resolved, not as an active "
            "opportunity, and do NOT mention days remaining",
        )
    elif rstate == "outcome_pending":
        add(
            "Redemption status",
            "redemption window has CLOSED; the final outcome (redeemed vs "
            "foreclosed) is still being confirmed from county records",
        )
        add("Redemption window end date", p.get("redemption_ends_at"))
        add(
            "Outcome note",
            "do NOT describe a countdown or days remaining — the window is "
            "over; say the resolution is pending confirmation",
        )
    else:
        if rstate:
            label = {
                "in_redemption": "in redemption",
                "expiring_soon": "redemption expiring soon",
                "expired": "redemption period expired",
            }.get(rstate, rstate)
            add("Redemption status", label)
        # Deal math (open windows only; premium payloads). Every number
        # arrives with its sample size — pass that honesty through.
        dm = p.get("deal_math") or {}
        if dm:
            add("Payoff floor", _money(dm.get("payoff_floor")))
            add(
                "Estimated market value",
                (_money(dm.get("est_market_value")) or "")
                + f" (assessed value calibrated by {dm.get('ratio_n')} recent "
                f"{dm.get('ratio_scope')} sales)",
            )
            add(
                "Confirmed in-window sale band",
                f"{_money(dm.get('inwindow_band_low'))} to "
                f"{_money(dm.get('inwindow_band_high'))} "
                f"(from {dm.get('inwindow_n')} confirmed sales during "
                "redemption windows)",
            )
            add(
                "Deal math note",
                "these are calibrated estimates with sample sizes, not an "
                "appraisal; the payoff floor reflects the foreclosing debt "
                "only and other liens may apply",
            )
        add("Redemption ends", p.get("redemption_ends_at"))
        days = p.get("redemption_days_left")
        if isinstance(days, int) and days >= 0:
            add("Days left in redemption", str(days))
        if p.get("redemption_is_estimated") is True:
            add(
                "Redemption note",
                "redemption date is estimated (sale + ~6 months), not "
                "county-published",
            )

    # Multi-signal overlay — the cross-reference that is govire's differentiator
    overlay = p.get("overlay") or {}
    if overlay:
        count = overlay.get("distinct_signal_count")
        if isinstance(count, int) and count >= 2:
            fams = overlay.get("signal_families") or []
            labels = [_FAMILY_LABEL.get(f, f) for f in fams]
            joined = ", ".join(labels)
            add(
                "Cross-signal overlap",
                f"appears on {count} independent government distress lists "
                f"({joined})",
            )

    return facts


def _money(value: Any) -> Optional[str]:
    """Format a numeric amount as $X,XXX. Returns None if not numeric."""
    if value is None:
        return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return f"${round(n):,}"


def summarize_property(shaped: dict[str, Any]) -> PropertySummary:
    """Produce a plain-English summary of one shaped property row. The summary
    can only restate facts present in the row — see module docstring. Fails
    safe: returns ok=False on any LLM problem rather than raising."""
    facts = _build_facts(shaped)

    if not facts:
        # Nothing to summarize — honest, don't fabricate a sentence.
        return PropertySummary(
            ok=False,
            error="no_facts",
        )

    user_msg = (
        "Summarize this property using ONLY these facts. Do not add anything "
        "not listed here:\n\n" + "\n".join(facts)
    )

    result = call_claude(
        system=_SYSTEM_PROMPT,
        user=user_msg,
        feature="property_summary",
        max_tokens=250,
    )

    if not result.ok:
        logger.warning("property summary LLM call failed", error=result.error)
        return PropertySummary(ok=False, error=result.error or "llm_failed")

    text = (result.text or "").strip()
    if not text:
        return PropertySummary(ok=False, error="empty_summary")

    return PropertySummary(ok=True, summary=text)


__all__ = ["summarize_property", "PropertySummary"]
