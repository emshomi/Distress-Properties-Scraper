"""
Natural-language search — the LLM as a QUERY COMPILER, not an answerer.

Flow:
  English query -> Claude returns ONLY a JSON object of filter values
  -> we validate every field against a strict allowlist (anything Claude
     invents is dropped) -> the caller runs the validated filters through
     the existing /properties query path.

Claude never sees data, never writes SQL, never returns results. It only
picks values for filters that already exist. The validated filter dict and
a plain-English interpretation are returned so the UI can show the user
exactly how their query was understood (full transparency, no black box).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Optional

from src.llm.client import call_claude


# ------------------------------------------------------------
# The ALLOWLIST — the only filters Claude may set, mirroring the
# /properties endpoint exactly. Any key/value outside this is rejected.
# ------------------------------------------------------------
_CATEGORIES = {"foreclosure", "tax_forfeit", "vacant", "tax_delinquent", "tax_assessment"}
_COUNTIES = {"Anoka", "Dakota", "Hennepin", "Ramsey", "Washington", "Scott", "Carver", "Statewide"}
_REDEMPTION = {"in_redemption", "expiring_soon", "expired"}
_STATUS = {"active", "postponed"}
_SORT = {"event_date", "event_value", "observed_at", "equity", "redemption_urgency"}
_ORDER = {"asc", "desc"}

# Common city -> county mapping so "Minneapolis" resolves to Hennepin, etc.
# The endpoint has no city filter, so we map to county and disclose it.
_CITY_TO_COUNTY = {
    "minneapolis": "Hennepin",
    "saint paul": "Ramsey",
    "st paul": "Ramsey",
    "st. paul": "Ramsey",
    "bloomington": "Hennepin",
    "brooklyn park": "Hennepin",
    "plymouth": "Hennepin",
    "maple grove": "Hennepin",
    "edina": "Hennepin",
    "minnetonka": "Hennepin",
    "eagan": "Dakota",
    "burnsville": "Dakota",
    "apple valley": "Dakota",
    "lakeville": "Dakota",
    "woodbury": "Washington",
    "stillwater": "Washington",
    "blaine": "Anoka",
    "coon rapids": "Anoka",
    "shakopee": "Scott",
    "chaska": "Carver",
}


_SYSTEM_PROMPT = """You translate a user's plain-English search for distressed \
Minnesota properties into a JSON filter object. You are a query compiler, not \
an assistant. Output ONLY a single JSON object — no prose, no markdown, no code \
fences.

The JSON may contain ONLY these keys (omit any that don't apply):

- "category": one of ["foreclosure","tax_forfeit","vacant","tax_delinquent","tax_assessment"]
- "county": one of ["Anoka","Dakota","Hennepin","Ramsey","Washington","Scott","Carver"]
- "redemption": one of ["in_redemption","expiring_soon","expired"] (foreclosure redemption window state)
- "multi_signal": an integer 2 or 3 (2 = on 2+ government lists; 3 = on 3+, "triple distress")
- "min_amount": a number (minimum dollar amount)
- "sale_date_from": "YYYY-MM-DD"
- "sale_date_to": "YYYY-MM-DD"
- "status": one of ["active","postponed"]
- "sort": one of ["event_date","event_value","observed_at","equity","redemption_urgency"]
- "order": one of ["asc","desc"]

Rules:
- There is NO city filter. If the user names a city, map it to its county and \
set "county". (e.g. Minneapolis -> Hennepin, Saint Paul -> Ramsey.)
- "expiring soon", "about to expire", "redemption running out" -> redemption=expiring_soon.
- "best deals", "most equity", "cheapest relative to value" -> sort=equity, order=desc.
- "most urgent", "expiring soonest" -> sort=redemption_urgency, order=asc.
- "on multiple lists", "multiple signals", "double distress" -> multi_signal=2.
- "triple distress", "on three lists", "worst" -> multi_signal=3.
- "vacant" or "condemned" -> category=vacant.
- "tax forfeited", "forfeit" -> category=tax_forfeit.
- "behind on taxes", "tax delinquent" -> category=tax_delinquent.
- "foreclosure", "sheriff sale", "foreclosed" -> category=foreclosure.
- If a constraint can't be expressed with the keys above, OMIT it (never invent keys).
- If the query is empty or unclear, return {}.

Return ONLY the JSON object."""


@dataclass
class NLSearchPlan:
    """Result of compiling an English query into validated filters."""
    ok: bool
    filters: dict[str, Any] = field(default_factory=dict)
    interpretation: str = ""
    notes: list[str] = field(default_factory=list)  # disclosures (e.g. city->county)
    raw_model_output: str = ""
    error: Optional[str] = None


def _strip_fences(text: str) -> str:
    """Remove ```json fences if the model added them despite instructions."""
    t = text.strip()
    if t.startswith("```"):
        t = t.split("```", 2)
        # ['', 'json\n{...}\n', ''] or ['', '{...}', '']
        if len(t) >= 2:
            body = t[1]
            if body.lstrip().lower().startswith("json"):
                body = body.lstrip()[4:]
            return body.strip()
    return t


def _validate(raw_obj: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Keep ONLY allowlisted keys with valid values. Returns (filters, notes).
    Anything the model invented or got wrong is silently dropped — the filter
    can only ever be a subset of what the real endpoint accepts."""
    out: dict[str, Any] = {}
    notes: list[str] = []

    cat = raw_obj.get("category")
    if isinstance(cat, str) and cat in _CATEGORIES:
        out["category"] = cat

    county = raw_obj.get("county")
    if isinstance(county, str) and county in _COUNTIES:
        out["county"] = county

    red = raw_obj.get("redemption")
    if isinstance(red, str) and red in _REDEMPTION:
        out["redemption"] = red

    ms = raw_obj.get("multi_signal")
    if isinstance(ms, int) and 2 <= ms <= 5:
        out["multi_signal"] = ms

    amt = raw_obj.get("min_amount")
    if isinstance(amt, (int, float)) and amt >= 0:
        out["min_amount"] = float(amt)

    for dk in ("sale_date_from", "sale_date_to"):
        dv = raw_obj.get(dk)
        if isinstance(dv, str) and len(dv) == 10 and dv[4] == "-" and dv[7] == "-":
            out[dk] = dv

    st = raw_obj.get("status")
    if isinstance(st, str) and st in _STATUS:
        out["status"] = st

    sort = raw_obj.get("sort")
    if isinstance(sort, str) and sort in _SORT:
        out["sort"] = sort

    order = raw_obj.get("order")
    if isinstance(order, str) and order in _ORDER:
        out["order"] = order

    return out, notes


def _describe(filters: dict[str, Any], notes: list[str]) -> str:
    """Plain-English description of what we're actually searching, so the user
    sees exactly how their query was interpreted."""
    if not filters:
        return "Showing all properties (no specific filters applied)."

    parts: list[str] = []
    cat_label = {
        "foreclosure": "foreclosures",
        "tax_forfeit": "tax-forfeited properties",
        "vacant": "vacant / condemned properties",
        "tax_delinquent": "tax-delinquent properties",
        "tax_assessment": "special-assessment properties",
    }
    parts.append(cat_label.get(filters.get("category", ""), "properties"))

    if "county" in filters:
        parts.append(f"in {filters['county']} County")
    if filters.get("multi_signal") == 3:
        parts.append("on 3+ government lists (triple-distress)")
    elif filters.get("multi_signal") == 2:
        parts.append("on 2+ government lists")
    if "redemption" in filters:
        rl = {
            "in_redemption": "still in their redemption window",
            "expiring_soon": "with redemption expiring soon",
            "expired": "with an expired redemption window",
        }
        parts.append(rl.get(filters["redemption"], ""))
    if "min_amount" in filters:
        parts.append(f"of at least ${int(filters['min_amount']):,}")
    if "sale_date_from" in filters:
        parts.append(f"sold on/after {filters['sale_date_from']}")
    if "sale_date_to" in filters:
        parts.append(f"sold on/before {filters['sale_date_to']}")
    if filters.get("sort") == "equity":
        parts.append("ranked by estimated equity")
    elif filters.get("sort") == "redemption_urgency":
        parts.append("ranked by redemption urgency")

    desc = "Searching: " + " ".join(p for p in parts if p).strip() + "."
    return desc


def compile_query(text: str) -> NLSearchPlan:
    """Turn an English query into a validated filter plan. Fails safe."""
    q = (text or "").strip()
    if not q:
        return NLSearchPlan(ok=True, filters={}, interpretation="Empty query — showing all.")

    # Pre-map any city mention to a county note (Claude also does this, but we
    # disclose it explicitly so the user knows we broadened city -> county).
    lower = q.lower()
    city_note: Optional[str] = None
    for city, cty in _CITY_TO_COUNTY.items():
        if city in lower:
            city_note = (
                f"No city-level filter exists yet, so '{city.title()}' was "
                f"broadened to {cty} County."
            )
            break

    result = call_claude(
        system=_SYSTEM_PROMPT,
        user=q,
        feature="nl_search",
        max_tokens=300,
    )

    if not result.ok:
        return NLSearchPlan(ok=False, error=result.error or "llm_unavailable")

    cleaned = _strip_fences(result.raw_model_output if False else result.text)
    try:
        obj = json.loads(cleaned)
        if not isinstance(obj, dict):
            raise ValueError("not a JSON object")
    except Exception:
        return NLSearchPlan(
            ok=False,
            raw_model_output=result.text,
            error="could_not_parse_filters",
        )

    filters, notes = _validate(obj)
    if city_note:
        notes.append(city_note)

    return NLSearchPlan(
        ok=True,
        filters=filters,
        interpretation=_describe(filters, notes),
        notes=notes,
        raw_model_output=result.text,
    )
