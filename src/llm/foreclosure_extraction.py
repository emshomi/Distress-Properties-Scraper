"""
LLM extraction of Minnesota mortgage-foreclosure-sale notices.

Takes the raw text of a published "Notice of Mortgage Foreclosure Sale" and
returns its structured statutory fields (mortgagor, mortgagee, property,
sale date, amounts, redemption, attorney) as a dict keyed exactly like the
ai.extracted_foreclosures table, ready to insert.

DISCIPLINE (same as the rest of the AI layer):
- The model is told to use ONLY what the notice states and to null anything
  absent — never guess. It self-rates a confidence and flags anything unusual
  (assignment chains, lien type, missing fields) for the human reviewer.
- We do NOT trust the model's formatting. Numbers and dates are defensively
  coerced in Python (the model may quote a number, include a "$", or vary
  spacing), so the result is always clean regardless of how the model wrote it.
- Fails safe: any LLM failure or unparseable output returns ok=False with the
  raw output preserved for debugging. Never raises into the caller.

Proven against a real Scott County notice (MERS->...->MidFirst assignment
chain) at ~0.95 confidence; the model correctly resolved the final assignee.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from src.llm.client import call_claude
from src.utils.logger import logger


# The 18 keys we extract — identical to the ai.extracted_foreclosures
# statutory + governance columns (minus provenance/audit, which the store
# step fills in). Order is for readability only.
_STRING_FIELDS = (
    "mortgagor",
    "mortgagee",
    "property_address",
    "city",
    "county",
    "parcel_id",
    "legal_description",
    "sale_time",
    "sale_location",
    "redemption_period",
    "attorney_firm",
    "attorney_file_no",
    "extraction_notes",
)
_NUMBER_FIELDS = ("original_principal", "amount_due")
_DATE_FIELDS = ("sale_date", "vacate_date")


@dataclass
class ForeclosureExtraction:
    """Result of extracting one notice. `data` is keyed exactly like the
    ai.extracted_foreclosures statutory/governance columns when ok=True."""
    ok: bool
    data: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    raw_output: Optional[str] = None
    model: Optional[str] = None


_SYSTEM_PROMPT = (
    "You extract structured data from Minnesota mortgage foreclosure sale "
    "notices (published 'Notice of Mortgage Foreclosure Sale' legal notices).\n\n"
    "Return ONLY a single JSON object — no prose, no markdown, no code "
    "fences — with EXACTLY these keys:\n"
    '{"mortgagor","mortgagee","property_address","city","county",'
    '"parcel_id","legal_description","original_principal","amount_due",'
    '"sale_date","sale_time","sale_location","redemption_period",'
    '"vacate_date","attorney_firm","attorney_file_no","confidence",'
    '"extraction_notes"}\n\n'
    "RULES:\n"
    "- Use ONLY information explicitly stated. If a field is not present, use "
    "null. NEVER guess, infer, or fabricate.\n"
    "- mortgagor = the borrower being foreclosed on (labeled 'MORTGAGOR(S)').\n"
    "- mortgagee = the CURRENT holder/assignee foreclosing now. If there is an "
    "assignment chain, use the FINAL assignee, and record the full chain in "
    "extraction_notes.\n"
    "- attorney_file_no: a notice often begins or ends with a bare reference "
    "or file number (e.g. 24-117341) that is the attorney/trustee file number, "
    "even when it is not explicitly labeled. Capture it if present.\n"
    "- Dates in YYYY-MM-DD. A partial or ambiguous date -> null, explained in "
    "extraction_notes.\n"
    "- Money as plain numbers: 210895.10 (strip $, commas, and words).\n"
    "- redemption_period: copy the stated period as text, e.g. '6 months'.\n"
    "- confidence: a number 0.0-1.0 for how cleanly this notice mapped to the "
    "fields. Lower it for unusual notices (a condominium-association "
    "assessment lien rather than a mortgage, a missing property address, an "
    "ambiguous party).\n"
    "- extraction_notes: briefly note anything a human reviewer should check "
    "(assignment chain, lien type, missing fields). null if nothing notable.\n"
    "- Output the JSON object and nothing else."
)


def _strip_fences(text: str) -> str:
    """Remove ```json ... ``` fences if the model wrapped its output in them
    (it sometimes does, despite instructions)."""
    t = text.strip()
    if t.startswith("```"):
        # drop first line (``` or ```json) and a trailing ```
        t = re.sub(r"^```[a-zA-Z]*\n?", "", t)
        t = re.sub(r"\n?```$", "", t)
    return t.strip()


def _to_number(v: Any) -> Optional[float]:
    """Coerce a money/number value to float regardless of how the model wrote
    it: 202020.0 (number), '202020.00' (string), or '$202,020.00' (formatted).
    Returns None if absent or unparseable — never raises."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s or s.lower() == "null":
        return None
    s = re.sub(r"[,$\s]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def _to_date(v: Any) -> Optional[str]:
    """Validate that a value is a real YYYY-MM-DD date and return it as a
    string (Supabase date column accepts ISO strings). The model is asked to
    pre-convert; this just confirms it didn't hand us garbage. None on failure."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "null":
        return None
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        return None


def _clean_str(v: Any) -> Optional[str]:
    """Trim a string field; None if empty or the literal 'null'."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "null":
        return None
    return s


def _to_confidence(v: Any) -> float:
    """Coerce confidence to a float clamped to [0, 1]. Defaults to 0.0 (treat
    as needs-review) if missing or unparseable, so a malformed confidence can
    never masquerade as high."""
    n = _to_number(v)
    if n is None:
        return 0.0
    return max(0.0, min(1.0, n))


def extract_foreclosure_notice(notice_text: str) -> ForeclosureExtraction:
    """Extract structured fields from one foreclosure-notice text. Returns a
    ForeclosureExtraction; on success, .data is keyed exactly like the
    ai.extracted_foreclosures statutory/governance columns. Fails safe."""
    text = (notice_text or "").strip()
    if not text:
        return ForeclosureExtraction(ok=False, error="empty_notice_text")

    result = call_claude(
        system=_SYSTEM_PROMPT,
        user=text,
        feature="foreclosure_extraction",
        max_tokens=1000,
    )

    if not result.ok:
        logger.warning("foreclosure extraction LLM call failed", error=result.error)
        return ForeclosureExtraction(
            ok=False,
            error=result.error or "llm_failed",
            model=result.model,
        )

    raw = result.text or ""
    cleaned = _strip_fences(raw)

    try:
        parsed = json.loads(cleaned)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(
            "foreclosure extraction returned unparseable JSON",
            error=str(e),
        )
        return ForeclosureExtraction(
            ok=False,
            error="unparseable_json",
            raw_output=raw,
            model=result.model,
        )

    if not isinstance(parsed, dict):
        return ForeclosureExtraction(
            ok=False,
            error="not_a_json_object",
            raw_output=raw,
            model=result.model,
        )

    # Build the clean, coerced record keyed like the table columns.
    data: dict[str, Any] = {}
    for k in _STRING_FIELDS:
        data[k] = _clean_str(parsed.get(k))
    for k in _NUMBER_FIELDS:
        data[k] = _to_number(parsed.get(k))
    for k in _DATE_FIELDS:
        data[k] = _to_date(parsed.get(k))
    data["confidence"] = _to_confidence(parsed.get("confidence"))

    return ForeclosureExtraction(
        ok=True,
        data=data,
        raw_output=raw,
        model=result.model,
    )


__all__ = ["extract_foreclosure_notice", "ForeclosureExtraction"]
