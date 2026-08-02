"""
Promotion of an approved ai.extracted_foreclosures row into the live signals
tables (signals.distress_events + signals.sheriff_sales), plus the core.parcels
row their foreign keys depend on.

This module does the TRANSFORMATION only — it builds the target rows from an
extracted-foreclosure record. The actual DB writes live in the endpoint, so
this logic stays pure and testable.

Rows are written OVERLAY-READY: the real parcel PID goes to
raw_data.detail.gis_pid (the path signals.parcel_distress_overlay already reads
for sheriff rows) and the lowercase county to raw_data.detail.county (which a
follow-up view edit will read so statewide/extracted rows can resolve their
county and participate in the multi-signal overlay).

These notices are SCHEDULED (future) sheriff sales, not completed ones, so they
are labeled event_subtype='scheduled' and worded accordingly — never presented
as a sale that already happened.

FK chain (why a parcel row is built first):
  signals.distress_events.parcel_id -> core.parcels.parcel_id
  core.parcels.county_code           -> core.counties.county_code  (lowercase slug)
So promotion must (1) ensure the parcel exists with a valid county_code slug,
then (2) insert distress_events. county_code uses the core.counties slug format
('scott', 'st_louis', 'otter_tail'), NOT the title-case county name.
"""

from __future__ import annotations

import re
from typing import Any, Optional


# The extraction LLM takes the county from free notice text, so it arrives
# worded however that particular notice worded it — "Washington" in one,
# "Washington County" in the next. Every downstream use (slug, synthetic
# PID, display text) must see the bare name.
_COUNTY_SUFFIX_RE = re.compile(r"\s*\bcount(?:y|ies)\b\s*$", re.IGNORECASE)


def _county_bare(county: Optional[str]) -> Optional[str]:
    """Strip a trailing 'County' from an extracted county name.

    ADDED 2026-07-28: without this, 'Washington County' slugged to
    'washington_county' and created a duplicate core.counties key alongside
    the real 'washington' — verified live: ramsey_county, st_louis_county,
    washington_county and polk_county each held one orphaned parcel. It also
    produced 'Scheduled Washington County County sheriff sale' in the
    public-facing description.
    """
    if not county:
        return None
    cleaned = _COUNTY_SUFFIX_RE.sub("", str(county)).strip()
    return cleaned or None


def _county_upper(county: Optional[str]) -> str:
    return (_county_bare(county) or "UNKNOWN").strip().upper()


def _county_lower(county: Optional[str]) -> str:
    return (_county_bare(county) or "unknown").strip().lower()


# County spelling variants that must collapse to the seeded core.counties slug.
# The extraction LLM writes counties inconsistently ("St. Louis" vs
# "Saint Louis"); both must map to the seeded code 'st_louis'.
_COUNTY_SLUG_ALIASES = {
    "saint_louis": "st_louis",
    "st_louis": "st_louis",
}


def _county_slug(county: Optional[str]) -> Optional[str]:
    """Convert a county name to the core.counties county_code slug: lowercase,
    non-alphanumeric collapsed to a single underscore. Matches the seeded
    values exactly: 'Scott' -> 'scott', 'St. Louis' -> 'st_louis',
    'Saint Louis' -> 'st_louis', 'Otter Tail' -> 'otter_tail'. Returns None if
    no county given."""
    county = _county_bare(county)
    if not county:
        return None
    s = str(county).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return None
    # Collapse known spelling variants to the seeded slug.
    return _COUNTY_SLUG_ALIASES.get(s, s)

def _money_str(value: Any) -> str:
    """Format a number as $X,XXX for human-readable title/description text.
    Returns '—' when absent (we never fabricate a number)."""
    if value is None:
        return "—"
    try:
        return f"${round(float(value)):,}"
    except (TypeError, ValueError):
        return "—"


# Redemption-period parsing. Notices state the period in wildly varied
# wording, and the period is the single most consequential field we extract:
# it determines the date an owner is told they must act by.
_WORD_NUMBERS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
}
_NO_REDEMPTION_RE = re.compile(r"\bno\s+right\s+of\s+redemption\b", re.I)
_PARENS_NUM_RE = re.compile(r"\((\d{1,2})\)")
_LEADING_MONTHS_RE = re.compile(r"^\s*(\d{1,2})(?:\.\d+)?\s*(?:month|mo\b)", re.I)
_ANY_DIGIT_MONTHS_RE = re.compile(r"\b(\d{1,2})(?:\.\d+)?\s*month", re.I)
_WORD_MONTHS_RE = re.compile(
    r"\b(" + "|".join(_WORD_NUMBERS) + r")\b\s*(?:\(\d{1,2}\))?\s*month", re.I
)


def _parse_redemption_months(text: Optional[str]) -> Optional[int]:
    """Redemption period in months. 0 = 'no right of redemption'. None = the
    notice does not state one.

    REWRITTEN 2026-07-28. The previous rule was `re.match(r"\\s*(\\d+)")` —
    a LEADING digit only — which failed on every spelled-out notice. Measured
    live: 114 of 215 sheriff_sales rows had a stated period that parsed to
    NULL, and 110 of those were spelled out. The largest single value,
    'six (6) months', accounted for 95 rows.

    Worse than the nulls: 'twelve (12) months' also returned None, and
    downstream the redemption tracker defaults an unknown period to 6 months
    — so a 12-month property was being given a deadline SIX MONTHS EARLY.
    Minnesota periods are genuinely 6 or 12 (Minn. Stat. ch. 580), so this
    is not a hypothetical.

    Verified against all 14 distinct values present in signals.sheriff_sales:
    215 of 215 correct, including 'twelve (12) months' -> 12 and
    'No right of redemption' -> 0.

    Deliberately returns None rather than guessing for:
      '5 weeks'  — §580.07 allows a five-WEEK period; weeks are not months
      'not stated' — the notice genuinely omits it
    An honest None lets the caller say "confirm on your notice"; a guess
    would put a false date in front of a homeowner.
    """
    if not text:
        return None
    s = str(text).strip()
    if not s:
        return None
    if _NO_REDEMPTION_RE.search(s):
        return 0
    for rx in (_LEADING_MONTHS_RE, _PARENS_NUM_RE, _ANY_DIGIT_MONTHS_RE):
        m = rx.search(s)
        if m:
            value = int(m.group(1))
            if 0 < value <= 24:
                return value
    m = _WORD_MONTHS_RE.search(s)
    if m:
        return _WORD_NUMBERS[m.group(1).lower()]
    return None

def _parse_sale_time(text: Optional[str]) -> Optional[str]:
    """'10:00 AM' -> '10:00:00' (Postgres time). Returns None if unparseable."""
    if not text:
        return None
    s = str(text).strip().upper()
    m = re.match(r"(\d{1,2}):(\d{2})\s*(AM|PM)?", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    ampm = m.group(3)
    if ampm == "PM" and hh != 12:
        hh += 12
    elif ampm == "AM" and hh == 12:
        hh = 0
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return f"{hh:02d}:{mm:02d}:00"


def _synthetic_pid(county: Optional[str], source_id: str) -> str:
    """'SCOTT-FC-24-117341' — mirrors the existing 'HENNEPIN-FC-2506001'
    synthetic-parcel convention for sheriff rows."""
    return f"{_county_upper(county)}-FC-{source_id}"


# Label to fall back on when an extraction somehow has no source_name.
# Verified 2026-08-02: source_name is populated on 371 of 371 rows in
# ai.extracted_foreclosures, so this should never fire. It exists so a
# malformed row produces a WRONG-BUT-KNOWN label rather than an empty string
# that would silently break every source-based join downstream.
_FALLBACK_SOURCE = "startribune_legal"


def derive_source(extracted: dict[str, Any]) -> str:
    """Which feed this notice actually came from.

    ADDED 2026-08-02. Until today this module hardcoded "startribune_legal"
    into distress_event.source, raw_data._source and parcel_row.data_sources.

    That was true when the Star Tribune scraper was the only feeder. It stopped
    being true when mnpublicnotice took over and nobody updated the constant.
    Measured: of 371 extractions, 369 are mnpublicnotice and 2 are
    startribune_legal (ONE of which is approved, from 2026-06-07). Of the 238
    promoted distress_events rows carrying the Star Tribune label, 237 are
    misattributed.

    Why it matters beyond tidiness: `source` is how coverage is attributed and
    what source_county_map keys on to assign county slugs. A mislabelled source
    is the documented mechanism by which a new source silently gets a NULL
    slug. It also made the run log unreadable — a source with 238 events and
    one approved notice is a contradiction that costs time to unpick.

    The extraction row already knows the answer; it just was not being asked.
    """
    name = extracted.get("source_name")
    if name and str(name).strip():
        return str(name).strip()
    return _FALLBACK_SOURCE


def derive_source_id(extracted: dict[str, Any]) -> str:
    """Stable, unique id for this notice on the foreclosure tab. Prefer the
    attorney file number (stable per notice); fall back to the ai row id."""
    afn = extracted.get("attorney_file_no")
    if afn and str(afn).strip():
        return str(afn).strip()
    return f"ef-{extracted.get('id')}"


def build_promotion_rows(extracted: dict[str, Any]) -> dict[str, Any]:
    """Given an ai.extracted_foreclosures record (as a dict), build the target
    rows. Returns {'source_id', 'parcel_row', 'distress_event', 'sheriff_sale'}.
    Pure — no DB access."""
    county = extracted.get("county")
    county_lo = _county_lower(county)
    county_code = _county_slug(county)  # FK-valid slug for core.counties
    source_id = derive_source_id(extracted)
    # Real feed name, not a hardcoded constant. See derive_source().
    source = derive_source(extracted)
    real_pid = extracted.get("parcel_id")  # the real GIS PID, e.g. '220570230'
    synthetic_pid = _synthetic_pid(county, source_id)

    address = extracted.get("property_address") or "address not stated"
    city = extracted.get("city") or ""
    sale_date = extracted.get("sale_date")
    amount_due = extracted.get("amount_due")
    mortgagor = extracted.get("mortgagor") or "not stated"
    mortgagee = extracted.get("mortgagee") or "not stated"
    redemption = extracted.get("redemption_period") or "not stated"

    # Shared raw_data, mirroring the existing sheriff-row shape + overlay-ready
    # detail block (gis_pid is the path the overlay view reads today; county is
    # for the follow-up view edit).
    raw_data: dict[str, Any] = {
        "_source": source,
        "address": address,
        "city": city or None,
        "mortgagee": mortgagee,
        "mortgagors": [{"display": mortgagor}],
        "dateOfSale": sale_date,
        "amount_due": amount_due,
        "redemption_period": redemption,
        "lawFirm": extracted.get("attorney_firm"),
        "attorney_file_no": extracted.get("attorney_file_no"),
        "legal_description": extracted.get("legal_description"),
        "source_url": extracted.get("source_url"),
        "extracted": True,
        "extraction_confidence": extracted.get("confidence"),
        "detail": {
            "gis_pid": real_pid,       # overlay reads this (existing behavior)
            "county": county_lo,       # overlay edit (Step 3b) will read this
        },
    }

    title = f"Scheduled sheriff foreclosure sale — {address}" + (
        f", {city}" if city else ""
    )
    description = (
        f"Scheduled {(_county_upper(county)).title()} County sheriff sale on "
        f"{sale_date or 'a date not stated'}. Mortgagor: {mortgagor}. "
        f"Lender: {mortgagee}. Amount due: {_money_str(amount_due)}. "
        f"Redemption: {redemption}."
    )

    distress_event = {
        "parcel_id": synthetic_pid,
        "event_type": "sheriff_sale",
        "event_subtype": "scheduled",
        "event_date": sale_date,
        "event_value": amount_due,
        "source": source,
        "source_id": source_id,
        "severity": "medium",
        "title": title,
        "description": description,
        "raw_data": raw_data,
    }

    sheriff_sale = {
        "parcel_id": synthetic_pid,
        "sale_date": sale_date,
        "sale_time": _parse_sale_time(extracted.get("sale_time")),
        "sale_location": extracted.get("sale_location"),
        "opening_bid": None,
        "total_debt": amount_due,
        "foreclosing_law_firm": extracted.get("attorney_firm"),
        "lender_name": mortgagee,
        "redemption_period_months": _parse_redemption_months(redemption),
        "sale_status": "scheduled",
        "postponement_count": 0,
        "county_code": county_code,
        "raw_data": raw_data,
    }

    # core.parcels row — distress_events.parcel_id has a FK to core.parcels,
    # so the synthetic parcel must exist there first (mirrors how the sheriff
    # scraper inserts a parcels row before its distress_events row). county_code
    # must be the core.counties slug, not the title-case name.
    parcel_row = {
        "parcel_id": synthetic_pid,
        "state": "MN",
        "county_code": county_code,
        "address": address if address != "address not stated" else None,
        "city": city or None,
        "data_sources": [source],
        "raw_data": {
            "gis_pid": real_pid,
            "source_url": extracted.get("source_url"),
            "extracted": True,
        },
    }

    return {
        "source_id": source_id,
        "parcel_row": parcel_row,
        "distress_event": distress_event,
        "sheriff_sale": sheriff_sale,
    }


__all__ = ["build_promotion_rows", "derive_source", "derive_source_id"]
