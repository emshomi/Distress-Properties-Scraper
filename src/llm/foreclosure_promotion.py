"""
Promotion of an approved ai.extracted_foreclosures row into the live signals
tables (signals.distress_events + signals.sheriff_sales).

This module does the TRANSFORMATION only — it builds the two target rows from
an extracted-foreclosure record. The actual DB writes live in the endpoint, so
this logic stays pure and testable.

Rows are written OVERLAY-READY: the real parcel PID goes to
raw_data.detail.gis_pid (the path signals.parcel_distress_overlay already reads
for sheriff rows) and the lowercase county to raw_data.detail.county (which a
follow-up view edit will read so statewide/extracted rows can resolve their
county and participate in the multi-signal overlay).

These notices are SCHEDULED (future) sheriff sales, not completed ones, so they
are labeled event_subtype='scheduled' and worded accordingly — never presented
as a sale that already happened.
"""

from __future__ import annotations

import re
from typing import Any, Optional


def _county_upper(county: Optional[str]) -> str:
    return (county or "UNKNOWN").strip().upper()


def _county_lower(county: Optional[str]) -> str:
    return (county or "unknown").strip().lower()


def _money_str(value: Any) -> str:
    """Format a number as $X,XXX for human-readable title/description text.
    Returns '—' when absent (we never fabricate a number)."""
    if value is None:
        return "—"
    try:
        return f"${round(float(value)):,}"
    except (TypeError, ValueError):
        return "—"


def _parse_redemption_months(text: Optional[str]) -> Optional[int]:
    """'6 Months' -> 6. Returns None if no leading integer is present."""
    if not text:
        return None
    m = re.match(r"\s*(\d+)", str(text))
    return int(m.group(1)) if m else None


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


def derive_source_id(extracted: dict[str, Any]) -> str:
    """Stable, unique id for this notice on the foreclosure tab. Prefer the
    attorney file number (stable per notice); fall back to the ai row id."""
    afn = extracted.get("attorney_file_no")
    if afn and str(afn).strip():
        return str(afn).strip()
    return f"ef-{extracted.get('id')}"


def build_promotion_rows(extracted: dict[str, Any]) -> dict[str, Any]:
    """Given an ai.extracted_foreclosures record (as a dict), build the two
    target rows. Returns {'source_id', 'distress_event', 'sheriff_sale'}.
    Pure — no DB access."""
    county = extracted.get("county")
    county_lo = _county_lower(county)
    source_id = derive_source_id(extracted)
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
        "_source": "startribune_legal",
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
        "source": "startribune_legal",
        "source_id": source_id,
        "severity": "medium",
        "title": title,
        "description": description,
        "raw_data": raw_data,
    }

    sheriff_sale = {
        "parcel_id": real_pid,
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
        "county_code": county,
        "raw_data": raw_data,
    }

    # core.parcels row — distress_events.parcel_id has a FK to core.parcels,
    # so the synthetic parcel must exist there first (mirrors how the sheriff
    # scraper inserts a parcels row before its distress_events row).
    parcel_row = {
        "parcel_id": synthetic_pid,
        "state": "MN",
        "county_code": county,
        "address": address if address != "address not stated" else None,
        "city": city or None,
        "data_sources": ["startribune_legal"],
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

__all__ = ["build_promotion_rows", "derive_source_id"]
