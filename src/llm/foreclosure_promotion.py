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


def _pid_digits(value: Any) -> Optional[str]:
    """Digits of a parcel identifier, or None if too few to be one.

    Counties print PIDs in incompatible shapes for the SAME spine value:
    Wright '155-154-004010', Beltrami '83.00180.00', Dakota
    '42-42800-01-071'. Digits-only is the form that matches, verified
    2026-08-10: exact match resolved 47 of 217 notices, digits-only
    resolved 185.

    The 6-digit floor rejects fragments. Measured example: cass '45-118'
    yields 5 digits and is not a whole PID -- matching on it could hit an
    unrelated parcel.
    """
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits if len(digits) >= 6 else None


# Certificate-of-title and document numbers that some notices append to a PID.
# 'Torrens' registrations print as '08.032.21.11.0036 COT# 77608', and the
# trailing 77608 is NOT part of the parcel identifier -- digits-only over the
# whole string yields '080322111003677608', which matches nothing. Measured
# 2026-08-15 on washington source_id 1544378.
_PID_NOISE_RE = re.compile(
    r"\b(?:COT|CERT|C\.?O\.?T\.?|DOC|TORRENS)\s*#?\s*\d+",
    re.IGNORECASE,
)

# A notice may cover SEVERAL parcels, listed with ';', ',' or the word 'and'.
_PID_SPLIT_RE = re.compile(r"\s*(?:;|,|\band\b)\s*", re.IGNORECASE)


def split_pids(value: Any) -> list[str]:
    """Every parcel identifier a notice's PID field names, in printed order.

    One Minnesota foreclosure notice can cover MANY parcels. Measured
    2026-08-15 across mnpublicnotice:

        washington 26-003536FC -- THIRTEEN parcels, twelve addresses on
            Keibler Ct and 211th St, Forest Lake, one bid of $261,140.77
        washington 26-003550FC -- four Meadowridge Trail parcels
        martin 058273-F1       -- '1228 & 1224 N Prairie Ave', two houses

    Before this, _pid_digits() ran over the WHOLE field, so
    '150063915; 150063922' became one 18-digit string, matched nothing, and
    the notice fell back to a synthetic stub. Thirteen distressed properties
    were represented to subscribers as a single row.

    Returns [] when nothing usable is found, so a caller can keep the existing
    synthetic-stub behaviour unchanged.
    """
    if value is None:
        return []
    text = _PID_NOISE_RE.sub(" ", str(value))
    out: list[str] = []
    for part in _PID_SPLIT_RE.split(text):
        part = part.strip()
        if part and _pid_digits(part):
            out.append(part)
    # Preserve printed order, drop repeats (a notice can list one parcel twice).
    seen: set[str] = set()
    unique: list[str] = []
    for p in out:
        d = _pid_digits(p)
        if d and d not in seen:
            seen.add(d)
            unique.append(p)
    return unique


def _synthetic_pid(
    county: Optional[str],
    source_id: str,
    real_pid: Optional[str] = None,
    sale_date: Optional[Any] = None,
) -> str:
    """Synthetic parcel key for a notice whose parcel is NOT in the spine.

    'SCOTT-FC-24-117341' — mirrors the existing 'HENNEPIN-FC-2506001'
    synthetic-parcel convention for sheriff rows.

    CHANGED 2026-08-10. This used source_id alone, and source_id falls back
    to `ef-{extraction id}` whenever a notice carries no attorney file
    number. Minnesota requires a foreclosure notice to run SIX CONSECUTIVE
    WEEKS (Minn. Stat. 580.03), so the same sale is extracted again every
    week with a NEW extraction id -- and therefore a new source_id, a new
    synthetic parcel, and a new distress event.

    Measured live: 289 mnpublicnotice sheriff_sale rows were 219 distinct
    properties. 68 duplicate rows, 24% inflation. 4318 Harvest Court,
    Monticello (wright 155-154-004010, sale 2026-09-02, $16,380.98)
    appeared NINETEEN times.

    Keying on (county, parcel digits, sale date) is stable across every
    republication of one sale, so the idempotency guard catches it. Where
    the notice has no usable PID we keep the old source_id form -- there is
    nothing more stable to use, and inventing one would merge sales that are
    genuinely different.
    """
    digits = _pid_digits(real_pid)
    if digits and sale_date:
        return f"{_county_upper(county)}-FC-{digits}-{sale_date}"
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


def build_promotion_rows(
    extracted: dict[str, Any],
    resolved_parcel_id: Optional[str] = None,
    package: Optional[dict[str, Any]] = None,
    member_address: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Given an ai.extracted_foreclosures record (as a dict), build the target
    rows. Returns {'source_id', 'parcel_row', 'distress_event', 'sheriff_sale'}.
    Pure — no DB access.

    resolved_parcel_id: the REAL core.parcels.parcel_id when the notice's PID
    was found in that county's spine. The caller does that lookup (it needs
    the DB; this module stays pure) and passes the result in.

    ADDED 2026-08-10. When supplied, the event hangs off the real parcel
    instead of a synthetic stub, which does three things at once:

      1. Republication collapses — same parcel, same sale date.
      2. The row INHERITS market value, coordinates, address, owner and lot
         size from the spine. That is why Beltrami rows render as em-dashes
         today while Hennepin rows show $344,800: a synthetic stub has none
         of it, and never will.
      3. No synthetic parcel is minted, so core.parcels stops accumulating
         one stub per notice.

    Measured 2026-08-10: 185 of 217 distinct notices resolve. Of the 32 that
    do not, several are counties with NO spine at all (beltrami holds 2 rows,
    both synthetic; redwood holds 1) and some are two-parcel notices whose
    PID field reads '505-0015-04912 and 505-0015-04913'. Those keep the
    synthetic path and behave exactly as they do now.
    """
    county = extracted.get("county")
    county_lo = _county_lower(county)
    county_code = _county_slug(county)  # FK-valid slug for core.counties
    source_id = derive_source_id(extracted)
    # Real feed name, not a hardcoded constant. See derive_source().
    source = derive_source(extracted)
    real_pid = extracted.get("parcel_id")  # the real GIS PID, e.g. '220570230'
    sale_date_raw = extracted.get("sale_date")
    # The spine parcel when the caller resolved one; otherwise a synthetic
    # key that is at least STABLE across the six weekly republications.
    effective_pid = resolved_parcel_id or _synthetic_pid(
        county, source_id, real_pid=real_pid, sale_date=sale_date_raw
    )

    address = extracted.get("property_address") or "address not stated"
    city = extracted.get("city") or ""

    # === A PACKAGE MEMBER MUST CARRY ITS OWN ADDRESS ===
    # extracted['property_address'] on a package notice is the notice's FULL
    # LIST, e.g. '21184 Keibler Ct. N., Forest Lake, MN; 21148 Keibler Ct. N.,
    # ...' -- twelve addresses. Written to raw_data.address it makes every one
    # of thirteen members display all twelve of its siblings, which is what the
    # 2026-08-15 migration produced before this was fixed.
    #
    # member_address is supplied by the CALLER, which has the DB (this module
    # stays pure) and already looks the parcel up in _resolve_spine_parcel.
    # {'address': ..., 'city': ...}; either may be None.
    #
    # A member whose parcel has NO address keeps an EMPTY address rather than
    # inheriting the list. One Forest Lake parcel is in that state, and the
    # notice prints 12 addresses for 13 parcels, so positional alignment cannot
    # be trusted to fill it. The county's own list is kept in
    # _package.notice_addresses, visible but not presented as this property's
    # address.
    #
    # ADDED 2026-08-19. The blank is correct ONLY when the member has a SPINE
    # parcel to inherit an address from. Washington's Forest Lake member is in
    # exactly that state: blank here, but core.parcels holds its real address
    # and geometry, so it renders and geocodes fine.
    #
    # When there is NO spine parcel (resolved_parcel_id is None) the stub built
    # below is the only record this property will ever have, and a blank
    # address is permanent: backfill_stub_geocode.py requires a usable address,
    # so the row can never acquire coordinates. Measured on 2026-08-19 across
    # all 34 package members: 3 blank addresses, and the 2 with no geometry are
    # both Martin, a county with no spine at all (5 parcels, every one
    # synthetic). Washington's blank has geom and is unaffected.
    #
    # In that case fall back to what the county PRINTED. It is a real address
    # and it is strictly better than nothing -- but it describes the NOTICE,
    # not this parcel ('1228 & 1224 N Prairie Ave' covers both members), so it
    # is flagged in _package.address_is_notice_level and the card can qualify
    # it rather than presenting it as this parcel's own.
    notice_addresses = None
    address_is_notice_level = False
    if package:
        notice_addresses = address
        address = (member_address or {}).get("address") or ""
        city = (member_address or {}).get("city") or ""
        if not address and resolved_parcel_id is None:
            address = notice_addresses or ""
            city = extracted.get("city") or ""
            address_is_notice_level = bool(address)
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

    # PACKAGE SALES (added 2026-08-15).
    #
    # When one notice covers several parcels, each parcel becomes its own event
    # so a subscriber searching that city SEES ALL OF THEM -- washington
    # 26-003536FC is thirteen Forest Lake properties that appeared as one row.
    #
    # But the bid is ONE figure for the WHOLE package. Copying $261,140.77 onto
    # each of thirteen parcels would fabricate thirteen equity spreads, the same
    # class of error as writing a county's $0 into emv_total. So event_value is
    # NULL on a package member and the total is carried in raw_data._package
    # instead. Deal math needs event_value, so it correctly declines to compute
    # -- market value, coordinates and imagery are all still real and still
    # shown.
    pkg_size = int((package or {}).get("size") or 1)
    is_package = pkg_size > 1
    if is_package:
        # === THE UNIQUE KEY FORCES A DISTINCT source_id PER MEMBER ===
        # distress_events_source_identity_key is
        #     (county_code, source, source_id, event_date) NULLS NOT DISTINCT
        # and every member of a package shares all four. Without a suffix the
        # first insert succeeds and the second raises 23505.
        #
        # The suffix is the PARCEL'S OWN DIGITS, never a loop index: Minnesota
        # requires a notice to run six consecutive weeks (Minn. Stat. 580.03)
        # and a republication may list the parcels in a different ORDER. An
        # index-based suffix would shift between runs and mint a fresh event
        # every week -- exactly the 24% inflation measured on 2026-08-10.
        #
        # This does not weaken the key. For a package notice the publisher's
        # identity for a GIVEN PARCEL genuinely is notice-plus-parcel, and the
        # suffix is derived from what the county published, not from anything
        # we rewrite.
        _member_digits = _pid_digits(real_pid)
        if _member_digits:
            source_id = f"{source_id}#{_member_digits}"
        raw_data["_package"] = {
            "size": pkg_size,
            "index": (package or {}).get("index"),
            "total_bid": amount_due,
            "parcel_ids": (package or {}).get("parcel_ids"),
            # What the county actually printed, preserved verbatim.
            "notice_addresses": notice_addresses,
            # True when raw_data.address holds the NOTICE's address string
            # rather than this parcel's own -- see the fallback above.
            "address_is_notice_level": address_is_notice_level,

               "note": (
                f"Part of a package sale of {pkg_size} properties sold together"
                f" for {_money_str(amount_due)}. No individual price was"
                f" published for this parcel."
            ),
        }

    distress_event = {
        "parcel_id": effective_pid,
        # ADDED 2026-08-10. signals.distress_events has a COMPOSITE foreign
        # key (county_code, parcel_id) -> core.parcels, and this dict never
        # set county_code — only sheriff_sale and parcel_row did. Measured
        # live: 8 mnpublicnotice sheriff rows carried county_code NULL, all
        # of them observed 2026-08-08 to 08-10, i.e. every promotion since
        # the column started mattering. A NULL member makes the composite FK
        # unenforced, so those rows pointed at nothing and could not be
        # re-keyed to their real parcel until the column was backfilled.
        "county_code": county_code,
        "event_type": "sheriff_sale",
        "event_subtype": "scheduled",
        "event_date": sale_date,
        # NULL on a package member -- see the _package note above.
        "event_value": None if is_package else amount_due,
        "source": source,
        "source_id": source_id,
        "severity": "medium",
        "title": title,
        "description": description,
        "raw_data": raw_data,
    }

    sheriff_sale = {
        "parcel_id": effective_pid,
        "sale_date": sale_date,
        "sale_time": _parse_sale_time(extracted.get("sale_time")),
        "sale_location": extracted.get("sale_location"),
        "opening_bid": None,
        "total_debt": None if is_package else amount_due,
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
    # None when the parcel already exists in the spine — the caller skips the
    # insert entirely rather than writing a stub over real assessor data.
    parcel_row = None if resolved_parcel_id else {
        "parcel_id": effective_pid,
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
        "parcel_id": effective_pid,
        "parcel_row": parcel_row,
        "distress_event": distress_event,
        "sheriff_sale": sheriff_sale,
    }


__all__ = ["build_promotion_rows", "derive_source", "derive_source_id"]
