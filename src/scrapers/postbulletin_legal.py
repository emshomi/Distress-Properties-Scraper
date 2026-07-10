"""
Post Bulletin legal-notices scraper — Rochester/Olmsted foreclosure notices.

THE FIRST OLMSTED SIGNAL SOURCE (2026-07-09 late session): turns the
75,039-parcel Olmsted spine loaded earlier tonight into a live county on
the Foreclosure sales tab. MN foreclosures are by-advertisement and must
publish in the county's qualified newspaper (Minn. Stat. 580.03) — for
Olmsted that is the Rochester Post Bulletin, whose public notices run on
the Column platform.

=== SOURCE (reverse-engineered live, 2026-07-09, from the widget at
    postbulletin.column.us/search) ===
POST https://us-central1-enotice-production.cloudfunctions.net/api/search/public-notices
Content-Type: application/json
{
  "search": "",
  "allFilters": [
    {"publishedtimestamp": {"from": <epoch_ms>, "to": <epoch_ms>}},
    {"newspapername": ["Post Bulletin"]},
    {"noticetype": ["Foreclosure Sale"]}
  ],
  "isDemo": false, "noneFilters": [], "pageSize": <n>,
  "sort": [{"publishedtimestamp": "desc"}]
}
Public endpoint, no auth, CORS *. The noticetype filter is belt — parse()
re-filters on the notice text itself (braces) in case the facet field name
drifts; unexpected response shapes degrade to 0 rows with logging, never
a crash.

=== WHAT A NOTICE CARRIES (verified on live notices tonight) ===
Standardized MN statute language with labeled fields:
  TAX PARCEL NO.: 743612021050        (12-digit Olmsted PARID — direct
                                       spine join; also appears dotted,
                                       "54.28.13.070714" -> digits)
  ADDRESS OF PROPERTY: 910 15th Ave Ne / Rochester, MN 55906
  MORTGAGOR(S): Katherine Jackson, a single person
  ORIGINAL PRINCIPAL AMOUNT OF MORTGAGE: $248,900.00
  AMOUNT DUE AND CLAIMED TO BE DUE...: $292,839.77
  DATE AND TIME OF SALE: July 27, 2026, 10:00 AM   (SCHEDULED, future)
  PLACE OF SALE / COUNTY IN WHICH PROPERTY IS LOCATED: Olmsted
  (optional) NOTICE OF POSTPONEMENT ... postponed to <date>

=== HONESTY RULES ===
- Only rows with a parseable parcel PIN are ingested (PB notices carry
  them reliably); rows without one are logged and skipped — NEVER a
  synthetic id (the MPLS-VBR lesson).
- Only Olmsted-located notices ingest (the pilot's county); others log.
- event_date = the SCHEDULED sale date (a real, published date); if the
  notice carries postponements, the LATEST postponed date wins.
- Rows are SCHEDULED sales — surfaced as such, never as completed.

Dedup identity: (parcel_id, 'sheriff_sale', <sale date>, source) — an
amended/postponed republication is a new fact and correctly a new row.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

import httpx

from src.models.signal import DistressEventInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import write_events_dedup
from src.utils.errors import SourceUnavailableError
from src.utils.logger import logger
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id


_API_URL = (
    "https://us-central1-enotice-production.cloudfunctions.net"
    "/api/search/public-notices"
)
_NEWSPAPER = "Post Bulletin"
_WINDOW_DAYS = 45   # covers the full 6-week publication run of a notice
_PAGE_SIZE = 100
_REQUEST_TIMEOUT = 30.0

_TITLE = "Scheduled sheriff's sale (Post Bulletin foreclosure notice)"
_DESC = (
    "Mortgage foreclosure sale notice published in the Rochester Post "
    "Bulletin (Olmsted County's qualified newspaper). The property is "
    "scheduled to be sold by the Sheriff of Olmsted County at public "
    "auction on the stated date unless the mortgage is reinstated or the "
    "property redeemed (as stated in the notice)."
)

# ---- Notice-text field extraction (built from live notices) ----

_RE_PIN = re.compile(
    # Label variants seen live: "TAX PARCEL NO.:", "TAX PARCEL I.D. NO.:",
    # and the older "PROPERTY IDENTIFICATION NUMBER:" (sometimes with an
    # RP prefix on the value). Non-digits are stripped afterwards.
    r"(?:TAX PARCEL(?:\s+I\.?D\.?)?\s+NO\.?|PROPERTY IDENTIFICATION NUMBER)"
    r"\s*:?\s*([A-Z]{0,3}[0-9.\- ]{8,30})", re.I
)
_RE_ADDRESS = re.compile(
    # Both label variants seen live: "ADDRESS OF PROPERTY:" (newer, multi-
    # line) and "PROPERTY ADDRESS:" (older, single-line).
    r"(?:(?:STREET\s+)?ADDRESS OF (?:THE )?PROPERTY|PROPERTY ADDRESS)\s*:?\s*(.{5,120}?)"
    r"(?=\s*(?:COUNTY IN WHICH|TAX PARCEL|PROPERTY IDENTIFICATION|THE AMOUNT|ORIGINAL PRINCIPAL|$))",
    re.I | re.S,
)
_RE_COUNTY = re.compile(
    r"COUNTY IN WHICH PROPERTY IS LOCATED\s*:?\s*([A-Za-z .]{3,30})", re.I
)
_RE_MORTGAGOR = re.compile(
    r"MORTGAGOR(?:\(S\))?\s*:?\s*(.{3,120}?)(?=\s*(?:MORTGAGEE|Mortgagee)\b)",
    re.I | re.S,
)
_RE_PRINCIPAL = re.compile(
    r"ORIGINAL PRINCIPAL AMOUNT OF MORTGAGE\s*:?\s*\$?([\d,]+(?:\.\d{2})?)",
    re.I,
)
_RE_AMOUNT_DUE = re.compile(
    r"AMOUNT (?:CLAIMED TO BE )?DUE(?: AND CLAIMED TO BE DUE)?"
    r"[^:$]{0,80}[:$]\s*\$?\s*([\d,]+(?:\.\d{2})?)",
    re.I,
)
_RE_SALE = re.compile(
    r"DATE AND TIME OF SALE\s*:?\s*"
    r"([A-Z][a-z]+ \d{1,2}, \d{4})(?:\s*,?\s*(?:at\s*)?(\d{1,2}:\d{2}\s*[AP]\.?M\.?))?",
    re.I,
)
_RE_POSTPONED = re.compile(
    r"postponed (?:until|to)\s+([A-Z][a-z]+ \d{1,2}, \d{4})", re.I
)
# Redemption period as STATED in the notice. MN is usually 6 months but can
# be 12 (and other values); the notice text carries the real figure, e.g.
# "subject to redemption within 6 Months from the date of said sale" or the
# tax variant "The time allowed for redemption ... is six (6) months". We
# extract it rather than assume, so the redemption-expiry clock is honest.
_RE_REDEMPTION = re.compile(
    r"redemption[^.]{0,80}?(?:within|is|of|allowed[^.]{0,20}?is)?\s*"
    r"(\d{1,2}|one|two|three|six|twelve)\s*(?:\(\s*\d{1,2}\s*\))?\s*months?",
    re.I,
)
_WORD_MONTHS = {
    "one": 1, "two": 2, "three": 3, "six": 6, "twelve": 12,
}
# The notice often states the redemption/vacate DEADLINE outright, e.g.
# "DATE TO VACATE PROPERTY: ... is January 27, 2027 at 11:59 p.m." This
# authoritative date beats any computed sale_date+period, so we prefer it.
_RE_VACATE_DATE = re.compile(
    r"(?:DATE TO VACATE|MUST VACATE)"
    r"(?:(?!DATE AND TIME OF SALE|MORTGAGOR\(S\) RELEASED).){0,300}?"
    r"\bis\s+([A-Z][a-z]+ \d{1,2}, \d{4})",
    re.I | re.S,
)
# Some notices warn redemption may be cut to 5 weeks if the property is
# judicially determined abandoned (Minn. Stat. 582.032). We flag this so
# the displayed clock can carry the caveat rather than overstate the time.
_RE_ABANDON_CAVEAT = re.compile(
    r"REDUCED TO FIVE WEEKS.*?ABANDONED", re.I | re.S,
)



def _add_months(d: date, months: int) -> date:
    """Add whole months to a date using stdlib only (no dateutil dep).
    Clamps the day to the target month's length (e.g. Aug 31 + 6mo ->
    Feb 28/29). Adequate for redemption-expiry dates."""
    import calendar
    m0 = d.month - 1 + months
    year = d.year + m0 // 12
    month = m0 % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _extract_redemption_months(text: str) -> int | None:
    """Months of redemption as stated in the notice; None if not stated
    (caller decides the fallback, and flags that it was a fallback)."""
    m = _RE_REDEMPTION.search(text)
    if not m:
        return None
    tok = m.group(1).strip().lower()
    if tok.isdigit():
        val = int(tok)
    else:
        val = _WORD_MONTHS.get(tok)
    # Sanity: MN redemption is 6 or 12 (occasionally other small values);
    # reject nonsense captures.
    if val is not None and 1 <= val <= 36:
        return val
    return None

_RE_CITY_ZIP = re.compile(
    r"([A-Za-z .]+?),?\s*(?:MN|Minnesota)\s*,?\s*(\d{5})", re.I
)


def _safe_money(text: str | None) -> Decimal | None:
    if not text:
        return None
    try:
        d = Decimal(text.replace(",", ""))
        return d if d >= 0 else None
    except (InvalidOperation, ValueError):
        return None


def _parse_long_date(text: str | None) -> date | None:
    if not text:
        return None
    try:
        return datetime.strptime(text.strip(), "%B %d, %Y").date()
    except ValueError:
        return None


def _clean_ws(text: str | None) -> str | None:
    if not text:
        return None
    s = " ".join(text.split())
    return s or None


def _extract_pin(text: str) -> str | None:
    m = _RE_PIN.search(text)
    if not m:
        return None
    digits = re.sub(r"[^0-9]", "", m.group(1))
    if not (10 <= len(digits) <= 14):
        return None
    pin, _err = safe_normalize_parcel_id("olmsted", digits)
    return pin


def _extract_notice_text(hit: dict[str, Any]) -> str | None:
    """The API hit's text field — name defensively probed (the response
    shape was observed, not documented). First non-trivial string wins."""
    for key in ("noticecontent", "notice_content", "content", "text",
                "noticetext", "notice_text", "body", "cleanedtext",
                "searchabletext"):
        v = hit.get(key)
        if isinstance(v, str) and len(v) > 200:
            return v
    # Nested one level (e.g. {"_source": {...}})
    for v in hit.values():
        if isinstance(v, dict):
            inner = _extract_notice_text(v)
            if inner:
                return inner
    return None


def _hits_from_response(data: Any) -> list[dict[str, Any]]:
    """Walk the response for the results list, shape-agnostically:
    the first list of dicts found under common keys, else any list of
    dicts that looks like notices."""
    if isinstance(data, list):
        return [h for h in data if isinstance(h, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("results", "hits", "notices", "data", "items", "docs"):
        v = data.get(key)
        if isinstance(v, list) and v and all(isinstance(h, dict) for h in v):
            return v
        if isinstance(v, dict):  # elastic style {"hits": {"hits": [...]}}
            inner = _hits_from_response(v)
            if inner:
                return inner
    for v in data.values():
        if isinstance(v, (dict, list)):
            inner = _hits_from_response(v)
            if inner:
                return inner
    return []


class PostBulletinLegalScraper(BaseScraper[dict[str, Any], DistressEventInsert]):
    """Post Bulletin (Column) foreclosure notices -> Olmsted sheriff sales."""

    source_name: ClassVar[str] = "postbulletin_legal"
    signal_type: ClassVar[str] = "sheriff_sale"
    county_code: ClassVar[str] = "olmsted"

    # ---- Fetch: one JSON POST ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        from_ms = now_ms - _WINDOW_DAYS * 24 * 3600 * 1000
        payload = {
            "search": "",
            "allFilters": [
                {"publishedtimestamp": {"from": from_ms, "to": now_ms}},
                {"newspapername": [_NEWSPAPER]},
                {"noticetype": ["Foreclosure Sale"]},
            ],
            "isDemo": False,
            "noneFilters": [],
            "pageSize": _PAGE_SIZE,
            "sort": [{"publishedtimestamp": "desc"}],
        }
        try:
            async with httpx.AsyncClient(
                timeout=_REQUEST_TIMEOUT,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; govire/1.0)",
                    "Content-Type": "application/json",
                    "Origin": "https://postbulletin.column.us",
                    "Referer": "https://postbulletin.column.us/",
                },
            ) as client:
                resp = await client.post(_API_URL, json=payload)
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"Column public-notices API failed: {str(e)[:300]}",
                source=self.source_name,
            ) from e
        except ValueError as e:
            raise SourceUnavailableError(
                f"Column API returned non-JSON: {str(e)[:200]}",
                source=self.source_name,
            ) from e

        hits = _hits_from_response(data)
        logger.info(
            "Post Bulletin notice fetch complete",
            source=self.source_name,
            hits=len(hits),
            window_days=_WINDOW_DAYS,
        )
        return hits

    # ---- Parse: notice text -> scheduled sheriff-sale events ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[DistressEventInsert]:
        signals: list[DistressEventInsert] = []
        skipped_no_text = skipped_not_fc = skipped_no_pin = 0
        skipped_county = 0

        for hit in raw_records:
            text = _extract_notice_text(hit)
            if not text:
                skipped_no_text += 1
                continue

            # Braces on top of the API's belt: the text must actually be a
            # mortgage foreclosure / sheriff sale notice.
            up = text.upper()
            if "FORECLOSURE" not in up and "SHERIFF" not in up:
                skipped_not_fc += 1
                continue

            county_m = _RE_COUNTY.search(text)
            county = _clean_ws(county_m.group(1)) if county_m else None
            if county and "olmsted" not in county.lower():
                skipped_county += 1
                logger.info(
                    "Notice outside the Olmsted pilot skipped",
                    source=self.source_name, county=county,
                )
                continue

            pin = _extract_pin(text)
            if not pin:
                skipped_no_pin += 1
                logger.warning(
                    "Foreclosure notice without a parseable PIN skipped "
                    "(never a synthetic id)",
                    source=self.source_name,
                )
                continue

            sale_m = _RE_SALE.search(text)
            sale_date = _parse_long_date(sale_m.group(1)) if sale_m else None
            sale_time = _clean_ws(sale_m.group(2)) if sale_m else None
            # Latest postponement wins as the effective scheduled date.
            postponements = [_parse_long_date(d) for d in _RE_POSTPONED.findall(text)]
            postponements = [d for d in postponements if d]
            if postponements:
                latest = max(postponements)
                if sale_date is None or latest > sale_date:
                    sale_date = latest

            addr_m = _RE_ADDRESS.search(text)
            address_block = None
            city = None
            zip_code = None
            if addr_m:
                # The notice formats the address as lines:
                #   910 15th Ave Ne
                #   Rochester, MN 55906
                # Parse line-wise BEFORE whitespace-collapsing, otherwise a
                # city regex eats into the street text. A line matching
                # "<City>, MN <zip>" is the city line; lines before it are
                # the street.
                street_lines: list[str] = []
                for ln in (l.strip() for l in addr_m.group(1).split("\n")):
                    if not ln:
                        continue
                    cz = _RE_CITY_ZIP.search(ln)
                    if cz:
                        city = _clean_ws(cz.group(1))
                        zip_code = cz.group(2)
                        # Single-line form: "422 7th St NW, Rochester, MN
                        # 55901" — the text before the city IS the street.
                        prefix = _clean_ws(ln[: cz.start()].rstrip(" ,"))
                        if prefix:
                            street_lines.append(prefix)
                        break
                    street_lines.append(ln)
                address_block = _clean_ws(" ".join(street_lines)) or None

            mortgagor_m = _RE_MORTGAGOR.search(text)
            redemption_months = _extract_redemption_months(text)
            # Prefer the notice's EXPLICITLY STATED vacate/redemption
            # deadline over any computed date — it's authoritative.
            vacate_m = _RE_VACATE_DATE.search(text)
            stated_expiry = _parse_long_date(vacate_m.group(1)) if vacate_m else None
            abandon_caveat = bool(_RE_ABANDON_CAVEAT.search(text))
            # Resolve the redemption-expiry date, honestly labeling basis:
            #   'stated'   -> the notice printed the date (best)
            #   'computed' -> sale_date + stated period
            #   'default'  -> sale_date + 6mo fallback (period not stated)
            if stated_expiry:
                redemption_expires = stated_expiry
                redemption_basis = "stated"
            elif sale_date and redemption_months:
                redemption_expires = _add_months(sale_date, redemption_months)
                redemption_basis = "computed"
            elif sale_date:
                redemption_expires = _add_months(sale_date, 6)
                redemption_basis = "default_6mo"
            else:
                redemption_expires = None
                redemption_basis = None
            amount_due = _safe_money(
                (_RE_AMOUNT_DUE.search(text) or [None, None])[1]
                if _RE_AMOUNT_DUE.search(text) else None
            )
            principal = _safe_money(
                _RE_PRINCIPAL.search(text).group(1)
                if _RE_PRINCIPAL.search(text) else None
            )

            notice_id = str(
                hit.get("id") or hit.get("_id") or hit.get("noticeid")
                or hit.get("objectID") or pin
            )

            signals.append(DistressEventInsert(
                parcel_id=pin,
                event_type="sheriff_sale",
                event_subtype="foreclosure_notice",
                # The SCHEDULED sale date — a real published date; honest
                # None if the notice somehow lacks one (dedup key is
                # NULLS NOT DISTINCT).
                event_date=sale_date,
                event_value=amount_due,
                source=self.source_name,
                source_id=notice_id,
                severity="high",  # type: ignore[arg-type]
                title=_TITLE,
                description=_DESC,
                raw_data={
                    "property_address": address_block,
                    "property_city": city,
                    "property_zip": zip_code,
                    "county": county or "Olmsted",
                    "mortgagor": _clean_ws(
                        mortgagor_m.group(1)) if mortgagor_m else None,
                    "amount_due": str(amount_due) if amount_due is not None else None,
                    "original_principal": (
                        str(principal) if principal is not None else None
                    ),
                    "sale_date": sale_date.isoformat() if sale_date else None,
                    "sale_time": sale_time,
                    # Redemption clock, extracted honestly from the notice:
                    #   redemption_months  = period stated in text (or null)
                    #   redemption_expires = the deadline; basis says how we
                    #     got it (stated in notice / computed / 6mo default)
                    #   redemption_abandonment_caveat = true if the notice
                    #     warns the period may drop to 5 weeks if abandoned
                    "redemption_months": redemption_months,
                    "redemption_expires": (
                        redemption_expires.isoformat()
                        if redemption_expires else None
                    ),
                    "redemption_basis": redemption_basis,
                    "redemption_abandonment_caveat": abandon_caveat,
                    "postponed": bool(postponements),
                    "notice_id": notice_id,
                    "newspaper": _NEWSPAPER,
                    "pin_matched": True,
                },
                observed_at=datetime.now(timezone.utc),
            ))

        logger.info(
            "Post Bulletin notices parsed",
            source=self.source_name,
            events=len(signals),
            skipped_no_text=skipped_no_text,
            skipped_not_foreclosure=skipped_not_fc,
            skipped_no_pin=skipped_no_pin,
            skipped_other_county=skipped_county,
        )
        return signals

    # ---- Write: idempotent dedup upsert ----

    async def write(
        self, signals: list[DistressEventInsert]
    ) -> tuple[int, int, int]:
        if not signals:
            # No current foreclosure notices is honest state (small county).
            return 0, 0, 0
        new_events, failed_events = write_events_dedup(signals)
        logger.info(
            "Post Bulletin write complete",
            source=self.source_name,
            events_new=new_events,
            failed=failed_events,
        )
        return new_events, 0, failed_events


__all__ = ["PostBulletinLegalScraper"]
