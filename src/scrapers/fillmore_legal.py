"""
Fillmore County Journal legal-notices scraper — Fillmore foreclosure notices.

THE FIRST FILLMORE SIGNAL SOURCE (2026-07-23): turns the 19,995-parcel
Fillmore spine loaded earlier today into a live county. MN foreclosures
must publish in the county's qualified newspaper (Minn. Stat. 580.03) —
for Fillmore that is the Fillmore County Journal, a WordPress site that
posts every legal notice as a public article in its "Legal Notice"
category (id 12, 2,480 posts of history at inspection).

=== SOURCE (verified live, 2026-07-23) ===
GET https://fillmorecountyjournal.com/wp-json/wp/v2/posts
    ?categories=12&per_page=100&orderby=date&order=desc&after=<ISO8601>
Open WordPress REST API (v7.0.2), no auth. Returns a JSON list of posts:
  id, date (ISO, site-local), link, title.rendered, content.rendered
  (HTML — tags stripped before parsing).

=== WHAT A NOTICE CARRIES (verified on live notices today) ===
Two families, both from standard attorney forms:
  BY-ADVERTISEMENT (580):   "NOTICE OF MORTGAGE FORECLOSURE SALE" —
    PROPERTY ADDRESS: 15531 COUNTY 21, CANTON, MN 55922
    "Property Tax ID No. 190279000"
    DATE AND TIME OF SALE: 10:00AM on August 25, 2022   (time-first!)
      ...or "on Thursday, June 12, 2025 at 10:00 a.m."  (weekday form)
    redemption "...is 6 months after the date of sale"
  JUDICIAL (581):           "NOTICE OF SHERIFF'S SALE UNDER JUDGMENT
    AND DECREE" — "commonly known as 98 River St, Peterson, MN 55962",
    "PID# 070126000", "public auction ... on June 25, 2026, at 10:00 AM"
The Journal ALSO publishes neighboring counties' notices (a Houston
County notice was observed) -> a Fillmore county gate is mandatory.

=== HONESTY RULES (identical to postbulletin_legal) ===
- Only rows with a parseable parcel PIN ingest; PIN-less rows are logged
  and skipped — NEVER a synthetic id.
- Only Fillmore-located notices ingest; others log and skip.
- event_date = the SCHEDULED sale date (a real, published date); the
  latest postponement wins when present.
- Rows are SCHEDULED sales — surfaced as such, never as completed.

Dedup identity: (parcel_id, 'sheriff_sale', <sale date>, source).
"""

from __future__ import annotations

import html as _html
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


_API_URL = "https://fillmorecountyjournal.com/wp-json/wp/v2/posts"
_CATEGORY_ID = 12          # "Legal Notice" (verified live 2026-07-23)
_NEWSPAPER = "Fillmore County Journal"
_WINDOW_DAYS = 45          # covers the full 6-week publication run
_PAGE_SIZE = 100           # WP REST max
_MAX_PAGES = 3             # weekly paper; 45 days is well under 300 legals
_REQUEST_TIMEOUT = 30.0

_TITLE = "Scheduled sheriff's sale (Fillmore County Journal notice)"
_DESC = (
    "Mortgage foreclosure sale notice published in the Fillmore County "
    "Journal (Fillmore County's qualified newspaper). The property is "
    "scheduled to be sold by the Sheriff of Fillmore County at public "
    "auction on the stated date unless the mortgage is reinstated or the "
    "property redeemed (as stated in the notice)."
)

# ---- Notice-text field extraction (built from live notices) ----

_RE_PIN = re.compile(
    # Label variants seen live in FCJ notices: "PID# 070126000" (judicial),
    # "Property Tax ID No. 190279000" (by-advertisement), plus the
    # statewide-form labels the sibling sees ("TAX PARCEL NO.:" etc.) —
    # foreclosure attorneys reuse the same templates across counties.
    r"(?:PID\s*#|PROPERTY TAX ID(?:ENTIFICATION)?\s*(?:NO\.?|NUMBER)?|"
    r"TAX PARCEL(?:\s+I\.?D\.?)?\s+NO\.?|PROPERTY IDENTIFICATION NUMBER|"
    r"PARCEL (?:ID|NO)\.?)"
    r"\s*:?\s*([A-Z]{0,3}[0-9.\- ]{6,30})", re.I
)
_WEEKDAY = r"(?:(?:Mon|Tues|Wednes|Thurs|Fri|Satur|Sun)day,?\s+)?"
_LONG_DATE = r"([A-Z][a-z]+ \d{1,2}, \d{4})"
_TIME = r"(\d{1,2}:\d{2}\s*[AP]\.?\s*M\.?)"
_RE_SALE_LABELED = re.compile(
    # "DATE AND TIME OF SALE:" followed by either order —
    #   date-first: "July 27, 2026, 10:00 AM"   (sibling's PB form)
    #   time-first: "10:00AM on August 25, 2022"  (FCJ 2022 form)
    r"DATE AND TIME OF SALE\s*:?\s*"
    r"(?:" + _TIME + r"\s*(?:on\s+)?" + _WEEKDAY + _LONG_DATE +
    r"|" + _WEEKDAY + _LONG_DATE + r"(?:\s*,?\s*(?:at\s*)?" + _TIME + r")?)",
    re.I,
)
_RE_SALE_AUCTION = re.compile(
    # Judicial form: "will sell at public auction ... on June 25, 2026,
    # at 10:00 AM" / "shall be sold ... at public auction on Thursday,
    # June 12, 2025 at 10:00 a.m."
    r"public auction[^.]{0,160}?\bon\s+" + _WEEKDAY + _LONG_DATE +
    r"(?:\s*,?\s*at\s*" + _TIME + r")?",
    re.I | re.S,
)
_RE_ADDRESS = re.compile(
    # FCJ variants: "PROPERTY ADDRESS: 15531 COUNTY 21, CANTON, MN 55922",
    # "commonly known as 98 River St, Peterson, MN 55962",
    # "the land located at 301 Kirkwood Street East, Lanesboro, Minnesota".
    r"(?:PROPERTY ADDRESS|ADDRESS OF (?:THE )?PROPERTY|commonly known as|"
    r"(?:land |premises )?located at)\s*:?\s*(.{5,120}?)"
    r"(?=\s*(?:,? and legally|,? legally described|COUNTY IN WHICH|TAX PARCEL|"
    r"PROPERTY TAX ID|PID\s*#|PROPERTY IDENTIFICATION|THE AMOUNT|"
    r"ORIGINAL PRINCIPAL|and shall be sold|$))",
    re.I | re.S,
)
_RE_COUNTY_LABEL = re.compile(
    r"COUNTY IN WHICH PROPERTY IS LOCATED\s*:?\s*([A-Za-z .]{3,30})", re.I
)
_RE_SHERIFF_OF = re.compile(r"Sheriff of ([A-Za-z .]{3,30}?) County", re.I)
_RE_MORTGAGOR = re.compile(
    r"MORTGAGOR(?:\(S\))?\s*:?\s*(.{3,120}?)(?=\s*(?:MORTGAGEE|Mortgagee)\b)",
    re.I | re.S,
)
_RE_PRINCIPAL = re.compile(
    r"ORIGINAL PRINCIPAL AMOUNT (?:SECURED BY THE MORTGAGE|OF MORTGAGE)"
    r"\s*(?:was)?\s*:?\s*\$?([\d,]+(?:\.\d{2})?)",
    re.I,
)
_RE_AMOUNT_DUE = re.compile(
    r"AMOUNT (?:CLAIMED TO BE )?DUE(?: AND CLAIMED TO BE DUE)?"
    r"[^:$]{0,120}[:$]\s*\$?\s*([\d,]+(?:\.\d{2})?)",
    re.I,
)
_RE_POSTPONED = re.compile(
    r"postponed (?:until|to)\s+" + _WEEKDAY + _LONG_DATE, re.I
)
_RE_REDEMPTION = re.compile(
    r"redemption[^.]{0,80}?(?:within|is|of|allowed[^.]{0,20}?is)?\s*"
    r"(\d{1,2}|one|two|three|six|twelve)\s*(?:\(\s*\d{1,2}\s*\))?\s*months?",
    re.I,
)
_WORD_MONTHS = {"one": 1, "two": 2, "three": 3, "six": 6, "twelve": 12}
_RE_VACATE_DATE = re.compile(
    # Sibling form "DATE TO VACATE ... is <date>" plus the FCJ form
    # "must vacate the property on or before 11:59 p.m. on Friday,
    # December 13, 2025".
    r"(?:DATE TO VACATE|MUST VACATE)"
    r"(?:(?!DATE AND TIME OF SALE|MORTGAGOR\(S\) RELEASED).){0,300}?"
    r"\b(?:is|on(?: or before)?)\s+(?:11\s*:\s*59[^,]{0,20},?\s*)?(?:on\s+)?"
    + _WEEKDAY + _LONG_DATE,
    re.I | re.S,
)
_RE_ABANDON_CAVEAT = re.compile(
    r"REDUCED TO FIVE WEEKS.*?ABANDONED", re.I | re.S,
)
_RE_CITY_ZIP = re.compile(
    r"([A-Za-z .]+?),?\s*(?:MN|Minnesota)\s*,?\s*(\d{5})?", re.I
)
_RE_TAG = re.compile(r"<[^>]+>")


def _add_months(d: date, months: int) -> date:
    """Add whole months to a date using stdlib only; clamps the day to the
    target month's length. Adequate for redemption-expiry dates."""
    import calendar
    m0 = d.month - 1 + months
    year = d.year + m0 // 12
    month = m0 % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _extract_redemption_months(text: str) -> int | None:
    m = _RE_REDEMPTION.search(text)
    if not m:
        return None
    tok = m.group(1).strip().lower()
    val = int(tok) if tok.isdigit() else _WORD_MONTHS.get(tok)
    if val is not None and 1 <= val <= 36:
        return val
    return None


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


def _strip_html(rendered: str) -> str:
    """WP content.rendered -> plain text: unescape entities (incl. smart
    quotes), drop tags, collapse whitespace."""
    text = _html.unescape(rendered)
    text = _RE_TAG.sub(" ", text)
    return " ".join(text.split())


def _extract_pin(text: str) -> str | None:
    m = _RE_PIN.search(text)
    if not m:
        return None
    digits = re.sub(r"[^0-9]", "", m.group(1))
    # Fillmore PINs are 9 digits (verified spine 2026-07-23); accept a
    # modest band around it for attorney formatting quirks.
    if not (8 <= len(digits) <= 13):
        return None
    pin, _err = safe_normalize_parcel_id("fillmore", digits)
    return pin


def _extract_sale(text: str) -> tuple[date | None, str | None]:
    """Scheduled sale date + time across the observed phrasings."""
    m = _RE_SALE_LABELED.search(text)
    if m:
        # Groups: (time1, date1, date2, time2) depending on branch.
        time_s = m.group(1) or m.group(4)
        date_s = m.group(2) or m.group(3)
        return _parse_long_date(date_s), _clean_ws(time_s)
    m = _RE_SALE_AUCTION.search(text)
    if m:
        return _parse_long_date(m.group(1)), _clean_ws(m.group(2))
    return None, None


def _trim_county(raw: str) -> str:
    """The county capture can bleed into the NEXT all-caps notice label
    when the text is whitespace-collapsed ('Fillmore THE AMOUNT...').
    Keep words up to the first all-caps token (county names are mixed
    case: 'Fillmore', 'Otter Tail', 'St. Louis')."""
    words: list[str] = []
    for w in raw.split():
        if len(w) > 1 and w.isupper():
            break
        words.append(w)
        if len(words) >= 3:
            break
    return " ".join(words) or raw


def _fillmore_gate(text: str) -> tuple[bool, str | None]:
    """(is_fillmore, detected_county). The Journal publishes neighboring
    counties' notices too, so location is checked on three cues in
    priority order: the labeled county field, 'Sheriff of X County', and
    the legal description's 'X County, Minnesota'."""
    m = _RE_COUNTY_LABEL.search(text)
    if m:
        county = _trim_county(_clean_ws(m.group(1)) or "")
        return ("fillmore" in county.lower(), county)
    m = _RE_SHERIFF_OF.search(text)
    if m:
        county = _clean_ws(m.group(1)) or ""
        return ("fillmore" in county.lower(), county)
    if re.search(r"Fillmore County, Minnesota", text, re.I):
        return True, "Fillmore"
    return False, None


class FillmoreLegalScraper(BaseScraper[dict[str, Any], DistressEventInsert]):
    """Fillmore County Journal foreclosure notices -> Fillmore sheriff sales."""

    source_name: ClassVar[str] = "fillmore_legal"
    signal_type: ClassVar[str] = "sheriff_sale"
    county_code: ClassVar[str] = "fillmore"

    # ---- Fetch: WP REST, windowed, defensively paged ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        after = (
            datetime.now(timezone.utc) - timedelta(days=_WINDOW_DAYS)
        ).strftime("%Y-%m-%dT%H:%M:%S")
        posts: list[dict[str, Any]] = []
        try:
            async with httpx.AsyncClient(
                timeout=_REQUEST_TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0 (compatible; govire/1.0)"},
            ) as client:
                for page in range(1, _MAX_PAGES + 1):
                    resp = await client.get(_API_URL, params={
                        "categories": _CATEGORY_ID,
                        "per_page": _PAGE_SIZE,
                        "page": page,
                        "orderby": "date",
                        "order": "desc",
                        "after": after,
                        "status": "publish",
                        "_fields": "id,date,link,title,content",
                    })
                    # WP returns 400 for a page past the last one; treat as
                    # end-of-results rather than failure.
                    if resp.status_code == 400 and page > 1:
                        break
                    resp.raise_for_status()
                    batch = resp.json()
                    if not isinstance(batch, list):
                        raise SourceUnavailableError(
                            "WP API returned a non-list payload",
                            source=self.source_name,
                        )
                    posts.extend(p for p in batch if isinstance(p, dict))
                    if len(batch) < _PAGE_SIZE:
                        break
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"Fillmore County Journal WP API failed: {str(e)[:300]}",
                source=self.source_name,
            ) from e
        except ValueError as e:
            raise SourceUnavailableError(
                f"WP API returned non-JSON: {str(e)[:200]}",
                source=self.source_name,
            ) from e

        logger.info(
            "Fillmore County Journal fetch complete",
            source=self.source_name,
            posts=len(posts),
            window_days=_WINDOW_DAYS,
        )
        return posts

    # ---- Parse: post HTML -> scheduled sheriff-sale events ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[DistressEventInsert]:
        signals: list[DistressEventInsert] = []
        skipped_no_text = skipped_not_fc = skipped_no_pin = 0
        skipped_county = 0

        for post in raw_records:
            rendered = ((post.get("content") or {}).get("rendered")
                        or "") + " " + ((post.get("title") or {}).get(
                            "rendered") or "")
            text = _strip_html(rendered)
            if len(text) < 200:
                skipped_no_text += 1
                continue

            up = text.upper()
            # The Legal Notice category mixes elections, board minutes,
            # assessments etc. — only foreclosure/sheriff-sale notices pass.
            if "FORECLOSURE" not in up and "SHERIFF" not in up:
                skipped_not_fc += 1
                continue

            is_fillmore, county = _fillmore_gate(text)
            if not is_fillmore:
                skipped_county += 1
                logger.info(
                    "Notice outside Fillmore skipped",
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

            sale_date, sale_time = _extract_sale(text)
            postponements = [
                _parse_long_date(d) for d in _RE_POSTPONED.findall(text)
            ]
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
                seg = _clean_ws(addr_m.group(1)) or ""
                cz = _RE_CITY_ZIP.search(seg)
                if cz:
                    city = _clean_ws(cz.group(1))
                    zip_code = cz.group(2)
                    prefix = _clean_ws(seg[: cz.start()].rstrip(" ,"))
                    address_block = prefix or None
                else:
                    address_block = seg or None

            mortgagor_m = _RE_MORTGAGOR.search(text)
            redemption_months = _extract_redemption_months(text)
            vacate_m = _RE_VACATE_DATE.search(text)
            stated_expiry = (
                _parse_long_date(vacate_m.group(1)) if vacate_m else None
            )
            abandon_caveat = bool(_RE_ABANDON_CAVEAT.search(text))
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
                _RE_AMOUNT_DUE.search(text).group(1)
                if _RE_AMOUNT_DUE.search(text) else None
            )
            principal = _safe_money(
                _RE_PRINCIPAL.search(text).group(1)
                if _RE_PRINCIPAL.search(text) else None
            )

            post_id = str(post.get("id") or pin)

            signals.append(DistressEventInsert(
                parcel_id=pin,
                event_type="sheriff_sale",
                event_subtype="foreclosure_notice",
                event_date=sale_date,
                event_value=amount_due,
                source=self.source_name,
                source_id=post_id,
                severity="high",  # type: ignore[arg-type]
                title=_TITLE,
                description=_DESC,
                raw_data={
                    "property_address": address_block,
                    "property_city": city,
                    "property_zip": zip_code,
                    "county": county or "Fillmore",
                    "mortgagor": _clean_ws(
                        mortgagor_m.group(1)) if mortgagor_m else None,
                    "amount_due": (
                        str(amount_due) if amount_due is not None else None
                    ),
                    "original_principal": (
                        str(principal) if principal is not None else None
                    ),
                    "sale_date": sale_date.isoformat() if sale_date else None,
                    "sale_time": sale_time,
                    "redemption_months": redemption_months,
                    "redemption_expires": (
                        redemption_expires.isoformat()
                        if redemption_expires else None
                    ),
                    "redemption_basis": redemption_basis,
                    "redemption_abandonment_caveat": abandon_caveat,
                    "postponed": bool(postponements),
                    "notice_url": post.get("link"),
                    "published_date": post.get("date"),
                    "newspaper": _NEWSPAPER,
                    "pin_matched": True,
                },
                observed_at=datetime.now(timezone.utc),
            ))

        logger.info(
            "Fillmore County Journal notices parsed",
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
            "Fillmore County Journal write complete",
            source=self.source_name,
            events_new=new_events,
            failed=failed_events,
        )
        return new_events, 0, failed_events


__all__ = ["FillmoreLegalScraper"]
