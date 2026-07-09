"""
Ramsey County Tax-Forfeited Lands (TFL) scraper — auction list + OTC.

Fills the Ramsey tax-forfeit coverage gap AND delivers sale-status
(auction / over-the-counter) in one source. Built 2026-07-09 while the
Spring 2026 cycle is live (Auction List dated March 13, 2026; rebid/OTC
window opened June 25, 2026).

=== SOURCES (verified live 2026-07-09) ===
1. AUCTION LIST — the county publishes each cycle's inventory as an HTML
   table on a cycle page under:
     https://www.ramseycountymn.gov/residents/property-home/taxes-values/
       productive-properties/tax-forfeited-public-sales
   Columns: Property Address (linked to Beacon with KeyValue=<12-digit
   Ramsey PIN>), City, Property Type, Legal Description, Specials Before
   Forfeiture, Specials After Forfeiture, Appraised Value. The page text
   carries a REAL list date ("Auction List dated ... March 13, 2026") —
   used as event_date (honest; no sentinel).
2. OTC LIST — "Parcels Available For Immediate Purchase":
     https://xnet.co.ramsey.mn.us/prr/tfl/purchase.aspx
   Same data family. EMPTY between cycles ("No property is available at
   the moment.") — an EMPTY LIST IS HONEST STATE, not a failure. The
   scraper emits 0 OTC rows and succeeds.

=== HONESTY RULES ===
- event_date = the published list date; None when the page carries none
  (dedup key is NULLS NOT DISTINCT since 2026-07-07, so NULL dedups).
- event_value = the county's APPRAISED value (the minimum bid), NOT a
  market estimate — titled/described as such.
- Sale mechanics (minimum bid = appraised + certified specials;
  purchaser-intent rules for residential <=4 units post-Tyler) are
  surfaced as the county states them, never paraphrased into advice.

=== ARCHITECTURE ===
fetch():  GET the public-sales parent page, discover current cycle
          page(s), GET each + the OTC page. Individual page failures are
          warned and skipped; ALL pages failing raises
          SourceUnavailableError.
parse():  stdlib-HTMLParser table extraction (ZERO new dependencies —
          requirements.txt untouched). The target table is identified by
          its header containing 'Appraised'. PIN comes from the Beacon
          link's KeyValue and is normalized via the standard Ramsey rule.
write():  write_events_dedup (idempotent). Parcels are NOT created here —
          Ramsey's 163K-parcel spine already exists; PINs join directly.

Dedup identity: (parcel_id, 'tax_forfeit', <list date>, 'ramsey_tfl') —
a NEW cycle publishes a NEW list date, so each cycle's listing is its own
event (correct: being listed twice is two facts).
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from typing import Any, ClassVar

import httpx

from src.models.signal import DistressEventInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import write_events_dedup
from src.utils.errors import SourceUnavailableError
from src.utils.logger import logger
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id

_PARENT_URL = (
    "https://www.ramseycountymn.gov/residents/property-home/taxes-values/"
    "productive-properties/tax-forfeited-public-sales"
)
_OTC_URL = "https://xnet.co.ramsey.mn.us/prr/tfl/purchase.aspx"
_OTC_EMPTY_MARKER = "no property is available"

_REQUEST_TIMEOUT = 30.0
_MAX_CYCLE_PAGES = 3  # defensive cap on discovered auction pages

_TITLE_AUCTION = "Tax-forfeited land (Ramsey auction list)"
_TITLE_OTC = "Tax-forfeited land (Ramsey, available over the counter)"
_DESC_AUCTION = (
    "Parcel on Ramsey County's tax-forfeited land auction list. Sold to the "
    "highest bidder, but not for less than the county's appraised value "
    "together with certified special assessments after forfeiture "
    "(county-stated terms)."
)
_DESC_OTC = (
    "Tax-forfeited parcel listed by Ramsey County as available for immediate "
    "over-the-counter purchase (county-stated terms apply)."
)

# "Auction List dated the March 13, 2026" / "dated March 13, 2026"
_LIST_DATE_RE = re.compile(
    r"Auction List dated(?:\s+the)?\s+([A-Z][a-z]+ \d{1,2}, \d{4})"
)
_KEYVALUE_RE = re.compile(r"KeyValue=(\d{9,14})", re.IGNORECASE)
_MONEY_RE = re.compile(r"[-$,\s]")


class _TableParser(HTMLParser):
    """Dependency-free HTML table extractor.

    Collects every <table> as a list of rows; each cell is
    {"text": <collapsed text>, "links": [hrefs]}. Tolerates nested inline
    tags, entities, and missing </td>s the way real county CMS markup
    demands. Not a general HTML parser — just enough, tested."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[dict[str, Any]]]] = []
        self._table_depth = 0
        self._row: list[dict[str, Any]] | None = None
        self._cell: dict[str, Any] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self.tables.append([])
        if self._table_depth != 1:
            return
        if tag == "tr":
            self._flush_cell()
            self._row = []
        elif tag in ("td", "th"):
            self._flush_cell()
            self._cell = {"text": "", "links": []}
        elif tag == "a" and self._cell is not None:
            href = dict(attrs).get("href")
            if href:
                self._cell["links"].append(href)

    def handle_endtag(self, tag: str) -> None:
        if tag == "table" and self._table_depth > 0:
            if self._table_depth == 1:
                self._flush_cell()
                self._flush_row()
            self._table_depth -= 1
            return
        if self._table_depth != 1:
            return
        if tag in ("td", "th"):
            self._flush_cell()
        elif tag == "tr":
            self._flush_cell()
            self._flush_row()

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell["text"] += data

    def _flush_cell(self) -> None:
        if self._cell is not None and self._row is not None:
            self._cell["text"] = " ".join(self._cell["text"].split())
            self._row.append(self._cell)
        self._cell = None

    def _flush_row(self) -> None:
        if self._row:
            self.tables[-1].append(self._row)
        self._row = None


def _extract_tables(html: str) -> list[list[list[dict[str, Any]]]]:
    p = _TableParser()
    p.feed(html)
    p.close()
    return p.tables


def _find_listing_table(
    html: str,
) -> tuple[list[str], list[list[dict[str, Any]]]] | None:
    """Find the table whose header row mentions 'Appraised'.
    Returns (lowercased header texts, data rows) or None."""
    for table in _extract_tables(html):
        if not table:
            continue
        header = [c["text"].lower() for c in table[0]]
        if any("appraised" in h for h in header):
            return header, table[1:]
    return None


def _col_index(header: list[str], *needles: str) -> int | None:
    """First column whose header contains ALL the given needles."""
    for i, h in enumerate(header):
        if all(n in h for n in needles):
            return i
    return None


def _cell(row: list[dict[str, Any]], idx: int | None) -> dict[str, Any]:
    if idx is None or idx >= len(row):
        return {"text": "", "links": []}
    return row[idx]


def _safe_money(text: str) -> Decimal | None:
    s = _MONEY_RE.sub("", text or "")
    if not s:
        return None
    try:
        d = Decimal(s)
        return d if d >= 0 else None
    except (InvalidOperation, ValueError):
        return None


def _parse_list_date(html_text: str) -> date | None:
    m = _LIST_DATE_RE.search(html_text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%B %d, %Y").date()
    except ValueError:
        return None


def _pin_from_links(links: list[str]) -> str | None:
    for href in links:
        m = _KEYVALUE_RE.search(href)
        if m:
            pin, err = safe_normalize_parcel_id("ramsey", m.group(1))
            if pin:
                return pin
    return None


class RamseyTflScraper(BaseScraper[dict[str, Any], DistressEventInsert]):
    """Ramsey tax-forfeited lands: auction-list + OTC pages."""

    source_name: ClassVar[str] = "ramsey_tfl"
    signal_type: ClassVar[str] = "tax_forfeit"
    county_code: ClassVar[str] = "ramsey"

    # ---- Fetch: parent -> cycle page(s) + OTC ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        failures = 0
        async with httpx.AsyncClient(
            timeout=_REQUEST_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; govire/1.0)"},
        ) as client:
            # 1. Parent page -> discover current auction cycle page(s).
            cycle_urls: list[str] = []
            try:
                resp = await client.get(_PARENT_URL)
                resp.raise_for_status()
                seen: set[str] = set()
                for href in re.findall(r'href="([^"]+)"', resp.text):
                    if "tax-forfeited" in href and (
                        "auction" in href or "sale" in href.split("/")[-1]
                    ):
                        url = (
                            href
                            if href.startswith("http")
                            else "https://www.ramseycountymn.gov" + href
                        )
                        if url.rstrip("/") == _PARENT_URL.rstrip("/"):
                            continue
                        if url not in seen:
                            seen.add(url)
                            cycle_urls.append(url)
                cycle_urls = cycle_urls[:_MAX_CYCLE_PAGES]
            except httpx.HTTPError as e:
                failures += 1
                logger.warning(
                    "TFL parent page fetch failed",
                    source=self.source_name, error=str(e)[:300],
                )

            # 2. Each cycle page.
            for url in cycle_urls:
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    records.append(
                        {"kind": "auction", "url": url, "html": resp.text}
                    )
                except httpx.HTTPError as e:
                    failures += 1
                    logger.warning(
                        "TFL cycle page fetch failed",
                        source=self.source_name, url=url, error=str(e)[:300],
                    )

            # 3. OTC page (empty between cycles is normal).
            try:
                resp = await client.get(_OTC_URL)
                resp.raise_for_status()
                records.append(
                    {"kind": "otc", "url": _OTC_URL, "html": resp.text}
                )
            except httpx.HTTPError as e:
                failures += 1
                logger.warning(
                    "TFL OTC page fetch failed",
                    source=self.source_name, error=str(e)[:300],
                )

        if not records and failures:
            raise SourceUnavailableError(
                "All Ramsey TFL pages failed to fetch",
                source=self.source_name,
                context={"failures": failures},
            )
        logger.info(
            "Ramsey TFL fetch complete",
            source=self.source_name,
            pages=len(records),
            cycle_pages=sum(1 for r in records if r["kind"] == "auction"),
        )
        return records

    # ---- Parse: table rows -> tax_forfeit events ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[DistressEventInsert]:
        signals: list[DistressEventInsert] = []
        for page in raw_records:
            kind = page["kind"]
            html = page["html"] or ""

            if kind == "otc" and _OTC_EMPTY_MARKER in html.lower():
                logger.info(
                    "Ramsey TFL OTC list is empty (honest between-cycles state)",
                    source=self.source_name,
                )
                continue

            found = _find_listing_table(html)
            if found is None:
                # A cycle page without a listing table (e.g. terms-only page)
                # is not an error; log and move on.
                logger.info(
                    "Ramsey TFL page had no listing table",
                    source=self.source_name, kind=kind, url=page["url"],
                )
                continue
            header, rows = found

            i_addr = _col_index(header, "address")
            i_city = _col_index(header, "city")
            i_type = _col_index(header, "type")
            i_legal = _col_index(header, "legal")
            i_sp_before = _col_index(header, "special", "before")
            i_sp_after = _col_index(header, "special", "after")
            i_appraised = _col_index(header, "appraised")

            list_date = _parse_list_date(html) if kind == "auction" else None
            subtype = "auction_listed" if kind == "auction" else "otc_available"
            title = _TITLE_AUCTION if kind == "auction" else _TITLE_OTC
            desc = _DESC_AUCTION if kind == "auction" else _DESC_OTC

            n_rows = 0
            for row in rows:
                addr_cell = _cell(row, i_addr)
                pin = _pin_from_links(addr_cell["links"])
                address = addr_cell["text"] or None
                if not pin and not address:
                    continue  # blank/decorative row
                appraised = _safe_money(_cell(row, i_appraised)["text"])
                signals.append(DistressEventInsert(
                    parcel_id=pin or f"RAMSEY-TFL-{abs(hash(address)) % 10**8}",
                    event_type="tax_forfeit",
                    event_subtype=subtype,
                    # Real published list date for auction rows; honest
                    # None otherwise (dedup key is NULLS NOT DISTINCT).
                    event_date=list_date,
                    event_value=appraised,
                    source=self.source_name,
                    source_id=pin or (address or "unknown"),
                    severity="medium",  # type: ignore[arg-type]
                    title=title,
                    description=desc,
                    raw_data={
                        "property_address": address,
                        "property_city": _cell(row, i_city)["text"] or None,
                        "property_type": _cell(row, i_type)["text"] or None,
                        "legal_description": _cell(row, i_legal)["text"] or None,
                        "specials_before_forfeiture": str(
                            _safe_money(_cell(row, i_sp_before)["text"]) or ""
                        ) or None,
                        "specials_after_forfeiture": str(
                            _safe_money(_cell(row, i_sp_after)["text"]) or ""
                        ) or None,
                        "appraised_value": str(appraised) if appraised is not None else None,
                        "sale_status": subtype,
                        "list_date": list_date.isoformat() if list_date else None,
                        "source_page": page["url"],
                        "beacon_links": addr_cell["links"][:3],
                        "pin_matched": pin is not None,
                    },
                    observed_at=datetime.now(timezone.utc),
                ))
                n_rows += 1

            logger.info(
                "Ramsey TFL page parsed",
                source=self.source_name, kind=kind, rows=n_rows,
                list_date=str(list_date),
            )
        return signals

    # ---- Write: idempotent dedup upsert (events only; parcels exist) ----

    async def write(
        self, signals: list[DistressEventInsert]
    ) -> tuple[int, int, int]:
        if not signals:
            # 0 rows between cycles is honest success, never a failure.
            return 0, 0, 0
        new_events, failed_events = write_events_dedup(signals)
        logger.info(
            "Ramsey TFL write complete",
            source=self.source_name,
            events_new=new_events,
            failed=failed_events,
        )
        return new_events, 0, failed_events


__all__ = ["RamseyTflScraper"]
