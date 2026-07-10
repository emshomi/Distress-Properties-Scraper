"""
Olmsted Tyler-portal (iasWorld) per-parcel tax detail scraper.

THE YEARS-BEHIND BUILD (specced from live recon, 2026-07-10): the annual
delinquent-tax list (signals.distress_events, source='olmsted_delq_list',
502 parcels) is a snapshot with no per-year breakdown — a parcel owing
$8k could be in year 1 of the ~3-year forfeiture clock or months from
judgment expiry. The county's Tyler iasWorld portal
(publicaccess.co.olmsted.mn.us) carries the authoritative per-parcel,
PER-YEAR delinquency detail. This scraper visits each listed parcel and
extracts it, turning the flat list into a ranked closest-to-forfeiture
view — plus list hygiene (recon found the top-2 parcels by amount owed
had BOTH redeemed since the list published) and owner mailing addresses.

=== SOURCE (reverse-engineered live, 2026-07-10, screenshots on file) ===
Search:  /search/CommonSearch.aspx?mode=REALPROP
         - "Parcel ID" input accepts our bare 12-digit PARID verbatim
         - shorter input prefix-matches (11 digits returned 2 rows)
         - results land either on a grid (click the exact-PARID row) or
           directly on the record
Record:  /Datalets/Datalet.aspx?sIndex=..&idx=..   (SESSION-scoped —
         no deep-linking; must replay search each parcel)
         - left-nav links swap datalet mode within the session:
           Property Overview (default) -> Parcel Status flags
             (In Forfeiture / COJ / In Bankruptcy / Delinquent / Homestead)
           Property Taxes Due (mode=tax_all) -> "Current Taxes Due" and
             "Delinquent Taxes" tables, PER PAY YEAR:
             Pay Year | Base Taxes | Penalty Due | Fees Due | Interest Due
             | Total Amnt Paid | Date Last Paid | Total Due
           Ownership (mode=owner) -> owner name(s) + MAILING address
             (recon: distinct from property address — the skip-trace field)

=== VERIFICATION PARCELS (from recon; the 5-PIN test plan) ===
  743544075694  HP PB LLC            2025 delq REDEEMED 22-APR-2026
  743542084934  Civic Center Hotel   2025 delq $1.16M REDEEMED 25-JUN-2026
  641013084421  Apache Hotel Group   TRUE delinquent: 2025 unpaid
                $155,062.04 (base 132,390.00 + pen 16,548.75 + fees 40.00
                + int 6,083.29), 2026 also unpaid; mailing addr
                3123 SHERBURN PL SW ROCHESTER MN 55902

=== HONESTY RULES ===
- Parcels not found in the portal are logged and skipped — NEVER a
  synthetic row (the MPLS-VBR lesson). They count toward records_failed
  so the run reports partial, not a false success.
- estimated_judgment_date / estimated_forfeiture_date are COMPUTED from
  the statutory sequence (delinquency -> judgment 2nd Monday of May of
  the following year -> 3-year redemption) and ALWAYS carry
  forfeiture_basis='computed_3yr_statutory'. Never presented as
  county-stated. NULL when the parcel has no unpaid delinquent year.
- redeemed_since_list=true is a REAL observed outcome (parcel was on the
  annual list; portal now shows no unpaid delinquent year) — outcome-
  capture fuel, and the list-hygiene signal for the UI.
- Amounts, dates, and flags are portal-verbatim; blanks stay NULL.

=== WRITE TARGETS (migration 2026-07-10) ===
  signals.tax_delinquency_detail  one row per (parcel, pay_year, kind);
    kind='delinquent' rows are portal-verbatim per delinquent year;
    kind='current' rows AGGREGATE the current-year payment cycles (the
    portal shows 1NAN/2NAN half-year rows; the dedup key has no cycle
    column, so cycles are summed and preserved verbatim in raw_data).
  signals.tax_delinquency_status  one row per parcel: computed summary,
    status flags, owner mailing fields.
Both via write_typed_signals_dedup (upsert-update), so weekly re-runs
refresh in place.

=== RUNNER NOTES ===
Playwright/Chromium (session-based ASP.NET portal; requirements.txt
already pins playwright==1.49.0). First scheduled home: GitHub Actions
(~502 parcels x ~3.5s ≈ 30-35 min). iasWorld is not Cloudflare-fronted,
but if the county blocks datacenter IPs this ports to the local-runner
pattern (mnpublicnotice lesson) — the first Actions run is the test.
Selectors were written from recon screenshots plus iasWorld conventions;
the 5-PIN test run exists precisely to shake them out before the full
502.
"""

from __future__ import annotations

import asyncio
import calendar
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from bs4 import BeautifulSoup

from src.db.supabase_client import signals_table
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import write_typed_signals_dedup
from src.utils.errors import SourceUnavailableError
from src.utils.logger import logger

_BASE_URL = "https://publicaccess.co.olmsted.mn.us"
_SEARCH_URL = f"{_BASE_URL}/search/CommonSearch.aspx?mode=REALPROP"
_COUNTY_SLUG = "olmsted"
_LIST_SOURCE = "olmsted_delq_list"

_NAV_TIMEOUT_MS = 30_000
_PER_PARCEL_ATTEMPTS = 2
_POLITE_DELAY_SECONDS = 2.0  # between parcels; keep the county happy

# Left-nav link texts (exact, from recon screenshots)
_NAV_TAXES_DUE = "Property Taxes Due"
_NAV_OWNERSHIP = "Ownership"

# Parcel Status labels on the Property Overview datalet
_STATUS_LABELS = {
    "in forfeiture": "in_forfeiture",
    "coj": "coj",
    "in bankruptcy": "in_bankruptcy",
    "homestead": "homestead",
}

_RE_YEAR = re.compile(r"^(19|20)\d{2}$")


# ============================================================
# PARSING HELPERS (pure functions — unit-testable without a browser)
# ============================================================


def parse_money(text: str | None) -> Decimal | None:
    """Portal money -> Decimal. Handles '$1,163,798.87', '$.00' (zero),
    '$0.00', blanks -> None. Negative amounts are kept (overpayments
    can show as credits)."""
    if text is None:
        return None
    s = text.strip().replace("$", "").replace(",", "")
    if not s:
        return None
    if s == ".00":
        s = "0.00"
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def parse_portal_date(text: str | None) -> date | None:
    """Portal dates like '22-APR-26' / '25-JUN-26' -> date. Blank -> None."""
    if not text:
        return None
    s = text.strip()
    if not s:
        return None
    for fmt in ("%d-%b-%y", "%d-%b-%Y", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s.title(), fmt).date()
        except ValueError:
            continue
    return None


def parse_yes_no(text: str | None) -> bool | None:
    """'Yes'/'No' (any case) -> bool; blank/other -> None (honest null)."""
    if text is None:
        return None
    s = text.strip().lower()
    if s in ("yes", "y", "true"):
        return True
    if s in ("no", "n", "false"):
        return False
    return None


def second_monday_of_may(year: int) -> date:
    """MN tax judgment enters the second Monday in May (Minn. Stat. ch.
    279 sequence, per the platform strategy doc)."""
    first_weekday = date(year, 5, 1).weekday()  # Mon=0
    first_monday = 1 + (7 - first_weekday) % 7
    return date(year, 5, first_monday + 7)


def _cells(tr: Any) -> list[str]:
    return [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]


def _find_section_table(soup: BeautifulSoup, heading: str) -> Any | None:
    """Find the data <table> that follows the section heading text
    (e.g. 'Delinquent Taxes') and whose header row contains 'Pay Year'.
    iasWorld nests tables heavily, so we search all tables after the
    heading node rather than trusting sibling structure."""
    marker = soup.find(
        string=lambda t: isinstance(t, str) and t.strip() == heading
    )
    if marker is None:
        return None
    for table in marker.find_all_next("table"):
        header_rows = table.find_all("tr")
        if not header_rows:
            continue
        head = " ".join(_cells(header_rows[0])).lower()
        if "pay year" in head:
            return table
    return None


def parse_tax_table(soup: BeautifulSoup, heading: str) -> list[dict[str, Any]]:
    """Parse a 'Current Taxes Due' / 'Delinquent Taxes' table into a list
    of dicts keyed by normalized header names. Skips the trailing
    'Total:' row. Returns [] when the section is absent (honest: e.g. a
    parcel with no delinquent table)."""
    table = _find_section_table(soup, heading)
    if table is None:
        return []
    rows = table.find_all("tr")
    if len(rows) < 2:
        return []
    headers = [h.lower().strip() for h in _cells(rows[0])]
    out: list[dict[str, Any]] = []
    for tr in rows[1:]:
        vals = _cells(tr)
        if not vals:
            continue
        first = vals[0].rstrip(":").strip().lower()
        if first == "total" or (vals[0].strip() == "" and "total" in " ".join(vals).lower()[:20]):
            continue
        # Data rows start with a 4-digit pay year; anything else (spacer
        # rows, the Total row with a blank lead cell) is skipped.
        year_cell = next((v for v in vals if v.strip()), "")
        if not _RE_YEAR.match(vals[0].strip()) and not _RE_YEAR.match(year_cell):
            continue
        row: dict[str, Any] = {}
        for i, h in enumerate(headers):
            row[h] = vals[i] if i < len(vals) else None
        out.append(row)
    return out


def _row_get(row: dict[str, Any], *keys: str) -> str | None:
    """Header names drift slightly ('Total Amnt Paid' vs 'Total Amt
    Paid'); probe candidates."""
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return None


def normalize_tax_row(row: dict[str, Any]) -> dict[str, Any]:
    """Portal table row -> typed dict matching tax_delinquency_detail
    columns (minus identity/kind, added by caller)."""
    return {
        "pay_year": int(row.get("pay year", "0") or 0) or None,
        "base_taxes": parse_money(_row_get(row, "base taxes")),
        "penalty_due": parse_money(_row_get(row, "penalty due", "penalty")),
        "fees_due": parse_money(_row_get(row, "fees due", "fees")),
        "interest_due": parse_money(_row_get(row, "interest due", "interest")),
        "total_amt_paid": parse_money(
            _row_get(row, "total amnt paid", "total amt paid", "total amount paid")
        ),
        "date_last_paid": parse_portal_date(_row_get(row, "date last paid")),
        "total_due": parse_money(_row_get(row, "total due")),
    }


def parse_status_flags(soup: BeautifulSoup) -> dict[str, bool | None]:
    """Parcel Status flags from the Property Overview datalet. Label
    cells like 'In Forfeiture:' with a Yes/No/blank value cell."""
    flags: dict[str, bool | None] = {v: None for v in _STATUS_LABELS.values()}
    for tr in soup.find_all("tr"):
        vals = _cells(tr)
        if len(vals) < 1:
            continue
        label = vals[0].rstrip(":").strip().lower()
        if label in _STATUS_LABELS:
            value = vals[1] if len(vals) > 1 else None
            flags[_STATUS_LABELS[label]] = parse_yes_no(value)
    return flags


def parse_ownership(soup: BeautifulSoup) -> dict[str, str | None]:
    """Owner Details datalet -> names + mailing address. Recon shape:
        Name:            APACHE HOTEL GROUP LLC
        (blank label)    STACK DOWNTOWN INVESTMENTS LLC
        Owner Address:   3123 SHERBURN PL SW
        City State Zip:  ROCHESTER MN 55902
    """
    owner_name: str | None = None
    owner_name_2: str | None = None
    mailing: str | None = None
    csz: str | None = None
    rows = soup.find_all("tr")
    for i, tr in enumerate(rows):
        vals = _cells(tr)
        if len(vals) < 2:
            continue
        label = vals[0].rstrip(":").strip().lower()
        value = vals[1].strip() or None
        if label == "name" and value:
            owner_name = value
            # Second-owner line: next row with an empty label cell.
            if i + 1 < len(rows):
                nxt = _cells(rows[i + 1])
                if len(nxt) >= 2 and not nxt[0].strip() and nxt[1].strip():
                    owner_name_2 = nxt[1].strip()
        elif label == "owner address" and value:
            mailing = value
        elif label == "city state zip" and value:
            csz = value
    return {
        "owner_name": owner_name,
        "owner_name_2": owner_name_2,
        "owner_mailing_address": mailing,
        "owner_mailing_city_state_zip": csz,
    }


def _money_f(d: Decimal | None) -> float | None:
    """Decimal -> float for the PostgREST payload (numeric columns)."""
    return float(d) if d is not None else None


def _date_s(d: date | None) -> str | None:
    return d.isoformat() if d else None


# ============================================================
# SCRAPER
# ============================================================


class OlmstedTaxDetailScraper(BaseScraper[dict[str, Any], dict[str, Any]]):
    """iasWorld per-parcel tax detail -> years-behind + status + owners."""

    source_name: ClassVar[str] = "olmsted_tax_detail"
    signal_type: ClassVar[str] = "tax_delinquency_detail"
    county_code: ClassVar[str] = _COUNTY_SLUG

    def __init__(self, pins: list[str] | None = None) -> None:
        """pins: optional explicit PIN list (the 5-PIN test path). When
        None, the full olmsted_delq_list parcel set is scraped."""
        self._pins_override = pins

    # ---- PIN universe ----

    def _load_pins(self) -> list[str]:
        if self._pins_override:
            pins = [p.strip() for p in self._pins_override if p and p.strip()]
            logger.info(
                "PIN override in effect (test mode)",
                source=self.source_name, pins=len(pins),
            )
            return sorted(set(pins))
        result = (
            signals_table("distress_events")
            .select("parcel_id")
            .eq("source", _LIST_SOURCE)
            .execute()
        )
        pins = sorted(
            {
                r["parcel_id"]
                for r in (result.data or [])
                if r.get("parcel_id")
            }
        )
        if not pins:
            raise SourceUnavailableError(
                f"No parcels found for source='{_LIST_SOURCE}' — nothing to "
                "scrape (was the annual list loaded?)",
                source=self.source_name,
            )
        return pins

    # ---- Browser plumbing ----

    @staticmethod
    async def _maybe_accept_disclaimer(page: Any) -> None:
        """iasWorld portals often gate the first search behind a
        disclaimer. Click through if present; silently continue if not."""
        for selector in (
            'input[value*="Agree" i]',
            'input[id*="Agree" i]',
            'button:has-text("Agree")',
            'a:has-text("Agree")',
        ):
            try:
                loc = page.locator(selector).first
                if await loc.count() and await loc.is_visible():
                    await loc.click()
                    await page.wait_for_load_state("domcontentloaded")
                    logger.info("Portal disclaimer accepted")
                    return
            except Exception:
                continue

    @staticmethod
    async def _fill_parcel_search(page: Any, pin: str) -> None:
        """Fill the Parcel ID input and submit. iasWorld's canonical input
        is name=inpParid; fall back to the first text input on the form
        (the Parcel ID box is first, per recon screenshot)."""
        filled = False
        for selector in (
            'input[name="inpParid"]',
            'input[id*="parid" i]',
            'input[type="text"]',
        ):
            try:
                loc = page.locator(selector).first
                if await loc.count():
                    await loc.fill(pin)
                    filled = True
                    break
            except Exception:
                continue
        if not filled:
            raise RuntimeError("Parcel ID input not found on search page")
        for selector in (
            'input[id="btSearch"]',
            'input[value="Search"]',
            'button:has-text("Search")',
        ):
            try:
                loc = page.locator(selector).first
                if await loc.count():
                    await loc.click()
                    await page.wait_for_load_state("domcontentloaded")
                    return
            except Exception:
                continue
        await page.keyboard.press("Enter")
        await page.wait_for_load_state("domcontentloaded")

    @staticmethod
    async def _on_record(page: Any) -> bool:
        try:
            return "PARID:" in (await page.content())
        except Exception:
            return False

    async def _open_record(self, page: Any, pin: str) -> bool:
        """From the search page: search the PIN and land on its record.
        Handles both direct-to-record and results-grid paths. Returns
        False (honest not-found) when the portal has no such parcel."""
        await page.goto(_SEARCH_URL, timeout=_NAV_TIMEOUT_MS)
        await page.wait_for_load_state("domcontentloaded")
        await self._maybe_accept_disclaimer(page)
        # The disclaimer may have redirected; ensure we're on the form.
        if "CommonSearch" not in page.url:
            await page.goto(_SEARCH_URL, timeout=_NAV_TIMEOUT_MS)
            await page.wait_for_load_state("domcontentloaded")
        await self._fill_parcel_search(page, pin)

        if await self._on_record(page):
            return True
        content = await page.content()
        if "did not return any" in content.lower() or "no records" in content.lower():
            return False
        # Results grid: click the row carrying the exact PARID.
        try:
            row = page.locator(f'tr:has-text("{pin}")').first
            if await row.count():
                await row.click()
                await page.wait_for_load_state("domcontentloaded")
                return await self._on_record(page)
        except Exception:
            pass
        return False

    @staticmethod
    async def _open_nav(page: Any, link_text: str) -> str | None:
        """Click a left-nav datalet link and return the resulting HTML;
        None when the link is absent (honest: e.g. no tax page)."""
        try:
            loc = page.locator(f'a:has-text("{link_text}")').first
            if not await loc.count():
                return None
            await loc.click()
            await page.wait_for_load_state("domcontentloaded")
            return await page.content()
        except Exception as e:
            logger.warning(
                "Datalet nav failed", link=link_text, error=str(e)[:200]
            )
            return None

    async def _scrape_parcel(self, page: Any, pin: str) -> dict[str, Any]:
        """One parcel: search -> overview -> taxes-due -> ownership.
        Raw HTML captured per datalet; parsing happens in parse()."""
        found = await self._open_record(page, pin)
        if not found:
            return {"pin": pin, "found": False}
        overview_html = await page.content()
        tax_html = await self._open_nav(page, _NAV_TAXES_DUE)
        owner_html = await self._open_nav(page, _NAV_OWNERSHIP)
        return {
            "pin": pin,
            "found": True,
            "overview_html": overview_html,
            "tax_html": tax_html,
            "owner_html": owner_html,
        }

    # ---- Lifecycle: fetch / parse / write ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        pins = self._load_pins()
        logger.info(
            "Tyler-portal scrape starting",
            source=self.source_name, parcels=len(pins),
        )
        try:
            from playwright.async_api import async_playwright
        except ImportError as e:  # pragma: no cover
            raise SourceUnavailableError(
                "playwright is not installed in this environment",
                source=self.source_name,
            ) from e

        raw: list[dict[str, Any]] = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0 Safari/537.36"
                )
            )
            page = await context.new_page()
            page.set_default_timeout(_NAV_TIMEOUT_MS)
            try:
                for n, pin in enumerate(pins, start=1):
                    record: dict[str, Any] | None = None
                    for attempt in range(1, _PER_PARCEL_ATTEMPTS + 1):
                        try:
                            record = await self._scrape_parcel(page, pin)
                            break
                        except Exception as e:
                            logger.warning(
                                "Parcel scrape attempt failed",
                                source=self.source_name, pin=pin,
                                attempt=attempt, error=str(e)[:300],
                            )
                            # Fresh page for the retry — a wedged session
                            # is the usual failure mode on ASP.NET portals.
                            try:
                                await page.close()
                            except Exception:
                                pass
                            page = await context.new_page()
                            page.set_default_timeout(_NAV_TIMEOUT_MS)
                    if record is None:
                        record = {"pin": pin, "found": False, "error": True}
                    raw.append(record)
                    if n % 25 == 0:
                        logger.info(
                            "Tyler-portal scrape progress",
                            source=self.source_name,
                            done=n, total=len(pins),
                        )
                    await asyncio.sleep(_POLITE_DELAY_SECONDS)
            finally:
                await browser.close()

        logger.info(
            "Tyler-portal fetch complete",
            source=self.source_name,
            parcels=len(raw),
            found=sum(1 for r in raw if r.get("found")),
            not_found=sum(
                1 for r in raw if not r.get("found") and not r.get("error")
            ),
            errored=sum(1 for r in raw if r.get("error")),
        )
        return raw

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Raw HTML bundles -> {'status': row, 'detail': [rows]} per
        parcel. Not-found / errored parcels pass through as markers so
        write() can count them as failures (partial-run honesty)."""
        parsed: list[dict[str, Any]] = []
        observed_at = datetime.now(timezone.utc).isoformat()

        for rec in raw_records:
            pin = rec["pin"]
            if not rec.get("found"):
                parsed.append(
                    {"pin": pin, "found": False, "error": rec.get("error", False)}
                )
                continue

            flags = (
                parse_status_flags(BeautifulSoup(rec["overview_html"], "lxml"))
                if rec.get("overview_html")
                else {v: None for v in _STATUS_LABELS.values()}
            )
            owner = (
                parse_ownership(BeautifulSoup(rec["owner_html"], "lxml"))
                if rec.get("owner_html")
                else {
                    "owner_name": None, "owner_name_2": None,
                    "owner_mailing_address": None,
                    "owner_mailing_city_state_zip": None,
                }
            )

            delq_rows_raw: list[dict[str, Any]] = []
            current_rows_raw: list[dict[str, Any]] = []
            if rec.get("tax_html"):
                tax_soup = BeautifulSoup(rec["tax_html"], "lxml")
                delq_rows_raw = parse_tax_table(tax_soup, "Delinquent Taxes")
                current_rows_raw = parse_tax_table(tax_soup, "Current Taxes Due")

            detail: list[dict[str, Any]] = []

            # Delinquent rows: portal-verbatim, one per pay year.
            delq_norm: list[dict[str, Any]] = []
            for row in delq_rows_raw:
                norm = normalize_tax_row(row)
                if norm["pay_year"] is None:
                    continue
                delq_norm.append(norm)
                detail.append({
                    "parcel_id": pin,
                    "county_slug": _COUNTY_SLUG,
                    "pay_year": norm["pay_year"],
                    "row_kind": "delinquent",
                    "base_taxes": _money_f(norm["base_taxes"]),
                    "penalty_due": _money_f(norm["penalty_due"]),
                    "fees_due": _money_f(norm["fees_due"]),
                    "interest_due": _money_f(norm["interest_due"]),
                    "total_amt_paid": _money_f(norm["total_amt_paid"]),
                    "date_last_paid": _date_s(norm["date_last_paid"]),
                    "total_due": _money_f(norm["total_due"]),
                    "raw_data": row,
                    "observed_at": observed_at,
                })

            # Current rows: the portal splits the year into payment
            # cycles (1NAN/2NAN). The dedup key has no cycle column, so
            # aggregate per pay_year; cycles preserved verbatim in
            # raw_data.
            by_year: dict[int, list[dict[str, Any]]] = {}
            for row in current_rows_raw:
                norm = normalize_tax_row(row)
                if norm["pay_year"] is None:
                    continue
                by_year.setdefault(norm["pay_year"], []).append(
                    {"norm": norm, "raw": row}
                )
            current_year_due = Decimal("0")
            have_current = False
            for year, items in sorted(by_year.items()):
                def _sum(field: str) -> Decimal | None:
                    vals = [i["norm"][field] for i in items if i["norm"][field] is not None]
                    return sum(vals, Decimal("0")) if vals else None
                dates = [
                    i["norm"]["date_last_paid"]
                    for i in items
                    if i["norm"]["date_last_paid"] is not None
                ]
                total_due_y = _sum("total_due")
                if total_due_y is not None:
                    current_year_due += total_due_y
                    have_current = True
                detail.append({
                    "parcel_id": pin,
                    "county_slug": _COUNTY_SLUG,
                    "pay_year": year,
                    "row_kind": "current",
                    "base_taxes": _money_f(_sum("base_taxes")),
                    "penalty_due": _money_f(_sum("penalty_due")),
                    "fees_due": _money_f(_sum("fees_due")),
                    "interest_due": _money_f(_sum("interest_due")),
                    "total_amt_paid": _money_f(_sum("total_amt_paid")),
                    "date_last_paid": _date_s(max(dates) if dates else None),
                    "total_due": _money_f(total_due_y),
                    "raw_data": {"cycles": [i["raw"] for i in items]},
                    "observed_at": observed_at,
                })

            # ---- Computed status (honestly basis-tagged) ----
            unpaid_years = sorted(
                n["pay_year"]
                for n in delq_norm
                if n["total_due"] is not None and n["total_due"] > 0
            )
            first_delq_year = unpaid_years[0] if unpaid_years else None
            total_delq_due = sum(
                (
                    n["total_due"]
                    for n in delq_norm
                    if n["total_due"] is not None and n["total_due"] > 0
                ),
                Decimal("0"),
            )
            if first_delq_year is not None:
                judgment = second_monday_of_may(first_delq_year + 1)
                forfeiture = date(
                    judgment.year + 3, judgment.month,
                    min(judgment.day, calendar.monthrange(
                        judgment.year + 3, judgment.month)[1]),
                )
                basis = "computed_3yr_statutory"
            else:
                judgment = None
                forfeiture = None
                basis = None

            status_row = {
                "parcel_id": pin,
                "county_slug": _COUNTY_SLUG,
                "first_delinquent_year": first_delq_year,
                "years_delinquent": len(unpaid_years),
                "total_delinquent_due": _money_f(total_delq_due),
                "current_year_due": (
                    _money_f(current_year_due) if have_current else None
                ),
                # On the annual list; portal shows no unpaid delinquent
                # year -> the owner cured since publication.
                "redeemed_since_list": first_delq_year is None,
                "estimated_judgment_date": _date_s(judgment),
                "estimated_forfeiture_date": _date_s(forfeiture),
                "forfeiture_basis": basis,
                "in_forfeiture": flags["in_forfeiture"],
                "coj": flags["coj"],
                "in_bankruptcy": flags["in_bankruptcy"],
                "homestead": flags["homestead"],
                **owner,
                "raw_data": {
                    "delinquent_rows": delq_rows_raw,
                    "current_rows": current_rows_raw,
                    "unpaid_years": unpaid_years,
                },
                "observed_at": observed_at,
            }
            parsed.append(
                {"pin": pin, "found": True, "status": status_row, "detail": detail}
            )

        found = [p for p in parsed if p.get("found")]
        logger.info(
            "Tyler-portal parse complete",
            source=self.source_name,
            parcels_parsed=len(found),
            true_delinquent=sum(
                1 for p in found if p["status"]["first_delinquent_year"]
            ),
            redeemed_since_list=sum(
                1 for p in found if p["status"]["redeemed_since_list"]
            ),
            detail_rows=sum(len(p["detail"]) for p in found),
        )
        return parsed

    async def write(
        self, signals: list[dict[str, Any]]
    ) -> tuple[int, int, int]:
        status_rows = [s["status"] for s in signals if s.get("found")]
        detail_rows = [
            row for s in signals if s.get("found") for row in s["detail"]
        ]
        missing = sum(1 for s in signals if not s.get("found"))

        detail_new, detail_failed = write_typed_signals_dedup(
            "tax_delinquency_detail",
            detail_rows,
            on_conflict="parcel_id,county_slug,pay_year,row_kind",
        )
        status_new, status_failed = write_typed_signals_dedup(
            "tax_delinquency_status",
            status_rows,
            on_conflict="parcel_id,county_slug",
        )
        logger.info(
            "Tyler-portal write complete",
            source=self.source_name,
            status_rows=status_new,
            detail_rows=detail_new,
            write_failed=detail_failed + status_failed,
            parcels_not_found=missing,
        )
        # Not-found parcels count as failures so the run reports
        # 'partial' — an honest signal that the list and the portal
        # disagree, worth eyeballing, never silently dropped.
        return status_new, 0, detail_failed + status_failed + missing


__all__ = ["OlmstedTaxDetailScraper"]
