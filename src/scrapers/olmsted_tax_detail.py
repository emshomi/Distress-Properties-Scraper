"""
Olmsted Tyler-portal (iasWorld) per-parcel tax detail scraper.

THE YEARS-BEHIND BUILD (specced from live recon 2026-07-10; v2 after the
first Actions test): the annual delinquent-tax list
(signals.distress_events, source='olmsted_delq_list', 502 parcels) is a
snapshot with no per-year breakdown — a parcel owing $8k could be in
year 1 of the ~3-year forfeiture clock or months from judgment expiry.
The county's Tyler iasWorld portal (publicaccess.co.olmsted.mn.us)
carries the authoritative per-parcel, PER-YEAR delinquency detail. This
scraper reads it for each listed parcel, turning the flat list into a
ranked closest-to-forfeiture view — plus list hygiene (recon found the
top-2 parcels by amount owed had BOTH redeemed since the list published)
and owner mailing addresses.

=== SOURCE — v2: DIRECT DEEP LINKS, NO BROWSER ===
v1 replayed the interactive search per parcel with Playwright and died
in the portal's disclaimer-redirect/frameset maze (5/5 parcels,
2026-07-10 17:53 UTC Actions run). The fix came from the portal itself:
datalet pages accept DIRECT per-PIN URLs that render with NO session,
NO disclaimer, NO JavaScript (verified live 2026-07-10 with a plain
HTTP GET):

  /datalets/datalet.aspx?mode=<MODE>&UseSearch=no&pin=<PARID>
      &jur=055&taxyr=<ASMT_YEAR>&LMparent=20

Modes used (the datalet left-nav enumerates them all):
  profileall -> Property Overview: Parcel Status flags
               (In Forfeiture / COJ / In Bankruptcy / Delinquent /
               Homestead)
  tax_all    -> Property Taxes Due: "Current Taxes Due" and
               "Delinquent Taxes" tables, PER PAY YEAR:
               Pay Year | Base Taxes | Penalty Due | Fees Due
               | Interest Due | Total Amnt Paid | Date Last Paid
               | Total Due
  owner      -> Ownership: owner name(s) + MAILING address (recon:
               distinct from property address — the skip-trace field)

taxyr is the ASSESSMENT year and lags the pay year by one: taxyr=2025
serves Pay Year 2026 (current) + the delinquent history; taxyr=2026
serves the not-yet-certified Pay Year 2027 as $.00 (verified live). So
ASMT_YEAR = today.year - 1.

So: 3 plain httpx GETs per parcel, BeautifulSoup parsing. No Playwright.

=== VERIFICATION PARCELS (from recon; the 5-PIN test plan) ===
  743544075694  HP PB LLC            2025 delq REDEEMED 22-APR-2026
  743542084934  Civic Center Hotel   2025 delq $1.16M REDEEMED 25-JUN-2026
  641013084421  Apache Hotel Group   TRUE delinquent: 2025 unpaid
                $155,062.04 (base 132,390.00 + pen 16,548.75 + fees 40.00
                + int 6,083.29), 2026 also unpaid; mailing addr
                3123 SHERBURN PL SW ROCHESTER MN 55902

=== HONESTY RULES ===
- Parcels whose datalet does not echo their PARID are logged and
  skipped — NEVER a synthetic row (the MPLS-VBR lesson). They count
  toward records_failed so the run reports partial, not a false success.
- A portal-wide maintenance banner with no parcel data raises
  SourceUnavailableError (fail loud, don't write garbage).
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
"""

from __future__ import annotations

import asyncio
import calendar
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

import httpx
from bs4 import BeautifulSoup

from src.db.supabase_client import signals_table
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import write_typed_signals_dedup
from src.utils.errors import SourceUnavailableError
from src.utils.logger import logger

_BASE_URL = "https://publicaccess.co.olmsted.mn.us"
_DATALET_URL = f"{_BASE_URL}/datalets/datalet.aspx"
_JUR = "055"  # Olmsted County jurisdiction code (from the portal's own links)
_COUNTY_SLUG = "olmsted"
_LIST_SOURCE = "olmsted_delq_list"

_REQUEST_TIMEOUT = 30.0
_PER_PARCEL_ATTEMPTS = 2
_POLITE_DELAY_SECONDS = 1.0  # between parcels (3 GETs each); keep the county happy

_MAINTENANCE_MARKER = "currently unavailable due to maintenance"

# Datalet modes (verified live 2026-07-10 from the portal's own nav links)
_MODE_OVERVIEW = "profileall"
_MODE_TAXES_DUE = "tax_all"
_MODE_OWNER = "owner"

# Parcel Status labels on the Property Overview datalet
_STATUS_LABELS = {
    "in forfeiture": "in_forfeiture",
    "coj": "coj",
    "in bankruptcy": "in_bankruptcy",
    "homestead": "homestead",
}

_RE_YEAR = re.compile(r"^(19|20)\d{2}$")


def assessment_year(today: date | None = None) -> int:
    """taxyr for datalet URLs. The portal's taxyr is the ASSESSMENT year
    and lags the pay year by one (verified live: taxyr=2025 -> Pay Year
    2026 with real amounts; taxyr=2026 -> Pay Year 2027 all $.00)."""
    d = today or date.today()
    return d.year - 1


# ============================================================
# PARSING HELPERS (pure functions — unit-testable without the network)
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
    heading node rather than trusting sibling structure. Exact-match
    first, contains-match fallback (live headings can carry &nbsp;
    padding)."""
    marker = soup.find(
        string=lambda t: isinstance(t, str) and t.strip() == heading
    )
    if marker is None:
        marker = soup.find(
            string=lambda t: isinstance(t, str) and heading in t
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
        # Data rows start with a 4-digit pay year; anything else (the
        # Total row, spacer rows) is skipped.
        if not _RE_YEAR.match(vals[0].strip()):
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

    # ---- HTTP plumbing (v2: direct deep links, no browser) ----

    @staticmethod
    def _datalet_params(mode: str, pin: str, taxyr: int) -> dict[str, str]:
        return {
            "mode": mode,
            "UseSearch": "no",
            "pin": pin,
            "jur": _JUR,
            "taxyr": str(taxyr),
            "LMparent": "20",
        }

    async def _get_datalet(
        self, client: httpx.AsyncClient, mode: str, pin: str, taxyr: int
    ) -> str:
        resp = await client.get(
            _DATALET_URL, params=self._datalet_params(mode, pin, taxyr)
        )
        resp.raise_for_status()
        return resp.text

    async def _scrape_parcel(
        self, client: httpx.AsyncClient, pin: str, taxyr: int
    ) -> dict[str, Any]:
        """One parcel: three direct datalet GETs. A datalet that does not
        echo the PARID means the portal has no such parcel (honest
        not-found). A maintenance page with no parcel data anywhere is a
        source outage, surfaced upward."""
        overview_html = await self._get_datalet(
            client, _MODE_OVERVIEW, pin, taxyr
        )
        if f"PARID: {pin}" not in overview_html and pin not in overview_html:
            if _MAINTENANCE_MARKER in overview_html.lower():
                raise SourceUnavailableError(
                    "Portal is in maintenance mode (no parcel data served)",
                    source=self.source_name,
                )
            return {"pin": pin, "found": False}
        tax_html = await self._get_datalet(client, _MODE_TAXES_DUE, pin, taxyr)
        owner_html = await self._get_datalet(client, _MODE_OWNER, pin, taxyr)
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
        taxyr = assessment_year()
        logger.info(
            "Tyler-portal scrape starting",
            source=self.source_name, parcels=len(pins), taxyr=taxyr,
        )
        raw: list[dict[str, Any]] = []
        async with httpx.AsyncClient(
            timeout=_REQUEST_TIMEOUT,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0 Safari/537.36"
                ),
            },
        ) as client:
            for n, pin in enumerate(pins, start=1):
                record: dict[str, Any] | None = None
                for attempt in range(1, _PER_PARCEL_ATTEMPTS + 1):
                    try:
                        record = await self._scrape_parcel(client, pin, taxyr)
                        break
                    except SourceUnavailableError:
                        raise  # portal-wide outage: fail the run loudly
                    except Exception as e:
                        logger.warning(
                            "Parcel scrape attempt failed",
                            source=self.source_name, pin=pin,
                            attempt=attempt, error=str(e)[:300],
                        )
                        await asyncio.sleep(1.5 * attempt)
                if record is None:
                    record = {"pin": pin, "found": False, "error": True}
                raw.append(record)
                if n % 25 == 0:
                    logger.info(
                        "Tyler-portal scrape progress",
                        source=self.source_name, done=n, total=len(pins),
                    )
                await asyncio.sleep(_POLITE_DELAY_SECONDS)

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
