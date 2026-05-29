"""
Anoka County Sheriff Foreclosure Sales scraper.

Source: Anoka County official foreclosure site (ASP.NET WebForms)
    List:   https://foreclosures.co.anoka.mn.us/ForeclosureList.aspx
    Detail: https://foreclosures.co.anoka.mn.us/ForeclosureNotice.aspx?id={id}

License / posture: official Anoka County government site. Public foreclosure
notice data under the Minnesota Government Data Practices Act. No anti-bot terms
identified. GREEN per the data-source audit. We fetch politely (small delays).

=== WHY ANOKA IS VALUABLE ===
Unlike Dakota (completed sales only), Anoka publishes BOTH:
  * Pending Sales   — a FORWARD CALENDAR of scheduled/upcoming auctions. This is
                      the highest-value window: the homeowner can still act, and
                      an investor/helper can reach them before the sale.
  * Completed Sales — the 12-month rolling history (redemption-window leads).

=== DATA AVAILABLE ===
List page (per row):   Scheduled Date, Address, City, Zip, and a Details link
                       to ForeclosureNotice.aspx?id={id}.
Detail page (per id):  Sale Date, Sale Time, Address, Mortgagor(s) [OWNER NAME],
                       Status (e.g. "Postponed"), and a full legal Notice that
                       contains TAX PARCEL NO. and AMOUNT DUE.

=== ARCHITECTURE ===
This is an ASP.NET WebForms app, so a search requires posting back the page's
hidden fields (__VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION) plus the
form control values. We DISCOVER the form field names at runtime with
BeautifulSoup rather than hard-coding them — robust to control-name changes.

  fetch():
    1. GET the list page; parse the <form> to learn every input/select name.
    2. For each of {Pending, Completed}: POST the form with that selection
       (city = all, dates blank) and parse the results table into list rows.
    3. For each row, GET the detail page and parse owner / parcel / amounts /
       status. Detail failures are tolerated (we keep the list row).

  parse():  convert each enriched row into a DistressEventInsert (sheriff_sale).
  write():  synthesize a stable parcel_id (ANOKA-FC-{id}); resolve_parcel +
            write_events_dedup, mirroring the Dakota scraper.

Severity:
  pending sale in the future        -> high     (actionable: can still help)
  pending sale (postponed/past)     -> medium
  completed sale                    -> low/medium (redemption window)
"""

from __future__ import annotations

import asyncio
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

import httpx
from bs4 import BeautifulSoup

from src.config import settings
from src.models.parcel import ParcelUpsert
from src.models.signal import DistressEventInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import write_events_dedup
from src.services.parcel_resolver import resolve_parcel
from src.utils.errors import ParseError, SourceUnavailableError
from src.utils.logger import logger


_BASE = "https://foreclosures.co.anoka.mn.us"
_LIST_URL = f"{_BASE}/ForeclosureList.aspx"
_DETAIL_URL = f"{_BASE}/ForeclosureNotice.aspx?id={{id}}"

# Politeness: small delay between detail-page fetches.
_DETAIL_DELAY_SECONDS = 0.4

# The two search modes we run. Values are matched case-insensitively against
# the Pending/Completed <select>'s option labels.
_SEARCH_MODES = ("Pending Sales", "Completed Sales")

# Regexes to pull structured facts out of the free-text legal Notice.
_RE_TAX_PARCEL = re.compile(r"TAX\s+PARCEL\s+NO\.?\s*:?\s*([0-9A-Za-z\-]+)", re.I)
_RE_AMOUNT_DUE = re.compile(
    r"AMOUNT\s+DUE[^$]*\$?\s*([0-9][0-9,]*\.?[0-9]{0,2})", re.I
)
_RE_ORIG_PRINCIPAL = re.compile(
    r"ORIGINAL\s+PRINCIPAL[^$]*\$?\s*([0-9][0-9,]*\.?[0-9]{0,2})", re.I
)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Full browser-like headers — the Anoka ASP.NET server stalls or blocks bare UAs.
_BROWSER_HEADERS = {
    "User-Agent": _USER_AGENT,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _safe_decimal(value: str | None) -> Decimal | None:
    if not value:
        return None
    cleaned = value.replace(",", "").strip()
    try:
        d = Decimal(cleaned)
        return d if d >= 0 else None
    except (InvalidOperation, ValueError):
        return None


def _parse_mmddyyyy(value: str | None) -> date | None:
    if not value:
        return None
    v = value.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    return None


def _hidden_form_fields(soup: BeautifulSoup) -> dict[str, str]:
    """Collect every hidden input (ASP.NET __VIEWSTATE etc.) by its real name."""
    fields: dict[str, str] = {}
    for inp in soup.find_all("input", {"type": "hidden"}):
        name = inp.get("name")
        if name:
            fields[name] = inp.get("value", "")
    return fields


def _find_select_name_by_options(
    soup: BeautifulSoup, must_contain: tuple[str, ...]
) -> tuple[str | None, dict[str, str]]:
    """Find a <select> whose option labels include the given text.

    Returns (select_name, {lowercased_label: option_value}). Used to locate the
    Pending/Completed and City dropdowns without hard-coding control names.
    """
    for sel in soup.find_all("select"):
        labels = {
            (opt.get_text() or "").strip().lower(): (opt.get("value") or "")
            for opt in sel.find_all("option")
        }
        if all(any(m.lower() in lbl for lbl in labels) for m in must_contain):
            return sel.get("name"), labels
    return None, {}


class AnokaSheriffScraper(BaseScraper[dict[str, Any], DistressEventInsert]):
    """Anoka County sheriff foreclosure sales — ASP.NET WebForms source."""

    source_name: ClassVar[str] = "anoka_sheriff"
    signal_type: ClassVar[str] = "sheriff_sale"
    county_code: ClassVar[str] = "anoka"

    # ---- Fetch (ASP.NET form discovery + post + detail enrichment) ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []

        # Generous timeout — the county server can be slow, especially on the
        # Completed Sales query (a heavier database read). Browser headers
        # because the ASP.NET app stalls on bare/unknown User-Agents.
        timeout = httpx.Timeout(connect=20.0, read=120.0, write=30.0, pool=30.0)
        async with httpx.AsyncClient(
            timeout=timeout,
            headers=_BROWSER_HEADERS,
            follow_redirects=True,
        ) as client:
            # 1. GET the list page once to learn the form structure.
            #    Retry a couple of times — the county server can be flaky.
            resp = None
            last_err: Exception | None = None
            for attempt in range(3):
                try:
                    resp = await client.get(_LIST_URL)
                    break
                except httpx.HTTPError as e:
                    last_err = e
                    logger.warning(
                        "Anoka list GET attempt failed",
                        source=self.source_name,
                        attempt=attempt + 1,
                        error_type=type(e).__name__,
                        error_repr=repr(e),
                    )
                    await asyncio.sleep(2.0)
            if resp is None:
                raise SourceUnavailableError(
                    f"Anoka list GET failed after retries: "
                    f"{type(last_err).__name__}: {last_err!r}",
                    source=self.source_name,
                )
            if resp.status_code != 200:
                raise SourceUnavailableError(
                    f"Anoka list returned {resp.status_code}",
                    source=self.source_name,
                )

            soup = BeautifulSoup(resp.text, "lxml")

            # Locate the Pending/Completed select and the City select by content.
            mode_select_name, mode_options = _find_select_name_by_options(
                soup, ("pending", "completed")
            )
            city_select_name, city_options = _find_select_name_by_options(
                soup, ("all cities",)
            )
            if not mode_select_name:
                raise ParseError(
                    "Could not locate the Pending/Completed dropdown on the "
                    "Anoka form.",
                    source=self.source_name,
                )

            # City "all" option value (label contains "all cities").
            city_all_value = ""
            for lbl, val in city_options.items():
                if "all cities" in lbl:
                    city_all_value = val
                    break

            # 2. Run a search for each mode (Pending + Completed).
            for mode in _SEARCH_MODES:
                # Resolve the option value whose label matches this mode.
                mode_value = ""
                for lbl, val in mode_options.items():
                    if mode.lower() in lbl:
                        mode_value = val
                        break

                form = _hidden_form_fields(soup)
                form[mode_select_name] = mode_value
                if city_select_name:
                    form[city_select_name] = city_all_value
                # Add the Submit button. Find a submit input and include its
                # name=value so ASP.NET treats this as that button's postback.
                submit = soup.find("input", {"type": "submit"})
                if submit and submit.get("name"):
                    form[submit["name"]] = submit.get("value", "Submit")

                # Retry the mode POST a few times — the Anoka ASP.NET server
                # is flaky and slow, especially on the heavier Completed Sales
                # query. If retries are exhausted, only Pending Sales is fatal:
                # Pending is the high-value forward calendar (upcoming auctions
                # we still have time to act on), while Completed Sales is just
                # the 12-month redemption-window history — useful but optional.
                post = None
                post_last_err: Exception | None = None
                for attempt in range(3):
                    try:
                        post = await client.post(_LIST_URL, data=form)
                        break
                    except httpx.HTTPError as e:
                        post_last_err = e
                        logger.warning(
                            "Anoka mode POST attempt failed",
                            source=self.source_name,
                            mode=mode,
                            attempt=attempt + 1,
                            error_type=type(e).__name__,
                            error_repr=repr(e),
                        )
                        await asyncio.sleep(3.0)

                if post is None:
                    if mode.lower() == "completed sales":
                        logger.warning(
                            "Skipping Anoka Completed Sales after retries exhausted",
                            source=self.source_name,
                            error_type=(
                                type(post_last_err).__name__ if post_last_err else None
                            ),
                        )
                        continue
                    raise SourceUnavailableError(
                        f"Anoka {mode} POST failed after retries: "
                        f"{type(post_last_err).__name__}: {post_last_err!r}",
                        source=self.source_name,
                    )

                if post.status_code != 200:
                    if mode.lower() == "completed sales":
                        logger.warning(
                            "Skipping Anoka Completed Sales due to non-200 response",
                            source=self.source_name,
                            status_code=post.status_code,
                        )
                        continue
                    raise SourceUnavailableError(
                        f"Anoka {mode} POST returned {post.status_code}",
                        source=self.source_name,
                    )

                rows = self._parse_list_table(post.text, mode)
                logger.info(
                    "Anoka list parsed",
                    source=self.source_name,
                    mode=mode,
                    rows=len(rows),
                )
                all_rows.extend(rows)

                # Refresh soup/hidden fields from the POST response so the next
                # mode's postback carries a valid (current) __VIEWSTATE.
                soup = BeautifulSoup(post.text, "lxml")

        # 3. Enrich each row with its detail page using Playwright (headless
        # Chromium). We tried direct httpx GETs to ForeclosureNotice.aspx with
        # cookies + Referer — the server still bounced every request to
        # error.aspx ("Web Page Has Expired"). The Anoka ASP.NET application
        # appears to require some combination of browser-native headers
        # (Sec-Fetch-*, sec-ch-ua), JS-established session tokens, or both,
        # that an HTTP-only client can't easily reproduce. Playwright is the
        # reliable answer: it's a real Chromium instance, so it behaves
        # exactly like the user's browser when they click "Details" from the
        # list page (which we verified works manually).
        #
        # Implementation note: Playwright is imported here (inside the method)
        # rather than at module-top because the binary it needs (`chromium`)
        # only exists in environments that ran `playwright install chromium`.
        # The GitHub Actions workflow does this; a pure-API import in places
        # without the binary would still work for the Python module, but the
        # cleaner pattern is to keep this dependency localized to where it's
        # used.
        await self._enrich_details_with_playwright(all_rows)

        logger.info(
            "Anoka fetch complete",
            source=self.source_name,
            total_rows=len(all_rows),
        )
        return all_rows

    # ---- Playwright detail-page enrichment ----

    async def _enrich_details_with_playwright(
        self, all_rows: list[dict[str, Any]]
    ) -> None:
        """Fill in mortgagor/amount-due/tax-parcel by browsing the detail
        pages with headless Chromium.

        Modifies `all_rows` in place via the existing `_parse_detail` helper.
        Tolerates per-row failures with a warning — Pending Sales rows still
        have list-level data (address, city, sale date) even if detail
        enrichment fails.
        """
        # Local import: only the workflow that runs this scraper installs
        # the Chromium binaries via `playwright install`. Keeping the import
        # inside the method means modules that don't run this code path
        # don't break if Playwright's runtime isn't available.
        from playwright.async_api import (
            async_playwright,
            Error as PlaywrightError,
            TimeoutError as PlaywrightTimeout,
        )

        if not all_rows:
            return

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                context = await browser.new_context(
                    user_agent=_USER_AGENT,
                    viewport={"width": 1280, "height": 800},
                    locale="en-US",
                )
                # Hide the standard headless-Chromium tell. Some ASP.NET
                # apps with anti-bot middleware check this; the cost of
                # masking it is zero and the cost of NOT masking is
                # potentially serving us a stripped-down page that
                # doesn't behave like the one a real user sees.
                await context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', "
                    "{ get: () => undefined });"
                )
                page = await context.new_page()

                # First navigate to the list page. Use `networkidle` (not
                # `domcontentloaded`) so any inline JS that initializes
                # __doPostBack / __VIEWSTATE has time to finish before we
                # submit.
                try:
                    await page.goto(
                        _LIST_URL,
                        wait_until="networkidle",
                        timeout=30000,
                    )
                    await asyncio.sleep(0.5)
                except (PlaywrightTimeout, PlaywrightError) as e:
                    logger.warning(
                        "Playwright: list-page warm-up failed; "
                        "continuing anyway",
                        source=self.source_name,
                        error_type=type(e).__name__,
                    )

                # Submit the search form. We bypass `page.click()` on the
                # Submit button — past attempts found that the click
                # registers but the form doesn't actually submit (no
                # navigation, no page change). Most likely the button's
                # native submit is being short-circuited by ASP.NET's JS,
                # which expects __EVENTTARGET to be set before submission.
                #
                # Instead, we set __EVENTTARGET ourselves (to the button's
                # name, which is the standard ASP.NET postback contract)
                # and call form.submit() directly via JS. This produces an
                # honest POST with all the right hidden fields and tells
                # the server which event handler to invoke.
                submit_result: dict = {}
                try:
                    submit_result = await page.evaluate(
                        """
                        () => {
                            const btn = document.querySelector(
                                'input[type=submit][value=Submit]'
                            );
                            if (!btn) return {ok: false,
                                reason: 'no submit button found'};
                            const form = btn.closest('form');
                            if (!form) return {ok: false,
                                reason: 'button has no form ancestor'};

                            // Ensure __EVENTTARGET exists and is set to
                            // the button's postback name. This is what
                            // ASP.NET WebForms uses to identify which
                            // button was clicked.
                            let et = form.querySelector(
                                'input[name=__EVENTTARGET]'
                            );
                            if (!et) {
                                et = document.createElement('input');
                                et.type = 'hidden';
                                et.name = '__EVENTTARGET';
                                form.appendChild(et);
                            }
                            et.value = btn.name || '';

                            // Also ensure __EVENTARGUMENT exists (often
                            // expected by ASP.NET even when empty).
                            let ea = form.querySelector(
                                'input[name=__EVENTARGUMENT]'
                            );
                            if (!ea) {
                                ea = document.createElement('input');
                                ea.type = 'hidden';
                                ea.name = '__EVENTARGUMENT';
                                form.appendChild(ea);
                            }

                            form.submit();
                            return {
                                ok: true,
                                target: btn.name,
                                action: form.action,
                            };
                        }
                        """
                    )
                    logger.info(
                        "Playwright: form.submit() invoked",
                        source=self.source_name,
                        submit_result=submit_result,
                    )

                    # Wait for the results to render. After a successful
                    # search the page contains "N pending sales records
                    # found" — that's the signal we're really after.
                    await page.wait_for_selector(
                        "text=records found",
                        timeout=30000,
                    )
                    await asyncio.sleep(0.5)
                    logger.info(
                        "Playwright: search form submitted; "
                        "results loaded, session warmed",
                        source=self.source_name,
                    )
                except (PlaywrightTimeout, PlaywrightError) as e:
                    # Diagnostic dump: what page did we actually end up
                    # on, and what does it contain? Without this we'd be
                    # guessing again.
                    try:
                        url_now = page.url
                        title_now = await page.title()
                        body_sample = await page.evaluate(
                            "document.body.innerText.substring(0, 500)"
                        )
                    except Exception:
                        url_now = "?"
                        title_now = "?"
                        body_sample = "?"
                    logger.warning(
                        "Playwright: search-form submit/result-wait failed; "
                        "detail fetches will likely bounce",
                        source=self.source_name,
                        error_type=type(e).__name__,
                        error_repr=repr(e),
                        submit_result=submit_result,
                        url_after=url_now,
                        title_after=title_now,
                        body_sample=body_sample[:400],
                    )

                detail_ok = 0
                detail_bounced = 0
                detail_errors = 0

                for row in all_rows:
                    detail_id = row.get("detail_id")
                    if not detail_id:
                        continue

                    url = _DETAIL_URL.format(id=detail_id)
                    try:
                        await asyncio.sleep(_DETAIL_DELAY_SECONDS)
                        await page.goto(
                            url,
                            wait_until="domcontentloaded",
                            timeout=30000,
                        )
                        final_url = page.url.lower()
                        if "error.aspx" in final_url:
                            detail_bounced += 1
                            logger.warning(
                                "Playwright: detail bounced to error",
                                source=self.source_name,
                                detail_id=detail_id,
                                final_url=page.url,
                            )
                            continue

                        html = await page.content()
                        parsed = self._parse_detail(html)
                        if parsed:
                            row.update(parsed)
                            detail_ok += 1
                        else:
                            # Got a non-error page but parsing returned
                            # nothing — log so we can investigate.
                            logger.warning(
                                "Playwright: detail parsed empty",
                                source=self.source_name,
                                detail_id=detail_id,
                                final_url=page.url,
                            )
                    except PlaywrightTimeout:
                        detail_errors += 1
                        logger.warning(
                            "Playwright: detail load timeout",
                            source=self.source_name,
                            detail_id=detail_id,
                        )
                    except PlaywrightError as e:
                        detail_errors += 1
                        logger.warning(
                            "Playwright: detail navigation error",
                            source=self.source_name,
                            detail_id=detail_id,
                            error=str(e),
                        )

                logger.info(
                    "Playwright detail enrichment complete",
                    source=self.source_name,
                    detail_ok=detail_ok,
                    detail_bounced=detail_bounced,
                    detail_errors=detail_errors,
                    total=len(all_rows),
                )
            finally:
                await browser.close()

    # ---- HTML parsing helpers ----

    def _parse_list_table(self, html: str, mode: str) -> list[dict[str, Any]]:
        """Parse the results table into row dicts.

        Expected columns (per the live page): Details | Scheduled Date |
        Address | City | Zip. The Details cell links to
        ForeclosureNotice.aspx?id={id}.
        """
        soup = BeautifulSoup(html, "lxml")
        rows: list[dict[str, Any]] = []

        # Find every link to a detail page; its containing row holds the data.
        for link in soup.find_all("a", href=re.compile(r"ForeclosureNotice\.aspx", re.I)):
            href = link.get("href", "")
            m = re.search(r"id=(\d+)", href)
            if not m:
                continue
            detail_id = m.group(1)

            tr = link.find_parent("tr")
            if tr is None:
                continue
            cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            # cells ~ ["Details", "10/19/2026", "2205 Foxtail Court", "Lino Lakes", "55110"]
            sched = cells[1] if len(cells) > 1 else None
            address = cells[2] if len(cells) > 2 else None
            city = cells[3] if len(cells) > 3 else None
            zip_code = cells[4] if len(cells) > 4 else None

            rows.append({
                "detail_id": detail_id,
                "mode": mode,
                "scheduled_date": sched,
                "address": address,
                "city": city,
                "zip": zip_code,
            })
        return rows

    def _parse_detail(self, html: str) -> dict[str, Any]:
        """Parse a ForeclosureNotice detail page into structured fields."""
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ", strip=True)
        out: dict[str, Any] = {}

        # Label/value pairs (Sale Date, Sale Time, Address, Mortgagor(s)).
        # The page lays these out as label cells followed by value cells.
        def _value_after(label_regex: str) -> str | None:
            el = soup.find(string=re.compile(label_regex, re.I))
            if not el:
                return None
            parent = el.find_parent(["td", "th", "div", "span", "li", "p"])
            if parent is None:
                return None
            # Try the next sibling cell first, else trailing text after the label.
            sib = parent.find_next_sibling(["td", "th", "div", "span"])
            if sib and sib.get_text(strip=True):
                return sib.get_text(strip=True)
            whole = parent.get_text(" ", strip=True)
            cleaned = re.sub(label_regex, "", whole, flags=re.I).strip(" :")
            return cleaned or None

        out["owner_name"] = _value_after(r"Mortgagor\(s\)") or _value_after(r"Mortgagor")
        out["sale_time"] = _value_after(r"Sale\s+Time")
        detail_addr = _value_after(r"Address")
        if detail_addr:
            out["detail_address"] = detail_addr

        # Status often shows in the small header table ("Postponed", etc.).
        for kw in ("Postponed", "Cancelled", "Canceled", "Sold", "Held", "Pending"):
            if re.search(rf"\b{kw}\b", text, re.I):
                out["status"] = kw
                break

        # Structured facts from the legal notice body.
        m = _RE_TAX_PARCEL.search(text)
        if m:
            out["tax_parcel_no"] = m.group(1)
        m = _RE_AMOUNT_DUE.search(text)
        if m:
            out["amount_due"] = m.group(1)
        m = _RE_ORIG_PRINCIPAL.search(text)
        if m:
            out["original_principal"] = m.group(1)

        return out

    # ---- Parse rows → signals ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[DistressEventInsert]:
        signals: list[DistressEventInsert] = []
        today = date.today()

        for r in raw_records:
            detail_id = r.get("detail_id")
            if not detail_id:
                continue

            parcel_id = f"ANOKA-FC-{detail_id}"
            sale_date = _parse_mmddyyyy(r.get("scheduled_date"))
            if sale_date is None:
                # No usable sale date → skip (can't form a sheriff_sale event).
                continue

            is_pending = "pending" in (r.get("mode") or "").lower()
            status = (r.get("status") or "").lower()

            # Severity:
            #   pending & future & not postponed -> high (still actionable)
            #   pending but postponed/past        -> medium
            #   completed                         -> low
            if is_pending:
                if sale_date >= today and "postpon" not in status and "cancel" not in status:
                    severity = "high"
                else:
                    severity = "medium"
            else:
                severity = "low"

            address = _safe_str(r.get("address"))
            city = _safe_str(r.get("city"))
            amount_due = _safe_decimal(r.get("amount_due"))

            mode_label = "Upcoming" if is_pending else "Completed"
            title_bits = [f"{mode_label} sheriff foreclosure sale"]
            if address:
                title_bits.append(f"— {address}")
            if city:
                title_bits.append(f", {city}")
            title = " ".join(title_bits)[:500]

            desc_parts = []
            if is_pending:
                desc_parts.append(
                    f"Scheduled Anoka County sheriff sale on {sale_date.isoformat()}."
                )
            else:
                desc_parts.append(
                    f"Completed Anoka County sheriff sale on {sale_date.isoformat()}."
                )
            if r.get("owner_name"):
                desc_parts.append(f"Mortgagor: {r['owner_name']}.")
            if amount_due is not None:
                desc_parts.append(f"Amount due: ${amount_due:,.0f}.")
            if r.get("status"):
                desc_parts.append(f"Status: {r['status']}.")
            description = " ".join(desc_parts)[:2000]

            signals.append(DistressEventInsert(
                parcel_id=parcel_id,
                event_type="sheriff_sale",
                event_subtype=("pending_sale" if is_pending else "completed_sale"),
                event_date=sale_date,
                event_value=amount_due,
                source=self.source_name,
                source_id=str(detail_id),
                severity=severity,  # type: ignore[arg-type]
                title=title,
                description=description,
                raw_data={
                    "list": {
                        "scheduled_date": r.get("scheduled_date"),
                        "address": address,
                        "city": city,
                        "zip": r.get("zip"),
                        "mode": r.get("mode"),
                    },
                    "detail": {
                        "owner_name": r.get("owner_name"),
                        "sale_time": r.get("sale_time"),
                        "detail_address": r.get("detail_address"),
                        "tax_parcel_no": r.get("tax_parcel_no"),
                        "amount_due": r.get("amount_due"),
                        "original_principal": r.get("original_principal"),
                        "status": r.get("status"),
                    },
                    "_source": self.source_name,
                },
                observed_at=datetime.now(timezone.utc),
            ))

        return signals

    # ---- Write (mirror Dakota: resolve parcels + dedup events) ----

    async def write(
        self, signals: list[DistressEventInsert]
    ) -> tuple[int, int, int]:
        if not signals:
            return 0, 0, 0

        unique_parcels: dict[str, ParcelUpsert] = {}
        for ev in signals:
            if ev.parcel_id in unique_parcels:
                continue
            raw = ev.raw_data or {}
            lst = raw.get("list") or {}
            detail = raw.get("detail") or {}

            address = _safe_str(lst.get("address")) or _safe_str(detail.get("detail_address"))
            city = _safe_str(lst.get("city"))
            zip_code = _safe_str(lst.get("zip"))

            unique_parcels[ev.parcel_id] = ParcelUpsert(
                parcel_id=ev.parcel_id,
                county_code=self.county_code,
                state="MN",
                address=address,
                city=city,
                zip=zip_code,
                raw_data={"anoka_foreclosure": {**lst, **detail}, "_source": self.source_name},
                data_sources=[self.source_name],
                last_observed_at=datetime.now(timezone.utc),
            )

        parcels_failed = 0
        for payload in unique_parcels.values():
            if resolve_parcel(payload) is None:
                parcels_failed += 1

        new_events, failed_events = write_events_dedup(signals)
        logger.info(
            "Anoka write complete",
            source=self.source_name,
            parcels=len(unique_parcels),
            events_new=new_events,
            failed=failed_events + parcels_failed,
        )
        return new_events, 0, failed_events + parcels_failed


__all__ = ["AnokaSheriffScraper"]
