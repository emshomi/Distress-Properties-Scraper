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
# Anoka detail pages come in (at least) two flavors with different
# labels for the same facts:
#   - HOA/condo lien notices use "Tax Parcel No." and prose like
#     "the amount of $X for unpaid association assessments".
#   - Bank-mortgage notices use "Tax Parcel ID Number" / "TAX PARCEL
#     IDENTIFICATION NUMBER" / "PROPERTY IDENTIFICATION NUMBER" and
#     "AMOUNT CLAIMED TO BE DUE ON THE MORTGAGE...".
# These regexes are written to span both.
_RE_TAX_PARCEL = re.compile(
    r"(?:TAX\s+PARCEL|PROPERTY)\s+"
    r"(?:NO\.?|ID(?:ENTIFICATION)?(?:\s+NUMBER)?)"
    r"\s*:?\s*"
    r"([0-9][0-9A-Za-z\-]+)",
    re.I,
)
# Matches "AMOUNT DUE", "AMOUNT CLAIMED TO BE DUE", and similar variants.
_RE_AMOUNT_DUE = re.compile(
    r"AMOUNT\s+(?:CLAIMED\s+TO\s+BE\s+)?DUE\b[^$]{0,200}\$\s*"
    r"([0-9][0-9,]*\.?[0-9]{0,2})",
    re.I,
)
# Fallback for HOA / condo notices that don't use an AMOUNT DUE label —
# they phrase it as prose: "...to [Association name], the amount of
# $X for unpaid association assessments...". This must be specific
# enough that it doesn't match every "amount of $X" in legal text.
_RE_HOA_AMOUNT = re.compile(
    r"the\s+amount\s+of\s+\$\s*([0-9][0-9,]*\.?[0-9]{0,2})"
    r"\s+for\s+unpaid\s+(?:association|condominium|HOA)",
    re.I,
)
_RE_ORIG_PRINCIPAL = re.compile(
    r"ORIGINAL\s+PRINCIPAL[^$]*\$\s*([0-9][0-9,]*\.?[0-9]{0,2})", re.I
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


def _tax_parcel_no_to_pid(value: Any) -> str | None:
    """Anoka detail-page `tax_parcel_no` -> a core.parcels parcel_id.

    The county writes the PIN dashed on the detail page
    ("08-31-24-22-0220") and undashed in the list page's propid
    ("053123420040"). core.parcels stores the undashed 12-digit form, so a
    pending-sale PIN has to have its punctuation removed before it can join.

    Returns None unless the result is EXACTLY 12 digits. A short or long
    value is not silently zero-padded or truncated: a wrong parcel_id does
    not fail loudly, it attaches a foreclosure to somebody else's property.
    Measured 2026-08-14: all 74 tax_parcel_no values present across Anoka's
    synthetic events were 12 digits after stripping, and 73 matched a real
    Anoka parcel — so the strict check costs nothing real and guards the
    case where the county changes format.
    """
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits if len(digits) == 12 else None


def _parse_mmddyyyy(value: str | None) -> date | None:
    """Parse the several date shapes the two Anoka tables use.

    Pending Sales renders `10/19/2026`. Completed Sales renders `8-04-2026`
    — HYPHENS, and a single-digit month.

    FIXED 2026-08-07. The hyphen formats were absent, so every Completed
    Sales row failed to parse and was dropped by the `continue` in parse().
    That was the SECOND silent filter discarding the same 159 rows: the
    first was _parse_list_table matching only ForeclosureNotice.aspx. Run
    557 captured all 159 rows and still wrote zero events because of this.

    The date cell can also carry the "Details" link text when the county
    renders sale date and link in one cell, so leading non-numeric words
    are stripped before parsing.
    """
    if not value:
        return None
    v = value.strip()

    # Strip any leading label text ("Details 8-04-2026" → "8-04-2026").
    m = re.search(r"(\d{1,4}[-/]\d{1,2}[-/]\d{2,4})", v)
    if m:
        v = m.group(1)

    for fmt in (
        "%m/%d/%Y", "%m/%d/%y",
        "%m-%d-%Y", "%m-%d-%y",   # Completed Sales table
        "%Y-%m-%d",
    ):
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

            # 3. Detail-page enrichment. Earlier attempts:
            #   - httpx detail GETs with warm cookies + 6 headers: bounce
            #   - httpx detail GETs with warm cookies + Sec-Fetch-*: bounce
            #   - Playwright detail GETs with cold cookies: bounce
            #   - Playwright form-submission to warm cookies: blocked
            # The single combination we haven't tried: Playwright (real
            # Chromium fingerprint + JS execution) with cookies WARMED
            # by httpx's known-working search POST. That's now the path.
            # We extract httpx's cookies here and pass them to Playwright,
            # which adds them to the context BEFORE any navigation.
            httpx_cookies = []
            for c in client.cookies.jar:
                httpx_cookies.append(
                    {
                        "name": c.name,
                        "value": c.value or "",
                        "domain": c.domain
                        or "foreclosures.co.anoka.mn.us",
                        "path": c.path or "/",
                    }
                )
            await self._enrich_details_with_playwright(
                httpx_cookies, all_rows
            )

        # 4. Parcel enrichment (owner / market value / homestead / absentee).
        #    The sheriff notices give us tax_parcel_no but no owner-of-record,
        #    market value, or homestead status. Anoka's attributed parcel layer
        #    carries those and exposes the SAME dashed PIN in its PIN2 field, so
        #    we join exactly (verified 2026-05-31). This is a SOFT step: any
        #    failure leaves rows un-enriched but never breaks the sheriff data,
        #    which is the core product. It runs OUTSIDE the httpx block above
        #    because the enrichment helper opens its own client against the
        #    (different, non-bot-resistant) county GIS server.
        await self._enrich_rows_with_parcel_data(all_rows)

        logger.info(
            "Anoka fetch complete",
            source=self.source_name,
            total_rows=len(all_rows),
        )
        return all_rows

    # ---- Parcel-data enrichment (owner / value / homestead via PIN2) ----

    async def _enrich_rows_with_parcel_data(
        self, all_rows: list[dict[str, Any]]
    ) -> None:
        """Attach owner / market value / homestead / absentee to each row by
        matching the sheriff's tax_parcel_no against the Anoka parcel layer's
        PIN2 field. Mutates rows in place, adding gis_* keys where matched.

        Soft-fail by construction: fetch_parcel_enrichment never raises, and a
        row with no tax_parcel_no (or no match) simply gets no gis_* fields.
        """
        # Local import keeps the dependency contained to this optional step.
        from src.scrapers.anoka_parcel_enrichment import (
            fetch_parcel_enrichment,
            _norm as _norm_pin,
        )

        # Collect the parcel numbers we actually have.
        pins = [
            r.get("tax_parcel_no")
            for r in all_rows
            if r.get("tax_parcel_no")
        ]
        if not pins:
            logger.info(
                "Anoka enrichment skipped (no tax_parcel_no on any row)",
                source=self.source_name,
            )
            return

        try:
            enrichment = await fetch_parcel_enrichment(pins)
        except Exception as e:
            # Defense in depth — the helper already soft-fails, but never let
            # an unexpected error here kill the (already-fetched) sheriff data.
            logger.warning(
                "Anoka parcel enrichment raised; continuing un-enriched",
                source=self.source_name,
                error_type=type(e).__name__,
                error_repr=repr(e),
            )
            return

        matched = 0
        for r in all_rows:
            tp = r.get("tax_parcel_no")
            if not tp:
                continue
            data = enrichment.get(_norm_pin(tp))
            if not data:
                continue
            r.update(data)  # adds gis_owner, gis_market_value, etc.
            matched += 1

        logger.info(
            "Anoka parcel enrichment applied",
            source=self.source_name,
            rows_with_pin=len(pins),
            rows_matched=matched,
        )

    # ---- httpx detail-page enrichment ----

    async def _enrich_details_with_httpx(
        self,
        client: httpx.AsyncClient,
        all_rows: list[dict[str, Any]],
    ) -> None:
        """Fetch each row's detail page over httpx, reusing the search
        POST's cookies, with browser-equivalent navigation headers.
        """
        # Headers that mirror what Chrome sends when the user clicks a
        # link from the list page to a same-origin detail page.
        # _BROWSER_HEADERS already covers Accept / Accept-Language /
        # Accept-Encoding / Connection / User-Agent / Upgrade-Insecure-
        # Requests; we add the Sec-Fetch-* and Referer here so the full
        # set looks like a real same-origin navigation.
        nav_headers = {
            **_BROWSER_HEADERS,
            "Referer": _LIST_URL,
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
        }

        detail_ok = 0
        detail_bounced = 0
        detail_errors = 0
        bounced_examples: list[str] = []

        for row in all_rows:
            detail_id = row.get("detail_id")
            if not detail_id:
                continue

            url = _DETAIL_URL.format(id=detail_id)
            try:
                await asyncio.sleep(_DETAIL_DELAY_SECONDS)
                resp = await client.get(url, headers=nav_headers)

                # The Anoka app handles a rejected/expired session by
                # serving a 200 response from error.aspx (after redirect)
                # OR by returning the error page inline at the original
                # URL. Check both — final URL AND body content.
                final_url = str(resp.url).lower()
                body_lower = resp.text[:2000].lower()
                bounced = (
                    "error.aspx" in final_url
                    or "web page has expired" in body_lower
                    or "page has expired" in body_lower
                )

                if bounced:
                    detail_bounced += 1
                    if len(bounced_examples) < 2:
                        bounced_examples.append(
                            f"id={detail_id} → {resp.url}"
                        )
                    continue

                parsed = self._parse_detail(resp.text)
                if parsed:
                    row.update(parsed)
                    detail_ok += 1
                else:
                    logger.warning(
                        "httpx: detail parsed empty",
                        source=self.source_name,
                        detail_id=detail_id,
                        final_url=str(resp.url),
                    )
            except httpx.HTTPError as e:
                detail_errors += 1
                logger.warning(
                    "httpx: detail fetch error",
                    source=self.source_name,
                    detail_id=detail_id,
                    error_type=type(e).__name__,
                    error=str(e),
                )

        logger.info(
            "httpx detail enrichment complete",
            source=self.source_name,
            detail_ok=detail_ok,
            detail_bounced=detail_bounced,
            detail_errors=detail_errors,
            bounced_examples=bounced_examples,
        )

    # ---- Playwright detail-page enrichment ----

    async def _enrich_details_with_playwright(
        self,
        httpx_cookies: list[dict[str, Any]],
        all_rows: list[dict[str, Any]],
    ) -> None:
        """Fill in mortgagor/amount-due/tax-parcel by browsing the detail
        pages with headless Chromium, using cookies that httpx ALREADY
        warmed via its successful search POST.

        Why this exists: every other combination bounced. httpx-only
        detail GETs (even with full Sec-Fetch-* headers) all redirect to
        error.aspx. Playwright trying to submit the form in-page (via
        click, JS form.submit, or context.request.post) cannot warm the
        session — the search either doesn't dispatch or is rejected by
        the server, leaving cold cookies. Real users succeed because
        their session is warmed by a real form click in a real browser.
        This method approximates that: httpx does the warming POST (the
        ONE thing httpx is known to do successfully here), we copy its
        cookies into a real Chromium context, and from then on it's a
        real browser hitting the detail pages with the right session.
        """
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
                # Mask the standard headless-Chromium tell. Cheap.
                await context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', "
                    "{ get: () => undefined });"
                )

                # KEY STEP: inject httpx's warm cookies BEFORE any
                # navigation. From the server's perspective, this
                # browser context already participated in a successful
                # search and is allowed to view detail pages.
                if httpx_cookies:
                    await context.add_cookies(httpx_cookies)
                    logger.info(
                        "Playwright: injected warm cookies from httpx",
                        source=self.source_name,
                        cookie_count=len(httpx_cookies),
                        cookie_names=[
                            c.get("name") for c in httpx_cookies
                        ],
                    )
                else:
                    logger.warning(
                        "Playwright: no httpx cookies to inject; "
                        "detail fetches will likely bounce",
                        source=self.source_name,
                    )

                page = await context.new_page()

                # Touch the list page once to establish navigation
                # context (Referer chain, JS-set state, etc.).
                try:
                    await page.goto(
                        _LIST_URL,
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                    await asyncio.sleep(0.5)
                except (PlaywrightTimeout, PlaywrightError) as e:
                    logger.warning(
                        "Playwright: list page pre-load failed; "
                        "continuing anyway",
                        source=self.source_name,
                        error_type=type(e).__name__,
                    )

                # Submit the search via in-page fetch(). Earlier we
                # tried page.context.request.post (Playwright's separate
                # API request stack) — server returned the empty form.
                # In-page fetch is different: it goes through Chromium's
                # NATIVE network stack, with the same TLS fingerprint,
                # HTTP version, and request internals as a real user's
                # click. If the server discriminates between API-style
                # POSTs and real-browser POSTs, this is the path that
                # passes for a real browser.
                #
                # Why we POST at all when we already have warm cookies:
                # cookie injection alone failed every detail GET. The
                # server must be checking something beyond the cookie —
                # likely server-side session state that's only set when
                # a search POST is processed *in this browser context*
                # (not just any POST that shares the SessionId).
                try:
                    fetch_result = await page.evaluate(
                        """
                        async () => {
                            const form = document.querySelector('form');
                            if (!form) return {
                                ok: false, reason: 'no form'
                            };

                            // Set Pending dropdown
                            for (const sel of form.querySelectorAll(
                                'select'
                            )) {
                                const labels = [...sel.options].map(
                                    o => (o.textContent || '')
                                        .toLowerCase()
                                );
                                if (labels.some(
                                        l => l.includes('pending')
                                    ) && labels.some(
                                        l => l.includes('completed')
                                    )) {
                                    for (const opt of sel.options) {
                                        if ((opt.textContent || '')
                                            .toLowerCase()
                                            .includes('pending')) {
                                            sel.value = opt.value;
                                            break;
                                        }
                                    }
                                    break;
                                }
                            }

                            // Build form data the way a real submit
                            // does: every input/select, plus the
                            // submit button's name=value.
                            const data = new URLSearchParams();
                            for (const inp of form.querySelectorAll(
                                'input'
                            )) {
                                if (inp.name && inp.type !== 'submit') {
                                    data.append(
                                        inp.name, inp.value || ''
                                    );
                                }
                            }
                            for (const sel of form.querySelectorAll(
                                'select'
                            )) {
                                if (sel.name) {
                                    data.append(
                                        sel.name, sel.value || ''
                                    );
                                }
                            }
                            const btn = form.querySelector(
                                'input[type=submit][value=Submit]'
                            );
                            if (btn && btn.name) {
                                data.append(btn.name, btn.value);
                            }

                            // Real-browser POST via native fetch.
                            const url = form.action
                                || window.location.href;
                            const response = await fetch(url, {
                                method: 'POST',
                                body: data,
                                credentials: 'include',
                                redirect: 'follow',
                                headers: {
                                    'Content-Type':
                                        'application/x-www-form-urlencoded'
                                }
                            });
                            const html = await response.text();
                            return {
                                ok: response.ok,
                                status: response.status,
                                url: response.url,
                                body_length: html.length,
                                has_results: html.toLowerCase()
                                    .includes('records found'),
                                has_detail_links: html.toLowerCase()
                                    .includes('foreclosurenotice')
                            };
                        }
                        """
                    )
                    logger.info(
                        "Playwright: in-page fetch POST",
                        source=self.source_name,
                        **fetch_result,
                    )
                except Exception as e:
                    logger.warning(
                        "Playwright: in-page fetch POST raised",
                        source=self.source_name,
                        error_type=type(e).__name__,
                        error_repr=repr(e),
                    )

                # Small pause so any server-side session work settles.
                await asyncio.sleep(1.0)

                detail_ok = 0
                detail_bounced = 0
                detail_errors = 0
                bounced_examples: list[str] = []
                # One-shot diagnostic: dump the text context around
                # field-name keywords whenever a detail page is fetched
                # OK but the parser misses amount_due or tax_parcel_no.
                # This is how we learn what label variants the pages use
                # without being able to open them manually in a browser.
                missing_field_dumps = 0

                for row in all_rows:
                    detail_id = row.get("detail_id")
                    if not detail_id:
                        continue

                    # Completed Sales rows link to ForeclosureDetail.aspx and
                    # are keyed by docnum/propid, NOT by the numeric id that
                    # _DETAIL_URL expects. Requesting
                    # ForeclosureNotice.aspx?id=053123420040 would fetch an
                    # unrelated page or bounce. Their list row already carries
                    # sale date, recorded date, address, city, zip and a real
                    # propid, which is the substance of the record.
                    if row.get("detail_kind") == "detail":
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
                            if len(bounced_examples) < 2:
                                bounced_examples.append(
                                    f"id={detail_id} -> {page.url}"
                                )
                            continue

                        html = await page.content()
                        parsed = self._parse_detail(html)
                        if parsed:
                            row.update(parsed)
                            detail_ok += 1

                            # Diagnostic: when amount_due or tax_parcel
                            # is missing, dump text context around the
                            # relevant keywords so we can see what
                            # label variants this page uses.
                            missing_amt = parsed.get("amount_due") is None
                            missing_tax = (
                                parsed.get("tax_parcel_no") is None
                            )
                            if (missing_amt or missing_tax) and (
                                missing_field_dumps < 2
                            ):
                                page_text = BeautifulSoup(
                                    html, "lxml"
                                ).get_text(" ", strip=True)
                                upper = page_text.upper()
                                contexts: dict[str, str] = {}
                                for kw in (
                                    "AMOUNT",
                                    "PARCEL",
                                    "PIN",
                                    "PROPERTY ID",
                                    "PROPERTY IDENTIFICATION",
                                    "PRINCIPAL",
                                ):
                                    idx = upper.find(kw)
                                    if idx >= 0:
                                        start = max(0, idx - 30)
                                        end = min(
                                            len(page_text), idx + 250
                                        )
                                        contexts[kw.lower()] = (
                                            page_text[start:end]
                                        )
                                logger.info(
                                    "detail field debug — text context",
                                    source=self.source_name,
                                    detail_id=detail_id,
                                    status=parsed.get("status"),
                                    got_amount_due=not missing_amt,
                                    got_tax_parcel=not missing_tax,
                                    contexts=contexts,
                                )
                                missing_field_dumps += 1
                        else:
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
                    bounced_examples=bounced_examples,
                    total=len(all_rows),
                )
            finally:
                await browser.close()


    # ---- HTML parsing helpers ----

    def _parse_list_table(self, html: str, mode: str) -> list[dict[str, Any]]:
        """Parse the results table into row dicts.

        THE TWO MODES RENDER DIFFERENT TABLES AND DIFFERENT LINKS.

        Pending Sales — 5 columns, Details link → ForeclosureNotice.aspx?id={n}
            Details | Scheduled Date | Address | City | Zip

        Completed Sales — 6 columns, Details link → ForeclosureDetail.aspx
            Details | Sale Date | Recorded Date | Address | City | Zip
            query string: ?docnum=DOC891S461&propid=053123420040&inputaddress=...

        === WHY THIS EXISTS (fixed 2026-08-07) ===
        This method matched ONLY `ForeclosureNotice\\.aspx`. Completed Sales
        rows link to `ForeclosureDetail.aspx`, so every one of them was
        discarded — silently, on every run, since the scraper went live.

        Measured 2026-08-07: signals.distress_events held 297 anoka_sheriff
        events dating back to 2026-05-31 and EVERY SINGLE ONE was mode
        'Pending Sales'. Zero completed sales had ever been captured. The
        county's own search returns 159 completed records for All Cities.

        The consequence was not just missing rows. Pending sales are sales
        that HAVE NOT HAPPENED, so they start no redemption clock — Anoka was
        contributing nothing whatsoever to outcomes.redemption_tracker, and
        45 of the fabricated future-anchored redemption rows deleted on
        2026-08-07 were Anoka pending sales. Completed sales are the ones
        that actually open a redemption window.

        The failure looked like flakiness: fetch() treats a Completed Sales
        POST failure as non-fatal and logs `rows=0`, which is
        indistinguishable from "the county had no completed sales". A
        ReadTimeout on 2026-08-07 made it look like a transient server
        problem. It was not; the parser could never have matched.

        Completed rows also carry TWO fields the Pending list does not:
        `propid` (a real Anoka parcel ID) and `docnum` (the recorded document
        number). Both are captured into the row dict. The synthetic
        ANOKA-FC-* identity scheme is deliberately NOT changed here — Anoka
        PIN normalisation is a known open problem (164 tracker rows carry
        synthetic case-number PINs) and re-keying is a separate change with a
        different risk profile. propid is carried through to raw_data so that
        work can use it later.
        """
        soup = BeautifulSoup(html, "lxml")
        rows: list[dict[str, Any]] = []

        # Match BOTH detail-page shapes. Notice = pending, Detail = completed.
        link_pattern = re.compile(
            r"Foreclosure(?:Notice|Detail)\.aspx", re.I
        )

        for link in soup.find_all("a", href=link_pattern):
            href = link.get("href", "")
            is_completed = "foreclosuredetail" in href.lower()

            detail_id: str | None = None
            propid: str | None = None
            docnum: str | None = None

            if is_completed:
                # ?docnum=DOC891S461&propid=053123420040&inputaddress=...
                # NOTE: do NOT reuse the `id=(\d+)` regex below — it would
                # match the tail of "propid=" and silently capture the PIN as
                # a detail id.
                m_doc = re.search(r"docnum=([^&]+)", href, re.I)
                m_prop = re.search(r"propid=([^&]+)", href, re.I)
                docnum = m_doc.group(1).strip() if m_doc else None
                propid = m_prop.group(1).strip() if m_prop else None
                # Identity for a COMPLETED row is the property (propid), so
                # two sales on one parcel resolve to one parcel. Fall back to
                # the document number when the county omits propid.
                detail_id = propid or docnum
            else:
                m = re.search(r"[?&]id=(\d+)", href)
                detail_id = m.group(1) if m else None

            if not detail_id:
                continue

            tr = link.find_parent("tr")
            if tr is None:
                continue
            cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]

            if is_completed:
                # ["Details 8-04-2026", "08/04/2026", "1533 128TH AVE NE",
                #  "BLAINE", "55449"] — the Details cell carries the sale date
                # inline, then Recorded Date, Address, City, Zip.
                # Column count differs from Pending by one; reading fixed
                # Pending indices here would put Recorded Date in `address`
                # and Address in `city`.
                sched = cells[1] if len(cells) > 1 else None
                address = cells[2] if len(cells) > 2 else None
                city = cells[3] if len(cells) > 3 else None
                zip_code = cells[4] if len(cells) > 4 else None
                if len(cells) >= 6:
                    # Full 6-column render: Details | Sale | Recorded |
                    # Address | City | Zip
                    sched = cells[1]
                    recorded = cells[2]
                    address = cells[3]
                    city = cells[4]
                    zip_code = cells[5]
                else:
                    recorded = None
            else:
                # ["Details", "10/19/2026", "2205 Foxtail Court",
                #  "Lino Lakes", "55110"]
                sched = cells[1] if len(cells) > 1 else None
                address = cells[2] if len(cells) > 2 else None
                city = cells[3] if len(cells) > 3 else None
                zip_code = cells[4] if len(cells) > 4 else None
                recorded = None

            rows.append({
                "detail_id": detail_id,
                "mode": mode,
                "scheduled_date": sched,
                "recorded_date": recorded,
                "address": address,
                "city": city,
                "zip": zip_code,
                # Completed-only. Absent (None) on pending rows.
                "propid": propid,
                "docnum": docnum,
                # Drives detail-page enrichment: only ForeclosureNotice pages
                # are fetchable by `id`, so completed rows must be skipped.
                "detail_kind": "detail" if is_completed else "notice",
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

        # Status: prefer a labeled "Status:" field (which Anoka's pages
        # do have — the user's manual visit confirmed "Status:
        # Postponed" appears as a structured label). Only fall back to
        # a free-text keyword scan for clear postponement/cancellation
        # signals — and crucially NOT for "Sold" or "Pending" which
        # appear in legal boilerplate on every active notice ("the
        # property will be sold at public auction... pending the
        # outcome of...") and cause 50% of rows to be mistagged.
        labeled_status = _value_after(r"Status")
        if labeled_status and len(labeled_status) < 60:
            out["status"] = labeled_status.strip(": ")
        else:
            for kw in ("Postponed", "Cancelled", "Canceled"):
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
        else:
            # HOA/condo notices use prose, not a labeled field.
            m = _RE_HOA_AMOUNT.search(text)
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

            # Completed Sales rows publish a REAL Anoka parcel id in the
            # Details href (`?propid=053123420040`). Use it — a real PIN joins
            # to the 139,420 Anoka parcels in core.parcels and the property
            # gains an assessed value, an owner and an address. A synthetic
            # id joins to nothing.
            #
            # ADDED 2026-08-07 (task 534). Before this, every Anoka event got
            # ANOKA-FC-{detail_id} and 292 synthetic parcels had accumulated.
            # Measured: 155 of 156 completed-sale propids matched core.parcels
            # EXACTLY — same 12-character format, no normalisation needed —
            # and all 151 distinct target parcels carried EMV and owner data,
            # 147 an address. The 136 events with no dedup collision were
            # re-keyed by hand; this stops the scraper recreating them.
            #
            # Deliberately NOT normalised through safe_normalize_parcel_id:
            # the county's propid is already the assessor's own format and
            # matched 155/156 verbatim. Normalising would risk reshaping a
            # value that is already correct.
            #
            # ── SECOND SOURCE, ADDED 2026-08-14 ────────────────────────────
            # The comment that stood here said Pending Sales "expose NO
            # propid ... the PIN only appears once a sale completes", and
            # treated that as a limitation of the source rather than
            # something to work around. Half of it is right: the LIST page
            # carries no propid for pending rows. But the DETAIL page carries
            # `tax_parcel_no` — dashed, e.g. "08-31-24-22-0220" — and THIS
            # SCRAPER ALREADY FETCHES AND STORES IT. _extract_detail() picks
            # it up at line ~1091 and it lands in raw_data.detail.
            #
            # It was even being USED: the enrichment step below keys on
            # tax_parcel_no to pull gis_owner, gis_market_value and
            # gis_homestead off the Anoka parcel layer. A pending row we
            # inspected carried gis_market_value 307700 — fetched via this
            # PIN — while its parcel_id stayed ANOKA-FC-23832 and the
            # property showed no map, no imagery, no valuation and no deal
            # math on the detail panel.
            #
            # So the PIN was present, stored, and proven to resolve. It just
            # never reached the one field that decides whether a property is
            # connected to core.parcels.
            #
            # Measured 2026-08-14 over all 181 synthetic Anoka events:
            #   74 carry detail.tax_parcel_no
            #   74 of 74 are exactly 12 digits once punctuation is stripped
            #   73 of 74 match an existing (anoka, parcel_id) in core.parcels
            # The remaining 107 have no tax_parcel_no at all — a separate
            # problem (the detail page was not fetched, or the county did not
            # publish one) needing its own fix, not this one.
            #
            # WHY DIGITS-ONLY AND NOT _norm(): _norm is imported above as
            # `_norm_pin`, which reads like a PIN normaliser and is not — it
            # upper-cases and collapses spaces, leaving "08-31-24-22-0220"
            # dashed. That is correct for the ENRICHMENT, which matches the
            # parcel layer's PIN2 field (dashed). core.parcels stores the
            # 12-digit form, so this side needs the dashes gone. Two formats,
            # two sources, both right — hence an explicit conversion here
            # rather than borrowing a function that does something else.
            propid = r.get("propid")
            tax_pin = _tax_parcel_no_to_pid(r.get("tax_parcel_no"))
            if propid and str(propid).strip():
                parcel_id = str(propid).strip()
            elif tax_pin:
                parcel_id = tax_pin
            else:
                parcel_id = f"ANOKA-FC-{detail_id}"

            sale_date = _parse_mmddyyyy(r.get("scheduled_date"))
            if sale_date is None:
                # Completed rows publish a Recorded Date alongside the sale
                # date. Recording follows the sale by days, so it is a sound
                # last resort — far better than discarding a real completed
                # foreclosure because of a date format.
                sale_date = _parse_mmddyyyy(r.get("recorded_date"))
            if sale_date is None:
                # Genuinely unusable. LOG it — a silent `continue` here hid
                # 159 completed sales through run 557 (see _parse_mmddyyyy).
                logger.warning(
                    "Anoka row dropped: no parseable date",
                    source=self.source_name,
                    mode=r.get("mode"),
                    detail_id=detail_id,
                    scheduled_date=r.get("scheduled_date"),
                    recorded_date=r.get("recorded_date"),
                )
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
                # ADDED 2026-08-10. This scraper builds DistressEventInsert
                # DIRECTLY (no to_event() projection), so it must set
                # county_code itself. Without it the composite FK
                # (county_code, parcel_id) -> core.parcels is unenforced AND
                # distress_events_dedup_key never collides, because NULL is
                # not equal to anything.
                #
                # Measured 2026-08-10: 2 Anoka events carried county_code
                # NULL and were exact duplicates of rows that already
                # existed with the county set. The dedup key should have
                # refused them. 1,316 rows across three sources were
                # affected in total; all were backfilled by hand.
                county_code=self.county_code,
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
                        "recorded_date": r.get("recorded_date"),
                        "address": address,
                        "city": city,
                        "zip": r.get("zip"),
                        "mode": r.get("mode"),
                        # Completed Sales only. `propid` is a REAL Anoka
                        # parcel ID published in the Details href — the
                        # pending list exposes nothing equivalent. Captured
                        # so the synthetic ANOKA-FC-* ids can later be
                        # resolved to core.parcels rows.
                        "propid": r.get("propid"),
                        "docnum": r.get("docnum"),
                    },
                    "detail": {
                        "owner_name": r.get("owner_name"),
                        "sale_time": r.get("sale_time"),
                        "detail_address": r.get("detail_address"),
                        "tax_parcel_no": r.get("tax_parcel_no"),
                        "amount_due": r.get("amount_due"),
                        "original_principal": r.get("original_principal"),
                        "status": r.get("status"),
                        # Parcel-layer enrichment (present only on rows whose
                        # tax_parcel_no matched Anoka's PIN2; null otherwise).
                        # gis_owner is the assessor owner-of-record, which is
                        # usually cleaner/fuller than the notice's owner_name.
                        "gis_owner": r.get("gis_owner"),
                        "gis_owner_mailing": r.get("gis_owner_mailing"),
                        "gis_is_absentee": r.get("gis_is_absentee"),
                        "gis_market_value": r.get("gis_market_value"),
                        "gis_homestead": r.get("gis_homestead"),
                        "gis_site_address": r.get("gis_site_address"),
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
