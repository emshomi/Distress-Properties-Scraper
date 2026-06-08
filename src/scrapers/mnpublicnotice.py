"""
mnpublicnotice.com statewide foreclosure-notice scraper (Feature #5 feeder).

The Minnesota Newspaper Association's statewide public-notice clearinghouse —
aggregates legal notices from EVERY participating MN newspaper, including the
Star Tribune. This supersedes the (bot-blocked) Star Tribune scraper: same
kind of free-text mortgage-foreclosure notices, but a source we can actually
fetch from the server.

Like the Star Tribune feeder (and unlike Hennepin's structured JSON API), these
are FREE-TEXT legal notices, so they flow through the Feature #5 pipeline:
fetch -> LLM extraction -> ai.extracted_foreclosures (pending) -> human review
on /admin -> promote. Nothing reaches the live site until an admin approves.

=== THE FLOW (proven from the Railway server via the probe) ===
The site is ASP.NET WebForms with an AJAX UpdatePanel. A search is multi-step:
  1. GET /Search.aspx  -> fresh ASP.NET session (URL-stamped /(S(...))/) +
     the full form (67KB __VIEWSTATE + ~22 active fields).
  2. POST the COMPLETE form back (every harvested field), changing only:
        txtSearch = "foreclosure"
        txtDateFrom / txtDateTo = recent window (MM/DD/YYYY)
        rdoType = "AND"
        the ScriptManager field = "<upSearch panel>|<btnGo>"  (async trigger)
        __ASYNCPOST = "true"
     with headers X-MicrosoftAjax: Delta=true, X-Requested-With: XMLHttpRequest.
  3. The response is an AJAX delta containing a `pageRedirect` directive:
        ...|pageRedirect||<url-encoded /(S(...))/Search.aspx>|...
     The results are NOT in this delta — they render on the redirect target.
  4. GET the redirect URL  -> the RESULTS page (~394KB) listing 10 notices.

=== WHY TEASER-LEVEL EXTRACTION (Path A) ===
Recon proved the Details.aspx full-text page is session/captcha-gated and
cannot be fetched server-side (a cold GET returns ~13.6KB of page chrome, no
notice body). The full untruncated notice lives only in a session-stamped PDF
behind that gated page. BUT the results page itself renders, inline per notice,
a teaser: notice type + mortgagor + the opening statutory text, truncated to a
few hundred chars and capped with "click 'view' to open the full text."

So this scraper extracts what is reliably available WITHOUT the detail page:
the notice ID (dedup key), publication, publish date, and the teaser body. The
teaser is fed to the existing LLM extractor (whose anti-fabrication guard means
it stores null for any field the teaser doesn't contain, rather than guessing).
Publication + date are folded into raw_notice_text as a header (the staging
table has no column for them), so the reviewer sees full provenance.

These land as `pending` for human review like every other Feature #5 row. Full
PDF enrichment (sale date, address, PID, amount due) is a separate later effort
gated on solving the Details.aspx session/captcha wall.

Dedup key: the notice ID (stable across sessions; the SID is per-session and
must NOT be part of the dedup key or source_url). source_url is normalized to
a SID-less canonical form so re-runs in new sessions still dedup correctly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from html import unescape
from typing import Any, Optional
from urllib.parse import unquote

import httpx

from src.config import settings
from src.db.supabase_client import ai_table
from src.llm.foreclosure_extraction import extract_foreclosure_notice
from src.utils.logger import logger


_BASE = "https://www.mnpublicnotice.com"
_SEARCH_PAGE = f"{_BASE}/Search.aspx"

# Advanced-search control prefix + the async trigger identifiers (from recon).
_P = "ctl00$ContentPlaceHolder1$as1$"
_SM_FIELD = "ctl00$ToolkitScriptManager1"
_SEARCH_PANEL = "ctl00$ContentPlaceHolder1$as1$upSearch"
_BTN_GO = _P + "btnGo"

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# Defaults
_DEFAULT_WINDOW_DAYS = 14      # recent notices only (avoids 100-page history)
_DEFAULT_MAX_NEW = 25          # per-run cap on NEW extractions (LLM cost)
_MAX_RESULT_PAGES = 5          # safety cap (reserved; pagination not yet used)

# A teaser shorter than this almost certainly lacks anything extractable.
_MIN_TEASER_CHARS = 40


@dataclass
class ScrapeResult:
    ok: bool
    notices_on_results: int = 0
    new_ids: int = 0
    already_staged: int = 0
    newly_extracted: int = 0
    extraction_failed: int = 0
    stored_ids: list[int] = field(default_factory=list)
    error: Optional[str] = None
    notes: list[str] = field(default_factory=list)


@dataclass
class NoticeRow:
    """One parsed notice from the results page (teaser-level)."""
    notice_id: str
    publication: Optional[str]
    pub_date: Optional[str]
    teaser: str


# ============================================================
# Form harvesting (POST the COMPLETE form, like the browser)
# ============================================================


def _harvest_form_fields(html: str) -> dict[str, str]:
    """Extract every form field (name -> value) so we POST the complete form.
    Mirrors browser behavior: only CHECKED checkboxes/radios are included;
    selects use the selected option (else first); buttons are excluded (we add
    the one trigger explicitly)."""
    fields: dict[str, str] = {}

    for tag in re.findall(r"<input\b[^>]*>", html, re.IGNORECASE):
        nm = re.search(r'name="([^"]+)"', tag)
        if not nm:
            continue
        name = nm.group(1)
        typ_m = re.search(r'type="([^"]+)"', tag)
        typ = (typ_m.group(1) if typ_m else "text").lower()
        val_m = re.search(r'value="([^"]*)"', tag)
        val = unescape(val_m.group(1)) if val_m else ""
        if typ in ("checkbox", "radio"):
            if re.search(r"\bchecked\b", tag, re.IGNORECASE):
                fields[name] = val or "on"
        elif typ in ("submit", "button", "image", "reset"):
            continue
        else:
            fields[name] = val

    for sel in re.findall(r"<select\b[^>]*>.*?</select>", html, re.IGNORECASE | re.DOTALL):
        nm = re.search(r'name="([^"]+)"', sel)
        if not nm:
            continue
        name = nm.group(1)
        chosen = None
        for opt in re.findall(r"<option\b[^>]*>", sel, re.IGNORECASE):
            if re.search(r"\bselected\b", opt, re.IGNORECASE):
                vm = re.search(r'value="([^"]*)"', opt)
                chosen = vm.group(1) if vm else ""
                break
        if chosen is None:
            first = re.search(r'<option[^>]*value="([^"]*)"', sel, re.IGNORECASE)
            chosen = first.group(1) if first else ""
        fields[name] = unescape(chosen)

    for ta in re.findall(r'<textarea\b[^>]*name="([^"]+)"[^>]*>(.*?)</textarea>',
                         html, re.IGNORECASE | re.DOTALL):
        fields[ta[0]] = unescape(ta[1]).strip()

    return fields


# ============================================================
# Results-page row parsing (teaser-level — verified against captured HTML)
# ============================================================

# Each notice's View button carries the notice ID in its onclick navigation:
#   ...GridView1$ctlNN$btnView2" ... onclick="...Details.aspx?SID=..&amp;ID=12345..
# We anchor on btnView2, then match the FULL Details.aspx link so we capture the
# real notice ID (the one after &ID=) and never a stray digit. The SID segment
# is [A-Za-z0-9]+ and the &amp; may be HTML-escaped — same pattern as the proven
# _DETAIL_ID_RE from the prior detail-fetch version.
_ROW_ANCHOR_RE = re.compile(
    r'GridView1\$ctl\d+\$btnView2"'              # the row's view button
    r'[^>]*?onclick="[^"]*?'                     # ...into the JS nav...
    r'Details\.aspx\?SID=[A-Za-z0-9]+&(?:amp;)?ID=(\d+)',  # the real notice ID
    re.IGNORECASE | re.DOTALL,
)


def _strip_tags(html: str) -> str:
    """Crude tag strip for isolating readable text from a fragment."""
    html = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", " ", html,
                  flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", html)
    text = unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


def _parse_notice_rows(results_html: str) -> list[NoticeRow]:
    """Parse the results page into one NoticeRow per notice, deduped by ID and
    order-preserved. Pulls notice ID, publication, publish date, and the teaser
    body (with the trailing "click 'view'..." prompt stripped off)."""
    rows: list[NoticeRow] = []
    seen: set[str] = set()

    for am in _ROW_ANCHOR_RE.finditer(results_html):
        notice_id = am.group(1)
        if notice_id in seen:
            continue
        seen.add(notice_id)

        start = am.start()
        window = results_html[start:start + 4000]

        # Publication + date live in the first <div class="left"> after the
        # button: <div class="left"><strong>Publication</strong><br/>Date</div>
        publication: Optional[str] = None
        pub_date: Optional[str] = None
        info_m = re.search(r'<div class="left">(.*?)</div>', window,
                           re.IGNORECASE | re.DOTALL)
        if info_m:
            info_html = info_m.group(1)
            strong = re.search(r'<strong>(.*?)</strong>', info_html,
                               re.IGNORECASE | re.DOTALL)
            if strong:
                publication = _strip_tags(strong.group(1)) or None
            after = re.sub(r'<strong>.*?</strong>', '', info_html,
                           flags=re.IGNORECASE | re.DOTALL)
            date_txt = _strip_tags(after)
            pub_date = date_txt or None

        # Teaser body: the next <td colspan="..."> cell after the button.
        teaser = ""
        teaser_m = re.search(r'<td colspan="\d+"[^>]*>(.*?)</td>', window,
                             re.IGNORECASE | re.DOTALL)
        if teaser_m:
            raw = teaser_m.group(1)
            # Drop the trailing "...click 'view' to open the full text." prompt.
            raw = re.split(r"<em[^>]*>\s*click", raw, flags=re.IGNORECASE)[0]
            teaser = _strip_tags(raw)
            # Also strip a bare trailing "..." left from the truncation.
            teaser = re.sub(r"\s*\.\.\.\s*$", "", teaser).strip()

        rows.append(NoticeRow(
            notice_id=notice_id,
            publication=publication,
            pub_date=pub_date,
            teaser=teaser,
        ))

    return rows


def _canonical_source_url(notice_id: str) -> str:
    """SID-less canonical URL used as the dedup key / source_url. The SID is
    per-session, so we must NOT include it — only the stable notice ID."""
    return f"{_BASE}/Details.aspx?ID={notice_id}"


def _build_notice_text(row: NoticeRow) -> str:
    """Compose the text we hand the LLM + store as raw_notice_text. The staging
    table has no publication/date columns, so we fold them in as a header. The
    header also makes clear to the reviewer that this is a teaser, not the full
    notice — honest labeling, matching the platform's data-accuracy stance."""
    header_bits = ["[mnpublicnotice teaser — partial notice text]"]
    if row.publication:
        header_bits.append(f"Publication: {row.publication}")
    if row.pub_date:
        header_bits.append(f"Published: {row.pub_date}")
    header_bits.append(f"Notice ID: {row.notice_id}")
    header = "\n".join(header_bits)
    return f"{header}\n\n{row.teaser}"


# ============================================================
# Dedup + store (mirror /ai/extract store path)
# ============================================================


def _already_staged(source_url: str) -> bool:
    """True if this canonical source_url is already in ai.extracted_foreclosures
    (any review_status). Fail-safe: treat a check error as 'staged' (skip) so a
    transient DB hiccup can't cause duplicate extraction + LLM cost."""
    try:
        existing = (
            ai_table("extracted_foreclosures")
            .select("id")
            .eq("source_url", source_url)
            .limit(1)
            .execute()
        )
        return bool(existing.data)
    except Exception as e:
        logger.warning(
            "mnpublicnotice dedup check failed; skipping URL to be safe",
            error_type=type(e).__name__,
        )
        return True


def _store_extraction(notice_text: str, source_url: str) -> Optional[int]:
    """Extract one notice and insert it as pending. Mirrors the /ai/extract
    store path exactly. Returns the new row id, or None on failure."""
    extraction = extract_foreclosure_notice(notice_text)
    if not extraction.ok:
        logger.info(
            "mnpublicnotice extraction not-ok",
            source_url=source_url,
            error=extraction.error,
        )
        return None

    row = dict(extraction.data)
    row["source_url"] = source_url
    row["source_name"] = "mnpublicnotice"
    row["raw_notice_text"] = notice_text
    row["model"] = extraction.model

    try:
        result = ai_table("extracted_foreclosures").insert(row).execute()
        return result.data[0]["id"] if result.data else None
    except Exception as e:
        logger.exception(
            "mnpublicnotice store insert failed",
            source_url=source_url,
            error_type=type(e).__name__,
        )
        return None


# ============================================================
# The search flow (GET form -> POST -> follow pageRedirect -> results)
# ============================================================


def _run_search(client: httpx.Client, window_days: int) -> Optional[str]:
    """Execute the full search flow and return the RESULTS-page HTML, or None
    on failure. (GET form -> POST complete form -> follow pageRedirect -> GET.)"""
    # 1. GET the search page (fresh session + full form).
    r1 = client.get(_SEARCH_PAGE)
    if r1.status_code != 200:
        logger.error("mnpublicnotice GET search non-200", status=r1.status_code)
        return None
    base_url = str(r1.url)  # session-stamped
    fields = _harvest_form_fields(r1.text or "")
    if "__VIEWSTATE" not in fields:
        logger.error("mnpublicnotice GET search: no __VIEWSTATE harvested")
        return None

    # 2. POST the complete form with our search criteria + async trigger.
    today = date.today()
    d_from = (today - timedelta(days=window_days)).strftime("%m/%d/%Y")
    d_to = today.strftime("%m/%d/%Y")

    body = dict(fields)
    body[_SM_FIELD] = _SEARCH_PANEL + "|" + _BTN_GO
    body["__ASYNCPOST"] = "true"
    body["__EVENTTARGET"] = ""
    body["__EVENTARGUMENT"] = ""
    body[_P + "txtSearch"] = "foreclosure"
    body[_P + "rdoType"] = "AND"
    body[_P + "txtDateFrom"] = d_from
    body[_P + "txtDateTo"] = d_to
    body[_BTN_GO] = "GO"

    post_headers = dict(_BROWSER_HEADERS)
    post_headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
    post_headers["X-MicrosoftAjax"] = "Delta=true"
    post_headers["X-Requested-With"] = "XMLHttpRequest"
    post_headers["Cache-Control"] = "no-cache"
    post_headers["Origin"] = _BASE
    post_headers["Referer"] = base_url

    r2 = client.post(base_url, data=body, headers=post_headers)
    if r2.status_code != 200:
        logger.error("mnpublicnotice POST search non-200", status=r2.status_code)
        return None

    # 3. Follow the pageRedirect directive in the delta.
    rd = re.search(r"pageRedirect\|\|([^|]+)\|", r2.text or "")
    if not rd:
        logger.error("mnpublicnotice: no pageRedirect in search response")
        return None
    redirect_url = unquote(rd.group(1))
    if redirect_url.startswith("/"):
        redirect_url = _BASE + redirect_url

    # 4. GET the results page.
    r3 = client.get(redirect_url, headers=_BROWSER_HEADERS)
    if r3.status_code != 200:
        logger.error("mnpublicnotice GET results non-200", status=r3.status_code)
        return None
    return r3.text or ""


# ============================================================
# Public entry point
# ============================================================


def run_mnpublicnotice_scrape(
    max_new: int = _DEFAULT_MAX_NEW,
    window_days: int = _DEFAULT_WINDOW_DAYS,
) -> ScrapeResult:
    """Search mnpublicnotice for recent foreclosure notices, extract each NEW
    one from its results-page teaser (dedup by notice ID), store as pending for
    review. Returns a summary."""
    result = ScrapeResult(ok=False)

    try:
        timeout = httpx.Timeout(connect=20.0, read=60.0, write=30.0, pool=30.0)
        with httpx.Client(timeout=timeout, headers=_BROWSER_HEADERS,
                          follow_redirects=True) as client:
            results_html = _run_search(client, window_days=window_days)
            if results_html is None:
                result.error = "Search flow failed (see logs)."
                return result

            notice_rows = _parse_notice_rows(results_html)
            result.notices_on_results = len(notice_rows)
            if not notice_rows:
                result.ok = True
                result.notes.append(
                    "Results page fetched but no notice rows parsed — the "
                    "results markup may have changed."
                )
                return result

            new_count = 0
            for row in notice_rows:
                if new_count >= max_new:
                    result.notes.append(
                        f"Stopped at per-run cap ({max_new}). Re-run to continue."
                    )
                    break

                source_url = _canonical_source_url(row.notice_id)
                if _already_staged(source_url):
                    result.already_staged += 1
                    continue
                result.new_ids += 1

                # Teaser must have enough text to be worth an LLM call.
                if not row.teaser or len(row.teaser) < _MIN_TEASER_CHARS:
                    result.extraction_failed += 1
                    logger.info(
                        "mnpublicnotice: teaser too short to extract",
                        notice_id=row.notice_id,
                        teaser_len=len(row.teaser or ""),
                    )
                    continue

                notice_text = _build_notice_text(row)
                stored_id = _store_extraction(notice_text, source_url)
                if stored_id is not None:
                    result.newly_extracted += 1
                    result.stored_ids.append(stored_id)
                    new_count += 1
                else:
                    result.extraction_failed += 1

        result.ok = True
        logger.info(
            "mnpublicnotice scrape complete",
            notices_on_results=result.notices_on_results,
            new_ids=result.new_ids,
            already_staged=result.already_staged,
            newly_extracted=result.newly_extracted,
            extraction_failed=result.extraction_failed,
        )
        return result

    except Exception as e:
        result.error = f"{type(e).__name__}: {e}"
        logger.exception("mnpublicnotice scrape failed", error_type=type(e).__name__)
        return result


__all__ = ["run_mnpublicnotice_scrape", "ScrapeResult", "NoticeRow"]
