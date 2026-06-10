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
  4. GET the redirect URL  -> the RESULTS page (~394KB) listing 10 notices,
     each with a detail link:  Details.aspx?SID=<session>&ID=<noticeId>
     and the notice text rendered inline on the results page.
  5. GET each Details.aspx?SID=..&ID=..  -> the full notice text to extract.

Dedup key: the notice ID (stable across sessions; the SID is per-session and
must NOT be part of the dedup key or source_url). source_url is normalized to
a SID-less canonical form so re-runs in new sessions still dedup correctly.

=== POLITENESS ===
Recent-window search (default last 14 days) keeps results to ~1-3 pages, so we
avoid crawling 100 pages of history. Real browser headers, small delays between
detail fetches, a per-run cap on new extractions (LLM cost).
"""

from __future__ import annotations

import re
import time
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
_DETAIL_DELAY_SECONDS = 1.0    # politeness between detail fetches
_MAX_RESULT_PAGES = 5          # safety cap on pagination within the window


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
# Notice ID + detail-link parsing
# ============================================================

# Detail links look like: Details.aspx?SID=<session>&ID=<noticeId>
# (the captured HTML had a trailing JS artifact like  ';return  — we ignore it).
_DETAIL_ID_RE = re.compile(r'Details\.aspx\?SID=([A-Za-z0-9]+)&(?:amp;)?ID=(\d+)')


def _extract_notice_ids(results_html: str) -> list[str]:
    """Return the distinct notice IDs on a results page, order preserved."""
    seen: set[str] = set()
    ordered: list[str] = []
    for m in _DETAIL_ID_RE.finditer(results_html):
        notice_id = m.group(2)
        if notice_id not in seen:
            seen.add(notice_id)
            ordered.append(notice_id)
    return ordered


def _canonical_source_url(notice_id: str) -> str:
    """SID-less canonical URL used as the dedup key / source_url. The SID is
    per-session, so we must NOT include it — only the stable notice ID."""
    return f"{_BASE}/Details.aspx?ID={notice_id}"


# ============================================================
# Notice text extraction from a Details page
# ============================================================

_NOTICE_START_MARKERS = (
    "THE RIGHT TO VERIFICATION OF THE DEBT",
    "NOTICE IS HEREBY GIVEN",
    "NOTICE OF MORTGAGE FORECLOSURE",
    "Minn. Stat.",
    "YOU ARE NOTIFIED",
)


def _strip_tags(html: str) -> str:
    """Crude tag strip for isolating notice body text from a detail page."""
    # Drop scripts/styles entirely first.
    html = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", " ", html,
                  flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", html)
    text = unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


def _slice_notice_text(detail_html: str) -> Optional[str]:
    """Isolate the statutory notice body from a Details page. Returns None if
    no recognizable notice opener is present (so we never feed junk to the LLM)."""
    if not detail_html:
        return None
    text = _strip_tags(detail_html)
    earliest = None
    for marker in _NOTICE_START_MARKERS:
        idx = text.find(marker)
        if idx != -1 and (earliest is None or idx < earliest):
            earliest = idx
    if earliest is None:
        return None
    return text[earliest:][:20000].strip()


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
# Results-page row parsing (text is INLINE in the results grid)
# ============================================================
# The Details.aspx page is session-state-locked (a cold GET returns only page
# chrome), so we extract the notice text DIRECTLY from the results-page grid.
# Each notice is a GridView row pair:
#   row A: a <td> with the View button + a hidden field
#          ...$hdnPKValue with value="<noticeId>"
#   row B: a <td colspan="3"> containing the notice text, which begins with a
#          marker like "NOTICE OF MORTGAGE FORECLOSURE" / "NOTICE IS HEREBY
#          GIVEN" and ends with "click 'view' to open the full text."
# The inline text is a TEASER (~2KB) — the statute, dates, and the start of the
# notice — not the full legal text. We extract what's present and let the LLM
# pull whatever fields it can; low-confidence rows surface for human review.

# hdnPKValue carries the notice ID for each grid row.
_PK_RE = re.compile(
    r'\$hdnPKValue"[^>]*value="(\d+)"', re.IGNORECASE
)
# The text cell: <td colspan="3" ...>NOTICE ...</td>
_TEXT_CELL_RE = re.compile(
    r'<td\b[^>]*colspan="3"[^>]*>(.*?)</td>',
    re.IGNORECASE | re.DOTALL,
)
# The publication name + date sit in the info cell:
#   <div class="left"><strong>Waseca County News</strong><br/> Wednesday, June 10, 2026</div>
_PUB_RE = re.compile(
    r'<div class="left">\s*<strong>(.*?)</strong>\s*<br\s*/?>\s*(.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)


def _clean_cell_text(html_fragment: str) -> str:
    """Strip the teaser cell down to plain text, dropping the trailing
    'click view to open the full text' boilerplate and the <span> wrappers."""
    txt = re.sub(r"<[^>]+>", " ", html_fragment)
    txt = unescape(txt)
    # Drop the trailing call-to-action.
    txt = re.split(r"click\s*'?view'?\s*to open the full text", txt, flags=re.IGNORECASE)[0]
    txt = re.sub(r"[ \t\r\f\v]+", " ", txt)
    txt = re.sub(r"\n\s*\n\s*\n+", "\n\n", txt)
    return txt.strip()


def _parse_results_rows(results_html: str) -> list[dict[str, str]]:
    """Return a list of {id, text, publication, pub_date} from the results grid.
    Pairs each hdnPKValue notice ID with the notice-text cell that follows it.
    Robust to ordering by walking the page: for each PK match, search forward
    for the next colspan=3 text cell."""
    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    # Find publication blocks + PK values + text cells with their positions,
    # then associate each PK with the nearest following text cell.
    pk_iter = list(_PK_RE.finditer(results_html))
    text_iter = list(_TEXT_CELL_RE.finditer(results_html))
    pub_iter = list(_PUB_RE.finditer(results_html))

    for pk_m in pk_iter:
        notice_id = pk_m.group(1)
        if notice_id in seen:
            continue
        pk_pos = pk_m.start()

        # nearest text cell AFTER this PK
        text_html = None
        for tm in text_iter:
            if tm.start() > pk_pos:
                text_html = tm.group(1)
                break
        if not text_html:
            continue
        text = _clean_cell_text(text_html)
        if len(text) < 60:  # too short to be a real notice teaser
            continue

        # nearest publication block BEFORE the text cell (the info cell sits
        # between the button cell and the text row)
        publication = ""
        pub_date = ""
        for pm in pub_iter:
            if pm.start() > pk_pos:
                publication = re.sub(r"<[^>]+>", " ", pm.group(1)).strip()
                pub_date = re.sub(r"<[^>]+>", " ", pm.group(2)).strip()
                break

        seen.add(notice_id)
        rows.append({
            "id": notice_id,
            "text": text,
            "publication": publication,
            "pub_date": pub_date,
        })

    return rows


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


def _fetch_detail_text(client: httpx.Client, results_url_session: str,
                       notice_id: str) -> Optional[str]:
    """GET one notice's Details page (within the current session) and slice its
    text. We reuse the session by reading the SID off the client's last URL."""
    # The session id lives in the path /(S(<sid>))/. Build the detail URL with
    # the same session segment so it resolves to our active search session.
    sid_m = re.search(r"/\(S\(([^)]+)\)\)/", results_url_session)
    if sid_m:
        sid = sid_m.group(1)
        url = f"{_BASE}/(S({sid}))/Details.aspx?SID={sid}&ID={notice_id}"
    else:
        url = f"{_BASE}/Details.aspx?ID={notice_id}"
    try:
        r = client.get(url, headers=_BROWSER_HEADERS)
        if r.status_code != 200:
            return None
        return _slice_notice_text(r.text or "")
    except httpx.HTTPError:
        return None


# ============================================================
# Public entry point
# ============================================================


def run_mnpublicnotice_scrape(
    max_new: int = _DEFAULT_MAX_NEW,
    window_days: int = _DEFAULT_WINDOW_DAYS,
) -> ScrapeResult:
    """Search mnpublicnotice for recent foreclosure notices, extract each NEW
    one (dedup by notice ID), store as pending for review. Returns a summary."""
    result = ScrapeResult(ok=False)

    try:
        timeout = httpx.Timeout(connect=20.0, read=60.0, write=30.0, pool=30.0)
        with httpx.Client(timeout=timeout, headers=_BROWSER_HEADERS,
                          follow_redirects=True) as client:
            results_html = _run_search(client, window_days=window_days)
            if results_html is None:
                result.error = "Search flow failed (see logs)."
                return result

            notices = _parse_results_rows(results_html)
            result.notices_on_results = len(notices)
            if not notices:
                result.ok = True
                result.notes.append(
                    "Results page fetched but no notice rows parsed — the "
                    "results markup may have changed."
                )
                return result

            new_count = 0
            for notice in notices:
                if new_count >= max_new:
                    result.notes.append(
                        f"Stopped at per-run cap ({max_new}). Re-run to continue."
                    )
                    break

                notice_id = notice["id"]
                source_url = _canonical_source_url(notice_id)
                if _already_staged(source_url):
                    result.already_staged += 1
                    continue
                result.new_ids += 1

                # The notice text is the inline results-page teaser (Details.aspx
                # is session-locked). Prepend the publication line so the LLM has
                # the paper + date context.
                header = ""
                if notice.get("publication"):
                    header = f"Publication: {notice['publication']}"
                    if notice.get("pub_date"):
                        header += f" — {notice['pub_date']}"
                    header += "\n\n"
                notice_text = header + notice["text"]

                if len(notice_text) < 80:
                    result.extraction_failed += 1
                    logger.info("mnpublicnotice: teaser too short", notice_id=notice_id)
                    continue

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


__all__ = ["run_mnpublicnotice_scrape", "ScrapeResult"]
