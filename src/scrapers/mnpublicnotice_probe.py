"""
Diagnostic probe for mnpublicnotice.com (the MN Newspaper Association's
statewide public-notice clearinghouse) — Step-4 source verification.

This is NOT a scraper. It's a one-shot diagnostic that runs FROM the Railway
server and reports exactly what our server receives when it tries to use the
site. We build this BEFORE any scraper because the failure that broke the
Star Tribune attempt was environment-specific (the site served our server a
different page than a browser gets). The only fetch environment that matters
is our own server — so we test there, first, and read the real result before
committing to a scraper build.

mnpublicnotice.com is ASP.NET WebForms, which means a search is stateful:
  1. GET the search page  -> receive an ASP.NET session + hidden form tokens
     (__VIEWSTATE, __EVENTVALIDATION, __VIEWSTATEGENERATOR).
  2. POST those tokens back along with the search fields -> receive results.
This probe walks both steps and reports what it finds at each, so we learn:
  - Can our server even reach the site? (status, length)
  - Does it get the WebForms tokens it would need to search? (viewstate etc.)
  - Does a search POST actually return notice results? (result markers found)

It writes NOTHING to the database. It only returns a diagnostic dict.
"""

from __future__ import annotations

import re
from typing import Any, Optional

import httpx

from src.utils.logger import logger


_BASE = "https://www.mnpublicnotice.com"
_SEARCH_PAGE = f"{_BASE}/Search.aspx"

# Browser-like headers (same lesson as Star Tribune: look like a browser).
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Hidden ASP.NET WebForms fields we need to capture from the first GET and
# replay on the search POST.
_HIDDEN_FIELD_RE = {
    "__VIEWSTATE": re.compile(
        r'id="__VIEWSTATE"\s+value="([^"]*)"'
    ),
    "__VIEWSTATEGENERATOR": re.compile(
        r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"'
    ),
    "__EVENTVALIDATION": re.compile(
        r'id="__EVENTVALIDATION"\s+value="([^"]*)"'
    ),
}

# Markers that, if present in a results page, indicate real notice rows came
# back (rather than just the empty search form again).
_RESULT_MARKERS = (
    "NOTICE OF MORTGAGE FORECLOSURE",
    "Public Notice Detail",
    "searchResults",
    "ViewNotice",
    "notice-detail",
)


def _extract_hidden_fields(html: str) -> dict[str, Optional[str]]:
    """Pull the ASP.NET hidden form tokens out of a page's HTML."""
    found: dict[str, Optional[str]] = {}
    for name, rx in _HIDDEN_FIELD_RE.items():
        m = rx.search(html)
        found[name] = m.group(1) if m else None
    return found



def _inventory_form_fields(html: str) -> dict[str, Any]:
    """Dump every form input/select/textarea name from the page so we can see
    the REAL field names (instead of guessing). Returns:
      - text_like:   names of text/search/hidden-ish inputs (+ their values
                     when short, so we can spot defaults)
      - selects:     select names + their option values
      - buttons:     submit/button input names + values (the search trigger)
      - all_names:   every name= seen (deduped)
    We only need names whose control id/name suggests the SEARCH form (we keep
    everything but flag the likely-relevant ones containing 'search', 'as1',
    'keyword', 'btn').
    """
    # name="..." type="..." value="..." in any order — capture name + type + value.
    input_rx = re.compile(
        r'<input\b[^>]*?>', re.IGNORECASE
    )
    select_rx = re.compile(
        r'<select\b[^>]*?name="([^"]+)"[^>]*?>(.*?)</select>',
        re.IGNORECASE | re.DOTALL,
    )
    name_rx = re.compile(r'name="([^"]+)"')
    type_rx = re.compile(r'type="([^"]+)"')
    value_rx = re.compile(r'value="([^"]*)"')
    option_rx = re.compile(r'<option[^>]*?value="([^"]*)"', re.IGNORECASE)

    text_like: list[dict[str, str]] = []
    buttons: list[dict[str, str]] = []
    all_names: list[str] = []

    for tag in input_rx.findall(html):
        nm = name_rx.search(tag)
        if not nm:
            continue
        name = nm.group(1)
        all_names.append(name)
        typ = (type_rx.search(tag).group(1) if type_rx.search(tag) else "text").lower()
        val = value_rx.search(tag).group(1) if value_rx.search(tag) else ""
        # Skip the giant viewstate values in the dump.
        if name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
            continue
        entry = {"name": name, "type": typ, "value": val[:40]}
        if typ in ("submit", "button", "image"):
            buttons.append(entry)
        else:
            text_like.append(entry)

    selects: list[dict[str, Any]] = []
    for s_name, s_body in select_rx.findall(html):
        all_names.append(s_name)
        opts = option_rx.findall(s_body)[:8]  # first few option values
        selects.append({"name": s_name, "option_values_sample": opts})

    # Flag the names most likely tied to the keyword search form.
    def _relevant(n: str) -> bool:
        low = n.lower()
        return any(k in low for k in ("search", "as1", "keyword", "btn", "txt", "ddl"))

    relevant = sorted({n for n in all_names if _relevant(n)})

    return {
        "text_inputs": text_like[:40],
        "selects": selects[:20],
        "buttons": buttons[:20],
        "likely_search_fields": relevant,
        "total_named_fields": len(set(all_names)),
    }


def probe_mnpublicnotice() -> dict[str, Any]:
    """Run the two-step WebForms probe and return a diagnostic dict.

    Step 1: GET the search page (with a cookie jar so the ASP.NET session
            cookie is retained).
    Step 2: If we got viewstate tokens, attempt a search POST for the keyword
            'foreclosure' and report whether results came back.

    Everything is wrapped so a failure at any step is reported, not raised.
    """
    diag: dict[str, Any] = {
        "step1_get": {},
        "step2_post": {},
        "verdict": "",
    }

    # ---- Step 1: GET the search page ----
    try:
        # follow_redirects so we land on the session-stamped URL; the client's
        # cookie jar keeps the ASP.NET_SessionId across the subsequent POST.
        with httpx.Client(
            timeout=30,
            headers=_HEADERS,
            follow_redirects=True,
        ) as client:
            r1 = client.get(_SEARCH_PAGE)
            html1 = r1.text or ""
            hidden = _extract_hidden_fields(html1)

            form_fields = _inventory_form_fields(html1)
            diag["step1_get"] = {
                "status": r1.status_code,
                "final_url": str(r1.url),
                "content_length": len(html1),
                "has_viewstate": bool(hidden.get("__VIEWSTATE")),
                "has_eventvalidation": bool(hidden.get("__EVENTVALIDATION")),
                "has_viewstategenerator": bool(hidden.get("__VIEWSTATEGENERATOR")),
                "cookies_set": list(client.cookies.keys()),
                "mentions_foreclosure": "oreclosure" in html1,
                # First 300 chars so we can eyeball a block/challenge page.
                "head_snippet": html1[:300],
                "form_fields": form_fields,
            }

            if r1.status_code != 200:
                diag["verdict"] = (
                    f"Step 1 returned {r1.status_code} — server cannot even "
                    f"load the search page. Likely IP/datacenter block."
                )
                return diag

            if not hidden.get("__VIEWSTATE"):
                diag["verdict"] = (
                    "Step 1 loaded (200) but NO __VIEWSTATE token found. Either "
                    "the page is JS-rendered, or the markup differs from "
                    "expectations — a WebForms POST search won't work as-is."
                )
                return diag

            # ---- Step 2: attempt a search POST for 'foreclosure' ----
            # Minimal WebForms postback. Field names are best-effort guesses
            # based on the ASP.NET control naming we saw (ctl00$ContentPlace
            # Holder1$as1$...). We send the tokens + a keyword and report what
            # comes back. This is a PROBE — if the field names are wrong, the
            # site returns the form again (no result markers), which is itself
            # useful information (tells us the exact field names to use).
            # Real field names discovered from the form inventory:
            #   keyword box  = ...as1$txtSearch
            #   match type   = ...as1$rdoType  (AND | OR | EXACT)
            #   date window  = ...as1$txtDateFrom / txtDateTo (MM/DD/YYYY)
            #   search button= ...as1$btnGo
            # ASP.NET WebForms also expects the hidden __EVENTTARGET/ARGUMENT
            # fields present (empty) plus the ToolkitScriptManager hidden field.
            P = "ctl00$ContentPlaceHolder1$as1$"
            from datetime import date, timedelta
            today = date.today()
            d_from = (today - timedelta(days=14)).strftime("%m/%d/%Y")
            d_to = today.strftime("%m/%d/%Y")
            post_data = {
                "ctl00_ToolkitScriptManager1_HiddenField": "",
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": hidden.get("__VIEWSTATE") or "",
                "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR") or "",
                P + "txtSearch": "foreclosure",
                P + "rdoType": "AND",
                P + "txtExclude": "",
                P + "txtDateFrom": d_from,
                P + "txtDateTo": d_to,
                P + "hdnLastScrollPos": "0",
                P + "hdnCountyScrollPosition": "-1",
                P + "hdnCityScrollPosition": "-1",
                P + "hdnPubScrollPosition": "-1",
                P + "hdnField": "",
                P + "btnGo": "GO",
            }

            post_headers = dict(_HEADERS)
            post_headers["Content-Type"] = "application/x-www-form-urlencoded"
            post_headers["Referer"] = str(r1.url)

            r2 = client.post(
                str(r1.url),  # POST back to the session-stamped search URL
                data=post_data,
                headers=post_headers,
            )
            html2 = r2.text or ""
            markers_hit = [m for m in _RESULT_MARKERS if m in html2]

            import re as _re
            # Detail-link pattern on this site (best-effort): notice detail
            # pages live under .../(S(session))/Details.aspx?SID=... or similar.
            detail_links = _re.findall(
                r'[A-Za-z0-9_./()-]*[Dd]etail[A-Za-z0-9_./?=&-]*', html2
            )
            # A "result count" label often appears, e.g. "1-25 of 312".
            count_hint = _re.search(r'\b\d+\s*-\s*\d+\s+of\s+\d+\b', html2)
            diag["step2_post"] = {
                "status": r2.status_code,
                "final_url": str(r2.url),
                "content_length": len(html2),
                "result_markers_found": markers_hit,
                "foreclosure_count_in_page": html2.count("FORECLOSURE")
                    + html2.count("Foreclosure"),
                "result_count_label": count_hint.group(0) if count_hint else None,
                "detail_link_samples": sorted(set(detail_links))[:8],
                "looks_like_results": bool(markers_hit) or bool(count_hint),
                "head_snippet": html2[:300],
            }

            if markers_hit:
                diag["verdict"] = (
                    "SUCCESS: server reached the site, got WebForms tokens, "
                    "and the search POST returned content with result markers. "
                    "A real scraper is viable from this server."
                )
            else:
                diag["verdict"] = (
                    "PARTIAL: server reached the site and got viewstate tokens, "
                    "but the search POST returned no result markers — likely the "
                    "POST field names are wrong (need the exact control names) "
                    "OR results load via a follow-up request. The fetch itself "
                    "works, which is the key finding; field names are a "
                    "next-step detail."
                )

    except httpx.HTTPError as e:
        diag["step1_get"]["error"] = f"{type(e).__name__}: {e}"
        diag["verdict"] = (
            f"Network error reaching mnpublicnotice.com from this server: "
            f"{type(e).__name__}. Could be a datacenter-IP block or timeout."
        )
        logger.exception("mnpublicnotice probe failed", error_type=type(e).__name__)

    return diag


__all__ = ["probe_mnpublicnotice"]
