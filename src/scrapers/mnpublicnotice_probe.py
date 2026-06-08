"""
Diagnostic probe for mnpublicnotice.com — full-form async search.

NOT a scraper. Runs FROM the Railway server and reports what comes back.

KEY INSIGHT (from DevTools capture of the working browser request):
  The search is a POST to the session-stamped /Search.aspx with:
    - header x-microsoftajax: Delta=true  (async partial postback)
    - a ~88KB body = the COMPLETE form: full __VIEWSTATE + EVERY field
      (all hidden inputs, every lstCounty/lstCity/lstPublication control,
      the ScriptManager trigger), not a hand-picked subset.
  Our earlier probes failed because we sent a minimal subset of fields.
  The fix: harvest EVERY form field from the GET page and POST it back
  complete, changing only the search inputs + the ScriptManager trigger.
  The response is ~53KB text/plain delta containing the result rows.

This probe reconstructs the full form automatically (so nothing needs to be
hand-copied, and this is exactly what the real scraper must do each run since
__VIEWSTATE is per-session), POSTs it, and reports whether real notices come
back. Writes NOTHING to the DB.
"""

from __future__ import annotations

import re
from html import unescape
from typing import Any, Optional

import httpx

from src.utils.logger import logger


_BASE = "https://www.mnpublicnotice.com"
_SEARCH_PAGE = f"{_BASE}/Search.aspx"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# Field-name fragments (controls in the as1 advanced-search panel).
_P = "ctl00$ContentPlaceHolder1$as1$"
_SM_FIELD = "ctl00$ToolkitScriptManager1"
_SEARCH_PANEL = "ctl00$ContentPlaceHolder1$as1$upSearch"
_BTN_GO = _P + "btnGo"

_RESULT_MARKERS = (
    "NOTICE OF MORTGAGE FORECLOSURE",
    "Details.aspx",
    "VIEW",
    "of 100 Pages",
    "of ",  # "Page 1 of N Pages"
)


def _harvest_form_fields(html: str) -> dict[str, str]:
    """Extract EVERY form field (name -> value) from the page so we can POST
    the complete form back, mirroring what the browser sends.

    Covers:
      - <input type=text/hidden/...>  -> name, value (value defaults to "")
      - <input type=checkbox/radio>   -> ONLY if 'checked' (WebForms only
        posts checked boxes); unchecked are omitted, like a real browser.
      - <select>                       -> the selected <option> value (or the
        first option if none marked selected).
      - <textarea>                     -> inner text.
    Values are HTML-unescaped. The giant __VIEWSTATE is captured intact.
    """
    fields: dict[str, str] = {}

    # --- inputs ---
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
            # Only posted if checked.
            if re.search(r"\bchecked\b", tag, re.IGNORECASE):
                # Radios share a name; checked one wins.
                fields[name] = val or "on"
            # else omit
        elif typ in ("submit", "button", "image", "reset"):
            # Don't auto-include buttons; we add the one trigger explicitly.
            continue
        else:
            fields[name] = val

    # --- selects ---
    for sel in re.findall(r"<select\b[^>]*>.*?</select>", html, re.IGNORECASE | re.DOTALL):
        nm = re.search(r'name="([^"]+)"', sel)
        if not nm:
            continue
        name = nm.group(1)
        # Find the selected <option>, tolerant of attribute order (selected
        # may appear before OR after value=). Fall back to the first option.
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

    # --- textareas ---
    for ta in re.findall(r'<textarea\b[^>]*name="([^"]+)"[^>]*>(.*?)</textarea>',
                         html, re.IGNORECASE | re.DOTALL):
        fields[ta[0]] = unescape(ta[1]).strip()

    return fields


def _count_result_rows(delta: str) -> dict[str, Any]:
    """Inspect a results page for result evidence + the patterns the scraper
    needs: full Details.aspx links and the pagination control."""
    from html import unescape as _un
    page_of = re.search(r'Page\s+(\d+)\s+of\s+(\d+)\s+Pages', delta)
    # Full detail links: capture the WHOLE href (SID + record id), unescaped.
    raw_details = re.findall(r'Details\.aspx\?[^"\'<>\s]+', delta)
    details = [_un(d) for d in raw_details]
    # Notice "VIEW" buttons / links — try several render styles.
    view_buttons = (
        len(re.findall(r'>\s*VIEW\s*<', delta, re.IGNORECASE))
        + len(re.findall(r'value="VIEW"', delta, re.IGNORECASE))
        + len(re.findall(r'class="[^"]*view[^"]*"', delta, re.IGNORECASE))
    )
    foreclosure_hits = delta.count("FORECLOSURE") + delta.count("Foreclosure")
    # Pagination control: the "next page" link/postback target.
    next_link = re.search(
        r'(href|onclick)="([^"]*(?:Page|page|pg|activePage)[^"]*)"', delta
    )
    # __doPostBack targets referencing the results grid (paging triggers).
    grid_posts = re.findall(
        r"__doPostBack\(['\"]([^'\"]*(?:Grid|Pager|Page)[^'\"]*)['\"]",
        delta,
    )
    return {
        "current_page": int(page_of.group(1)) if page_of else None,
        "total_pages": int(page_of.group(2)) if page_of else None,
        "total_pages_label": page_of.group(0) if page_of else None,
        "details_links_found": len(set(details)),
        "details_samples": sorted(set(details))[:10],
        "foreclosure_hits": foreclosure_hits,
        "view_buttons": view_buttons,
        "next_link_sample": next_link.group(2) if next_link else None,
        "grid_postback_targets": sorted(set(grid_posts))[:10],
    }


def probe_mnpublicnotice() -> dict[str, Any]:
    """GET the search page, harvest the FULL form, POST it complete with the
    search criteria + async trigger, and report whether results come back."""
    diag: dict[str, Any] = {"step1_get": {}, "step2_search": {}, "verdict": ""}

    try:
        with httpx.Client(timeout=45, headers=_HEADERS, follow_redirects=True) as client:
            # --- 1. GET the search page (fresh session + full form) ---
            r1 = client.get(_SEARCH_PAGE)
            html1 = r1.text or ""
            base_url = str(r1.url)  # session-stamped /(S(...))/Search.aspx

            fields = _harvest_form_fields(html1)
            diag["step1_get"] = {
                "status": r1.status_code,
                "final_url": base_url,
                "content_length": len(html1),
                "fields_harvested": len(fields),
                "has_viewstate": "__VIEWSTATE" in fields,
                "viewstate_len": len(fields.get("__VIEWSTATE", "")),
                "has_scriptmanager_field": _SM_FIELD in fields,
                "field_names": sorted(fields.keys()),
            }
            if r1.status_code != 200 or "__VIEWSTATE" not in fields:
                diag["verdict"] = "Could not load the search page / no viewstate."
                return diag

            # --- 2. Build the COMPLETE form body, set search criteria ---
            from datetime import date, timedelta
            today = date.today()
            d_from = (today - timedelta(days=30)).strftime("%m/%d/%Y")
            d_to = today.strftime("%m/%d/%Y")

            body = dict(fields)  # everything harvested from the page
            # The async ScriptManager trigger: "panel|button"
            body[_SM_FIELD] = _SEARCH_PANEL + "|" + _BTN_GO
            body["__ASYNCPOST"] = "true"
            body["__EVENTTARGET"] = ""
            body["__EVENTARGUMENT"] = ""
            # Search criteria
            body[_P + "txtSearch"] = "foreclosure"
            body[_P + "rdoType"] = "AND"
            body[_P + "txtDateFrom"] = d_from
            body[_P + "txtDateTo"] = d_to
            # The trigger button value (browser includes the clicked button).
            body[_BTN_GO] = "GO"

            post_headers = dict(_HEADERS)
            post_headers["Content-Type"] = (
                "application/x-www-form-urlencoded; charset=UTF-8"
            )
            post_headers["X-MicrosoftAjax"] = "Delta=true"
            post_headers["X-Requested-With"] = "XMLHttpRequest"
            post_headers["Cache-Control"] = "no-cache"
            post_headers["Origin"] = _BASE
            post_headers["Referer"] = base_url

            r2 = client.post(base_url, data=body, headers=post_headers)
            html2 = r2.text or ""

            # Approximate the body size we sent (sanity vs the browser's ~88KB).
            approx_body_len = sum(len(k) + len(str(v)) + 2 for k, v in body.items())

            # ASP.NET AJAX may answer the search with a pageRedirect directive
            # ("...|pageRedirect||<url>|..."), meaning the results render on a
            # SUBSEQUENT GET (criteria were stored in session). Detect + follow.
            redirect_url = None
            rd = re.search(r"pageRedirect\|\|([^|]+)\|", html2)
            if rd:
                from urllib.parse import unquote
                redirect_url = unquote(rd.group(1))
                if redirect_url.startswith("/"):
                    redirect_url = _BASE + redirect_url

            results_html = html2
            followed = False
            if redirect_url:
                try:
                    rget = client.get(redirect_url, headers=_HEADERS)
                    if rget.status_code == 200 and rget.text:
                        results_html = rget.text
                        followed = True
                except httpx.HTTPError:
                    pass

            results = _count_result_rows(results_html)
            diag["step2_search"] = {
                "followed_page_redirect": followed,
                "redirect_url": redirect_url,
                "results_html_length": len(results_html),
                "status": r2.status_code,
                "request_field_count": len(body),
                "approx_request_body_len": approx_body_len,
                "response_length": len(html2),
                "is_delta": html2[:20].count("|") >= 2 and html2.lstrip()[:1].isdigit(),
                **results,
                "head_snippet": results_html[:300],
            }

            if (results["details_links_found"] or 0) > 0 or results["total_pages"]:
                diag["verdict"] = (
                    "SUCCESS: full-form search returned results "
                    f"({results.get('total_pages_label') or str(results['details_links_found']) + ' detail links'}). "
                    "Scraper is viable — parse rows from this response."
                )
            else:
                diag["verdict"] = (
                    "Full-form POST sent (field_count="
                    f"{len(body)}, ~{approx_body_len} bytes) but no result rows "
                    "detected. Check head_snippet — may need the exact ScriptManager "
                    "trigger value or a missing field."
                )

    except httpx.HTTPError as e:
        diag["verdict"] = f"Network error: {type(e).__name__}: {e}"
        logger.exception("mnpublicnotice full-form probe failed", error_type=type(e).__name__)

    return diag


__all__ = ["probe_mnpublicnotice"]
