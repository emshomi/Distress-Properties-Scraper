"""
Diagnostic probe for mnpublicnotice.com (the MN Newspaper Association's
statewide public-notice clearinghouse) — Step-4 source verification.

NOT a scraper. A one-shot diagnostic run FROM the Railway server that reports
exactly what our server receives when it tries to use the site. We build this
BEFORE any scraper because the failure that broke the Star Tribune attempt was
environment-specific. The only fetch environment that matters is our server —
so we test there, first, and read the real result before building.

mnpublicnotice.com is ASP.NET WebForms with an AJAX UpdatePanel: the search
runs as a partial async postback that updates a #searchResults panel in place
(confirmed: results live at /Search.aspx#searchResults; plain full POSTs return
the unchanged landing page). To replay the search server-side we need the real
ScriptManager field name + UpdatePanel id, which this probe dumps.

Writes NOTHING to the database — returns a diagnostic dict only.
"""

from __future__ import annotations

import re
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
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

_HIDDEN_FIELD_RE = {
    "__VIEWSTATE": re.compile(r'id="__VIEWSTATE"\s+value="([^"]*)"'),
    "__VIEWSTATEGENERATOR": re.compile(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"'),
    "__EVENTVALIDATION": re.compile(r'id="__EVENTVALIDATION"\s+value="([^"]*)"'),
}

_RESULT_MARKERS = (
    "NOTICE OF MORTGAGE FORECLOSURE",
    "Public Notice Detail",
    "searchResults",
    "ViewNotice",
    "notice-detail",
)


def _extract_hidden_fields(html: str) -> dict[str, Optional[str]]:
    found: dict[str, Optional[str]] = {}
    for name, rx in _HIDDEN_FIELD_RE.items():
        m = rx.search(html)
        found[name] = m.group(1) if m else None
    return found


def _inventory_form_fields(html: str) -> dict[str, Any]:
    """Dump every form input/select name so we see the REAL field names."""
    input_rx = re.compile(r'<input\b[^>]*?>', re.IGNORECASE)
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
        opts = option_rx.findall(s_body)[:8]
        selects.append({"name": s_name, "option_values_sample": opts})

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


def _recon_ajax(page: str) -> dict[str, Any]:
    """Dump the real ScriptManager + UpdatePanel identifiers and postback
    wiring — exactly what an async (partial) postback needs."""
    out: dict[str, Any] = {}

    sm_ids = re.findall(r'(?:id|name)="([^"]*[Ss]cript[Mm]anager[^"]*)"', page)
    out["scriptmanager_candidates"] = sorted(set(sm_ids))[:10]

    up_ids = re.findall(r'(?:id|name)="([^"]*[Uu]p(?:date)?[Pp]anel[^"]*)"', page)
    out["updatepanel_candidates"] = sorted(set(up_ids))[:10]

    # __doPostBack targets (first arg), tolerant of single/double quotes.
    q = chr(39)
    dq = chr(34)
    dopost_re = re.compile(r"__doPostBack\(\s*[" + q + dq + r"]([^" + q + dq + r"]+)")
    out["dopostback_targets"] = sorted(set(dopost_re.findall(page)))[:15]

    bidx = page.find("btnGo")
    out["btngo_context"] = page[max(0, bidx - 120):bidx + 200] if bidx != -1 else None

    up_tokens = re.findall(r'[A-Za-z0-9_]*[Uu]pdate[Pp]anel[A-Za-z0-9_]*', page)
    out["updatepanel_tokens"] = sorted(set(up_tokens))[:10]

    # PageRequestManager registration often lists panel ids; capture the script
    # block that registers the async framework.
    prm = re.search(r'Sys\.WebForms\.PageRequestManager[^<]{0,400}', page)
    out["prm_snippet"] = prm.group(0)[:400] if prm else None

    return out


def probe_mnpublicnotice() -> dict[str, Any]:
    """Two-step WebForms probe + AJAX recon. Reports what our server receives."""
    diag: dict[str, Any] = {"step1_get": {}, "step2_post": {}, "step3_recon": {}, "verdict": ""}

    # ---- Step 1: GET the search page ----
    try:
        with httpx.Client(timeout=30, headers=_HEADERS, follow_redirects=True) as client:
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
                "mentions_foreclosure": "oreclosure" in html1,
                "head_snippet": html1[:200],
                "form_fields": form_fields,
            }

            if r1.status_code != 200:
                diag["verdict"] = f"Step 1 returned {r1.status_code} — cannot load search page."
                return diag
            if not hidden.get("__VIEWSTATE"):
                diag["verdict"] = "Step 1 loaded but NO __VIEWSTATE — page may be JS-rendered."
                return diag

            # ---- Step 2: attempt the search postback (real field names) ----
            P = "ctl00$ContentPlaceHolder1$as1$"
            from datetime import date, timedelta
            today = date.today()
            d_from = (today - timedelta(days=14)).strftime("%m/%d/%Y")
            d_to = today.strftime("%m/%d/%Y")
            post_data = {
                "ctl00_ToolkitScriptManager1_HiddenField": "",
                "__EVENTTARGET": P + "btnGo",
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
            }
            if hidden.get("__EVENTVALIDATION"):
                post_data["__EVENTVALIDATION"] = hidden["__EVENTVALIDATION"]

            post_headers = dict(_HEADERS)
            post_headers["Content-Type"] = "application/x-www-form-urlencoded"
            post_headers["Referer"] = str(r1.url)

            # ASP.NET AJAX async postback. The PageRequestManager registration
            # revealed the real UpdatePanel: ctl00$ContentPlaceHolder1$as1$upSearch.
            # An async postback sets the ScriptManager field to
            # "<UpdatePanelID>|<triggerButton>" and sends the X-MicrosoftAjax
            # header — this is what makes the search actually execute (a plain
            # full POST just re-renders the static landing page).
            SM_FIELD = "ctl00$ToolkitScriptManager1"
            UP_SEARCH = "ctl00$ContentPlaceHolder1$as1$upSearch"
            BTN_GO = P + "btnGo"

            # For async, the ScriptManager field carries "panel|trigger" and
            # __EVENTTARGET is cleared (the trigger is in the SM field instead).
            post_data[SM_FIELD] = UP_SEARCH + "|" + BTN_GO
            post_data["__EVENTTARGET"] = ""
            post_data["__ASYNCPOST"] = "true"
            # btnGo must still be present as the clicked control for some forms.
            post_data[BTN_GO] = "GO"

            post_headers["X-MicrosoftAjax"] = "Delta=true"
            post_headers["X-Requested-With"] = "XMLHttpRequest"
            post_headers["Cache-Control"] = "no-cache"

            r2 = client.post(str(r1.url), data=post_data, headers=post_headers)
            html2 = r2.text or ""
            markers_hit = [m for m in _RESULT_MARKERS if m in html2]
            count_hint = re.search(r'\b\d+\s*-\s*\d+\s+of\s+\d+\b', html2)
            # Detail links: this site uses /Details.aspx?SID=... per notice.
            detail_links = re.findall(
                r'[A-Za-z0-9_./()-]*[Dd]etails?\.aspx\?[A-Za-z0-9_./?=&-]*', html2
            )
            # Async delta responses are pipe-delimited: "len|type|id|content|".
            is_delta = html2[:20].count("|") >= 2 and html2.lstrip()[:1].isdigit()

            # Parse the ASP.NET AJAX delta. Format is repeating segments:
            #   length|type|id|content|  (type 'updatePanel' carries panel HTML)
            # We list each segment's (type,id,length) so we can SEE which panels
            # updated — crucially whether the results grid (updateWSGrid /
            # WSExtendedGrid) is among them.
            delta_segments = []
            grid_present = False
            grid_foreclosure_hits = 0
            if is_delta:
                parts = html2.split("|")
                i = 0
                while i + 3 < len(parts):
                    seg_len = parts[i]
                    seg_type = parts[i + 1]
                    seg_id = parts[i + 2]
                    # content is parts[i+3], but may itself contain pipes; we
                    # only record the id/type map here for visibility.
                    if seg_type in ("updatePanel", "hiddenField", "scriptBlock",
                                    "asyncPostBackControlIDs", "pageTitle"):
                        delta_segments.append({
                            "type": seg_type, "id": seg_id, "len": seg_len,
                        })
                        if "Grid" in seg_id or "Result" in seg_id or "WSExt" in seg_id:
                            grid_present = True
                        i += 4
                    else:
                        i += 1
                # Does the WHOLE response mention the results grid / any notices?
                grid_foreclosure_hits = (
                    html2.count("Details.aspx") + html2.lower().count("foreclosure")
                )

            diag["step2_post"] = {
                "status": r2.status_code,
                "final_url": str(r2.url),
                "content_length": len(html2),
                "is_async_delta": is_delta,
                "delta_panels": delta_segments[:20],
                "results_grid_panel_present": grid_present,
                "result_markers_found": markers_hit,
                "foreclosure_count_in_page": html2.count("FORECLOSURE") + html2.count("Foreclosure"),
                "details_aspx_count": html2.count("Details.aspx"),
                "result_count_label": count_hint.group(0) if count_hint else None,
                "detail_link_count": len(set(detail_links)),
                "detail_link_samples": sorted(set(detail_links))[:8],
                "looks_like_results": bool(count_hint) or len(set(detail_links)) > 0,
                "head_snippet": html2[:300],
            }

            # ---- Step 3: AJAX recon (dump the real identifiers) ----
            g = client.get(_SEARCH_PAGE)
            page = g.text or ""
            diag["step3_recon"] = _recon_ajax(page)
            diag["step3_recon"]["has_scriptmanager"] = (
                "ToolkitScriptManager" in page or "Sys.WebForms" in page
            )
            diag["step3_recon"]["has_updatepanel"] = (
                "__doPostBack" in page and "UpdatePanel" in page
            )
            results_links = re.findall(
                r'(?:href|action)="([^"]*(?:Results|Search)\.aspx[^"]*)"', page
            )
            diag["step3_recon"]["results_or_search_links"] = sorted(set(results_links))[:10]

            if count_hint:
                diag["verdict"] = "SUCCESS: search postback returned a real result count."
            else:
                diag["verdict"] = (
                    "Search postback still returns the landing page (no count). "
                    "It's an AJAX UpdatePanel — see step3_recon for the real "
                    "ScriptManager/UpdatePanel ids needed for an async postback."
                )

    except httpx.HTTPError as e:
        diag["verdict"] = f"Network error: {type(e).__name__}: {e}"
        logger.exception("mnpublicnotice probe failed", error_type=type(e).__name__)

    return diag


__all__ = ["probe_mnpublicnotice"]
