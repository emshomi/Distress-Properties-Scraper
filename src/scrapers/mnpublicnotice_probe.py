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

    # ---- Step 3: results-text structure recon ----
    # GOAL: prove the notice body is INLINE in the results GridView (so the
    # scraper can parse text from rows and skip the broken detail fetch).
    # We capture the full first GridView ROW, the chunk AFTER each btnView2
    # button, the <td> class fingerprint, and counts to confirm 10 full rows.
    try:
        from urllib.parse import unquote as _unq
        with httpx.Client(timeout=45, headers=_HEADERS, follow_redirects=True) as c:
            g = c.get(_SEARCH_PAGE)
            base = str(g.url)
            flds = _harvest_form_fields(g.text or "")
            from datetime import date as _d, timedelta as _td
            t = _d.today()
            b = dict(flds)
            b["ctl00$ToolkitScriptManager1"] = (
                "ctl00$ContentPlaceHolder1$as1$upSearch|"
                "ctl00$ContentPlaceHolder1$as1$btnGo"
            )
            b["__ASYNCPOST"] = "true"; b["__EVENTTARGET"] = ""; b["__EVENTARGUMENT"] = ""
            b["ctl00$ContentPlaceHolder1$as1$txtSearch"] = "foreclosure"
            b["ctl00$ContentPlaceHolder1$as1$rdoType"] = "AND"
            b["ctl00$ContentPlaceHolder1$as1$txtDateFrom"] = (t - _td(days=14)).strftime("%m/%d/%Y")
            b["ctl00$ContentPlaceHolder1$as1$txtDateTo"] = t.strftime("%m/%d/%Y")
            b["ctl00$ContentPlaceHolder1$as1$btnGo"] = "GO"
            ph = dict(_HEADERS)
            ph["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
            ph["X-MicrosoftAjax"] = "Delta=true"; ph["X-Requested-With"] = "XMLHttpRequest"
            ph["Origin"] = _BASE; ph["Referer"] = base
            pr = c.post(base, data=b, headers=ph)
            rd = re.search(r"pageRedirect\|\|([^|]+)\|", pr.text or "")
            results = ""
            if rd:
                ru = _unq(rd.group(1))
                if ru.startswith("/"):
                    ru = _BASE + ru
                results = (c.get(ru, headers=_HEADERS).text or "")

            recon: dict[str, Any] = {"results_len": len(results)}

            # First Details link -> sid/id anchor.
            m = re.search(r'Details\.aspx\?SID=([A-Za-z0-9]+)&(?:amp;)?ID=(\d+)', results)
            if m:
                sid, nid = m.group(1), m.group(2)
                recon["first_sid"] = sid
                recon["first_id"] = nid

            # (a) Count the structural anchors so we know how many full rows
            #     are inline. btnView2 = one per notice row.
            recon["btnview2_count"] = len(re.findall(r'\$btnView2', results))
            recon["btnview_count"] = len(re.findall(r'\$btnView\b', results))
            recon["details_link_count"] = len(re.findall(r'Details\.aspx\?', results))
            recon["gridview_row_ctl_count"] = len(
                set(re.findall(r'GridView1\$ctl(\d+)\$btnView2', results))
            )

            # (b) Capture the chunk AFTER the FIRST btnView2 button — this is
            #     where the adjacent text cell(s) live. 2500 chars forward.
            bm = re.search(r'GridView1\$ctl\d+\$btnView2"', results)
            if bm:
                start = bm.start()
                recon["results_context_after"] = results[start:start + 2500]
            else:
                recon["results_context_after"] = None

            # (c) Capture the ENTIRE first GridView data row, button cell to
            #     row close. Anchor on the row's first <tr> before btnView2.
            if bm:
                # Walk back to the <tr that opens this row.
                pre = results[:bm.start()]
                tr_open = pre.rfind("<tr")
                # Walk forward to the matching </tr> after the button.
                tr_close = results.find("</tr>", bm.start())
                if tr_open != -1 and tr_close != -1:
                    recon["first_full_row"] = results[tr_open:tr_close + 5][:4000]
                else:
                    recon["first_full_row"] = None
            else:
                recon["first_full_row"] = None

            # (d) <td> class fingerprint across the results — which cell holds
            #     the notice text? Sample distinct class strings.
            td_classes = re.findall(r'<td[^>]*class="([^"]+)"', results)
            seen: list[str] = []
            for cls in td_classes:
                if cls not in seen:
                    seen.append(cls)
                if len(seen) >= 25:
                    break
            recon["td_class_samples"] = seen
            recon["has_noticeText_class"] = bool(
                re.search(r'class="[^"]*notice[^"]*"', results, re.IGNORECASE)
            )

            # (e) Where the real notice body sits: find the first occurrence of
            #     a legal-notice marker and show 1500 chars around it.
            nm2 = re.search(
                r'(NOTICE IS HEREBY GIVEN|MORTGAGE FORECLOSURE SALE|'
                r'NOTICE OF MORTGAGE|YOU ARE NOTIFIED|Minn\. Stat\.)',
                results, re.IGNORECASE,
            )
            if nm2:
                p = nm2.start()
                recon["notice_body_marker"] = nm2.group(1)
                recon["notice_body_context"] = results[max(0, p - 300):p + 1200]
            else:
                recon["notice_body_marker"] = None
                recon["notice_body_context"] = None

            # (f) SAME-SESSION detail fetch. The results page proved the body
            #     is truncated ("click 'view' to open the full text."), so we
            #     MUST hit Details.aspx. The earlier cold fetches failed
            #     because they used fresh requests; the browser's View click
            #     reuses the live session. Replicate: same client `c` (cookie
            #     jar intact), Referer = the results URL we just GET'd.
            if m:
                sid, nid = m.group(1), m.group(2)
                results_url = ru if rd else base
                detail_urls = [
                    f"{_BASE}/(S({sid}))/Details.aspx?SID={sid}&ID={nid}",
                    f"{_BASE}/(S({sid}))/Details.aspx?ID={nid}",
                    f"{_BASE}/Details.aspx?SID={sid}&ID={nid}",
                ]
                dh = dict(_HEADERS)
                dh["Referer"] = results_url
                same_session_attempts = []
                for u in detail_urls:
                    try:
                        dr = c.get(u, headers=dh)
                        dbody = dr.text or ""
                        # Full text = the part the results page truncated.
                        has_full = ("NOTICE IS HEREBY GIVEN" in dbody and
                                    len(dbody) > 20000)
                        # Find the notice marker inside the detail page.
                        dn = re.search(
                            r'(NOTICE IS HEREBY GIVEN|MORTGAGE FORECLOSURE)',
                            dbody, re.IGNORECASE)
                        same_session_attempts.append({
                            "url": u,
                            "status": dr.status_code,
                            "len": len(dbody),
                            "has_notice_marker": bool(dn),
                            "looks_full": has_full,
                            "body_context": (
                                dbody[max(0, dn.start() - 100):dn.start() + 1400]
                                if dn else None
                            ),
                        })
                        # Stop at the first one that returns full text.
                        if has_full:
                            break
                    except Exception as de:
                        same_session_attempts.append(
                            {"url": u, "error": str(de)[:160]})
                recon["same_session_detail"] = same_session_attempts

            # (g) Is the FULL text hiding in the results payload (hidden div /
            #     second copy), or does the server only ship the teaser?
            #     Check for content PAST the visible truncation point.
            tease = re.search(
                r'NOTICE IS HEREBY GIVEN.{0,400}?click &#39;view&#39;',
                results, re.DOTALL | re.IGNORECASE)
            recon["teaser_len_first"] = (
                tease.end() - tease.start() if tease else None)
            # Count how many times the FIRST notice's distinctive mortgagor
            # text appears — >1 would mean a hidden full copy exists.
            mort = re.search(r'Mortgagor:\s*([A-Za-z ]{8,40})', results)
            if mort:
                frag = mort.group(1).strip()[:20]
                recon["mortgagor_fragment"] = frag
                recon["mortgagor_occurrences"] = results.count(frag)
            # Longest run between tags (proxy for "is any full body inline?").
            text_blocks = re.findall(r'>([^<]{200,})<', results)
            recon["longest_inline_text_block"] = (
                max((len(t) for t in text_blocks), default=0))
            recon["inline_blocks_over_1000"] = sum(
                1 for t in text_blocks if len(t) > 1000)

            # (h) POSTBACK detail attempt: btnView (the hidden ASP.NET submit;
            #     btnView2 is the cosmetic JS-nav twin). Clicking it stores the
            #     selected record server-side, then ASP.NET redirects to
            #     Details.aspx which renders from that session state. We POST
            #     btnView, follow the pageRedirect, and read the detail page —
            #     all on the same client.
            if m and rd:
                try:
                    rflds = _harvest_form_fields(results)
                    # Use the HIDDEN btnView (not btnView2) for the first row.
                    ctl_m = re.search(
                        r'(ctl00\$ContentPlaceHolder1\$WSExtendedGridNP1\$'
                        r'GridView1\$ctl\d+\$btnView)\b(?!2)', results)
                    sm_m = re.search(r'name="(ctl00\$[^"]*ScriptManager[^"]*)"', results)
                    if ctl_m:
                        ctl_name = ctl_m.group(1)
                        pb = dict(rflds)
                        pb["__EVENTTARGET"] = ""
                        pb["__EVENTARGUMENT"] = ""
                        pb["__ASYNCPOST"] = "true"
                        pb[ctl_name] = ""
                        # ScriptManager trigger pointing at this button's panel.
                        if sm_m:
                            pb[sm_m.group(1)] = (
                                "ctl00$ContentPlaceHolder1$WSExtendedGridNP1$"
                                "upGrid|" + ctl_name)
                        pbh = dict(_HEADERS)
                        pbh["Content-Type"] = (
                            "application/x-www-form-urlencoded; charset=UTF-8")
                        pbh["X-MicrosoftAjax"] = "Delta=true"
                        pbh["X-Requested-With"] = "XMLHttpRequest"
                        pbh["Referer"] = ru
                        pbh["Origin"] = _BASE
                        pbr = c.post(ru, data=pb, headers=pbh)
                        pbody = pbr.text or ""
                        # Did the postback answer with a redirect to Details?
                        pbrd = re.search(r"pageRedirect\|\|([^|]+)\|", pbody)
                        detail_after = None
                        detail_ctx = None
                        detail_len = None
                        if pbrd:
                            du = _unq(pbrd.group(1))
                            if du.startswith("/"):
                                du = _BASE + du
                            dh2 = dict(_HEADERS)
                            dh2["Referer"] = ru
                            dgr = c.get(du, headers=dh2)
                            dtext = dgr.text or ""
                            detail_len = len(dtext)
                            ddn = re.search(
                                r'(NOTICE IS HEREBY GIVEN|MORTGAGE FORECLOSURE)',
                                dtext, re.IGNORECASE)
                            detail_after = du
                            if ddn:
                                # Full = marker present AND we're past the
                                # teaser cutoff (real body keeps going).
                                tail = dtext[ddn.start():ddn.start() + 2000]
                                detail_ctx = tail
                        recon["postback_detail"] = {
                            "ctl_name": ctl_name,
                            "postback_status": pbr.status_code,
                            "postback_len": len(pbody),
                            "had_pageRedirect": bool(pbrd),
                            "detail_url": detail_after,
                            "detail_len": detail_len,
                            "detail_has_full_body": (
                                detail_ctx is not None and
                                "click &#39;view&#39;" not in (detail_ctx or "") and
                                "click 'view'" not in (detail_ctx or "")),
                            "detail_body_context": detail_ctx,
                        }
                    else:
                        recon["postback_detail"] = {"error": "no btnView ctl found"}
                except Exception as pe:
                    recon["postback_detail"] = {"error": str(pe)[:200]}

            # (i) THE REAL SOURCE: PDFDocument.aspx?...&FileName=*.pdf
            #     The browser screenshot proved the full notice lives in a PDF
            #     served via PDFDocument.aspx (session-stamped). Check whether
            #     that link is in the RESULTS payload (best case: no detail
            #     page needed) and try downloading it on the SAME client.
            pdf_links = re.findall(
                r'PDFDocument\.aspx\?[^"\'<>\s]+', results)
            from html import unescape as _un2
            pdf_links = [_un2(p) for p in pdf_links]
            recon["pdf_links_in_results"] = len(set(pdf_links))
            recon["pdf_link_samples"] = sorted(set(pdf_links))[:5]
            # Also look for the bare FileName= pattern anywhere.
            fnames = re.findall(r'FileName=([^"\'&<>\s]+\.pdf)', results, re.IGNORECASE)
            recon["filename_params_in_results"] = sorted(set(fnames))[:5]

            # If a PDF link exists in results, download it on the same client.
            if pdf_links:
                pdf_url = pdf_links[0]
                if pdf_url.startswith("/"):
                    pdf_url = _BASE + pdf_url
                elif not pdf_url.startswith("http"):
                    # Relative to the session-stamped base.
                    sb = re.match(r'(https://www\.mnpublicnotice\.com/\(S\([^)]+\)\)/)', ru or "")
                    pdf_url = (sb.group(1) if sb else _BASE + "/") + pdf_url
                try:
                    ph2 = dict(_HEADERS)
                    ph2["Referer"] = ru
                    ph2["Accept"] = "application/pdf,*/*"
                    pdfr = c.get(pdf_url, headers=ph2)
                    raw = pdfr.content or b""
                    recon["pdf_download_test"] = {
                        "url": pdf_url,
                        "status": pdfr.status_code,
                        "bytes": len(raw),
                        "content_type": pdfr.headers.get("content-type", ""),
                        "is_pdf": raw[:5] == b"%PDF-",
                    }
                except Exception as xe:
                    recon["pdf_download_test"] = {"url": pdf_url, "error": str(xe)[:160]}

            # (j) DECISIVE DETAIL-WALL DIAGNOSTIC. We need the full PDF, which
            #     lives behind Details.aspx. Fetch it SAME-SESSION and dissect
            #     the 13.6KB chrome to learn WHICH wall blocks us:
            #       - reCAPTCHA token wall (hard)
            #       - session-state wall (View-click sets state a GET misses)
            #       - or the PDF link is right there in the chrome (cheap win)
            if m:
                sid, nid = m.group(1), m.group(2)
                results_url = ru if rd else base
                durl = f"{_BASE}/(S({sid}))/Details.aspx?SID={sid}&ID={nid}"
                dh = dict(_HEADERS)
                dh["Referer"] = results_url
                try:
                    dr = c.get(durl, headers=dh)
                    dbody = dr.text or ""
                    low = dbody.lower()
                    # Pull title + any visible headers.
                    title_m = re.search(r"<title>(.*?)</title>", dbody, re.I | re.S)
                    headers_found = re.findall(r"<h[12][^>]*>(.*?)</h[12]>", dbody, re.I | re.S)
                    headers_clean = [re.sub(r"<[^>]+>", " ", h).strip()[:80] for h in headers_found[:5]]
                    # Does the chrome carry the PDF link already?
                    pdf_in_chrome = re.findall(r'PDFDocument\.aspx\?[^"\'<>\s]+', dbody)
                    fname_in_chrome = re.findall(r'FileName=([^"\'&<>\s]+\.pdf)', dbody, re.I)
                    # New viewstate? (would mean detail page wants its own postback)
                    dvs = re.search(r'id="__VIEWSTATE"[^>]*value="([^"]*)"', dbody)
                    diag["detail_wall_diagnostic"] = {
                        "url": durl,
                        "status": dr.status_code,
                        "len": len(dbody),
                        "title": (re.sub(r"<[^>]+>", " ", title_m.group(1)).strip()
                                  if title_m else None),
                        "visible_headers": headers_clean,
                        # Wall signals
                        "has_recaptcha": any(k in low for k in (
                            "recaptcha", "g-recaptcha", "grecaptcha")),
                        "has_captcha_word": "captcha" in low,
                        "mentions_expired": "expired" in low,
                        "mentions_session": "session" in low,
                        "mentions_search_again": ("search again" in low or
                                                  "return to search" in low),
                        "mentions_no_record": ("no record" in low or
                                               "not found" in low or
                                               "no results" in low),
                        "has_login_wall": ("sign in" in low or "log in" in low or
                                           "login" in low),
                        "has_notice_marker": ("notice is hereby given" in low or
                                              "mortgage foreclosure" in low),
                        # Cheap-win signals
                        "pdf_link_in_chrome": len(set(pdf_in_chrome)),
                        "pdf_link_sample": sorted(set(pdf_in_chrome))[:3],
                        "filename_in_chrome": sorted(set(fname_in_chrome))[:3],
                        "has_new_viewstate": bool(dvs),
                        "new_viewstate_len": len(dvs.group(1)) if dvs else 0,
                        # Raw body so we can read what the chrome literally says
                        "body_text_sample": re.sub(
                            r"\s+", " ",
                            re.sub(r"<[^>]+>", " ", dbody))[:1500],
                    }
                except Exception as je:
                    diag["detail_wall_diagnostic"] = {"error": str(je)[:200]}

            diag["step3_results_recon"] = recon
    except Exception as e:
        diag["step3_results_recon"] = {"error": f"{type(e).__name__}: {e}"}

    return diag


__all__ = ["probe_mnpublicnotice"]
