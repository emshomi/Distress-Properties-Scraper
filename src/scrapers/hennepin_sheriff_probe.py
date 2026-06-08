"""
Diagnostic probe for the Hennepin County Sheriff foreclosure API.

NOT a scraper — a one-shot diagnostic that runs FROM the Railway server and
confirms the JSON API works there before we build the real scraper. The API
shape was captured from the live site's DevTools (2026-06):

  POST https://api.hennepincounty.gov/hcso-public-services-api/v1/Foreclosure/Search
  Headers: Content-Type: application/json
           Ocp-Apim-Subscription-Key: <fixed public key from the site frontend>
           Origin / Referer: https://foreclosure.hennepin.us
  Body:    {"dateOfSale":null,"address":null,"city":null,"mortgagorName":null,
            "pagination":{"activePage":1,"pageSize":N}}
  Response:{"data":[{saleRecordNumber,dateOfSale,typeOfSale,address,city,
            mortgagors:[{display}]}...],
            "pagination":{totalRecords,totalPages,pageSize,activePage}}

The list response carries summary fields only (no $ amounts / redemption /
full notice text). This probe ALSO tries to discover a per-record DETAIL
endpoint, since the scraper will likely need it for the full notice.

Writes NOTHING to the DB — returns a diagnostic dict.
"""

from __future__ import annotations

from typing import Any, Optional

import httpx

from src.utils.logger import logger


_API_BASE = "https://api.hennepincounty.gov/hcso-public-services-api/v1"
_SEARCH_URL = f"{_API_BASE}/Foreclosure/Search"

# Fixed public subscription key baked into the foreclosure.hennepin.us frontend
# (Azure API Management). Required or the gateway returns 401.
_SUBSCRIPTION_KEY = "e522a816143443189f09de85c4288b98"

_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Ocp-Apim-Subscription-Key": _SUBSCRIPTION_KEY,
    "Origin": "https://foreclosure.hennepin.us",
    "Referer": "https://foreclosure.hennepin.us/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
}


def _search(client: httpx.Client, active_page: int, page_size: int) -> httpx.Response:
    body = {
        "dateOfSale": None,
        "address": None,
        "city": None,
        "mortgagorName": None,
        "pagination": {"activePage": active_page, "pageSize": page_size},
    }
    return client.post(_SEARCH_URL, json=body, headers=_HEADERS)


def probe_hennepin_sheriff() -> dict[str, Any]:
    """Confirm the Search API works from this server, then probe for a detail
    endpoint. Returns a diagnostic dict; never raises."""
    diag: dict[str, Any] = {"search": {}, "detail_probe": {}, "verdict": ""}

    try:
        with httpx.Client(timeout=30) as client:
            # --- 1. Search: small page to confirm shape ---
            r = _search(client, active_page=1, page_size=10)
            diag["search"]["status"] = r.status_code
            diag["search"]["content_type"] = r.headers.get("content-type")

            if r.status_code != 200:
                diag["search"]["body_head"] = (r.text or "")[:300]
                diag["verdict"] = (
                    f"Search returned {r.status_code} from this server. If 401, "
                    f"the subscription key/headers need adjusting; if 403, the "
                    f"server IP may be blocked."
                )
                return diag

            data = r.json()
            rows = data.get("data") or []
            pag = data.get("pagination") or {}
            diag["search"]["total_records"] = pag.get("totalRecords")
            diag["search"]["total_pages"] = pag.get("totalPages")
            diag["search"]["page_size"] = pag.get("pageSize")
            diag["search"]["rows_returned"] = len(rows)
            diag["search"]["first_record"] = rows[0] if rows else None
            # Distinct typeOfSale values on this page (Mortgage/Judgment/etc).
            diag["search"]["sale_types_sample"] = sorted(
                {row.get("typeOfSale") for row in rows if row.get("typeOfSale")}
            )

            # --- 2. Can we pull ALL records in one big page? ---
            total = pag.get("totalRecords") or 0
            if total:
                r_all = _search(client, active_page=1, page_size=int(total) + 50)
                if r_all.status_code == 200:
                    all_rows = (r_all.json().get("data") or [])
                    diag["search"]["single_page_all_rows"] = len(all_rows)
                    diag["search"]["single_page_works"] = len(all_rows) >= total
                else:
                    diag["search"]["single_page_all_rows"] = None
                    diag["search"]["single_page_status"] = r_all.status_code

            # --- 3. Detail endpoint discovery ---
            # Try the common REST patterns for one saleRecordNumber.
            srn = rows[0].get("saleRecordNumber") if rows else None
            attempts = []
            if srn:
                candidates = [
                    ("GET Foreclosure/Detail/{id}", "GET", f"{_API_BASE}/Foreclosure/Detail/{srn}", None),
                    ("GET Foreclosure/{id}", "GET", f"{_API_BASE}/Foreclosure/{srn}", None),
                    ("GET Foreclosure/Sale/{id}", "GET", f"{_API_BASE}/Foreclosure/Sale/{srn}", None),
                    ("GET Foreclosure/GetDetail?saleRecordNumber=", "GET",
                     f"{_API_BASE}/Foreclosure/GetDetail?saleRecordNumber={srn}", None),
                    ("POST Foreclosure/Detail", "POST", f"{_API_BASE}/Foreclosure/Detail",
                     {"saleRecordNumber": srn}),
                ]
                for label, method, url, jbody in candidates:
                    try:
                        if method == "GET":
                            dr = client.get(url, headers=_HEADERS)
                        else:
                            dr = client.post(url, json=jbody, headers=_HEADERS)
                        entry = {
                            "label": label,
                            "status": dr.status_code,
                            "content_type": dr.headers.get("content-type"),
                            "len": len(dr.text or ""),
                        }
                        if dr.status_code == 200 and "json" in (dr.headers.get("content-type") or ""):
                            try:
                                j = dr.json()
                                # Show the KEYS so we see what extra fields a
                                # detail record carries (amounts, redemption, etc).
                                if isinstance(j, dict):
                                    entry["keys"] = sorted(j.keys())[:40]
                                elif isinstance(j, list) and j and isinstance(j[0], dict):
                                    entry["keys"] = sorted(j[0].keys())[:40]
                            except Exception:
                                entry["keys"] = None
                        attempts.append(entry)
                    except Exception as de:
                        attempts.append({"label": label, "error": str(de)[:120]})
            diag["detail_probe"]["srn_tried"] = srn
            diag["detail_probe"]["attempts"] = attempts

            hit = next((a for a in attempts if a.get("status") == 200 and a.get("keys")), None)
            diag["verdict"] = (
                "SUCCESS: Search API works from this server"
                + (f"; total_records={total}")
                + (
                    f"; detail endpoint found ({hit['label']})."
                    if hit else
                    "; no detail endpoint found among common patterns — list "
                    "fields may be all we get, or the detail path differs."
                )
            )

    except httpx.HTTPError as e:
        diag["verdict"] = f"Network error from this server: {type(e).__name__}: {e}"
        logger.exception("hennepin sheriff probe failed", error_type=type(e).__name__)
    except Exception as e:
        diag["verdict"] = f"Unexpected error: {type(e).__name__}: {e}"
        logger.exception("hennepin sheriff probe error", error_type=type(e).__name__)

    return diag


__all__ = ["probe_hennepin_sheriff"]
