"""
One-off probe: does Anoka's Assessor_Sales service expose year_built /
property_type?

Anoka's foreclosure enrichment uses Parcels/MapServer/0, which carries owner /
market value / homestead but NO year_built and an empty USE_DESC. Anoka also
runs a separate Assessor_Sales MapServer — assessor layers are where
year-built / use-class typically live. This probe dumps that service's layer
list, every layer's fields, and one sample row, so we can see for certain
whether year_built / property_type are available before deciding to ingest
Anoka parcels into core.parcels. Read-only; writes nothing.
"""

from __future__ import annotations

from typing import Any

import httpx

from src.utils.logger import logger


_ANOKA_ASSESSOR_ROOT = (
    "https://gisservices.co.anoka.mn.us/anoka_gis/rest/services/"
    "Assessor_Sales/MapServer"
)
_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=30.0)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
}


def probe_anoka_assessor() -> dict[str, Any]:
    """Fetch the Assessor_Sales service: layers, each layer's fields, and one
    sample row per layer. Synchronous (httpx.Client) so it matches the other
    probes' run-in-thread pattern. Soft-fail: any error is captured in the
    returned dict rather than raised."""
    out: dict[str, Any] = {"root": _ANOKA_ASSESSOR_ROOT, "layers": [], "error": None}

    with httpx.Client(
        timeout=_TIMEOUT, headers=_HEADERS, follow_redirects=True
    ) as client:
        try:
            r = client.get(_ANOKA_ASSESSOR_ROOT, params={"f": "json"})
            svc = r.json()
        except Exception as e:
            out["error"] = f"service fetch failed: {type(e).__name__}: {e}"
            return out

        if isinstance(svc, dict) and svc.get("error"):
            out["error"] = f"service ArcGIS error: {str(svc.get('error'))[:300]}"
            return out

        layer_list = (svc.get("layers") or []) + (svc.get("tables") or [])
        for lyr in layer_list:
            lid = lyr.get("id")
            layer_info: dict[str, Any] = {
                "id": lid,
                "name": lyr.get("name"),
                "fields": [],
                "sample": None,
            }

            # Field list for this layer.
            try:
                lr = client.get(
                    f"{_ANOKA_ASSESSOR_ROOT}/{lid}", params={"f": "json"}
                )
                ldata = lr.json()
                layer_info["fields"] = [
                    {
                        "name": f.get("name"),
                        "type": f.get("type"),
                        "alias": f.get("alias"),
                    }
                    for f in (ldata.get("fields") or [])
                ]
            except Exception as e:
                layer_info["fields_error"] = f"{type(e).__name__}: {e}"

            # One sample record so we can see real values (not just field names).
            try:
                sr = client.get(
                    f"{_ANOKA_ASSESSOR_ROOT}/{lid}/query",
                    params={
                        "where": "1=1",
                        "outFields": "*",
                        "returnGeometry": "false",
                        "resultRecordCount": "1",
                        "f": "json",
                    },
                )
                sdata = sr.json()
                feats = sdata.get("features") or []
                if feats:
                    layer_info["sample"] = feats[0].get("attributes")
            except Exception as e:
                layer_info["sample_error"] = f"{type(e).__name__}: {e}"

            out["layers"].append(layer_info)

    logger.info("anoka assessor probe complete", layers=len(out["layers"]))
    return out


__all__ = ["probe_anoka_assessor"]
