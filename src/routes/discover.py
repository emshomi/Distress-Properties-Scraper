"""
Data-source discovery endpoints.

Admin-only helpers for finding hard-to-locate public data services. These
run server-side on Railway (open network egress), so they can reach ArcGIS
Online's search API and arbitrary government ArcGIS servers that the local
dev sandbox cannot.

Primary use: definitively locate the real Feature Service URLs behind public
ArcGIS dashboards (e.g., Minneapolis vacant/condemned properties) instead of
guessing URLs.

Endpoints:
  GET /discover/arcgis?q=<query>     — search ArcGIS Online for feature services
  GET /discover/probe?url=<url>      — inspect any ArcGIS service/layer URL
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException, Query, status as http_status

from src.middleware.auth import AdminKeyRequired
from src.utils.errors import success_envelope
from src.utils.logger import logger


router = APIRouter(tags=["discover"])

# Esri's public ArcGIS Online content search API.
_ARCGIS_SEARCH_URL = "https://www.arcgis.com/sharing/rest/search"

# Hosts we allow /discover/probe to hit. ArcGIS Online + common government
# ArcGIS server patterns. We keep this permissive but require https/http and
# an arcgis-looking path to avoid turning this into an open proxy.
_ALLOWED_PROBE_HINTS = ("arcgis", "/rest/services", "gis.")


@router.get(
    "/discover/arcgis",
    status_code=http_status.HTTP_200_OK,
    summary="Search ArcGIS Online for feature services",
    dependencies=[AdminKeyRequired],
)
async def discover_arcgis(
    q: str = Query(
        ...,
        min_length=2,
        max_length=300,
        description=(
            "ArcGIS Online search query. Examples: "
            "'Minneapolis vacant', 'Minneapolis condemned building', "
            "'title:vacant owner:CityofMinneapolis'."
        ),
    ),
    num: int = Query(
        default=25,
        ge=1,
        le=100,
        description="Max results to return.",
    ),
    types: str | None = Query(
        default="Feature Service",
        description=(
            "Item type filter (ArcGIS 'type' field). Default 'Feature Service'. "
            "Pass empty to search all types (maps, dashboards, etc.)."
        ),
    ),
) -> dict[str, Any]:
    """
    Search ArcGIS Online and return clean results with the real service URLs.

    This hits Esri's public search API from Railway's network, which can reach
    it reliably. Returns each item's title, owner, type, id, and — crucially —
    the `url` (the FeatureServer/MapServer endpoint we can scrape).
    """
    # Build the search query. If a type filter is given, AND it in.
    query = q.strip()
    if types:
        query = f'({query}) AND (type:"{types}")'

    params: dict[str, Any] = {
        "q": query,
        "f": "json",
        "num": num,
        "sortField": "numviews",
        "sortOrder": "desc",
    }

    logger.info("ArcGIS discovery search", query=query, num=num)

    try:
        async with httpx.AsyncClient(
            timeout=30,
            headers={"User-Agent": "DistressProperties/1.0 (discovery)"},
        ) as client:
            resp = await client.get(_ARCGIS_SEARCH_URL, params=params)
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=http_status.HTTP_502_BAD_GATEWAY,
            detail={"message": f"ArcGIS search request failed: {e}"},
        ) from e

    if resp.status_code != 200:
        raise HTTPException(
            status_code=http_status.HTTP_502_BAD_GATEWAY,
            detail={
                "message": f"ArcGIS search returned {resp.status_code}",
                "body": resp.text[:500],
            },
        )

    try:
        data = resp.json()
    except ValueError as e:
        raise HTTPException(
            status_code=http_status.HTTP_502_BAD_GATEWAY,
            detail={"message": f"ArcGIS search returned non-JSON: {e}"},
        ) from e

    results = data.get("results") or []

    # Clean down to the fields we care about
    cleaned = []
    for item in results:
        cleaned.append({
            "title": item.get("title"),
            "owner": item.get("owner"),
            "type": item.get("type"),
            "id": item.get("id"),
            "url": item.get("url"),  # the FeatureServer/MapServer URL
            "snippet": item.get("snippet"),
            "num_views": item.get("numViews"),
            "access": item.get("access"),
        })

    return success_envelope({
        "query": query,
        "total": data.get("total", len(cleaned)),
        "returned": len(cleaned),
        "results": cleaned,
    })


@router.get(
    "/discover/probe",
    status_code=http_status.HTTP_200_OK,
    summary="Inspect an ArcGIS service or layer URL",
    dependencies=[AdminKeyRequired],
)
async def discover_probe(
    url: str = Query(
        ...,
        min_length=10,
        max_length=1000,
        description=(
            "An ArcGIS REST URL (a service root, a layer, or a /query). "
            "Returns the service/layer metadata as JSON so we can see layers, "
            "fields, and record counts."
        ),
    ),
) -> dict[str, Any]:
    """
    Fetch and return the JSON metadata for any ArcGIS REST URL.

    Used to inspect a service's layers and a layer's fields once /discover/arcgis
    has located it. Appends f=json automatically.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail={"message": "URL must be http(s)."},
        )

    # Light guard: only allow ArcGIS-looking URLs (not an open proxy)
    low = url.lower()
    if not any(hint in low for hint in _ALLOWED_PROBE_HINTS):
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail={
                "message": (
                    "URL does not look like an ArcGIS REST endpoint "
                    "(expected 'arcgis' or '/rest/services' in the URL)."
                )
            },
        )

    # Ensure f=json
    sep = "&" if "?" in url else "?"
    fetch_url = url if "f=json" in low or "f=pjson" in low else f"{url}{sep}f=json"

    logger.info("ArcGIS probe", url=fetch_url)

    try:
        async with httpx.AsyncClient(
            timeout=30,
            headers={"User-Agent": "DistressProperties/1.0 (discovery)"},
            follow_redirects=True,
        ) as client:
            resp = await client.get(fetch_url)
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=http_status.HTTP_502_BAD_GATEWAY,
            detail={"message": f"Probe request failed: {e}"},
        ) from e

    if resp.status_code != 200:
        raise HTTPException(
            status_code=http_status.HTTP_502_BAD_GATEWAY,
            detail={
                "message": f"Probe returned {resp.status_code}",
                "body": resp.text[:500],
            },
        )

    try:
        data = resp.json()
    except ValueError:
        # Not JSON — return a snippet so we can see what it was
        return success_envelope({
            "url": fetch_url,
            "note": "Response was not JSON",
            "body_snippet": resp.text[:1000],
        })

    # If it's a service root, summarize layers. If a layer, summarize fields.
    summary: dict[str, Any] = {"url": fetch_url}

    if isinstance(data, dict):
        if "layers" in data or "tables" in data:
            summary["service_description"] = data.get("serviceDescription") or data.get("description")
            summary["layers"] = [
                {"id": l.get("id"), "name": l.get("name")}
                for l in (data.get("layers") or [])
            ]
            summary["tables"] = [
                {"id": t.get("id"), "name": t.get("name")}
                for t in (data.get("tables") or [])
            ]
        elif "fields" in data:
            summary["layer_name"] = data.get("name")
            summary["geometry_type"] = data.get("geometryType")
            summary["max_record_count"] = data.get("maxRecordCount")
            summary["fields"] = [
                {"name": f.get("name"), "type": f.get("type"), "alias": f.get("alias")}
                for f in (data.get("fields") or [])
            ]
        else:
            # Some other JSON (e.g., query result) — return as-is, trimmed
            summary["raw"] = data

    return success_envelope(summary)


__all__ = ["router"]
