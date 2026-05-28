"""
Dakota County Sheriff Foreclosure Sales scraper.

Source: Dakota County GIS ArcGIS Server (county-operated, public)
Service: http://gis2.co.dakota.mn.us/arcgis/rest/services/DCGIS_OL_PropertyInformation/MapServer
Hub:     https://gis.data.mn.gov/datasets/DakotaCounty::sheriff-foreclosure-sales

License: Dakota County GIS data provided under the Minnesota Government Data
         Practices Act (Minn. Stat. ch. 13), "AS IS without warranty." Part of
         the seven-county MetroGIS free-and-open data policy. No commercial /
         scraping restrictions. GREEN per the data-source audit.

=== WHAT THIS IS ===
Dakota publishes COMPLETED sheriff foreclosure sales as live ArcGIS layers,
one per year. This is one of the only MN counties that exposes foreclosure
sales as open data at all (Hennepin & Ramsey do not).

  Layer 82 = Foreclosure Sales (2026)  ← current year
  Layer 80 = Foreclosure Sales (2025)  ← previous year

These are completed sales (the auction already happened), NOT a forward
calendar of scheduled sales — Dakota's metadata is explicit that the Sheriff
does not publish upcoming sales. But completed sales start the 6-month
redemption clock, which is itself an actionable investment window, and the
data refreshes monthly with a "Recent" flag marking the latest month.

=== FIELDS (layer 82/80) ===
  OBJECTID    (oid)
  SaleDate    (date, ms-epoch)   the foreclosure sale date
  SaleAmount  (double)           winning bid / sale amount
  GeoAddress  (string)           "Unverified Address" — property street address
  GeoCity     (string)           city
  Recent      (string Y/N)       "Y" = most-recent-month sale (map symbology)
  Mortgagor   (string)           borrower being foreclosed (often blank/short;
                                 service metadata mislabels it — captured raw)
  Attorney    (mislabeled geom)  service config bug — ignored, captured raw
  Shape       (point geom)       UTM 26915 — we request outSR=4326 for lat/lng

=== ARCHITECTURE ===
These foreclosure points carry NO parcel ID. Our core.parcels.parcel_id is the
FK backbone, so we synthesize a stable ID per record:

    parcel_id = "DAKOTA-FC-{year}-{OBJECTID}"

Then we write, mirroring the proven Saint Paul pattern:
  1. core.parcels row (county=dakota, address/city/lat/lng, raw_data)
  2. signals.distress_events row (event_type='sheriff_sale', dated by SaleDate)

Later, real Dakota parcel IDs can be spatial-joined from the parcels layer.

=== SEVERITY ===
  Recent='Y' (latest month)  -> high   (freshest opportunity)
  current year (2026)        -> medium
  previous year (2025)       -> low    (redemption window likely closing/closed)
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

import httpx

from src.config import settings
from src.models.parcel import ParcelUpsert
from src.models.signal import DistressEventInsert
from src.scrapers.base_arcgis_scraper import BaseArcGISScraper, arcgis_date_to_date_only
from src.services.event_writer import write_events_dedup
from src.services.parcel_resolver import resolve_parcel
from src.utils.errors import ParseError, SourceUnavailableError
from src.utils.logger import logger


# Base MapServer (no layer suffix — we append the layer id per year)
_MAPSERVER_BASE = (
    "http://gis2.co.dakota.mn.us/arcgis/rest/services"
    "/DCGIS_OL_PropertyInformation/MapServer"
)

# Year → layer id mapping (from the live service, May 2026).
# We scrape current + previous year. Older layers exist (2007–2024) but are
# not useful for active distress discovery.
_YEAR_LAYERS: dict[int, int] = {
    2026: 82,
    2025: 80,
}


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        d = Decimal(str(value))
        return d if d >= 0 else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _title_case(value: str | None) -> str | None:
    return value.title() if value else None


class DakotaSheriffScraper(BaseArcGISScraper[DistressEventInsert]):
    """Dakota County completed sheriff foreclosure sales — ArcGIS source."""

    source_name: ClassVar[str] = "dakota_sheriff"
    signal_type: ClassVar[str] = "sheriff_sale"
    county_code: ClassVar[str] = "dakota"

    # We override fetch() to pull multiple year-layers, so feature_service_url
    # is set per-layer at fetch time rather than as a single class constant.
    feature_service_url: ClassVar[str] = _MAPSERVER_BASE

    where_clause: ClassVar[str] = "1=1"
    return_geometry: ClassVar[bool] = True
    page_size: ClassVar[int] = 2000
    max_pages: ClassVar[int] = 20  # plenty: a few hundred sales/year/county

    # ---- Custom multi-layer fetch ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """
        Fetch foreclosure sales from each year-layer (2026 + 2025).

        Tags each feature with `_year` and `_layer_id` so parse_feature can
        build a stable synthetic parcel_id and score severity by recency.
        """
        all_features: list[dict[str, Any]] = []

        async with httpx.AsyncClient(
            timeout=settings.scraper_request_timeout_seconds,
            headers={"User-Agent": "DistressProperties/1.0"},
        ) as client:
            for year, layer_id in _YEAR_LAYERS.items():
                layer_url = f"{_MAPSERVER_BASE}/{layer_id}/query"
                offset = 0

                while True:
                    params: dict[str, Any] = {
                        "where": self.where_clause,
                        "outFields": "*",
                        "returnGeometry": "true",
                        "outSR": 4326,  # convert UTM 26915 → WGS84 lat/lng
                        "resultOffset": offset,
                        "resultRecordCount": self.page_size,
                        "f": "json",
                    }

                    try:
                        resp = await client.get(layer_url, params=params)
                    except httpx.HTTPError as e:
                        raise SourceUnavailableError(
                            f"Dakota ArcGIS request failed: {e}",
                            source=self.source_name,
                            context={"layer_id": layer_id, "offset": offset},
                        ) from e

                    if resp.status_code != 200:
                        raise SourceUnavailableError(
                            f"Dakota ArcGIS returned {resp.status_code}",
                            source=self.source_name,
                            context={"layer_id": layer_id, "offset": offset},
                        )

                    try:
                        data = resp.json()
                    except ValueError as e:
                        raise ParseError(
                            f"Dakota ArcGIS returned non-JSON: {e}",
                            source=self.source_name,
                        ) from e

                    if isinstance(data, dict) and "error" in data:
                        raise ParseError(
                            f"Dakota ArcGIS API error: "
                            f"{data['error'].get('message', 'unknown')}",
                            source=self.source_name,
                            context={"layer_id": layer_id},
                        )

                    features = data.get("features") or []
                    for feat in features:
                        # Tag inside ATTRIBUTES (not the feature) — the base
                        # parse() passes feature["attributes"] to parse_feature,
                        # so tags on the feature itself would be invisible.
                        attrs = feat.setdefault("attributes", {})
                        attrs["_year"] = year
                        attrs["_layer_id"] = layer_id
                    all_features.extend(features)

                    logger.info(
                        "Dakota sheriff layer page fetched",
                        source=self.source_name,
                        year=year,
                        layer_id=layer_id,
                        page_count=len(features),
                        cumulative=len(all_features),
                    )

                    if len(features) < self.page_size:
                        break
                    if not data.get("exceededTransferLimit", False):
                        break
                    offset += self.page_size

        logger.info(
            "Dakota sheriff fetch complete",
            source=self.source_name,
            total_features=len(all_features),
        )
        return all_features

    # ---- Feature parsing ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> DistressEventInsert | None:
        """Convert one Dakota foreclosure-sale feature into a DistressEventInsert."""
        objectid = attributes.get("OBJECTID")
        year = attributes.get("_year")
        if objectid is None or year is None:
            return None

        # Synthetic, stable parcel_id (no real PID in this dataset)
        parcel_id = f"DAKOTA-FC-{year}-{objectid}"

        # Sale date (required for a sheriff_sale event)
        sale_date_str = arcgis_date_to_date_only(attributes.get("SaleDate"))
        sale_date: date | None = None
        if sale_date_str:
            try:
                sale_date = date.fromisoformat(sale_date_str)
            except ValueError:
                sale_date = None
        if sale_date is None:
            # Without a sale date this isn't a usable sheriff_sale event — skip.
            return None

        address = _safe_str(attributes.get("GeoAddress"))
        city = _title_case(_safe_str(attributes.get("GeoCity")))
        sale_amount = _safe_decimal(attributes.get("SaleAmount"))
        is_recent = _safe_str(attributes.get("Recent")) == "Y"

        # Severity by recency
        if is_recent:
            severity = "high"
        elif int(year) >= datetime.now(timezone.utc).year:
            severity = "medium"
        else:
            severity = "low"

        # Build a human-readable title
        title_bits = ["Sheriff foreclosure sale"]
        if address:
            title_bits.append(f"— {address}")
        if city:
            title_bits.append(f", {city}")
        title = " ".join(title_bits)[:500]

        description_parts = [f"Completed Dakota County sheriff sale ({year})."]
        if sale_amount is not None:
            description_parts.append(f"Sale amount: ${sale_amount:,.0f}.")
        if is_recent:
            description_parts.append("Flagged as most-recent-month sale.")
        description = " ".join(description_parts)[:2000]

        return DistressEventInsert(
            parcel_id=parcel_id,
            event_type="sheriff_sale",
            event_subtype=f"completed_sale_{year}",
            event_date=sale_date,
            event_value=sale_amount,
            source=self.source_name,
            source_id=str(objectid),
            severity=severity,  # type: ignore[arg-type]
            title=title,
            description=description,
            raw_data={
                "attributes": {
                    k: v for k, v in attributes.items()
                    if k not in ("_year", "_layer_id")
                },
                "geometry": geometry,
                "_year": year,
                "_source": self.source_name,
            },
            observed_at=datetime.now(timezone.utc),
        )

    # ---- Write ----

    async def write(
        self,
        signals: list[DistressEventInsert],
    ) -> tuple[int, int, int]:
        """
        Persist Dakota foreclosure sales.

        1. resolve_parcel() for each synthetic parcel (address/city/lat/lng)
        2. signals.distress_events (unified feed)
        """
        if not signals:
            return 0, 0, 0

        # --- Step 1: one parcel per synthetic id ---
        unique_parcels: dict[str, ParcelUpsert] = {}
        for ev in signals:
            if ev.parcel_id in unique_parcels:
                continue

            raw = ev.raw_data or {}
            attrs = raw.get("attributes") or {}
            geom = raw.get("geometry") or {}

            # lat/lng from point geometry (outSR=4326 → x=lng, y=lat)
            lng = _safe_float(geom.get("x"))
            lat = _safe_float(geom.get("y"))
            if lat is not None and not (43.0 <= lat <= 50.0):
                lat = None
            if lng is not None and not (-97.5 <= lng <= -89.0):
                lng = None

            address = _safe_str(attrs.get("GeoAddress"))
            city = _title_case(_safe_str(attrs.get("GeoCity")))

            unique_parcels[ev.parcel_id] = ParcelUpsert(
                parcel_id=ev.parcel_id,
                county_code=self.county_code,
                state="MN",
                address=address,
                city=city,
                lat=lat,
                lng=lng,
                raw_data={"dakota_foreclosure": attrs, "_source": self.source_name},
                data_sources=[self.source_name],
                last_observed_at=datetime.now(timezone.utc),
            )

        parcels_ok = 0
        parcels_failed = 0
        for payload in unique_parcels.values():
            if resolve_parcel(payload) is not None:
                parcels_ok += 1
            else:
                parcels_failed += 1

        logger.info(
            "Dakota sheriff parcels resolved",
            source=self.source_name,
            parcels_ok=parcels_ok,
            parcels_failed=parcels_failed,
        )

        # --- Step 2: unified distress_events ---
        new_events, failed_events = write_events_dedup(signals)

        return new_events, 0, failed_events + parcels_failed


__all__ = ["DakotaSheriffScraper"]
