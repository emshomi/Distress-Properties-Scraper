"""
Parcel enrichment from MnGeo Open Parcels.

Source: Minnesota Geospatial Commons — "Parcels, Compiled from Opt-In Open
        Data Counties" — served as an open ArcGIS REST feature layer.
    Service: enterprise.gisdata.mn.gov/aghost/rest/services/
             us_mn_state_mngeo/plan_parcels_open/MapServer/1/query
    Standard: GAC Parcel Data Standard for Minnesota.

License / posture: public-domain government data (MN Government Data Practices
Act). Open ArcGIS endpoint — no auth, no captcha, no datacenter-IP block, so
unlike mnpublicnotice this runs fine from Railway. We query politely in
batches.

=== WHAT THIS SCRAPER DOES (it ENRICHES, it does not create) ===
Every other scraper CREATES parcels + distress events. This one is different:
it READS parcels we already hold that are linked to a distress event, looks
each up in MnGeo, and UPDATES the parcel row with property characteristics a
buyer needs when they cannot physically inspect a distressed property:
year built, finished sq ft, lot size, last sale price/date, assessor value
(land / building / total), annual tax, special assessments, number of units,
use class, school district, homestead status, garage, basement, heating,
cooling, and the abbreviated legal description.

Because it only updates existing rows, write() reports everything as
records_updated (never records_new), which slots cleanly into the BaseScraper
audit/health lifecycle.

=== COUNTY COVERAGE & MATCH STRATEGY ===
MnGeo populates the rich building fields well only for the metro counties.
Match keys differ by county, so we use two strategies:

  PIN match (exact county_pin):  Hennepin, Ramsey, Washington
      Our distress parcels store either the bare PIN or a synthetic
      '<COUNTY>-FC-<pin>' key; we strip to the trailing digit-run to recover
      the bare county_pin MnGeo uses, then match exactly.

  Address match (one-match-only): Dakota
      Dakota's distress parcels carry a synthetic id (DAKOTA-FC-2025-79) and
      only a combined address string, and Dakota's PIN scheme does not match
      MnGeo's. We parse house-number + street and match on
      anumber + st_name LIKE 'WORD%', writing ONLY when exactly one parcel
      matches (ambiguous or zero matches are skipped — never guessed).

Outstate counties are intentionally not enriched here: MnGeo leaves their
building fields largely empty, so there is nothing to gain.

=== SAFETY ===
  * Only non-empty MnGeo values are written.
  * year_built / property_type are filled ONLY when the existing row is empty
    (never overwrite an authoritative value already present).
  * One-match-only rule for address matching prevents wrong-parcel writes.
"""

from __future__ import annotations

import asyncio
import re
from datetime import date, datetime, timezone
from typing import Any, ClassVar

import httpx

from src.db.supabase_client import core_table, signals_table
from src.scrapers.base_scraper import BaseScraper
from src.utils.errors import SourceUnavailableError
from src.utils.logger import logger


# ArcGIS feature layer 1 = the parcel polygons with attributes.
_MNGEO_QUERY = (
    "https://enterprise.gisdata.mn.gov/aghost/rest/services/"
    "us_mn_state_mngeo/plan_parcels_open/MapServer/1/query"
)

# core.parcels county_code -> MnGeo co_name, for the PIN-matched counties.
_PIN_COUNTY_MAP: dict[str, str] = {
    "hennepin": "Hennepin",
    "ramsey": "Ramsey",
    "washington": "Washington",
}
# Address-matched counties.
_ADDR_COUNTY_MAP: dict[str, str] = {
    "dakota": "Dakota",
}

# Fields we pull from MnGeo.
_OUT_FIELDS = ",".join([
    "county_pin", "anumber", "st_name",
    "year_built", "fin_sq_ft", "acres_poly",
    "sale_date", "sale_value",
    "emv_land", "emv_bldg", "emv_total",
    "total_tax", "spec_asses",
    "num_units", "useclass1", "school_dst", "homestead",
    "dwell_type", "home_style",
    "garage", "garagesqft", "basement", "heating", "cooling",
    "abb_legal",
])

_PIN_BATCH = 150           # county_pins per IN(...) query
_BATCH_DELAY_SECONDS = 0.3
_ADDR_DELAY_SECONDS = 0.15
_DIRECTIONALS = {"E", "W", "N", "S", "NE", "NW", "SE", "SW"}
_DB_PAGE = 1000


def _epoch_ms_to_date(ms: Any) -> str | None:
    """MnGeo date fields arrive as epoch milliseconds; return ISO date str."""
    if ms in (None, 0, ""):
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, OverflowError, OSError):
        return None


def _clean_str(value: Any) -> str | None:
    if value in (None, "", 0):
        return None
    s = str(value).strip()
    return s or None


def _build_update_record(a: dict[str, Any], existing: dict[str, Any] | None) -> dict[str, Any]:
    """Map a MnGeo attribute dict to a core.parcels update record.

    Includes only non-empty values. year_built and property_type are filled
    only when the existing row lacks them (never overwrite).
    """
    existing = existing or {}
    rec: dict[str, Any] = {}

    # Always-fillable numerics/strings.
    if a.get("fin_sq_ft") not in (None, 0, ""):
        rec["sqft"] = a["fin_sq_ft"]
    if a.get("sale_value") not in (None, 0, ""):
        rec["last_sale_price"] = a["sale_value"]
    sd = _epoch_ms_to_date(a.get("sale_date"))
    if sd:
        rec["last_sale_date"] = sd

    # Lot size: acres_poly (acres) -> lot_sqft.
    acres = a.get("acres_poly")
    if acres not in (None, 0, "", 0.0):
        try:
            rec["lot_sqft"] = int(round(float(acres) * 43560))
        except (ValueError, TypeError):
            pass

    # Assessor value split + tax.
    for src_field, col in (
        ("emv_land", "emv_land"),
        ("emv_bldg", "emv_building"),
        ("emv_total", "emv_total"),
        ("total_tax", "annual_tax"),
        ("spec_asses", "special_assessments"),
    ):
        v = a.get(src_field)
        if v not in (None, 0, ""):
            rec[col] = v

    # Mirror emv_total into estimated_market_value when that's still empty,
    # so the existing UI column benefits too (fill-in only).
    if a.get("emv_total") not in (None, 0, "") and not existing.get("estimated_market_value"):
        rec["estimated_market_value"] = a["emv_total"]

    if a.get("num_units") not in (None, 0, ""):
        rec["num_units"] = a["num_units"]

    # Text characteristics.
    for src_field, col in (
        ("useclass1", "use_class"),
        ("school_dst", "school_district"),
        ("homestead", "homestead_status"),
        ("garage", "garage"),
        ("basement", "basement"),
        ("heating", "heating"),
        ("cooling", "cooling"),
        ("abb_legal", "legal_description"),
    ):
        sv = _clean_str(a.get(src_field))
        if sv:
            rec[col] = sv

    if a.get("garagesqft") not in (None, 0, ""):
        rec["garage_sqft"] = a["garagesqft"]

    # year_built: fill only if missing, with a sanity guard.
    if a.get("year_built") not in (None, 0, "") and not existing.get("year_built"):
        try:
            yb = int(a["year_built"])
            if 1700 < yb <= date.today().year + 1:
                rec["year_built"] = yb
        except (ValueError, TypeError):
            pass

    # property_type from home_style (fallback dwell_type), fill-in only.
    if not existing.get("property_type"):
        ptype = _clean_str(a.get("home_style")) or _clean_str(a.get("dwell_type"))
        if ptype:
            rec["property_type"] = ptype

    return rec


def _parse_address(addr: str | None) -> tuple[str | None, str | None]:
    """'13850 GARRET AVE' -> ('13850', 'GARRET'); ('', '') style failures
    return (None, None)."""
    if not addr:
        return None, None
    m = re.match(r"\s*(\d+)\s+(.*)", addr.strip())
    if not m:
        return None, None
    num = m.group(1)
    rest = m.group(2).upper()
    words = [w for w in re.split(r"\s+", rest) if w and w not in _DIRECTIONALS]
    return num, (words[0] if words else None)


class ParcelEnrichScraper(BaseScraper[dict[str, Any], dict[str, Any]]):
    """Enrich existing distress-linked metro parcels with MnGeo characteristics.

    RAW    = MnGeo attribute dict already paired with its target parcel_id.
    SIGNAL = a {'parcel_id', 'county_code', 'rec'} update instruction.
    """

    source_name: ClassVar[str] = "parcel_enrich_mngeo"
    signal_type: ClassVar[str] = "parcel_enrichment"

    # ---- Fetch: gather distress-linked parcels, look each up in MnGeo ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        # raw items carry both the MnGeo attributes and the target parcel id.
        raw: list[dict[str, Any]] = []
        timeout = httpx.Timeout(connect=20.0, read=60.0, write=30.0, pool=30.0)
        headers = {"User-Agent": "govire-parcel-enrich/1.0", "Accept": "application/json"}

        async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
            # --- PIN-matched counties ---
            for cc, co_name in _PIN_COUNTY_MAP.items():
                targets = self._load_distress_targets(cc, with_address=False)
                if not targets:
                    continue
                # Map bare PIN -> original parcel_id (+ existing values).
                bare_to_target: dict[str, dict[str, Any]] = {}
                for t in targets:
                    m = re.search(r"(\d{10,16})$", t["parcel_id"])
                    bare = m.group(1) if m else t["parcel_id"]
                    bare_to_target[bare] = t
                bare_pins = list(bare_to_target.keys())

                for i in range(0, len(bare_pins), _PIN_BATCH):
                    batch = bare_pins[i:i + _PIN_BATCH]
                    try:
                        feats = await self._query_pins(client, co_name, batch)
                    except SourceUnavailableError:
                        logger.warning(
                            "MnGeo PIN batch failed; continuing",
                            source=self.source_name, county=cc, batch_start=i,
                        )
                        continue
                    for a in feats:
                        pin = a.get("county_pin")
                        tgt = bare_to_target.get(pin)
                        if tgt:
                            raw.append({
                                "parcel_id": tgt["parcel_id"],
                                "county_code": cc,
                                "existing": tgt,
                                "attrs": a,
                            })
                    await asyncio.sleep(_BATCH_DELAY_SECONDS)

                logger.info(
                    "MnGeo PIN county complete",
                    source=self.source_name, county=cc,
                    targets=len(targets), matched=len(
                        [r for r in raw if r["county_code"] == cc]),
                )

            # --- Address-matched counties (Dakota) ---
            for cc, co_name in _ADDR_COUNTY_MAP.items():
                targets = self._load_distress_targets(cc, with_address=True)
                if not targets:
                    continue
                matched = ambiguous = no_match = 0
                for t in targets:
                    num, street = _parse_address(t.get("address"))
                    if not num or not street:
                        continue
                    try:
                        feats = await self._query_address(client, co_name, num, street)
                    except SourceUnavailableError:
                        continue
                    if len(feats) != 1:
                        if len(feats) == 0:
                            no_match += 1
                        else:
                            ambiguous += 1
                        await asyncio.sleep(_ADDR_DELAY_SECONDS)
                        continue
                    raw.append({
                        "parcel_id": t["parcel_id"],
                        "county_code": cc,
                        "existing": t,
                        "attrs": feats[0],
                    })
                    matched += 1
                    await asyncio.sleep(_ADDR_DELAY_SECONDS)

                logger.info(
                    "MnGeo address county complete",
                    source=self.source_name, county=cc,
                    targets=len(targets), matched=matched,
                    ambiguous=ambiguous, no_match=no_match,
                )

        logger.info("MnGeo fetch complete", source=self.source_name, raw=len(raw))
        return raw

    def _load_distress_targets(self, cc: str, with_address: bool) -> list[dict[str, Any]]:
        """Load distress-linked parcels for a county from the prebuilt helper
        views, then attach the existing core.parcels values we need for the
        non-overwrite checks (and address, for address matching).

        Views (created once in SQL, already in place):
          signals.metro_enrich_targets         (hennepin/ramsey/washington)
          signals.metro_enrich_targets_dakota  (dakota)
        Each returns the distinct distress-linked parcel_ids for its scope.
        """
        # 1. Pull target parcel_ids from the appropriate view.
        view = "metro_enrich_targets_dakota" if with_address else "metro_enrich_targets"
        target_ids: list[str] = []
        page = 0
        while True:
            try:
                q = signals_table(view).select("parcel_id" if with_address
                                               else "county_code,parcel_id")
                if not with_address:
                    q = q.eq("county_code", cc)
                res = q.range(page * _DB_PAGE, page * _DB_PAGE + _DB_PAGE - 1).execute()
            except Exception as e:
                logger.warning("enrich-target view fetch failed",
                               county=cc, view=view, error=str(e))
                break
            rows = res.data or []
            if not rows:
                break
            target_ids.extend(r["parcel_id"] for r in rows if r.get("parcel_id"))
            if len(rows) < _DB_PAGE:
                break
            page += 1

        if not target_ids:
            return []

        # 2. Pull existing values for those parcels (chunked IN queries) so we
        #    can honor non-overwrite rules and (for Dakota) match by address.
        cols = ("parcel_id,year_built,property_type,estimated_market_value"
                + (",address" if with_address else ""))
        by_id: dict[str, dict[str, Any]] = {}
        for j in range(0, len(target_ids), 300):
            chunk = target_ids[j:j + 300]
            try:
                res = (core_table("parcels")
                       .select(cols)
                       .eq("county_code", cc)
                       .in_("parcel_id", chunk)
                       .execute())
                for row in (res.data or []):
                    by_id[row["parcel_id"]] = row
            except Exception as e:
                logger.warning("core.parcels existing-values fetch failed",
                               county=cc, error=str(e))

        targets = list(by_id.values())
        logger.info(
            "Loaded distress targets",
            source=self.source_name, county=cc,
            target_ids=len(target_ids), resolved=len(targets),
        )
        return targets

    async def _query_pins(
        self, client: httpx.AsyncClient, co_name: str, pins: list[str]
    ) -> list[dict[str, Any]]:
        quoted = ",".join("'" + p.replace("'", "''") + "'" for p in pins)
        where = f"co_name='{co_name}' AND county_pin IN ({quoted})"
        return await self._query(client, where, result_count=2000)

    async def _query_address(
        self, client: httpx.AsyncClient, co_name: str, num: str, street: str
    ) -> list[dict[str, Any]]:
        street_esc = street.replace("'", "''")
        where = (f"co_name='{co_name}' AND anumber={num} "
                 f"AND UPPER(st_name) LIKE '{street_esc}%'")
        return await self._query(client, where, result_count=5)

    async def _query(
        self, client: httpx.AsyncClient, where: str, result_count: int
    ) -> list[dict[str, Any]]:
        params = {
            "where": where,
            "outFields": _OUT_FIELDS,
            "returnGeometry": "false",
            "resultRecordCount": str(result_count),
            "f": "json",
        }
        last_err: Exception | None = None
        for attempt in range(3):
            try:
                resp = await client.get(_MNGEO_QUERY, params=params)
                if resp.status_code != 200:
                    raise SourceUnavailableError(
                        f"MnGeo query returned {resp.status_code}",
                        source=self.source_name,
                    )
                data = resp.json()
                if "error" in data:
                    raise SourceUnavailableError(
                        f"MnGeo query error: {data['error']}",
                        source=self.source_name,
                    )
                return [f.get("attributes", {}) for f in data.get("features", [])]
            except (httpx.HTTPError, ValueError) as e:
                last_err = e
                await asyncio.sleep(1.5)
        raise SourceUnavailableError(
            f"MnGeo query failed after retries: {type(last_err).__name__}: {last_err!r}",
            source=self.source_name,
        )

    # ---- Parse: turn raw matches into update instructions ----

    async def parse(self, raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for item in raw_records:
            rec = _build_update_record(item["attrs"], item.get("existing"))
            if rec:
                out.append({
                    "parcel_id": item["parcel_id"],
                    "county_code": item["county_code"],
                    "rec": rec,
                })
        logger.info("Parcel enrich parse complete",
                    source=self.source_name, updates=len(out))
        return out

    # ---- Write: UPDATE existing core.parcels rows ----

    async def write(self, signals: list[dict[str, Any]]) -> tuple[int, int, int]:
        if not signals:
            return 0, 0, 0

        updated = 0
        failed = 0
        now_iso = datetime.now(timezone.utc).isoformat()

        for s in signals:
            rec = dict(s["rec"])
            rec["last_observed_at"] = now_iso
            try:
                (core_table("parcels")
                 .update(rec)
                 .eq("county_code", s["county_code"])
                 .eq("parcel_id", s["parcel_id"])
                 .execute())
                updated += 1
            except Exception as e:
                failed += 1
                logger.warning(
                    "Parcel enrich update failed",
                    source=self.source_name,
                    parcel_id=s["parcel_id"],
                    error=str(e),
                )

        logger.info(
            "Parcel enrich write complete",
            source=self.source_name, updated=updated, failed=failed,
        )
        # Enrichment only updates existing rows: report as records_updated.
        return 0, updated, failed


__all__ = ["ParcelEnrichScraper"]
