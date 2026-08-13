"""
Probe Google Street View coverage against Govire's actual distress parcels.

Run: python outcome_capture/probe_streetview_coverage.py

=== WHY THIS EXISTS (2026-08-13) ===
Before building core.parcel_imagery, the resolver service, the tier gate and
the detail-panel UI, one number has to be real: does Street View actually
cover the parcels we show, or only the metro ones?

Hennepin is 5,648 of 8,299 distress parcels — 68%. A pooled statewide hit
rate would be 68% Hennepin and would tell us nothing about Aitkin, Crow Wing
or Carlton, which is the only part in doubt. So this reports STRICTLY PER
COUNTY and never pools.

=== IT WRITES NOTHING ===
No table, no column, no schema change. Pure measurement. If outstate coverage
comes back weak the design changes shape, and that must be knowable before a
migration exists rather than after.

=== COST: ZERO ===
Street View METADATA requests are documented as free and do not consume image
quota. This never requests an image. It is also the exact call the production
resolver will make, so the hit rate reported here is the real one, not a proxy.

=== THREE MEASURES, NOT ONE ===
1. Does a panorama exist at all.
2. HOW FAR the camera stood from the parcel. A panorama 400m away on a
   township road is a picture of a field. Counting it as "covered" would
   overstate exactly the rural number in doubt, so distance is bucketed and
   a NEAR rate is reported alongside the raw hit rate.
3. Capture date. A 2016 panorama and a 2025 panorama are different products,
   and pano_date vs the distress timeline is the feature that makes this ours
   rather than a Street View embed.

=== SAMPLING ===
RANDOM within each county (ORDER BY random()), never ORDER BY ... LIMIT.
A top-N slice returns whatever the planner returned first, which is an index
artefact, not a sample. Where a county holds fewer parcels than the sample
size, ALL of them are taken and the real n is printed beside every rate — a
rate over n=4 is reported as a rate over n=4, not dressed up as a percentage.

=== WHY DISTRESS PARCELS AND NOT THE SPINE ===
core.parcels is 2.66M rows, but it is the SPINE — the lookup layer that lets
a foreclosure notice resolve to a real address and market value. Nobody
browses it. The product renders signals.distress_with_parcel (8,299 parcels),
and imagery follows what is shown. Sampling the spine would mix in vacant
land, farm and forest parcels and produce a coverage number no subscriber
would ever experience.
"""

from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from math import atan2, cos, radians, sin, sqrt, degrees
from typing import Any

import psycopg2
import requests


META_URL = "https://maps.googleapis.com/maps/api/streetview/metadata"

# Counties chosen to span the coverage question, not to flatter it: three
# metro, one regional centre, four rural. Rural is the part in doubt.
COUNTIES = [
    "hennepin", "ramsey", "dakota",
    "olmsted",
    "crow_wing", "aitkin", "fillmore", "carlton",
]

SAMPLE_PER_COUNTY = 100

# A panorama further than this from the parcel is not a picture OF the
# property. 60m covers a normal street frontage and a deep suburban setback;
# beyond it, the house is not reliably what the camera is pointing at.
NEAR_METRES = 60.0

REQUEST_TIMEOUT = 10
PACE_SECONDS = 0.05
MAX_RETRIES = 3


SAMPLE_SQL = """
SELECT d.county_slug,
       d.eff_parcel_id,
       p.lat::float8  AS lat,
       p.lng::float8  AS lng
FROM  (SELECT DISTINCT county_slug, eff_parcel_id
       FROM   signals.distress_with_parcel) d
JOIN   core.parcels p
       ON  p.county_code = d.county_slug
       AND p.parcel_id   = d.eff_parcel_id
WHERE  d.county_slug = %(county)s
  AND  p.lat IS NOT NULL
  AND  p.lng IS NOT NULL
ORDER  BY random()
LIMIT  %(n)s;
"""

# Total distress parcels per county, and how many carry a coordinate at all.
# Printed so a hit rate is never read without knowing what share of the
# county could even be asked about: a county with 174 parcels and 0
# coordinates has no Street View problem, it has a geocoding problem.
POPULATION_SQL = """
SELECT d.county_slug,
       count(*)                                                  AS parcels,
       count(*) FILTER (WHERE p.lat IS NOT NULL
                         AND  p.lng IS NOT NULL)                 AS with_point
FROM  (SELECT DISTINCT county_slug, eff_parcel_id
       FROM   signals.distress_with_parcel) d
LEFT   JOIN core.parcels p
       ON  p.county_code = d.county_slug
       AND p.parcel_id   = d.eff_parcel_id
WHERE  d.county_slug = ANY(%(counties)s)
GROUP  BY 1;
"""


def log(msg: str) -> None:
    print(f"[sv-probe] {msg}", flush=True)


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Metres between two WGS84 points."""
    r = 6371000.0
    p1, p2 = radians(lat1), radians(lat2)
    dp = radians(lat2 - lat1)
    dl = radians(lng2 - lng1)
    a = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 2 * r * atan2(sqrt(a), sqrt(1 - a))


def bearing_deg(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Compass bearing FROM the camera TO the parcel.

    This is the heading the production resolver will store. Street View
    defaults the camera to the PANORAMA's own heading, not toward the
    requested point — which on a residential street regularly frames the
    house opposite. Computing it is the difference between a photo of the
    property and a photo of its neighbour.
    """
    p1, p2 = radians(lat1), radians(lat2)
    dl = radians(lng2 - lng1)
    y = sin(dl) * cos(p2)
    x = cos(p1) * sin(p2) - sin(p1) * cos(p2) * cos(dl)
    return (degrees(atan2(y, x)) + 360.0) % 360.0


def fetch_metadata(key: str, lat: float, lng: float) -> dict[str, Any] | None:
    """One metadata lookup. Returns Google's parsed JSON, or None on failure.

    Distinguishes a transport failure (retry) from an honest ZERO_RESULTS
    (do not retry — the answer is "no imagery", and retrying would turn a
    real finding into a slow one).
    """
    params = {"location": f"{lat},{lng}", "key": key}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(META_URL, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            log(f"HTTP {resp.status_code}: {resp.text[:200]}")
            return None
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                log(f"request failed after {MAX_RETRIES}: {type(e).__name__}")
                return None
            time.sleep(2 ** attempt)
    return None


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1
    if not key:
        log("FATAL: GOOGLE_MAPS_API_KEY is not set")
        return 1

    conn = psycopg2.connect(dsn)
    results: dict[str, list[dict[str, Any]]] = defaultdict(list)
    population: dict[str, tuple[int, int]] = {}

    try:
        with conn.cursor() as cur:
            cur.execute(POPULATION_SQL, {"counties": COUNTIES})
            for county, parcels, with_point in cur.fetchall():
                population[county] = (parcels, with_point)

        # One key check before spending the run: an invalid or unauthorised
        # key returns REQUEST_DENIED on every call, and 800 identical denials
        # reported as "0% coverage" would be a false finding about Minnesota
        # rather than a true one about the key.
        probe = fetch_metadata(key, 44.977053, -93.066465)
        if probe is None:
            log("FATAL: metadata endpoint unreachable")
            return 1
        if probe.get("status") == "REQUEST_DENIED":
            log(f"FATAL: REQUEST_DENIED — {probe.get('error_message', 'no detail')}")
            log("Check the key is enabled for Street View Static API.")
            return 1
        log(f"key OK — control point returned status={probe.get('status')}")

        for county in COUNTIES:
            with conn.cursor() as cur:
                cur.execute(SAMPLE_SQL, {"county": county, "n": SAMPLE_PER_COUNTY})
                rows = cur.fetchall()

            if not rows:
                log(f"{county}: no distress parcels with coordinates — skipped")
                continue

            log(f"{county}: probing {len(rows)} parcels")
            for _, parcel_id, lat, lng in rows:
                meta = fetch_metadata(key, lat, lng)
                time.sleep(PACE_SECONDS)

                if meta is None:
                    results[county].append({"status": "error"})
                    continue

                status = meta.get("status")
                if status != "OK":
                    results[county].append({"status": status})
                    continue

                loc = meta.get("location") or {}
                plat, plng = loc.get("lat"), loc.get("lng")
                dist = (
                    haversine_m(lat, lng, plat, plng)
                    if plat is not None and plng is not None
                    else None
                )
                results[county].append({
                    "status": "OK",
                    "date": meta.get("date"),
                    "distance_m": dist,
                    "heading": (
                        bearing_deg(plat, plng, lat, lng)
                        if plat is not None and plng is not None
                        else None
                    ),
                })

        # ---- report ----
        print()
        log("=" * 68)
        log("COVERAGE BY COUNTY — never pooled; Hennepin is 68% of all")
        log("distress parcels and would dominate any statewide average.")
        log("=" * 68)
        print()
        print(f"{'county':<12} {'n':>4} {'pano':>6} {'near':>6} "
              f"{'med_m':>7} {'oldest':>8} {'newest':>8}  of_county")

        for county in COUNTIES:
            rows = results.get(county) or []
            if not rows:
                continue
            n = len(rows)
            ok = [r for r in rows if r["status"] == "OK"]
            near = [r for r in ok
                    if r["distance_m"] is not None and r["distance_m"] <= NEAR_METRES]
            dists = sorted(r["distance_m"] for r in ok if r["distance_m"] is not None)
            med = dists[len(dists) // 2] if dists else None
            dates = sorted(r["date"] for r in ok if r.get("date"))

            parcels, with_point = population.get(county, (0, 0))
            print(
                f"{county:<12} {n:>4} "
                f"{100.0 * len(ok) / n:>5.0f}% "
                f"{100.0 * len(near) / n:>5.0f}% "
                f"{(f'{med:.0f}' if med is not None else '—'):>7} "
                f"{(dates[0] if dates else '—'):>8} "
                f"{(dates[-1] if dates else '—'):>8}  "
                f"{with_point}/{parcels} have coords"
            )

        print()
        log("pano% = a panorama exists.  near% = AND within "
            f"{NEAR_METRES:.0f}m of the parcel.")
        log("near% is the honest coverage figure: a panorama 400m away on a")
        log("township road is a picture of a field, not of the property.")

        # Failure modes, named. A silent 0% could be no coverage, a denied
        # key, or an over-quota account — three different problems.
        bad = defaultdict(int)
        for rows in results.values():
            for r in rows:
                if r["status"] not in ("OK", "ZERO_RESULTS"):
                    bad[r["status"]] += 1
        if bad:
            print()
            log("NON-OK STATUSES (not 'no coverage' — investigate):")
            for status, count in sorted(bad.items(), key=lambda kv: -kv[1]):
                log(f"  {status}: {count}")

        return 0
    except Exception as e:
        log(f"FAILED — {type(e).__name__}: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
