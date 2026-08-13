"""
Probe Google satellite (Maps Static) imagery quality over Govire's parcels.

Run: python outcome_capture/probe_aerial_coverage.py

=== WHY THIS EXISTS (2026-08-13) ===
probe_streetview_coverage.py measured Street View and found metro strong
(hennepin 92% near, ramsey 92%) and rural weak (crow_wing 45%, fillmore 37%,
carlton 37%). Satellite fills that gap — and on acreage it is arguably the
BETTER image: a 40-acre Fillmore parcel shot from the road is a mailbox and a
treeline; from above it is the house, the outbuildings, the access track and
how much is tilled versus wooded.

Both images ship on every parcel that has them. This is not a fallback.

=== WHAT THIS CAN AND CANNOT ASK ===
Street View has a metadata endpoint: a free, honest "is there imagery here?".
MAPS STATIC HAS NO EQUIVALENT. Google returns an image for every coordinate
on Earth. At zoom 19 over Fillmore that may be a crisp roof or it may be a
green smear, and no status field distinguishes them.

So this cannot ask about coverage. It has to FETCH the tiles and measure
whether they carry detail:

  * byte size    — a detailed tile does not compress small. A flat green or
                   grey placeholder does.
  * pixel stddev — real aerial has edges (roofs, roads, field boundaries).
                   Low variance means nothing is resolving.
  * unique colours — a smear has few; a real scene has thousands.

The QUESTION IS NOT "does satellite exist" (it always does). It is "at what
zoom does a structure actually resolve in THIS county" — a design parameter
that decides the rural default, and one that would otherwise be guessed at
using metro settings.

=== THIS ONE COSTS MONEY ===
Unlike the Street View probe, there is no free metadata tier. Every request
here is billable. Sample is deliberately small — 20 parcels x 8 counties x
2-3 zooms is ~400 requests, comfortably inside the Essentials monthly free
threshold, and enough to separate crisp from blurry.

=== ZOOM LEVELS ===
z=18  suburban lot scale — frames a typical metro parcel
z=19  structure scale — should resolve a roof
z=17  ADDED FOR RURAL ONLY. On a 40-acre parcel z=19 shows one corner of a
      field. Acreage needs to be seen whole, so the rural default may well
      be a lower zoom than the metro one — which is exactly the parameter
      this probe exists to establish rather than assume.

=== SAMPLING ===
RANDOM within each county, never ORDER BY ... LIMIT: a top-N slice returns
whatever the planner returned first, which is an index artefact and not a
sample. Reported strictly per county — hennepin is 68% of all distress
parcels and would dominate any pooled figure.
"""

from __future__ import annotations

import io
import os
import statistics
import sys
import time
from collections import defaultdict
from typing import Any

import psycopg2
import requests

try:
    from PIL import Image
except ImportError:
    Image = None


STATIC_URL = "https://maps.googleapis.com/maps/api/staticmap"

METRO = ["hennepin", "ramsey", "dakota", "olmsted"]
RURAL = ["crow_wing", "fillmore", "carlton", "cass"]
COUNTIES = METRO + RURAL

SAMPLE_PER_COUNTY = 20

METRO_ZOOMS = [18, 19]
RURAL_ZOOMS = [17, 18, 19]

SIZE = "400x400"
SCALE = 1

REQUEST_TIMEOUT = 15
PACE_SECONDS = 0.08
MAX_RETRIES = 3

# Below this, a tile is almost certainly not resolving detail. Calibrated as
# an ORDER OF MAGNITUDE, not a precise threshold: a 400x400 aerial with roofs
# and roads runs tens of KB, a flat green field a few KB. Reported alongside
# the raw numbers so the judgement stays visible rather than buried.
LOW_DETAIL_BYTES = 12000
LOW_DETAIL_STDDEV = 18.0


SAMPLE_SQL = """
SELECT d.county_slug,
       d.eff_parcel_id,
       p.lat::float8  AS lat,
       p.lng::float8  AS lng,
       p.property_type,
       p.lot_sqft
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


def log(msg: str) -> None:
    print(f"[aerial-probe] {msg}", flush=True)


def fetch_tile(key: str, lat: float, lng: float, zoom: int) -> bytes | None:
    """One satellite tile. Returns raw PNG bytes, or None on failure.

    maptype=satellite, no markers, no labels — a marker pin would add its own
    pixels and inflate both size and variance, which are the two things being
    measured. The tile must be the bare image.
    """
    params = {
        "center": f"{lat},{lng}",
        "zoom": str(zoom),
        "size": SIZE,
        "scale": str(SCALE),
        "maptype": "satellite",
        "format": "png",
        "key": key,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(STATIC_URL, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.content
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


def measure(png: bytes) -> dict[str, Any]:
    """Detail metrics for one tile.

    stddev over greyscale is the honest discriminator: a flat field and a
    crisp roof can compress to similar sizes under some encoders, but they
    cannot have similar pixel variance.
    """
    out: dict[str, Any] = {"bytes": len(png), "stddev": None, "colours": None}
    if Image is None:
        return out
    try:
        img = Image.open(io.BytesIO(png)).convert("RGB")
        grey = img.convert("L")
        px = list(grey.getdata())
        if px:
            out["stddev"] = statistics.pstdev(px)
        out["colours"] = len(set(img.getdata()))
    except Exception as e:
        log(f"measure failed: {type(e).__name__}")
    return out


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1
    if not key:
        log("FATAL: GOOGLE_MAPS_API_KEY is not set")
        return 1
    if Image is None:
        log("FATAL: Pillow not installed — stddev and colour metrics need it")
        return 1

    # One control fetch before spending the run. An unauthorised key returns
    # a small error IMAGE with HTTP 200 — it does not raise — so a denied key
    # would otherwise read as "every county has low detail", a false finding
    # about Minnesota rather than a true one about the key.
    control = fetch_tile(key, 44.977053, -93.066465, 19)
    if control is None:
        log("FATAL: static maps endpoint unreachable")
        return 1
    cm = measure(control)
    if cm["bytes"] < LOW_DETAIL_BYTES or (cm["stddev"] or 0) < LOW_DETAIL_STDDEV:
        log(f"FATAL: control tile has no detail "
            f"(bytes={cm['bytes']}, stddev={cm['stddev']})")
        log("This is downtown Saint Paul at z19 — it MUST be detailed.")
        log("Almost certainly a denied key or an unenabled Maps Static API.")
        return 1
    log(f"key OK — control tile bytes={cm['bytes']} "
        f"stddev={cm['stddev']:.1f} colours={cm['colours']}")

    conn = psycopg2.connect(dsn)
    results: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)

    try:
        for county in COUNTIES:
            zooms = RURAL_ZOOMS if county in RURAL else METRO_ZOOMS
            with conn.cursor() as cur:
                cur.execute(SAMPLE_SQL, {"county": county, "n": SAMPLE_PER_COUNTY})
                rows = cur.fetchall()

            if not rows:
                log(f"{county}: no distress parcels with coordinates — skipped")
                continue

            log(f"{county}: {len(rows)} parcels x zooms {zooms}")
            for _, _pid, lat, lng, _ptype, _lot in rows:
                for zoom in zooms:
                    png = fetch_tile(key, lat, lng, zoom)
                    time.sleep(PACE_SECONDS)
                    if png is None:
                        results[(county, zoom)].append({"bytes": 0, "stddev": 0.0})
                        continue
                    results[(county, zoom)].append(measure(png))

        print()
        log("=" * 72)
        log("AERIAL DETAIL BY COUNTY AND ZOOM — never pooled.")
        log("Satellite ALWAYS returns an image; the question is whether it")
        log("resolves anything. low% = tiles below both detail thresholds.")
        log("=" * 72)
        print()
        print(f"{'county':<12} {'z':>3} {'n':>4} {'med_kb':>7} {'med_sd':>7} "
              f"{'med_col':>8} {'low%':>6}")

        for county in COUNTIES:
            zooms = RURAL_ZOOMS if county in RURAL else METRO_ZOOMS
            for zoom in zooms:
                rows = results.get((county, zoom)) or []
                if not rows:
                    continue
                n = len(rows)
                sizes = sorted(r["bytes"] for r in rows)
                sds = sorted(r["stddev"] or 0.0 for r in rows)
                cols = sorted(r.get("colours") or 0 for r in rows)
                low = sum(
                    1 for r in rows
                    if r["bytes"] < LOW_DETAIL_BYTES
                    or (r["stddev"] or 0) < LOW_DETAIL_STDDEV
                )
                print(
                    f"{county:<12} {zoom:>3} {n:>4} "
                    f"{sizes[n // 2] / 1024:>7.1f} "
                    f"{sds[n // 2]:>7.1f} "
                    f"{cols[n // 2]:>8} "
                    f"{100.0 * low / n:>5.0f}%"
                )

        print()
        log(f"low% counts tiles under {LOW_DETAIL_BYTES // 1000}KB or stddev "
            f"{LOW_DETAIL_STDDEV:.0f} — a green smear, not a property.")
        log("Compare zooms WITHIN a county: the zoom where low% collapses is")
        log("that county's usable default. Rural may sit lower than metro,")
        log("which is the parameter this probe exists to establish.")
        return 0
    except Exception as e:
        log(f"FAILED — {type(e).__name__}: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
