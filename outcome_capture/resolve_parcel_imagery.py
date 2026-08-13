"""
Resolve Street View panoramas and aerial availability into core.parcel_imagery.

Run: python outcome_capture/resolve_parcel_imagery.py

=== THE ONLY WRITER ===
core.parcel_imagery has exactly one writer, and this is it. No parcel loader
touches it, no enrichment path can clobber it.

That is deliberate. core.parcels has TWO writer classes that disagree — county
loaders upserting direct with exclude_none=True, and signal scrapers going
through parcel_resolver's fill-in merge — and a column there is subject to
both. Measured 2026-08-13: 6,268 rows carried ANOTHER county's property_type
because exclude_none dropped the key from the UPDATE, and parcel_resolver's
fill-in rule can only write when the existing value is null, so nothing in the
system could ever correct it. geom was worse: a column with an index, no
writer and no owner, stale on 1.5M rows and WRONG on 18,415 for eleven days.
A field nobody owns rots. This one has an owner.

=== SELF-RESUMING, AND RE-RESOLVING ===
The working set is "distress parcels with coordinates that have no imagery row
for this source yet, OR whose row says no_location but which now HAVE
coordinates". A parcel with a settled answer is never revisited, so this can be
re-run at will and picks up where it stopped — same property as
backfill_parcel_geom.py, and for the same reason: a run cut off half way costs
a re-run, not a restart.

The distress set GROWS (8,299 -> 8,321 in thirteen hours), so re-running after
a scrape is the normal way new parcels get imagery.

WHY no_location IS NOT SETTLED (added 2026-08-13):
no_location means "we looked and there was nowhere to look" — a GEOCODING gap,
not an imagery one. Geocoding gaps get fixed. Washington County is the worked
example: all 182 of its distress parcels had no lat/lng because
washington_parcels.py requested geometry=false, so every one got a no_location
row. When that loader was changed to fetch centroids, 118,386 Washington
parcels gained coordinates in a single 6-minute run — and the resolver skipped
every one of them, because a row existed and "a completed parcel is never
revisited".

That is the failure shape this whole codebase keeps producing: a value that was
true when written, silently outliving the condition that made it true. geom sat
wrong on 18,415 rows for eleven days for the same reason.

So no_location is provisional by construction, and the predicate re-opens it
the moment the parcel has somewhere to look. UPSERT_SQL already carries
ON CONFLICT ... DO UPDATE, so a re-resolved row overwrites cleanly and
resolved_at advances — which is what that column exists for.

=== STREET VIEW IS ASKED. AERIAL IS DERIVED. ===
Street View has a FREE metadata endpoint and coverage genuinely varies —
measured 2026-08-13, near-coverage was 92% in ramsey and hennepin but 45% in
crow_wing and 37% in fillmore and carlton. Per-parcel, the answer is unknowable
without asking, and asking is free.

Aerial is the opposite. Maps Static has NO metadata endpoint — Google returns
an image for every coordinate on Earth and no status field says whether it
resolves anything. The only way to check per parcel is to FETCH the tile, and
every fetch is BILLABLE. Resolving aerial for 7,453 parcels would burn 7,453
of the 10,000/month Essentials free requests in a single run, leaving almost
nothing for actual subscriber views.

And there is nothing to discover. The aerial probe measured tile detail across
eight counties and three zooms: low% was ZERO everywhere except fillmore at
z19 (5%, flat farmland), with median tiles of 97-135 KB against a 12 KB
low-detail floor. Carlton — the WORST Street View county at 37% — had the
HIGHEST aerial detail at 135 KB.

So aerial availability follows from having a coordinate, and the row is
written without calling Google at all. Pretending to measure per-parcel what
was measured statistically would cost money and add nothing.

=== too_far IS NOT no_imagery ===
Street View snaps to the nearest panorama, which may be a long way off. A
panorama 400m up a township road is a picture of a field, and calling that
"covered" overstates exactly the rural number that was in doubt.

The threshold DEPENDS ON WHAT YOU ARE TRYING TO SEE:
  * A HOUSE: beyond ~60m you are looking at a streetscape containing three
    houses and cannot tell which is the subject. Condition is unjudgeable.
  * LAND: there is no building to frame. A panorama 150m along the road still
    shows the access track, the treeline, whether it is tilled or wooded —
    which is what a land buyer is asking.

Same distance, opposite answer, because the subject differs. So the threshold
is per property type.

property_type is NULL on all 483,218 MnGeo-county rows (the layer does not
publish it — that is how the exclude_none defect was found), which is exactly
the rural population where the land threshold matters most. So lot_sqft is the
fallback: over ~2 acres is treated as land. It is populated on 96.3% of those
rows and is a better proxy than defaulting everything to 60m and marking half
of Fillmore too_far.

=== HEADING IS COMPUTED, NOT DEFAULTED ===
Street View Static points the camera at the PANORAMA's own default heading,
not at the location requested. On a residential street that regularly frames
the house OPPOSITE. The stored heading is the bearing from where the camera
stood to the parcel — the difference between a photo of the property and a
photo of its neighbour.

=== WHAT IS NEVER STORED ===
Google's terms prohibit pre-fetching, indexing, storing, resharing or
rehosting Maps Content, and name bulk download of Street View images
specifically. The PANORAMA ID is exempt and storable indefinitely.

pano_id yes. Pixels never. The browser fetches each image from Google directly
at render time.
"""

from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from math import atan2, cos, radians, sin, sqrt, degrees
from typing import Any

import psycopg2
import psycopg2.extras
import requests


META_URL = "https://maps.googleapis.com/maps/api/streetview/metadata"

BATCH_SIZE = 500
PACE_SECONDS = 0.05
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
MAX_BATCHES = 200

# See "too_far IS NOT no_imagery" above. Two thresholds because the subject
# differs, not because rural deserves a lower bar.
NEAR_METRES_STRUCTURE = 60.0
NEAR_METRES_LAND = 200.0

# ~2 acres. The fallback when property_type is NULL, which it is on every
# MnGeo-county row. A parcel this size is not a house on a lot.
LAND_LOT_SQFT = 87120

LAND_TYPES = {"land", "agricultural"}

# Measured 2026-08-13 across eight counties at z17/18/19: detail is equivalent
# at all three in every county, INCLUDING rural. Zoom was expected to be
# county-dependent and is not. z18 frames more of the lot than z19 and avoids
# fillmore's only soft spot (5% low at z19), so one global value, no per-county
# table to maintain.
AERIAL_ZOOM = 18


WORKING_SET_SQL = """
SELECT d.county_slug                          AS county_code,
       d.eff_parcel_id                        AS parcel_id,
       p.lat::float8                          AS lat,
       p.lng::float8                          AS lng,
       p.property_type,
       p.lot_sqft
FROM  (SELECT DISTINCT county_slug, eff_parcel_id
       FROM   signals.distress_with_parcel) d
JOIN   core.parcels p
       ON  p.county_code = d.county_slug
       AND p.parcel_id   = d.eff_parcel_id
LEFT   JOIN core.parcel_imagery i
       ON  i.county_code = p.county_code
       AND i.parcel_id   = p.parcel_id
       AND i.source      = 'google_streetview'
WHERE  p.lat IS NOT NULL
  AND  p.lng IS NOT NULL
  AND  (
         -- never resolved
         i.parcel_id IS NULL
         -- OR resolved as no_location BEFORE the parcel had coordinates.
         -- That verdict was true when written and is not any more; see the
         -- module docstring. Anything else (ok / no_imagery / too_far) is a
         -- real answer about Google's coverage and stays settled.
      OR i.status = 'no_location'
       )
LIMIT  %(batch)s;
"""

# Parcels that exist in the spine but STILL carry no coordinate. These get a
# no_location row rather than being skipped silently: "we checked and there is
# nowhere to look" is a different fact from "we never checked", and a missing
# row cannot express either. 868 parcels were in this state on 2026-08-13;
# Washington's 182 have since left it.
#
# This and WORKING_SET_SQL are mutually exclusive by their lat/lng predicates —
# a parcel either has coordinates (main loop) or does not (here), never both.
# The main loop re-opens a no_location row only once coordinates exist, so the
# two cannot fight over the same parcel in the same run.
NO_LOCATION_SQL = """
SELECT d.county_slug                          AS county_code,
       d.eff_parcel_id                        AS parcel_id
FROM  (SELECT DISTINCT county_slug, eff_parcel_id
       FROM   signals.distress_with_parcel) d
JOIN   core.parcels p
       ON  p.county_code = d.county_slug
       AND p.parcel_id   = d.eff_parcel_id
LEFT   JOIN core.parcel_imagery i
       ON  i.county_code = p.county_code
       AND i.parcel_id   = p.parcel_id
       AND i.source      = 'google_streetview'
WHERE  i.parcel_id IS NULL
  AND (p.lat IS NULL OR p.lng IS NULL)
LIMIT  %(batch)s;
"""

UPSERT_SQL = """
INSERT INTO core.parcel_imagery
    (county_code, parcel_id, source, status, pano_id, pano_date,
     pano_lat, pano_lng, distance_m, heading_deg, zoom,
     resolved_at, error_detail, updated_at)
VALUES %s
ON CONFLICT (county_code, parcel_id, source) DO UPDATE SET
    status       = EXCLUDED.status,
    pano_id      = EXCLUDED.pano_id,
    pano_date    = EXCLUDED.pano_date,
    pano_lat     = EXCLUDED.pano_lat,
    pano_lng     = EXCLUDED.pano_lng,
    distance_m   = EXCLUDED.distance_m,
    heading_deg  = EXCLUDED.heading_deg,
    zoom         = EXCLUDED.zoom,
    resolved_at  = EXCLUDED.resolved_at,
    error_detail = EXCLUDED.error_detail,
    updated_at   = now();
"""


def log(msg: str) -> None:
    print(f"[imagery] {msg}", flush=True)


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371000.0
    p1, p2 = radians(lat1), radians(lat2)
    dp = radians(lat2 - lat1)
    dl = radians(lng2 - lng1)
    a = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 2 * r * atan2(sqrt(a), sqrt(1 - a))


def bearing_deg(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Bearing FROM the camera TO the parcel. See the module docstring."""
    p1, p2 = radians(lat1), radians(lat2)
    dl = radians(lng2 - lng1)
    y = sin(dl) * cos(p2)
    x = cos(p1) * sin(p2) - sin(p1) * cos(p2) * cos(dl)
    return (degrees(atan2(y, x)) + 360.0) % 360.0


def near_threshold(property_type: str | None, lot_sqft: int | None) -> float:
    """How far is too far, for THIS parcel.

    property_type first where it exists; lot_sqft as the fallback, because
    property_type is NULL on every MnGeo-county row and those are exactly the
    rural parcels the land threshold is for.
    """
    if property_type and property_type.lower() in LAND_TYPES:
        return NEAR_METRES_LAND
    if property_type is None and lot_sqft and lot_sqft >= LAND_LOT_SQFT:
        return NEAR_METRES_LAND
    return NEAR_METRES_STRUCTURE


def fetch_metadata(key: str, lat: float, lng: float) -> dict[str, Any] | None:
    """One metadata lookup. Free — does not consume image quota.

    A transport failure is retried. ZERO_RESULTS is NOT: it is an answer, and
    retrying it turns a real finding into a slow one.
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
                return None
            time.sleep(2 ** attempt)
    return None


def resolve_one(key: str, row: dict[str, Any], now_iso: str) -> list[tuple]:
    """Resolve one parcel into its Street View row and its aerial row."""
    cc, pid = row["county_code"], row["parcel_id"]
    lat, lng = row["lat"], row["lng"]

    meta = fetch_metadata(key, lat, lng)

    if meta is None:
        sv = (cc, pid, "google_streetview", "error", None, None,
              None, None, None, None, None, now_iso,
              "metadata request failed", now_iso)
    else:
        status = meta.get("status")
        if status == "OK":
            loc = meta.get("location") or {}
            plat, plng = loc.get("lat"), loc.get("lng")
            pano = meta.get("pano_id")
            if plat is None or plng is None or not pano:
                # OK without a usable location or id would violate
                # parcel_imagery_ok_has_pano_ck. Record it as an error rather
                # than letting the database reject the whole batch.
                sv = (cc, pid, "google_streetview", "error", None, None,
                      None, None, None, None, None, now_iso,
                      "OK without pano_id or location", now_iso)
            else:
                dist = haversine_m(lat, lng, plat, plng)
                limit = near_threshold(row.get("property_type"),
                                       row.get("lot_sqft"))
                if dist <= limit:
                    sv = (cc, pid, "google_streetview", "ok", pano,
                          meta.get("date"), plat, plng, round(dist, 1),
                          round(bearing_deg(plat, plng, lat, lng), 2),
                          None, now_iso, None, now_iso)
                else:
                    # A panorama exists but is not looking at this property.
                    # pano_id must be NULL here — notok_has_no_pano_ck — and
                    # that is correct: an ID we will never render is a stale
                    # value waiting to be mistaken for coverage. distance_m is
                    # kept, so the reason stays inspectable.
                    sv = (cc, pid, "google_streetview", "too_far", None,
                          meta.get("date"), plat, plng, round(dist, 1),
                          None, None, now_iso,
                          f"nearest panorama {dist:.0f}m away "
                          f"(limit {limit:.0f}m)", now_iso)
        elif status == "ZERO_RESULTS":
            sv = (cc, pid, "google_streetview", "no_imagery", None, None,
                  None, None, None, None, None, now_iso, None, now_iso)
        else:
            sv = (cc, pid, "google_streetview", "error", None, None,
                  None, None, None, None, None, now_iso,
                  f"metadata status={status}", now_iso)

    # Aerial: derived, never fetched. See the module docstring.
    aerial = (cc, pid, "google_aerial", "ok", None, None,
              None, None, None, None, AERIAL_ZOOM, now_iso, None, now_iso)

    return [sv, aerial]


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1
    if not key:
        log("FATAL: GOOGLE_MAPS_API_KEY is not set")
        return 1
    dry_run = os.environ.get("DRY_RUN") == "1"

    # Control point before spending a run. A denied key returns REQUEST_DENIED
    # on every call, and 7,453 identical denials written as status='error'
    # would be a false record about Minnesota rather than a true one about the
    # key. 810 Maryland Ave E, Saint Paul — the same point
    # backfill_parcel_geom.py verified its coordinate order against.
    probe = fetch_metadata(key, 44.977053, -93.066465)
    if probe is None:
        log("FATAL: metadata endpoint unreachable")
        return 1
    if probe.get("status") == "REQUEST_DENIED":
        log(f"FATAL: REQUEST_DENIED — {probe.get('error_message', 'no detail')}")
        return 1
    log(f"key OK — control point status={probe.get('status')}")

    conn = psycopg2.connect(dsn)
    counts: dict[str, int] = defaultdict(int)
    written = 0
    batches = 0
    started = time.monotonic()

    try:
        # no_location rows first: cheap, no API calls, and it makes the
        # 868 unlocatable parcels visible as a recorded fact rather than as
        # an absence indistinguishable from "not yet processed".
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(NO_LOCATION_SQL, {"batch": 5000})
            nl = cur.fetchall()
        if nl and not dry_run:
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
            rows = []
            for r in nl:
                for src in ("google_streetview", "google_aerial"):
                    rows.append((r["county_code"], r["parcel_id"], src,
                                 "no_location", None, None, None, None, None,
                                 None, None, now_iso,
                                 "parcel has no lat/lng", now_iso))
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, UPSERT_SQL, rows)
            conn.commit()
            counts["no_location"] += len(nl)
            log(f"{len(nl)} parcels recorded as no_location (no API calls)")

        while batches < MAX_BATCHES:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(WORKING_SET_SQL, {"batch": BATCH_SIZE})
                work = [r for r in cur.fetchall()
                        if r["lat"] is not None and r["lng"] is not None]

            if not work:
                break

            if dry_run:
                log(f"DRY_RUN=1 — {len(work)} parcels would be resolved")
                return 0

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
            rows: list[tuple] = []
            for r in work:
                pair = resolve_one(key, r, now_iso)
                counts[pair[0][3]] += 1
                rows.extend(pair)
                time.sleep(PACE_SECONDS)

            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, UPSERT_SQL, rows)
            conn.commit()

            written += len(work)
            batches += 1
            elapsed = time.monotonic() - started
            log(f"{written} parcels resolved "
                f"({batches} batches, {written / elapsed:.1f}/sec)")

        if batches >= MAX_BATCHES:
            log(f"WARNING: hit MAX_BATCHES ({MAX_BATCHES}) — re-run to finish")

        print()
        log(f"done: {written} parcels resolved")
        for status, n in sorted(counts.items(), key=lambda kv: -kv[1]):
            log(f"  {status}: {n}")
        log("Every parcel also has a google_aerial row — derived from having "
            "a coordinate, not fetched. See the module docstring.")
        return 0
    except Exception as e:
        conn.rollback()
        log(f"FAILED — {type(e).__name__}: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
