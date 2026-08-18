"""
Geocode stub parcels that carry a real address but no coordinates.

Run: python outcome_capture/backfill_stub_geocode.py
     DRY_RUN=1 python outcome_capture/backfill_stub_geocode.py   (no writes)

=== WHY THIS EXISTS (2026-08-18) ===
222 parcels on synthetic '{COUNTY}-FC-...' keys carry a real street address --
'20088 FERRET ST, NOWTHEN' , '7969 East River Road, Fridley' -- and NULL
lat/lng. A person could drive to any of them. The product shows no map pin and
no Street View, because every downstream consumer of position reads lat/lng.

This surfaced from the wrong end. audit.run_integrity_checks() reports 406
imagery rows on stubs; 348 of them are status='no_location' with the error
'parcel has no lat/lng'. The first instinct was to delete those 348 as noise.
That would have been wrong three times over:

  1. They are not noise. They are the resolver's own record that it looked and
     there was nowhere to look -- outcome_capture/resolve_parcel_imagery.py
     says it outright: "'we checked and there is nowhere to look' is a
     different fact from 'we never checked', and a missing row cannot express
     either."
  2. WORKING_SET_SQL in that same file RE-OPENS a no_location row the moment
     the parcel gains coordinates ("that verdict was true when written and is
     not any more"). Deleting them would have dropped the parcels into
     NO_LOCATION_SQL instead, which only matches rows with no imagery row at
     all -- back to square one.
  3. The imagery rows were a symptom correctly reporting a real gap. Fix the
     gap and they resolve themselves.

So: no deletes. Geocode the parcels, and the existing resolver does the rest
on its next run.

=== WHY A STUB WITH COORDINATES IS NOT A CORRUPTION ===
MIGRATION_integrity_findings_20260818.sql, check 5:
    "DO NOT let anything delete these automatically. Dakota's stubs DO carry
     lat/lng from ArcGIS geometry, and two of its rows held a real
     google_streetview pano_id. status='ok' rows are genuine images."
Dakota's stubs already work this way and produced 23 real panoramas. This makes
the other counties match the case that already works.

Coordinates give a map pin and an imagery lookup. They do NOT identify a
parcel: src/services/spine_resolver.py records that core.parcels.geom is
geography(Point,4326) -- centroids, not boundaries -- so point-in-polygon is
impossible against this table. Nothing here changes that.

=== geom NEEDS NO SECOND PASS ===
MIGRATION_parcels_geom_generated_2026-08-13.sql made geom a GENERATED column
derived from lat/lng. Writing lat/lng writes geom. Do not run
backfill_parcel_geom.py after this; there is nothing for it to do.

=== THE STUB SCOPING WAS WRONG (corrected 2026-08-18, same day) ===
The first version of this file selected only synthetic '{COUNTY}-FC-...' keys,
because the 222 were found through an imagery counter that happens to be
stub-scoped. Measured after that run cleared them:

    parcels with lat IS NULL and a usable address
      NOT a stub (real spine parcel_id)   136
      a stub                                2

So the stub condition excluded 136 REAL parcels with real addresses and no
map pin -- dakota 111, washington 15, hennepin 9, anoka 1, created between
2026-06-03 and 2026-08-08, so a steady trickle rather than one bad load.
Whether a parcel_id is synthetic has nothing to do with whether the property
can be located; the predicate is now just "usable address, no coordinates".

SEPARATELY AND NOT ADDRESSED HERE: 97 parcels in pine (65), rock (23) and
le_sueur (9) have no coordinates AND NO ADDRESS AT ALL, every one created at
2026-08-11 13:38:20.20743 -- the same microsecond, so one load by one writer.
Nothing to geocode; they need whatever wrote them looked at instead. They fail
the address predicate below and are correctly untouched by this script.

=== THE TWO ADDRESS SHAPES (measured 2026-08-18, all 222) ===
  212  street only            -> compose '<street>, <city>, MN <zip>'
   10  already full           -> use the address verbatim

The 10 self-contained ones look like '1017 9th Street N, Moorhead, Minnesota
56560' -- city and state already inside the field, and zip NULL in its own
column because it lives in the string. Composing those would repeat the city
and drop the zip; split_part() on the comma would delete the city entirely.
Detected by ', MN' / ', Minnesota' rather than by county or by length.

Three of the 212 are multi-property notices:
    '101 Charles Street NE, 179 Charles Street NE, 180 Charles Street NE'
    '1320  1340 & 1350 Lagoon Ave, Unit #s'
    '301 Clifton Ave Units 4G, G3 and G5'
split_part(address, ',', 1) takes the first, which is the right answer for a
package: one pin on the first property beats no pin on any of them. It is a
no-op on the 209 addresses with no comma at all.

115 of 222 carry a unit suffix ('UNIT 132', '#5', 'Apt 4'). Mapbox returns the
BUILDING for these, which is what an imagery lookup wants -- a Street View
panorama is of the building whatever the unit number.

=== COORDINATE ORDER -- the one thing that must not be got wrong ===
Mapbox GeoJSON returns coordinates as [longitude, latitude]. Reversing them
does not fail loudly: it places every Minnesota parcel in Somalia and the
imagery resolver simply finds nothing. src/services/geocoder.py unpacks
`lng, lat = coords[0], coords[1]` and this file does the same, in one place,
with the sample printed every run so a swap is visible immediately.

The Minnesota bounding box is a second guard, matching the one in the geom
trigger. A result outside it is SKIPPED and reported, never written -- a bad
geocode stays visibly ungeocoded instead of becoming a plausible wrong point.

=== WHY THE OUTCOMES ARE COUNTED SEPARATELY ===
Three defects found on 2026-08-18 shared one shape: a job reporting success for
work it did not do. records_new counting upserts as inserts; the mnpn scraper
alerting HARD FAILURE on a caught-up run; saved_search_alerts returning a bare
bool so no_matches and send_failed both read sent=0 failed=0. So:

    written      Mapbox returned a point inside Minnesota, lat/lng saved
    not_found    Mapbox answered, no match -- the address is not geocodable
    out_of_bounds  a point, but outside Minnesota -- skipped, listed
    failed       the call itself errored -- retryable, NOT the same as no match

not_found and failed are different facts and are never summed. The buckets are
checked against the attempt count at the end; if they ever stop summing, a
return path was added that lands nowhere.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

import psycopg2
import requests


# Mapbox handles 600/min; 222 addresses is nothing. The delay is politeness,
# not a limit, and it keeps a full run near 30 seconds.
REQUEST_DELAY_SECONDS = 0.1
REQUEST_TIMEOUT_SECONDS = 20

# Minnesota, generously bounded. Same numbers as the geom trigger's CHECK
# (MIGRATION_parcels_geom_trigger_2026-08-13.sql) so a row that would be
# rejected there is never offered to it.
LAT_MIN, LAT_MAX = 43.0, 49.5
LNG_MIN, LNG_MAX = -97.5, -89.0

# Bias toward the metro. Same value src/services/geocoder.py uses.
PROXIMITY = "-93.265,44.977"

MAPBOX_URL = "https://api.mapbox.com/search/geocode/v6/forward"

# Defensive ceiling. 222 today; a run that tries to geocode thousands means
# the predicate has drifted and should stop rather than spend.
MAX_ADDRESSES = 2000


# ANY parcel with a usable address and no coordinates.
#
#   lat IS NULL            SELF-RESUMING: a geocoded row is never revisited,
#                          so re-running always resumes and eventually no-ops
#   address ~ '[0-9]'      has a house number
#   address ~* '[a-z]{3}'  has a street name (excludes ',' and other debris)
#   city present           needed to disambiguate a street name statewide
#
# NO parcel_id CONDITION -- see "THE STUB SCOPING WAS WRONG" in the docstring.
#
# MATERIALIZED on the stub CTE deliberately: without it the planner evaluates
# the regexes across all 2.2M rows of core.parcels and the statement times out
# at the gateway. Narrow on the cheap predicate first -- the same lesson as
# CROSS JOIN LATERAL in the address-resolution work.
SELECT_SQL = """
WITH candidates AS MATERIALIZED (
  SELECT county_code, parcel_id, address, city, zip
  FROM   core.parcels
  WHERE  lat IS NULL
)
SELECT county_code,
       parcel_id,
       address,
       CASE
         WHEN address ~* ', *(MN|Minnesota)'
           THEN address
         ELSE concat_ws(', ',
                split_part(address, ',', 1),
                city,
                nullif(concat_ws(' ', 'MN', nullif(trim(zip), '')), 'MN'))
       END AS geocode_query
FROM   candidates
WHERE  address ~ '[0-9]'
  AND  address ~* '[a-z]{3}'
  AND  city IS NOT NULL
  AND  city <> ''
ORDER  BY county_code, parcel_id
LIMIT  %(cap)s;
"""

# lat IS NULL in the predicate as well as the select. Two runs overlapping, or
# a row geocoded by something else mid-run, must not be overwritten -- a real
# coordinate always beats a derived one.
UPDATE_SQL = """
UPDATE core.parcels
SET    lat = %(lat)s,
       lng = %(lng)s
WHERE  county_code = %(county_code)s
  AND  parcel_id   = %(parcel_id)s
  AND  lat IS NULL;
"""

SAMPLE_SQL = """
SELECT county_code, parcel_id, address, lat, lng, ST_AsText(geom::geometry)
FROM   core.parcels
WHERE  county_code = %(county_code)s
  AND  parcel_id   = %(parcel_id)s;
"""


def log(msg: str) -> None:
    print(f"[stub-geocode] {msg}", flush=True)


def geocode(session: requests.Session, token: str,
            query: str) -> tuple[str, tuple[float, float] | None]:
    """Resolve one address.

    Returns (outcome, coords) where outcome is 'ok' | 'not_found' | 'failed'.
    'not_found' means Mapbox answered and had no match -- a fact about the
    address. 'failed' means the call did not complete -- a fact about the
    network. Collapsing them would hide a broken token behind 222 addresses
    that all look unmatchable.
    """
    try:
        response = session.get(
            MAPBOX_URL,
            params={
                "q": query,
                "access_token": token,
                "limit": 1,
                "country": "us",
                "proximity": PROXIMITY,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
    except Exception as e:  # noqa: BLE001
        log(f"  call failed for {query!r}: {type(e).__name__}: {str(e)[:160]}")
        return "failed", None

    features = data.get("features") or []
    if not features:
        return "not_found", None

    coords = (features[0].get("geometry") or {}).get("coordinates")
    if not coords or len(coords) < 2:
        return "not_found", None

    # [lng, lat] -- see the coordinate-order note in the module docstring.
    lng, lat = float(coords[0]), float(coords[1])
    return "ok", (lat, lng)


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1

    token = os.environ.get("MAPBOX_TOKEN")
    if not token:
        log("FATAL: MAPBOX_TOKEN is not set")
        return 1

    dry_run = os.environ.get("DRY_RUN") == "1"

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(SELECT_SQL, {"cap": MAX_ADDRESSES})
            rows = cur.fetchall()

        log(f"start: {len(rows)} stub parcels have an address and no lat/lng")
        if not rows:
            log("nothing to do")
            return 0

        if dry_run:
            log("DRY_RUN=1 -- no Mapbox calls, no writes. Sample queries:")
            for county_code, parcel_id, address, query in rows[:10]:
                log(f"  {county_code}/{parcel_id}")
                log(f"      address: {address!r}")
                log(f"      query:   {query!r}")
            if len(rows) > 10:
                log(f"  ... and {len(rows) - 10} more")
            return 0

        outcomes = {"written": 0, "not_found": 0,
                    "out_of_bounds": 0, "failed": 0, "no_row_updated": 0}
        out_of_bounds: list[tuple[str, str, float, float]] = []
        first_written: dict[str, Any] | None = None
        started = time.monotonic()

        session = requests.Session()
        for n, (county_code, parcel_id, address, query) in enumerate(rows, 1):
            outcome, coords = geocode(session, token, query)

            if outcome == "failed":
                outcomes["failed"] += 1
            elif outcome == "not_found":
                outcomes["not_found"] += 1
            else:
                lat, lng = coords  # type: ignore[misc]
                if not (LAT_MIN <= lat <= LAT_MAX and LNG_MIN <= lng <= LNG_MAX):
                    # Skipped, not written. A wrong point that looks right is
                    # worse than no point: nothing downstream would notice.
                    outcomes["out_of_bounds"] += 1
                    out_of_bounds.append((county_code, parcel_id, lat, lng))
                else:
                    with conn.cursor() as cur:
                        cur.execute(UPDATE_SQL, {
                            "lat": lat, "lng": lng,
                            "county_code": county_code,
                            "parcel_id": parcel_id,
                        })
                        updated = cur.rowcount
                    conn.commit()
                    if updated:
                        outcomes["written"] += 1
                        if first_written is None:
                            first_written = {"county_code": county_code,
                                             "parcel_id": parcel_id}
                    else:
                        # lat stopped being NULL between SELECT and UPDATE.
                        outcomes["no_row_updated"] += 1

            if n % 25 == 0:
                elapsed = time.monotonic() - started
                log(f"{n}/{len(rows)} attempted "
                    f"({outcomes['written']} written, {elapsed:.0f}s)")
            time.sleep(REQUEST_DELAY_SECONDS)

        log(f"done: written={outcomes['written']} "
            f"not_found={outcomes['not_found']} "
            f"out_of_bounds={outcomes['out_of_bounds']} "
            f"failed={outcomes['failed']} "
            f"no_row_updated={outcomes['no_row_updated']}")

        counted = sum(outcomes.values())
        if counted != len(rows):
            log(f"WARNING: outcomes sum to {counted} but {len(rows)} were "
                f"attempted -- a return path lands in no bucket")

        if first_written:
            # Printed EVERY run. A reversed coordinate pair produces a valid
            # point in the wrong hemisphere and nothing downstream would
            # notice. Longitude must be NEGATIVE and near -93 for Minnesota.
            with conn.cursor() as cur:
                cur.execute(SAMPLE_SQL, first_written)
                sample = cur.fetchone()
            if sample:
                log(f"sample: {sample[0]}/{sample[1]} {sample[2]!r} -> "
                    f"lat={sample[3]} lng={sample[4]} geom={sample[5]}")

        if out_of_bounds:
            log("OUT OF BOUNDS -- geocoded outside Minnesota, left "
                "ungeocoded deliberately (inspect these):")
            for county_code, parcel_id, lat, lng in out_of_bounds:
                log(f"  {county_code}/{parcel_id}: {lat}, {lng}")

        if outcomes["failed"]:
            log(f"NOTE: {outcomes['failed']} call(s) errored rather than "
                f"returning no match. Those addresses are unchanged and a "
                f"re-run will retry them.")

        return 0
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        log(f"FAILED -- {type(e).__name__}: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())"""
Geocode stub parcels that carry a real address but no coordinates.

Run: python outcome_capture/backfill_stub_geocode.py
     DRY_RUN=1 python outcome_capture/backfill_stub_geocode.py   (no writes)

=== WHY THIS EXISTS (2026-08-18) ===
222 parcels on synthetic '{COUNTY}-FC-...' keys carry a real street address --
'20088 FERRET ST, NOWTHEN' , '7969 East River Road, Fridley' -- and NULL
lat/lng. A person could drive to any of them. The product shows no map pin and
no Street View, because every downstream consumer of position reads lat/lng.

This surfaced from the wrong end. audit.run_integrity_checks() reports 406
imagery rows on stubs; 348 of them are status='no_location' with the error
'parcel has no lat/lng'. The first instinct was to delete those 348 as noise.
That would have been wrong three times over:

  1. They are not noise. They are the resolver's own record that it looked and
     there was nowhere to look -- outcome_capture/resolve_parcel_imagery.py
     says it outright: "'we checked and there is nowhere to look' is a
     different fact from 'we never checked', and a missing row cannot express
     either."
  2. WORKING_SET_SQL in that same file RE-OPENS a no_location row the moment
     the parcel gains coordinates ("that verdict was true when written and is
     not any more"). Deleting them would have dropped the parcels into
     NO_LOCATION_SQL instead, which only matches rows with no imagery row at
     all -- back to square one.
  3. The imagery rows were a symptom correctly reporting a real gap. Fix the
     gap and they resolve themselves.

So: no deletes. Geocode the parcels, and the existing resolver does the rest
on its next run.

=== WHY A STUB WITH COORDINATES IS NOT A CORRUPTION ===
MIGRATION_integrity_findings_20260818.sql, check 5:
    "DO NOT let anything delete these automatically. Dakota's stubs DO carry
     lat/lng from ArcGIS geometry, and two of its rows held a real
     google_streetview pano_id. status='ok' rows are genuine images."
Dakota's stubs already work this way and produced 23 real panoramas. This makes
the other counties match the case that already works.

Coordinates give a map pin and an imagery lookup. They do NOT identify a
parcel: src/services/spine_resolver.py records that core.parcels.geom is
geography(Point,4326) -- centroids, not boundaries -- so point-in-polygon is
impossible against this table. Nothing here changes that.

=== geom NEEDS NO SECOND PASS ===
MIGRATION_parcels_geom_generated_2026-08-13.sql made geom a GENERATED column
derived from lat/lng. Writing lat/lng writes geom. Do not run
backfill_parcel_geom.py after this; there is nothing for it to do.

=== THE STUB SCOPING WAS WRONG (corrected 2026-08-18, same day) ===
The first version of this file selected only synthetic '{COUNTY}-FC-...' keys,
because the 222 were found through an imagery counter that happens to be
stub-scoped. Measured after that run cleared them:

    parcels with lat IS NULL and a usable address
      NOT a stub (real spine parcel_id)   136
      a stub                                2

So the stub condition excluded 136 REAL parcels with real addresses and no
map pin -- dakota 111, washington 15, hennepin 9, anoka 1, created between
2026-06-03 and 2026-08-08, so a steady trickle rather than one bad load.
Whether a parcel_id is synthetic has nothing to do with whether the property
can be located; the predicate is now just "usable address, no coordinates".

SEPARATELY AND NOT ADDRESSED HERE: 97 parcels in pine (65), rock (23) and
le_sueur (9) have no coordinates AND NO ADDRESS AT ALL, every one created at
2026-08-11 13:38:20.20743 -- the same microsecond, so one load by one writer.
Nothing to geocode; they need whatever wrote them looked at instead. They fail
the address predicate below and are correctly untouched by this script.

=== THE TWO ADDRESS SHAPES (measured 2026-08-18, all 222) ===
  212  street only            -> compose '<street>, <city>, MN <zip>'
   10  already full           -> use the address verbatim

The 10 self-contained ones look like '1017 9th Street N, Moorhead, Minnesota
56560' -- city and state already inside the field, and zip NULL in its own
column because it lives in the string. Composing those would repeat the city
and drop the zip; split_part() on the comma would delete the city entirely.
Detected by ', MN' / ', Minnesota' rather than by county or by length.

Three of the 212 are multi-property notices:
    '101 Charles Street NE, 179 Charles Street NE, 180 Charles Street NE'
    '1320  1340 & 1350 Lagoon Ave, Unit #s'
    '301 Clifton Ave Units 4G, G3 and G5'
split_part(address, ',', 1) takes the first, which is the right answer for a
package: one pin on the first property beats no pin on any of them. It is a
no-op on the 209 addresses with no comma at all.

115 of 222 carry a unit suffix ('UNIT 132', '#5', 'Apt 4'). Mapbox returns the
BUILDING for these, which is what an imagery lookup wants -- a Street View
panorama is of the building whatever the unit number.

=== COORDINATE ORDER -- the one thing that must not be got wrong ===
Mapbox GeoJSON returns coordinates as [longitude, latitude]. Reversing them
does not fail loudly: it places every Minnesota parcel in Somalia and the
imagery resolver simply finds nothing. src/services/geocoder.py unpacks
`lng, lat = coords[0], coords[1]` and this file does the same, in one place,
with the sample printed every run so a swap is visible immediately.

The Minnesota bounding box is a second guard, matching the one in the geom
trigger. A result outside it is SKIPPED and reported, never written -- a bad
geocode stays visibly ungeocoded instead of becoming a plausible wrong point.

=== WHY THE OUTCOMES ARE COUNTED SEPARATELY ===
Three defects found on 2026-08-18 shared one shape: a job reporting success for
work it did not do. records_new counting upserts as inserts; the mnpn scraper
alerting HARD FAILURE on a caught-up run; saved_search_alerts returning a bare
bool so no_matches and send_failed both read sent=0 failed=0. So:

    written      Mapbox returned a point inside Minnesota, lat/lng saved
    not_found    Mapbox answered, no match -- the address is not geocodable
    out_of_bounds  a point, but outside Minnesota -- skipped, listed
    failed       the call itself errored -- retryable, NOT the same as no match

not_found and failed are different facts and are never summed. The buckets are
checked against the attempt count at the end; if they ever stop summing, a
return path was added that lands nowhere.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

import psycopg2
import requests


# Mapbox handles 600/min; 222 addresses is nothing. The delay is politeness,
# not a limit, and it keeps a full run near 30 seconds.
REQUEST_DELAY_SECONDS = 0.1
REQUEST_TIMEOUT_SECONDS = 20

# Minnesota, generously bounded. Same numbers as the geom trigger's CHECK
# (MIGRATION_parcels_geom_trigger_2026-08-13.sql) so a row that would be
# rejected there is never offered to it.
LAT_MIN, LAT_MAX = 43.0, 49.5
LNG_MIN, LNG_MAX = -97.5, -89.0

# Bias toward the metro. Same value src/services/geocoder.py uses.
PROXIMITY = "-93.265,44.977"

MAPBOX_URL = "https://api.mapbox.com/search/geocode/v6/forward"

# Defensive ceiling. 222 today; a run that tries to geocode thousands means
# the predicate has drifted and should stop rather than spend.
MAX_ADDRESSES = 2000


# ANY parcel with a usable address and no coordinates.
#
#   lat IS NULL            SELF-RESUMING: a geocoded row is never revisited,
#                          so re-running always resumes and eventually no-ops
#   address ~ '[0-9]'      has a house number
#   address ~* '[a-z]{3}'  has a street name (excludes ',' and other debris)
#   city present           needed to disambiguate a street name statewide
#
# NO parcel_id CONDITION -- see "THE STUB SCOPING WAS WRONG" in the docstring.
#
# MATERIALIZED on the stub CTE deliberately: without it the planner evaluates
# the regexes across all 2.2M rows of core.parcels and the statement times out
# at the gateway. Narrow on the cheap predicate first -- the same lesson as
# CROSS JOIN LATERAL in the address-resolution work.
SELECT_SQL = """
WITH candidates AS MATERIALIZED (
  SELECT county_code, parcel_id, address, city, zip
  FROM   core.parcels
  WHERE  lat IS NULL
)
SELECT county_code,
       parcel_id,
       address,
       CASE
         WHEN address ~* ', *(MN|Minnesota)'
           THEN address
         ELSE concat_ws(', ',
                split_part(address, ',', 1),
                city,
                nullif(concat_ws(' ', 'MN', nullif(trim(zip), '')), 'MN'))
       END AS geocode_query
FROM   candidates
WHERE  address ~ '[0-9]'
  AND  address ~* '[a-z]{3}'
  AND  city IS NOT NULL
  AND  city <> ''
ORDER  BY county_code, parcel_id
LIMIT  %(cap)s;
"""

# lat IS NULL in the predicate as well as the select. Two runs overlapping, or
# a row geocoded by something else mid-run, must not be overwritten -- a real
# coordinate always beats a derived one.
UPDATE_SQL = """
UPDATE core.parcels
SET    lat = %(lat)s,
       lng = %(lng)s
WHERE  county_code = %(county_code)s
  AND  parcel_id   = %(parcel_id)s
  AND  lat IS NULL;
"""

SAMPLE_SQL = """
SELECT county_code, parcel_id, address, lat, lng, ST_AsText(geom::geometry)
FROM   core.parcels
WHERE  county_code = %(county_code)s
  AND  parcel_id   = %(parcel_id)s;
"""


def log(msg: str) -> None:
    print(f"[stub-geocode] {msg}", flush=True)


def geocode(session: requests.Session, token: str,
            query: str) -> tuple[str, tuple[float, float] | None]:
    """Resolve one address.

    Returns (outcome, coords) where outcome is 'ok' | 'not_found' | 'failed'.
    'not_found' means Mapbox answered and had no match -- a fact about the
    address. 'failed' means the call did not complete -- a fact about the
    network. Collapsing them would hide a broken token behind 222 addresses
    that all look unmatchable.
    """
    try:
        response = session.get(
            MAPBOX_URL,
            params={
                "q": query,
                "access_token": token,
                "limit": 1,
                "country": "us",
                "proximity": PROXIMITY,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
    except Exception as e:  # noqa: BLE001
        log(f"  call failed for {query!r}: {type(e).__name__}: {str(e)[:160]}")
        return "failed", None

    features = data.get("features") or []
    if not features:
        return "not_found", None

    coords = (features[0].get("geometry") or {}).get("coordinates")
    if not coords or len(coords) < 2:
        return "not_found", None

    # [lng, lat] -- see the coordinate-order note in the module docstring.
    lng, lat = float(coords[0]), float(coords[1])
    return "ok", (lat, lng)


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1

    token = os.environ.get("MAPBOX_TOKEN")
    if not token:
        log("FATAL: MAPBOX_TOKEN is not set")
        return 1

    dry_run = os.environ.get("DRY_RUN") == "1"

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(SELECT_SQL, {"cap": MAX_ADDRESSES})
            rows = cur.fetchall()

        log(f"start: {len(rows)} stub parcels have an address and no lat/lng")
        if not rows:
            log("nothing to do")
            return 0

        if dry_run:
            log("DRY_RUN=1 -- no Mapbox calls, no writes. Sample queries:")
            for county_code, parcel_id, address, query in rows[:10]:
                log(f"  {county_code}/{parcel_id}")
                log(f"      address: {address!r}")
                log(f"      query:   {query!r}")
            if len(rows) > 10:
                log(f"  ... and {len(rows) - 10} more")
            return 0

        outcomes = {"written": 0, "not_found": 0,
                    "out_of_bounds": 0, "failed": 0, "no_row_updated": 0}
        out_of_bounds: list[tuple[str, str, float, float]] = []
        first_written: dict[str, Any] | None = None
        started = time.monotonic()

        session = requests.Session()
        for n, (county_code, parcel_id, address, query) in enumerate(rows, 1):
            outcome, coords = geocode(session, token, query)

            if outcome == "failed":
                outcomes["failed"] += 1
            elif outcome == "not_found":
                outcomes["not_found"] += 1
            else:
                lat, lng = coords  # type: ignore[misc]
                if not (LAT_MIN <= lat <= LAT_MAX and LNG_MIN <= lng <= LNG_MAX):
                    # Skipped, not written. A wrong point that looks right is
                    # worse than no point: nothing downstream would notice.
                    outcomes["out_of_bounds"] += 1
                    out_of_bounds.append((county_code, parcel_id, lat, lng))
                else:
                    with conn.cursor() as cur:
                        cur.execute(UPDATE_SQL, {
                            "lat": lat, "lng": lng,
                            "county_code": county_code,
                            "parcel_id": parcel_id,
                        })
                        updated = cur.rowcount
                    conn.commit()
                    if updated:
                        outcomes["written"] += 1
                        if first_written is None:
                            first_written = {"county_code": county_code,
                                             "parcel_id": parcel_id}
                    else:
                        # lat stopped being NULL between SELECT and UPDATE.
                        outcomes["no_row_updated"] += 1

            if n % 25 == 0:
                elapsed = time.monotonic() - started
                log(f"{n}/{len(rows)} attempted "
                    f"({outcomes['written']} written, {elapsed:.0f}s)")
            time.sleep(REQUEST_DELAY_SECONDS)

        log(f"done: written={outcomes['written']} "
            f"not_found={outcomes['not_found']} "
            f"out_of_bounds={outcomes['out_of_bounds']} "
            f"failed={outcomes['failed']} "
            f"no_row_updated={outcomes['no_row_updated']}")

        counted = sum(outcomes.values())
        if counted != len(rows):
            log(f"WARNING: outcomes sum to {counted} but {len(rows)} were "
                f"attempted -- a return path lands in no bucket")

        if first_written:
            # Printed EVERY run. A reversed coordinate pair produces a valid
            # point in the wrong hemisphere and nothing downstream would
            # notice. Longitude must be NEGATIVE and near -93 for Minnesota.
            with conn.cursor() as cur:
                cur.execute(SAMPLE_SQL, first_written)
                sample = cur.fetchone()
            if sample:
                log(f"sample: {sample[0]}/{sample[1]} {sample[2]!r} -> "
                    f"lat={sample[3]} lng={sample[4]} geom={sample[5]}")

        if out_of_bounds:
            log("OUT OF BOUNDS -- geocoded outside Minnesota, left "
                "ungeocoded deliberately (inspect these):")
            for county_code, parcel_id, lat, lng in out_of_bounds:
                log(f"  {county_code}/{parcel_id}: {lat}, {lng}")

        if outcomes["failed"]:
            log(f"NOTE: {outcomes['failed']} call(s) errored rather than "
                f"returning no match. Those addresses are unchanged and a "
                f"re-run will retry them.")

        return 0
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        log(f"FAILED -- {type(e).__name__}: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
