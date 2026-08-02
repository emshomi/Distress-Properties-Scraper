"""
Backfill core.parcels.geom from lat/lng.

Run: python outcome_capture/backfill_parcel_geom.py

=== WHY THIS EXISTS (2026-08-02) ===
core.parcels has carried a `geom geography(Point,4326)` column and an index on
it (idx_parcels_geom) since the schema was designed. NOTHING EVER POPULATED
IT — 0 of 845,000 parcels had a value, while 860,687 had perfectly good lat and
lng. Same shape as scraper_run_id being NULL on 100% of distress_events: a
column built for a purpose nobody wired up.

It matters because geometry is what makes neighbourhood-level questions
answerable. "Distressed properties in Highland Park" is not a city filter —
Highland Park is one of Saint Paul's 17 District Council areas, and picking it
out means a point-in-polygon join against a boundary layer. Without geom that
join cannot happen at all; with it, it is one query.

=== WHY A SCRIPT AND NOT A SINGLE UPDATE ===
The Supabase SQL editor dies on this. A whole-table UPDATE times out at the API
gateway (not statement_timeout — the gateway gives up while the statement is
very possibly still running server-side, which is its own hazard: you cannot
tell a slow success from a failure).

Batching is not just about time. core.parcels has an index ON geom, so
updating it means every row gets new index entries and Postgres cannot use HOT
updates — each batch does far more write work than the row count suggests.
1,000 rows per batch was measured comfortable in the editor; this uses the same
size and commits each batch separately, so a failure costs one batch rather
than the whole run.

=== SELF-RESUMING ===
The predicate is `geom IS NULL`, so a completed row is never revisited. Re-run
this as many times as you like: it always picks up where it stopped, and once
finished it does nothing. No cursor, no state file, no offset to get wrong.

=== COORDINATE ORDER — the one thing that must not be got wrong ===
ST_MakePoint takes (X, Y) = (LONGITUDE, LATITUDE). Reversing it is the classic
error and it does not fail loudly: it silently places every Minnesota parcel
off the coast of Somalia, and a later spatial join simply matches nothing.

Verified live before this script was written: 810 Maryland Ave E, Saint Paul
came back as POINT(-93.066465 44.977053) — longitude first and negative. If a
sample ever reads POINT(44.x -93.x), stop; the arguments are swapped.

The Minnesota bounding box in the WHERE clause is a second guard. A row with a
projected or transposed coordinate is SKIPPED rather than converted, so bad
input stays visibly unconverted instead of becoming a plausible-looking point
in the wrong place.
"""

from __future__ import annotations

import os
import sys
import time

import psycopg2


# Measured comfortable in the Supabase editor. The limit is index maintenance
# on geom, not the geography construction, so raising this buys less than it
# looks like it should.
BATCH_SIZE = 1000

# Minnesota, generously bounded. Guard rather than filter: anything outside is
# left alone for a human to look at.
LAT_MIN, LAT_MAX = 43.0, 49.5
LNG_MIN, LNG_MAX = -97.5, -89.0

# Stop rather than loop forever if something stops making progress.
MAX_BATCHES = 2000


BATCH_SQL = """
UPDATE core.parcels p
SET geom = ST_SetSRID(ST_MakePoint(p.lng::float8, p.lat::float8), 4326)::geography
FROM (
  SELECT ctid
  FROM core.parcels
  WHERE geom IS NULL
    AND lat IS NOT NULL
    AND lng IS NOT NULL
    AND lat BETWEEN %(lat_min)s AND %(lat_max)s
    AND lng BETWEEN %(lng_min)s AND %(lng_max)s
  LIMIT %(batch)s
) s
WHERE p.ctid = s.ctid;
"""

REMAINING_SQL = """
SELECT count(*)
FROM core.parcels
WHERE geom IS NULL
  AND lat IS NOT NULL
  AND lng IS NOT NULL
  AND lat BETWEEN %(lat_min)s AND %(lat_max)s
  AND lng BETWEEN %(lng_min)s AND %(lng_max)s;
"""

SAMPLE_SQL = """
SELECT county_code, address, ST_AsText(geom::geometry)
FROM core.parcels
WHERE geom IS NOT NULL
LIMIT 1;
"""

SKIPPED_SQL = """
SELECT county_code, count(*)
FROM core.parcels
WHERE geom IS NULL
  AND lat IS NOT NULL
  AND lng IS NOT NULL
  AND (lat NOT BETWEEN %(lat_min)s AND %(lat_max)s
       OR lng NOT BETWEEN %(lng_min)s AND %(lng_max)s)
GROUP BY county_code
ORDER BY 2 DESC;
"""


def log(msg: str) -> None:
    print(f"[geom-backfill] {msg}", flush=True)


def _bounds() -> dict[str, float]:
    return {
        "lat_min": LAT_MIN, "lat_max": LAT_MAX,
        "lng_min": LNG_MIN, "lng_max": LNG_MAX,
    }


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1
    dry_run = os.environ.get("DRY_RUN") == "1"

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(REMAINING_SQL, _bounds())
            remaining = cur.fetchone()[0]
        log(f"start: {remaining} parcels need geom")

        if dry_run:
            log("DRY_RUN=1 — no writes performed")
            return 0
        if remaining == 0:
            log("nothing to do")
            return 0

        written = 0
        batches = 0
        started = time.monotonic()

        while batches < MAX_BATCHES:
            with conn.cursor() as cur:
                cur.execute(BATCH_SQL, {**_bounds(), "batch": BATCH_SIZE})
                n = cur.rowcount
            conn.commit()

            if n == 0:
                # Nothing left that matches. Not an error — the normal exit.
                break

            written += n
            batches += 1
            if batches % 25 == 0 or n < BATCH_SIZE:
                elapsed = time.monotonic() - started
                rate = written / elapsed if elapsed > 0 else 0
                log(f"{written}/{remaining} written "
                    f"({batches} batches, {rate:.0f} rows/sec)")

        if batches >= MAX_BATCHES:
            # Defensive: should never fire at 1000 x 2000 = 2M capacity, but a
            # silent infinite loop in a scheduled job is worse than a stop.
            log(f"WARNING: hit MAX_BATCHES ({MAX_BATCHES}) — re-run to finish")

        with conn.cursor() as cur:
            cur.execute(REMAINING_SQL, _bounds())
            still = cur.fetchone()[0]
            cur.execute(SAMPLE_SQL)
            sample = cur.fetchone()
            cur.execute(SKIPPED_SQL, _bounds())
            skipped = cur.fetchall() or []

        log(f"done: {written} written, {still} still without geom")

        if sample:
            # Printed EVERY run on purpose. A reversed ST_MakePoint produces a
            # valid-looking point in the wrong hemisphere and nothing else in
            # this pipeline would notice. Longitude must come first and be
            # negative for Minnesota.
            log(f"sample: {sample[0]} {sample[1]} -> {sample[2]}")

        if skipped:
            log("SKIPPED — coordinates outside the Minnesota bounding box "
                "(left unconverted deliberately; inspect these):")
            for county, n in skipped:
                log(f"  {county}: {n}")

        return 0
    except Exception as e:
        conn.rollback()
        log(f"FAILED — {type(e).__name__}: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
