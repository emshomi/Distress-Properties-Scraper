"""
Backfill core.parcels.emv_total for hennepin from the legacy column.

Run: python scripts/run_hennepin_emv_backfill.py

=== WHAT IS WRONG ===
Hennepin holds 448,719 parcels. emv_total -- the column the API, deal math and
every product surface read -- is populated on 39,631 of them (8.8%).
estimated_market_value, a LEGACY column nothing displays, is populated on
443,610 (98.9%).

The values were loaded. They went to the wrong destination column. Compare:
washington 100.0% with value, dakota 99.9%, chisago 99.5%, anoka 98.7%,
HENNEPIN 8.8% -- in the county holding by far the most distress inventory.

Same defect fixed in washington_parcels.py on 2026-08-14 ("values were going to
the LEGACY column nothing displays"), still live in the hennepin loader.

=== WHICH COLUMN IS RIGHT: DECIDED BY EVIDENCE ===
39,631 rows have both set; 39,345 agree (99.3%). The 286 that disagree were
checked against the county's own raw payload:

    raw_data->>'MKT_VAL_TOT' = estimated_market_value : 286
    raw_data->>'MKT_VAL_TOT' = emv_total              :   0

Unanimous. 282 of the 286 are LOWER in emv_total -- a stale assessment year
that stopped refreshing while the legacy column kept updating. So this
OVERWRITES, and audit.hennepin_emv_backfill_20260815 holds the prior state of
all 385,962 affected rows (taken BEFORE any write).

=== WHY A RUNNER AND NOT THE SQL EDITOR ===
The editor's gateway timed out on the whole UPDATE, again at 50,000 rows, and
again at 20,000. 5,000 succeeded. Bisecting further is pointless because the
cost is NOT in finding the rows:

    EXPLAIN ANALYZE on the SELECT: Index Scan, 20,000 rows in 630ms.

Reading is fast. The cost is the WRITE -- row updates plus index maintenance on
a 7.7GB table with 3.7GB of indexes. A direct psycopg2 connection has no
gateway in front of it, which is how every other batch job has run tonight
without a single timeout.

=== KEYSET, NOT LIMIT ===
Each batch resumes at the last parcel_id rather than re-running the same
predicate from the top. Repeating LIMIT re-scans the same leading rows every
time and degrades as it goes -- the exact defect recorded against offset paging
on the Washington loader.

=== SAFE TO RE-RUN ===
The predicate is self-limiting: emv_total IS DISTINCT FROM
estimated_market_value stops matching once a row is written. A run that dies
half way costs a re-run, never a double-apply. Every batch commits on its own.

=== ZEROS ARE SKIPPED ON PURPOSE ===
18,303 hennepin parcels carry MKT_VAL_TOT = 0. Some are genuine (government
land, right-of-way, church property); some are failed parses, and we cannot
tell them apart. Writing 0 would render "$0" where an em-dash belongs AND feed
$0 into _compute_deal_math as a real market value -- an equity spread equal to
the entire payoff, on 18,303 parcels. A fabricated number is worse than a
blank. They stay NULL.
"""

from __future__ import annotations

import os
import sys
import time

import psycopg2
import psycopg2.extras


COUNTY = "hennepin"
BATCH_SIZE = 5000          # proven in the editor; the runner has more headroom
MAX_BATCHES = 500
PROGRESS_EVERY = 10


# Keyset page: everything still needing a write, after the last id seen.
SELECT_SQL = """
SELECT parcel_id
FROM   core.parcels
WHERE  county_code = %(county)s
  AND  parcel_id > %(after)s
  AND  estimated_market_value IS NOT NULL
  AND  estimated_market_value > 0
  AND  emv_total IS DISTINCT FROM estimated_market_value
ORDER  BY parcel_id
LIMIT  %(batch)s;
"""

# The predicate is REPEATED here, not just the id list. If a row changed
# between the select and the update, it is skipped rather than written blind.
UPDATE_SQL = """
UPDATE core.parcels
SET    emv_total  = estimated_market_value,
       updated_at = now()
WHERE  county_code = %(county)s
  AND  parcel_id = ANY(%(pids)s)
  AND  estimated_market_value IS NOT NULL
  AND  estimated_market_value > 0
  AND  emv_total IS DISTINCT FROM estimated_market_value;
"""

REMAINING_SQL = """
SELECT count(*) AS remaining
FROM   core.parcels
WHERE  county_code = %(county)s
  AND  estimated_market_value IS NOT NULL
  AND  estimated_market_value > 0
  AND  emv_total IS DISTINCT FROM estimated_market_value;
"""

FILLED_SQL = """
SELECT count(*) AS filled
FROM   core.parcels
WHERE  county_code = %(county)s
  AND  emv_total IS NOT NULL;
"""


def log(msg: str) -> None:
    print(f"[emv] {msg}", flush=True)


def scalar(conn, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, {"county": COUNTY})
        return int(cur.fetchone()[0])


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1

    max_rows = int(os.environ.get("MAX_ROWS", "0"))  # 0 = uncapped
    log(f"county={COUNTY} batch={BATCH_SIZE} "
        f"max_rows={max_rows or 'uncapped'}")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    try:
        # The backup MUST already exist. It records the prior state of all
        # 385,962 rows including 285 overwrites, and an overwrite is
        # unrecoverable without it. Refuse to write if it is missing.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('audit.hennepin_emv_backfill_20260815')")
            if cur.fetchone()[0] is None:
                log("FATAL: audit.hennepin_emv_backfill_20260815 does not "
                    "exist. 285 of these rows are OVERWRITES and the prior "
                    "value is recoverable only from that table. Create it "
                    "before running this.")
                return 1
            cur.execute(
                "SELECT count(*) FROM audit.hennepin_emv_backfill_20260815")
            log(f"backup present: {cur.fetchone()[0]} rows")
        conn.rollback()

        before_filled = scalar(conn, FILLED_SQL)
        before_remaining = scalar(conn, REMAINING_SQL)
        log(f"before: emv_total set on {before_filled}, "
            f"{before_remaining} still to write")

        if before_remaining == 0:
            log("nothing to do — already complete")
            return 0

        after = ""
        written = 0
        batches = 0
        started = time.monotonic()

        while batches < MAX_BATCHES:
            size = BATCH_SIZE
            if max_rows:
                size = min(BATCH_SIZE, max_rows - written)
                if size <= 0:
                    log(f"MAX_ROWS={max_rows} reached")
                    break

            with conn.cursor() as cur:
                cur.execute(SELECT_SQL, {"county": COUNTY, "after": after,
                                         "batch": size})
                pids = [r[0] for r in cur.fetchall()]

            if not pids:
                break

            with conn.cursor() as cur:
                cur.execute(UPDATE_SQL, {"county": COUNTY, "pids": pids})
                touched = cur.rowcount
            conn.commit()

            after = pids[-1]
            written += touched
            batches += 1

            if batches % PROGRESS_EVERY == 0 or touched != len(pids):
                elapsed = time.monotonic() - started
                log(f"{written} rows written ({batches} batches, "
                    f"{written / max(elapsed, 0.001):.0f}/sec, "
                    f"last id {after})")

        if batches >= MAX_BATCHES:
            log(f"WARNING: hit MAX_BATCHES ({MAX_BATCHES}) — re-run to finish")

        after_filled = scalar(conn, FILLED_SQL)
        after_remaining = scalar(conn, REMAINING_SQL)
        elapsed = time.monotonic() - started

        print()
        log(f"done in {elapsed:.0f}s: {written} rows written "
            f"across {batches} batches")
        log(f"emv_total set on {before_filled} -> {after_filled} "
            f"(+{after_filled - before_filled})")
        log(f"still to write: {before_remaining} -> {after_remaining}")
        if after_remaining:
            log("NOT COMPLETE — re-run. The predicate is self-limiting, so a "
                "re-run picks up exactly where this stopped.")
        log("The log is a convenience. Verify with a query against "
            "core.parcels.")
        return 0
    except Exception as e:
        conn.rollback()
        log(f"FAILED — {type(e).__name__}: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
