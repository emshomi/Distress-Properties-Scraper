"""
Rebuild scoring.avm_training. Run after every eCRV ingest.

    python -m scripts.rebuild_avm_training
    python -m scripts.rebuild_avm_training --dry-run
    python -m scripts.rebuild_avm_training --source-file <name>

=== WHY THIS EXISTS ===

scoring.avm_training had NO refresh path. It is a plain table, not a
materialized view, so the Monday `refresh-deal-math` pg_cron job does not
touch it -- that job refreshes scoring.comp_ratios, which is a different
object and IS current. avm_training was built by hand three times
(n_tup_ins 565,002 = 182,020 + 182,020 + 200,962) and drifted in between.

sql/avm_training_set.sql builds it with two pg_cron jobs and says twice:
"UNSCHEDULE BOTH WHEN DONE. Neither self-terminates." That is why it was
never scheduled -- nobody wants a cadence job that must be stopped by hand.
The pg_cron shape exists because a single fill statement times out. A
script holding its own connection has no such limit, loops county by
county, and exits.

=== WHY A FULL REBUILD AND NOT AN APPEND ===

knn_ratio is the median sale-to-assessment of the 10 nearest PRIOR sales.
Prior-only, so appending NEWER sales cannot change an existing row's value
-- which suggests append-only would be correct and would take seconds.

MEASURED 2026-08-25 AGAINST ONE REAL WEEKLY EXTRACT, AND IT IS NOT:

    eligible new rows      2,054
    strictly newer           391   (19%)
    dated at or before     1,663   (81%)   earliest deed 2024-02-01

81% of a weekly file is LATE-FILED -- eCRV filing lags the deed by up to
two and a half years. Every late arrival becomes a prior neighbour for
rows already in the table, so their knn_ratio goes stale. Append-only
would leave most of each affected county wrong.

A full rebuild is ~32 minutes and nobody waits on it. That is cheaper than
a correct incremental version is to write or to trust.

=== SAFETY ===

Builds into scoring.avm_training_new and swaps only after validating. The
live table keeps serving until the swap, so a mid-run failure costs
nothing. Never TRUNCATE the live table: the KNN pass alone is ~22 minutes
of exposure, and ml/train_avm.py reading an empty table would report a
result rather than an error.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

LIVE = "scoring.avm_training"
NEW = "scoring.avm_training_new"
OLD = "scoring.avm_training_old"
VIEW = "scoring.avm_training_set"

# A rebuild that produces far fewer rows than the last one means an input
# broke -- a dropped ecrv_county_map row, a parcel loader that emptied a
# county, a changed filter. Refuse the swap and leave the good table live.
# 0.90 tolerates normal week-to-week movement (a weekly extract adds ~1% of
# the population) while catching anything structural.
MIN_RATIO_OF_PREVIOUS = 0.90

# Guards against a first run or a truncated source doing damage.
ABSOLUTE_MIN_ROWS = 50_000


def log(msg):
    print("[avm-rebuild] %s %s"
          % (datetime.now(timezone.utc).strftime("%H:%M:%S"), msg),
          flush=True)


def ensure_build_log(cur):
    """Provenance. avm_training drifted invisibly because NOTHING recorded
    when it was last built or from what -- three hand-builds had to be
    inferred from pg_stat n_tup_ins arithmetic."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scoring.avm_build_log (
          id             bigserial PRIMARY KEY,
          started_at     timestamptz NOT NULL,
          finished_at    timestamptz,
          rows_loaded    integer,
          counties       integer,
          rows_with_knn  integer,
          newest_deed    date,
          source_file    text,
          previous_rows  integer,
          status         text NOT NULL,
          notes          text
        );
    """)


def counties_to_fill(cur):
    cur.execute("""
        SELECT county_slug FROM outcomes.ecrv_county_map ORDER BY county_slug
    """)
    return [r[0] for r in cur.fetchall()]


def build_empty(cur):
    log("creating %s" % NEW)
    cur.execute("DROP TABLE IF EXISTS %s" % NEW)
    # WHERE false copies column types and no rows -- instant. Creating it
    # from the populated view times out. (sql/avm_training_set.sql, B.)
    cur.execute("CREATE TABLE %s AS SELECT * FROM %s WHERE false"
                % (NEW, VIEW))
    cur.execute("ALTER TABLE %s ADD PRIMARY KEY (ecrv_id)" % NEW)
    cur.execute("ALTER TABLE %s ADD COLUMN knn_ratio numeric" % NEW)
    cur.execute("ALTER TABLE %s ADD COLUMN knn_count integer" % NEW)


def fill(conn, counties):
    """One county per statement. A single INSERT over all counties times
    out; that is why the original used pg_cron ticks."""
    total = 0
    t0 = time.time()
    for i, county in enumerate(counties, 1):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO %s (ecrv_id, crv_number_id, county_code, "
                "parcel_id, deed_date, purchase_amt, emv_total, "
                "sale_to_assessment, lat, lng, geom, lot_sqft, sqft, "
                "year_built, property_type, homestead_status, deed_type, "
                "finance_type) "
                "SELECT ecrv_id, crv_number_id, county_code, parcel_id, "
                "deed_date, purchase_amt, emv_total, sale_to_assessment, "
                "lat, lng, geom, lot_sqft, sqft, year_built, property_type, "
                "homestead_status, deed_type, finance_type "
                "FROM %s WHERE county_code = %%s "
                "ON CONFLICT (ecrv_id) DO NOTHING" % (NEW, VIEW),
                (county,),
            )
            n = cur.rowcount
        conn.commit()
        total += n
        # Zero is a VALID result: 17 mapped counties have no assessments
        # loaded. That is exactly why the original needed a separate
        # progress ledger -- a county's own output cannot mark it done.
        # Here the loop index does that job and the ledger is unnecessary.
        if n:
            log("  fill %2d/%d %-18s %6d rows" % (i, len(counties), county, n))
    log("fill complete: %d rows in %.1f min" % (total, (time.time()-t0)/60))
    return total


def build_indexes(conn):
    """AFTER the fill -- index maintenance during a 200k-row load is wasted
    work. The GiST index is what makes the KNN pass 2.2ms/row instead of
    a top-N heapsort: 143ms vs 30,770ms for 50 rows, measured 2026-08-23."""
    log("building indexes")
    with conn.cursor() as cur:
        cur.execute("CREATE INDEX avm_training_new_geom_idx "
                    "ON %s USING gist (geom)" % NEW)
        cur.execute("CREATE INDEX avm_training_new_cty_date "
                    "ON %s (county_code, deed_date)" % NEW)
    conn.commit()


def knn(conn):
    """Median sale_to_assessment of the 10 nearest PRIOR sales, same county.

    PRIOR ONLY. Using later sales leaks the future into the past -- the
    same error a random train/test split makes."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT county_code FROM %s ORDER BY 1" % NEW)
        counties = [r[0] for r in cur.fetchall()]
    log("knn: %d counties" % len(counties))
    total = 0
    t0 = time.time()
    for i, county in enumerate(counties, 1):
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE {new} t
                SET knn_ratio = k.med, knn_count = k.n
                FROM (
                  SELECT t2.ecrv_id,
                         (SELECT percentile_cont(0.5) WITHIN GROUP
                                   (ORDER BY n.sale_to_assessment)
                            FROM (SELECT x.sale_to_assessment
                                    FROM {new} x
                                   WHERE x.deed_date < t2.deed_date
                                     AND x.county_code = t2.county_code
                                   ORDER BY x.geom <-> t2.geom
                                   LIMIT 10) n) AS med,
                         (SELECT count(*)
                            FROM (SELECT 1
                                    FROM {new} x
                                   WHERE x.deed_date < t2.deed_date
                                     AND x.county_code = t2.county_code
                                   ORDER BY x.geom <-> t2.geom
                                   LIMIT 10) n) AS n
                  FROM {new} t2
                  WHERE t2.county_code = %s
                ) k
                WHERE t.ecrv_id = k.ecrv_id
            """.format(new=NEW), (county,))
            n = cur.rowcount
        conn.commit()
        total += n
        log("  knn %2d/%d %-18s %6d rows" % (i, len(counties), county, n))
    log("knn complete: %d rows in %.1f min" % (total, (time.time()-t0)/60))
    return total


def validate(cur, previous_rows):
    """A green build is not evidence. Measure the new table before trusting
    it with the live name."""
    cur.execute("SELECT count(*), count(DISTINCT county_code), "
                "count(knn_ratio), max(deed_date) FROM %s" % NEW)
    rows, counties, with_knn, newest = cur.fetchone()
    log("new table: %d rows, %d counties, %d with knn, newest deed %s"
        % (rows, counties, with_knn, newest))

    problems = []
    if rows < ABSOLUTE_MIN_ROWS:
        problems.append("only %d rows (floor is %d)"
                        % (rows, ABSOLUTE_MIN_ROWS))
    if previous_rows and rows < previous_rows * MIN_RATIO_OF_PREVIOUS:
        problems.append("%d rows is below %.0f%% of the previous build's %d"
                        % (rows, MIN_RATIO_OF_PREVIOUS * 100, previous_rows))
    # Each county's earliest sale legitimately has no prior neighbour, so a
    # handful of nulls is expected -- 40 of 200,962 on 2026-08-24. Many
    # nulls means the KNN pass did not run or the GiST index was missed.
    if rows and with_knn < rows * 0.95:
        problems.append("only %d of %d rows have knn_ratio" % (with_knn, rows))
    return rows, counties, with_knn, newest, problems


def swap(conn):
    """Single transaction. Either the new table is live with its indexes
    renamed, or nothing changed."""
    log("swapping")
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS %s" % OLD)
        cur.execute("ALTER TABLE %s RENAME TO avm_training_old" % LIVE)
        cur.execute("ALTER INDEX scoring.avm_training_geom_idx "
                    "RENAME TO avm_training_old_geom_idx")
        cur.execute("ALTER INDEX scoring.avm_training_cty_date "
                    "RENAME TO avm_training_old_cty_date")
        cur.execute("ALTER TABLE %s RENAME TO avm_training" % NEW)
        cur.execute("ALTER INDEX scoring.avm_training_new_geom_idx "
                    "RENAME TO avm_training_geom_idx")
        cur.execute("ALTER INDEX scoring.avm_training_new_cty_date "
                    "RENAME TO avm_training_cty_date")
    conn.commit()
    log("swap committed; previous build retained as %s" % OLD)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="build and validate, but do not swap")
    ap.add_argument("--source-file", default=None,
                    help="eCRV extract that triggered this, for the log")
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1

    started = datetime.now(timezone.utc)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    previous_rows = None
    try:
        with conn.cursor() as cur:
            ensure_build_log(cur)
            cur.execute("SELECT count(*) FROM %s" % LIVE)
            previous_rows = cur.fetchone()[0]
        conn.commit()
        log("current %s holds %d rows" % (LIVE, previous_rows))

        with conn.cursor() as cur:
            build_empty(cur)
            counties = counties_to_fill(cur)
        conn.commit()
        log("%d mapped counties to fill" % len(counties))

        fill(conn, counties)
        build_indexes(conn)
        knn(conn)

        with conn.cursor() as cur:
            rows, cty, with_knn, newest, problems = validate(cur, previous_rows)
        conn.commit()

        if problems:
            for p in problems:
                log("REFUSING SWAP: %s" % p)
            log("%s is untouched and still serving" % LIVE)
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO scoring.avm_build_log (started_at, "
                    "finished_at, rows_loaded, counties, rows_with_knn, "
                    "newest_deed, source_file, previous_rows, status, notes) "
                    "VALUES (%s, now(), %s, %s, %s, %s, %s, %s, 'refused', %s)",
                    (started, rows, cty, with_knn, newest, args.source_file,
                     previous_rows, "; ".join(problems)),
                )
            conn.commit()
            return 1

        if args.dry_run:
            log("DRY RUN: built and validated, not swapping")
            log("%s left in place for inspection" % NEW)
            return 0

        swap(conn)

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scoring.avm_build_log (started_at, finished_at, "
                "rows_loaded, counties, rows_with_knn, newest_deed, "
                "source_file, previous_rows, status, notes) "
                "VALUES (%s, now(), %s, %s, %s, %s, %s, %s, 'ok', %s)",
                (started, rows, cty, with_knn, newest, args.source_file,
                 previous_rows,
                 "delta %+d rows" % (rows - (previous_rows or 0))),
            )
        conn.commit()
        log("done: %d rows, %d counties, newest deed %s (%+d vs previous)"
            % (rows, cty, newest, rows - (previous_rows or 0)))
        return 0

    except Exception as exc:
        conn.rollback()
        err = "%s: %s" % (type(exc).__name__, exc)
        log("FAILED -- %s" % err)
        log("%s is untouched and still serving" % LIVE)
        try:
            with conn.cursor() as cur:
                ensure_build_log(cur)
                cur.execute(
                    "INSERT INTO scoring.avm_build_log (started_at, "
                    "finished_at, source_file, previous_rows, status, notes) "
                    "VALUES (%s, now(), %s, %s, 'error', %s)",
                    (started, args.source_file, previous_rows, err[:1000]),
                )
            conn.commit()
        except Exception:
            pass
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
