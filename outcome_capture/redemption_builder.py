"""
Redemption tracker builder.

Populates outcomes.redemption_tracker from distress signals — one row per
foreclosed property, carrying the date its redemption window closes. That
date is the single most consequential fact Govire publishes to a homeowner:
it is the day they lose the right to save their home.

Run: python outcome_capture/redemption_builder.py

=== WHY THIS FILE EXISTS (2026-07-28) ===
The tracker held 1,088 rows and NOTHING IN THE REPO POPULATED IT. Not a
function, not a trigger, not a cron job, not a workflow — the original
loader was run from somewhere else and was never committed. It is the third
such case found in one day (the eCRV loader and the Ramsey parcels workflow
were the others).

That mattered because the uncommitted loader defaulted EVERY row to a
6-month redemption period. Measured live:

  * 193 of 529 Hennepin rows contradicted the county's OWN published
    expiry date — by up to 375 days.
  * 31 were true 12-month redemptions shown as 6-month: an owner would be
    told their window had closed while they still had six months of right
    to redeem.
  * 28 were SHORTENED periods (Minn. Stat. 580.07 allows five weeks) shown
    as 6-month: the owner would believe they had months they did not have.
  * One of the wrong rows had outcome 'redeemed_by_owner' — a real person
    who saved their home on a date our tracker said had already passed.

Hennepin publishes `redemptionExpirationDate` in every single record and
nothing was reading it. Same class of defect as Dakota's TAXPIN: the
authoritative field sat in the feed, unrequested, for months.

=== SOURCE PRECEDENCE ===
Highest-confidence source wins. Never compute when the source states it.

  1. COUNTY-PUBLISHED EXPIRY  (period_source='scraped')
     hennepin_sheriff → raw_data.redemptionExpirationDate
     The recording county's own figure. Authoritative. Verified 2026-07-28:
     529/529 Hennepin rows, periods ranging 1 to 12 months.

  2. NOTICE-STATED PERIOD     (period_source='scraped')
     signals.sheriff_sales.redemption_period_months, parsed from the
     statutory notice text by foreclosure_promotion._parse_redemption_months.
     Verified against all 14 distinct phrasings: 215/215 correct.
     A value of 0 means 'No right of redemption' — NO tracker row is
     created, because there is no window to track.

  3. STATUTORY DEFAULT        (period_source='default_6mo')
     Six months from the sale date. Used ONLY where the source genuinely
     publishes nothing. Verified 2026-07-28 by enumerating every field
     these three feeds expose:
       dakota_sheriff     — SaleDate, SaleAmount, GeoAddress, GeoCity + ids
       anoka_sheriff      — address, city, zip, mode, scheduled_date
       washington_sheriff — document_number, instrument, lender, purchaser
     None carries a redemption period or expiry. This is a real limitation
     of the source, not an extraction failure, and the calculator MUST say
     so for these counties rather than presenting the date as known.

Anoka additionally publishes `mode` = 'Pending Sales' for SCHEDULED sales.
The redemption clock does not start until the sale actually happens, so a
default computed from a scheduled date is doubly assumed. Those rows are
skipped — see _is_scheduled_not_sold.

=== IDEMPOTENCE ===
Upserts on (source_table, source_id). Re-running is safe and will CORRECT
rows whose source has since published a better date — which is the whole
point: run it after every sheriff scrape.

Never downgrades. A row already on a county-published date is not
overwritten by a computed default.

The upsert also recomputes next_check_date on UPDATE, not only on INSERT
(fixed 2026-08-02). See the comment in UPSERT_SQL: a corrected expiry that
left a stale check date caused outcome_checker to run before the window had
closed and record an outcome for a redemption that was still open.
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta
from typing import Any, Optional

import psycopg2
import psycopg2.extras


DEFAULT_PERIOD_MONTHS = 6

# Sources whose raw_data carries a county-published expiry date, and the
# JSON path to it. Verified live 2026-07-28.
COUNTY_EXPIRY_PATHS: dict[str, tuple[str, ...]] = {
    "hennepin_sheriff": ("redemptionExpirationDate",),
}

# Sources verified to publish NOTHING about the redemption period. Listed
# explicitly so that a future field appearing in one of these feeds is a
# visible surprise rather than a silent default.
NO_PERIOD_SOURCES = {
    "dakota_sheriff",
    "anoka_sheriff",
    "washington_sheriff",
}


def log(msg: str) -> None:
    print(f"[redemption-builder] {msg}", flush=True)


def _as_date(value: Any) -> Optional[date]:
    """Parse a date from the several shapes these feeds use."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:19] if "T" in s else s, fmt).date()
        except ValueError:
            continue
    return None


def _add_months(d: date, months: int) -> date:
    """Add calendar months, clamping to the last valid day.

    Deliberately NOT `d + timedelta(days=months*30)`. A redemption deadline
    is a calendar date; 'six months from 31 August' is 28 February, not
    'roughly 180 days later'.
    """
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    day = d.day
    while day > 0:
        try:
            return date(year, month, day)
        except ValueError:
            day -= 1
    raise ValueError(f"could not add {months} months to {d}")


def _months_between(anchor: date, expiry: date) -> Optional[int]:
    """Approximate whole months between two dates, for reporting only.

    The stored expiry is always the authoritative date; this figure exists
    so a human reading the table can see at a glance whether a row is a
    6-month, 12-month or shortened period.
    """
    if not anchor or not expiry or expiry < anchor:
        return None
    return max(1, round((expiry - anchor).days / 30.44))


def _county_expiry(source: str, raw: dict[str, Any]) -> Optional[date]:
    """The county's own published expiry date, if this source has one."""
    for key in COUNTY_EXPIRY_PATHS.get(source, ()):
        found = _as_date(raw.get(key))
        if found:
            return found
    return None


def _is_scheduled_not_sold(source: str, raw: dict[str, Any]) -> bool:
    """True when the sale has NOT happened yet, so no clock has started.

    Anoka publishes both pending and completed sales in the same feed and
    distinguishes them with `list.mode`. Creating a redemption window for a
    sale that has not occurred would put a fabricated deadline in front of
    an owner whose sale might still be postponed or cancelled.
    """
    if source == "anoka_sheriff":
        mode = ((raw.get("list") or {}).get("mode") or "").strip().lower()
        return "pending" in mode
    return False


def build_rows(conn) -> list[dict[str, Any]]:
    """Read distress signals and derive one tracker row per property."""
    rows: list[dict[str, Any]] = []
    skipped_scheduled = 0
    skipped_no_redemption = 0
    skipped_no_date = 0

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # --- 1. sheriff-sale signals -------------------------------------
        cur.execute(
            """
            SELECT de.id, de.source, de.parcel_id, de.event_date, de.raw_data,
                   ss.redemption_period_months AS notice_months
            FROM signals.distress_events de
            LEFT JOIN signals.sheriff_sales ss ON ss.parcel_id = de.parcel_id
            WHERE de.event_type = 'sheriff_sale'
            """
        )
        for r in cur.fetchall():
            raw = r["raw_data"] or {}
            source = r["source"]

            if _is_scheduled_not_sold(source, raw):
                skipped_scheduled += 1
                continue

            # 'No right of redemption' — there is no window to track.
            if r["notice_months"] == 0:
                skipped_no_redemption += 1
                continue

            anchor = (
                _as_date(raw.get("dateOfSale"))
                or _as_date((raw.get("sale") or {}).get("sale_date"))
                or _as_date((raw.get("list") or {}).get("scheduled_date"))
                or _as_date(raw.get("SaleDate"))
                or _as_date(r["event_date"])
            )
            if not anchor:
                skipped_no_date += 1
                continue

            # --- precedence ---
            expiry = _county_expiry(source, raw)
            if expiry:
                period_source = "scraped"
            elif r["notice_months"]:
                expiry = _add_months(anchor, int(r["notice_months"]))
                period_source = "scraped"
            else:
                expiry = _add_months(anchor, DEFAULT_PERIOD_MONTHS)
                period_source = "default_6mo"

            rows.append({
                "county_code": None,          # resolved in SQL below
                "parcel_id": r["parcel_id"],
                "source_table": "signals.distress_events",
                "source_id": r["id"],
                "anchor_date": anchor,
                "anchor_type": "sheriff_sale",
                "redemption_period_months": _months_between(anchor, expiry),
                "period_source": period_source,
                "redemption_expiry_date": expiry,
            })

    log(f"derived {len(rows)} rows "
        f"(skipped: {skipped_scheduled} scheduled, "
        f"{skipped_no_redemption} no-redemption-right, "
        f"{skipped_no_date} no sale date)")
    return rows


UPSERT_SQL = """
INSERT INTO outcomes.redemption_tracker
    (county_code, parcel_id, source_table, source_id,
     anchor_date, anchor_type, redemption_period_months,
     period_source, redemption_expiry_date, check_stage, outcome,
     next_check_date)
SELECT COALESCE(p.county_code, 'unknown'),
       %(parcel_id)s, %(source_table)s, %(source_id)s,
       %(anchor_date)s, %(anchor_type)s, %(redemption_period_months)s,
       %(period_source)s, %(redemption_expiry_date)s, 0, 'pending',
       -- next_check_date MUST be set on insert. outcome_checker selects on
       -- `next_check_date <= today`, so a NULL here makes the row invisible
       -- to it forever: the property sits 'pending' and its outcome never
       -- reaches distressed_exit_sales. Found live 2026-07-28 — 266 rows
       -- inserted without it were stranded.
       -- Mirrors outcome_checker.LADDER_OFFSETS = [30, 60, 90, 180]: the
       -- first rung past expiry that is still in the future.
       CASE
         WHEN %(redemption_expiry_date)s::date + 30  > CURRENT_DATE
           THEN %(redemption_expiry_date)s::date + 30
         WHEN %(redemption_expiry_date)s::date + 60  > CURRENT_DATE
           THEN %(redemption_expiry_date)s::date + 60
         WHEN %(redemption_expiry_date)s::date + 90  > CURRENT_DATE
           THEN %(redemption_expiry_date)s::date + 90
         WHEN %(redemption_expiry_date)s::date + 180 > CURRENT_DATE
           THEN %(redemption_expiry_date)s::date + 180
         ELSE CURRENT_DATE
       END
FROM (SELECT 1) x
LEFT JOIN core.parcels p ON p.parcel_id = %(parcel_id)s
ON CONFLICT (source_table, source_id) DO UPDATE
SET redemption_expiry_date   = EXCLUDED.redemption_expiry_date,
    redemption_period_months = EXCLUDED.redemption_period_months,
    period_source            = EXCLUDED.period_source,
    anchor_date              = EXCLUDED.anchor_date,
    -- next_check_date MUST be recomputed here too, not only on INSERT.
    -- ADDED 2026-08-02. Until today this clause moved the expiry date and
    -- left next_check_date on the ladder derived from the OLD expiry. Any
    -- row whose window was corrected LATER — a postponed sheriff sale, or a
    -- county-published expiry replacing a computed one — kept a check date
    -- that had already passed relative to its new window. Measured live:
    -- 56 rows across 43 parcels, the worst scheduled 164 days BEFORE its
    -- own expiry, six of them having already burned a ladder rung.
    --
    -- The consequence is not a wrong date shown to an owner; it is a wrong
    -- OUTCOME. outcome_checker selects on `next_check_date <= today`, looks
    -- for an REO owner match or a post-expiry sale, finds neither because
    -- the window is still open, advances the stage and reschedules. A row
    -- can burn all four rungs before its window closes and then resolve to
    -- 'unknown' — the same stranding as the 266 NULL rows of 2026-07-28,
    -- reached from the opposite direction.
    --
    -- GREATEST() so a check date can only ever move LATER. If the expiry is
    -- ever corrected EARLIER, the existing (later) check still stands: a
    -- check that runs late costs a delayed label, one that runs early
    -- records a conclusion about a window that had not closed. GREATEST
    -- also ignores NULL, so a row stranded with no check date at all is
    -- repaired on the next run rather than staying invisible forever.
    next_check_date = GREATEST(
      outcomes.redemption_tracker.next_check_date,
      CASE
        WHEN EXCLUDED.redemption_expiry_date + 30  > CURRENT_DATE
          THEN EXCLUDED.redemption_expiry_date + 30
        WHEN EXCLUDED.redemption_expiry_date + 60  > CURRENT_DATE
          THEN EXCLUDED.redemption_expiry_date + 60
        WHEN EXCLUDED.redemption_expiry_date + 90  > CURRENT_DATE
          THEN EXCLUDED.redemption_expiry_date + 90
        WHEN EXCLUDED.redemption_expiry_date + 180 > CURRENT_DATE
          THEN EXCLUDED.redemption_expiry_date + 180
        -- Ladder exhausted: hand it to the checker NOW. next_ladder_date()
        -- returns None in that state and decide() resolves the row to
        -- 'unknown', so checking today RESOLVES it. Parking it further out
        -- would only delay that.
        ELSE CURRENT_DATE
      END
    ),
    updated_at               = now()
-- NEVER downgrade: a row already on a county-published date is not
-- replaced by a computed default.
WHERE NOT (outcomes.redemption_tracker.period_source = 'scraped'
           AND EXCLUDED.period_source = 'default_6mo');
"""


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1
    dry_run = os.environ.get("DRY_RUN") == "1"

    conn = psycopg2.connect(dsn)
    try:
        rows = build_rows(conn)
        if not rows:
            log("no rows derived; nothing to do")
            return 0

        by_source: dict[str, int] = {}
        for r in rows:
            by_source[r["period_source"]] = by_source.get(r["period_source"], 0) + 1
        log(f"period_source breakdown: {by_source}")

        if dry_run:
            log("DRY_RUN=1 — no writes performed")
            return 0

        written = 0
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(UPSERT_SQL, r)
                written += cur.rowcount
        conn.commit()
        log(f"upserted {written} tracker rows")
        return 0
    except Exception as e:
        conn.rollback()
        log(f"FAILED — {type(e).__name__}: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
