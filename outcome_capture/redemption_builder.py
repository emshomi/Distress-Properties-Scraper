"""
Redemption tracker builder.

Populates outcomes.redemption_tracker from distress signals — one row per
property in a redemption window, carrying the date that window closes. That
date is the single most consequential fact Govire publishes to a homeowner:
it is the day they lose the right to save their home.

Run: python outcome_capture/redemption_builder.py

=== TWO STATUTORY TRACKS (second added 2026-08-10) ===
1. MORTGAGE FORECLOSURE (Minn. Stat. 580/582) — event_type='sheriff_sale'.
   anchor_type='sheriff_sale', anchor is the sale date, and the expiry is
   derived through the precedence ladder below.

2. TAX FORFEITURE (Minn. Stat. ch. 281) — event_type='tax_forfeiture_
   redemption', from county auditors' Notices of Expiration of Redemption
   scraped by mnpublicnotice and promoted by redemption_promoter.py.
   anchor_type='tax_judgment_sale', period_source='county_stated'.

   NOTHING IS DERIVED for these. The county published the expiry date and it
   is stored untouched. The ladder does not apply because there is nothing
   to fall back TO — and this file's own history is the argument for that
   restraint.

   The anchor is the bid-in date when the county states one and the EXPIRY
   ITSELF when it does not. Measured 2026-08-10: only 151 of 288 events
   carry a bid-in date (crow_wing 138, jackson 13); nine counties publish a
   terser notice. anchor_date is NOT NULL and part of the fact key, so those
   rows anchor on a real published date rather than a NULL or an invented
   one, with redemption_period_months left NULL to show no period was ever
   stated.

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

=== A SALE THAT HAS NOT HAPPENED STARTS NO CLOCK (fixed 2026-08-07) ===
Measured live before this fix: 275 tracker rows carried an anchor_date in
the FUTURE — 234 of them surfacing through outcomes.redemption_current onto
the public homepage, inflating "IN REDEMPTION NOW" from roughly 535 to 769.

Every one was a property whose sheriff's sale had not yet occurred. Minn.
Stat. 580.03 requires six weeks of published notice before a sale, so a
foreclosure notice necessarily announces a FUTURE date. build_rows anchored
the redemption clock on that announced date and computed an expiry from it.
The owner of a property whose sale is two months away was recorded — and
published — as being in redemption today.

By source: mnpublicnotice 187, anoka_sheriff 45 (pre-guard rows),
postbulletin_legal 2. mnpublicnotice is statewide and publishes NOTHING BUT
scheduled sales, so as that feed grew this defect grew with it.

signals.sheriff_sales.sale_status was 'scheduled' on every affected row.
build_rows read redemption_period_months off that same row and never looked
at the column beside it. The authoritative field was present and unrequested
— the identical shape to the redemptionExpirationDate defect above.

TWO guards now, because they fail in different places:

  1. sale_status = 'scheduled' — the source's own explicit statement. Exists
     only where a sheriff_sales row matched, so it cannot stand alone: the
     45 anoka_sheriff and 2 postbulletin_legal rows had no such row.
  2. anchor_date in the future — the universal invariant. A redemption
     window cannot open before the sale that opens it. Needs no per-feed
     knowledge and covers feeds not yet written.

Both are SELF-HEALING. Nothing is lost permanently: once the sale date
passes, the guard stops firing and the next run creates the row with a real
anchor and, where a county publishes one, a real expiry. The signals
themselves are untouched in signals.distress_events throughout — the
marketplace, property pages and /connect/lookup still show these
pre-foreclosure properties. Only the redemption tracker abstains, and it
abstains because a wrong legal deadline in front of a homeowner is worse
than no deadline at all.

_is_scheduled_not_sold is KEPT. It catches Anoka rows whose scheduled date
has already passed without a sale, which the future-anchor guard cannot see.

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
Upserts on (county_code, parcel_id, anchor_date) — the FACT, not the record
that reported it. Re-running is safe and will CORRECT rows whose source has
since published a better date — which is the whole point: run it after every
sheriff scrape. Two sources reporting the same sale land on ONE row.

Never downgrades. A row already on a county-published date is not
overwritten by a computed default.

The upsert also recomputes next_check_date on UPDATE, not only on INSERT
(fixed 2026-08-02). See the comment in UPSERT_SQL: a corrected expiry that
left a stale check date caused outcome_checker to run before the window had
closed and record an outcome for a redemption that was still open.

NOTE on skipping: a skip prevents a row being WRITTEN or UPDATED; it does
not remove one already present. Rows that predate a new guard must be
deleted separately. The 275 future-anchored rows found on 2026-08-07 were
cleared by hand after this fix shipped; none had dependent
outcomes.owner_checks rows, an advanced check_stage or a recorded outcome,
so the error never reached the checker pipeline or distressed_exit_sales.

=== COUNTY CODE IS CARRIED, NOT LOOKED UP (fixed 2026-08-07) ===
This file died on every run for roughly 24 hours with:

    CardinalityViolation: ON CONFLICT DO UPDATE command cannot affect row
    a second time

core.parcels moved to PRIMARY KEY (county_code, parcel_id) because Minnesota
county PINs are not globally unique — 51,662 nine-character PINs are shared
across counties. UPSERT_SQL resolved county_code with

    LEFT JOIN core.parcels p ON p.parcel_id = %(parcel_id)s

which, for a PIN present in two counties, turned the statement's single
source row into two — two INSERTs proposing the same (source_table,
source_id), which is precisely what ON CONFLICT DO UPDATE forbids.

Measured 2026-08-07: 1,457 sheriff-sale events produced 1,461 rows through
that join. FOUR events. main() runs every upsert inside one transaction and
rolls back on any exception, so four ambiguous PINs discarded all 1,457
writes and froze the homepage OUTCOMES block from 2026-08-06 14:57.

The join is now GONE rather than made composite. signals.distress_events
carries county_code directly (backfilled 2026-08-07, 7,460 of 7,472; the 12
NULLs are orphans whose parcel does not exist), which is the same value the
join was reaching for. Carrying it through build_rows removes the fanout at
its source and removes a per-row join from 1,457 statements. The 12 orphans
land on 'unknown' exactly as the old COALESCE put them.

NEVER reintroduce a single-column parcel_id join here. Under the composite
key it is not a slow query, it is a wrong one.
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

# signals.sheriff_sales.sale_status values meaning the sale has NOT been
# held. A redemption clock must not start from any of these. Measured
# 2026-08-07: every one of the 275 future-anchored tracker rows that had a
# matching sheriff_sales row carried 'scheduled'.
UNHELD_SALE_STATUSES = {
    "scheduled",
    "pending",
    "postponed",
    "cancelled",
    "canceled",
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

    This is the SOURCE-SPECIFIC guard and it remains necessary: it catches
    Anoka rows whose scheduled date has already PASSED without a sale, which
    the future-anchor guard in build_rows cannot see.
    """
    if source == "anoka_sheriff":
        mode = ((raw.get("list") or {}).get("mode") or "").strip().lower()
        return "pending" in mode
    return False


def _is_unheld_sale(sale_status: Any) -> bool:
    """True when signals.sheriff_sales states the sale has not been held.

    The source's own explicit signal, and the strongest available. Only
    usable where a sheriff_sales row matched — see the future-anchor guard
    in build_rows for the cases this cannot reach.
    """
    if sale_status is None:
        return False
    return str(sale_status).strip().lower() in UNHELD_SALE_STATUSES


def build_rows(conn) -> list[dict[str, Any]]:
    """Read distress signals and derive one tracker row per property."""
    rows: list[dict[str, Any]] = []
    skipped_scheduled = 0
    skipped_unheld_status = 0
    skipped_future_anchor = 0
    skipped_no_redemption = 0
    skipped_no_date = 0
    tax_rows = 0
    skipped_anchor_after_expiry = 0
    today = date.today()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # --- 1. sheriff-sale signals -------------------------------------
        # de.county_code is SELECTED and carried into the row dict: it is the
        # value UPSERT_SQL used to reach for via core.parcels. See the
        # module docstring for why that join is gone.
        #
        # ss.sale_status is SELECTED because it is the source's own statement
        # that a sale has or has not been held. It sat one column away from
        # redemption_period_months, which this query has always read, while
        # 275 rows were anchored on sales that had not happened.
        #
        # The sheriff_sales join is composite. Measured 2026-08-07 it returns
        # 266 rows with or without county_code, so nothing changes today —
        # this is a latent trap being closed, not active damage. Under the
        # composite key a bare parcel_id can match another county's sale and
        # hand this event ITS redemption period, which is the exact class of
        # wrong-deadline defect this file was written to eliminate.
        #
        # Deliberately NOT also matching ss.sale_date = de.event_date. That
        # narrows 266 to 262: four events carry an event_date that is not the
        # sale date, and matching on it would strip their notice-stated
        # period and silently fall them back to the 6-month default —
        # inventing a wrong deadline to close a fanout that is not occurring.
        cur.execute(
            """
            SELECT de.id, de.source, de.parcel_id, de.county_code,
                   de.event_date, de.raw_data,
                   ss.redemption_period_months AS notice_months,
                   ss.sale_status              AS sale_status
            FROM signals.distress_events de
            LEFT JOIN signals.sheriff_sales ss
                   ON ss.parcel_id   = de.parcel_id
                  AND ss.county_code = de.county_code
            WHERE de.event_type = 'sheriff_sale'
            """
        )
        for r in cur.fetchall():
            raw = r["raw_data"] or {}
            source = r["source"]

            if _is_scheduled_not_sold(source, raw):
                skipped_scheduled += 1
                continue

            # GUARD 1 — the source says the sale has not been held.
            if _is_unheld_sale(r["sale_status"]):
                skipped_unheld_status += 1
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

            # GUARD 2 — a redemption window cannot open before the sale that
            # opens it. Universal: needs no per-feed knowledge and covers
            # feeds not yet written. Self-healing: once the date passes, the
            # next run creates this row with a real anchor.
            if anchor > today:
                skipped_future_anchor += 1
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
                "county_code": r["county_code"],
                "parcel_id": r["parcel_id"],
                "source_table": "signals.distress_events",
                "source_id": r["id"],
                "anchor_date": anchor,
                "anchor_type": "sheriff_sale",
                "redemption_period_months": _months_between(anchor, expiry),
                "period_source": period_source,
                "redemption_expiry_date": expiry,
            })

        # --- 2. tax-forfeiture redemption signals ------------------------
        #
        # ADDED 2026-08-10. Minn. Stat. ch. 281 Notices of Expiration of
        # Redemption, scraped from mnpublicnotice and promoted by
        # outcome_capture/redemption_promoter.py.
        #
        # A DIFFERENT STATUTORY TRACK from everything above. These parcels
        # were bid in for the State at a TAX JUDGMENT SALE (ch. 281), not
        # sold at a mortgage foreclosure sheriff's sale (ch. 580/582). Hence
        # anchor_type='tax_judgment_sale' — forcing 'sheriff_sale' onto them
        # would make every row in this table ambiguous about which law it
        # describes.
        #
        # NOTHING IS COMPUTED HERE. The expiry is the date the county
        # auditor PUBLISHED, carried on de.event_date, and it goes in
        # untouched. There is no precedence ladder for these rows because
        # there is nothing to fall back to: this file's own history is 193
        # Hennepin rows contradicting a county's published date by up to 375
        # days, one of them belonging to an owner who redeemed on a day the
        # tracker said had passed. period_source='county_stated' marks that
        # distinction — it is stronger than 'scraped', which means a period
        # was scraped and an expiry derived from it.
        cur.execute(
            """
            SELECT de.id, de.source, de.parcel_id, de.county_code,
                   de.event_date, de.raw_data
            FROM signals.distress_events de
            WHERE de.event_type = 'tax_forfeiture_redemption'
              AND de.event_date IS NOT NULL
              AND de.county_code IS NOT NULL
              AND de.parcel_id IS NOT NULL
            """
        )
        for r in cur.fetchall():
            raw = r["raw_data"] or {}

            expiry = _as_date(r["event_date"])
            if not expiry:
                skipped_no_date += 1
                continue

            # ANCHOR: the bid-in date when the county publishes one, the
            # expiry itself when it does not.
            #
            # Measured 2026-08-10: only 151 of 288 events carry a bid-in
            # date — Crow Wing 138 and Jackson 13. The other NINE counties
            # publish a terser notice that never states it. anchor_date is
            # NOT NULL and is part of the fact key
            # (county_code, parcel_id, anchor_date), so those rows need an
            # anchor that is a REAL PUBLISHED DATE rather than a NULL or a
            # computed one.
            #
            # Falling back to the expiry is honest: the row then says "this
            # window ends on the date the county published", and
            # redemption_period_months is left NULL because no period was
            # ever stated or derived. It is not a claim about when the tax
            # judgment sale happened.
            anchor = _as_date(raw.get("bid_in_date")) or expiry

            # A window cannot end before it opens. Mirrors GUARD 2 above.
            if anchor > expiry:
                skipped_anchor_after_expiry += 1
                continue

            rows.append({
                "county_code": r["county_code"],
                "parcel_id": r["parcel_id"],
                "source_table": "signals.distress_events",
                "source_id": r["id"],
                "anchor_date": anchor,
                "anchor_type": "tax_judgment_sale",
                # NULL, not 0, when the anchor IS the expiry: no period was
                # stated and none was computed, and 0 would read as a
                # same-day window.
                "redemption_period_months": (
                    _months_between(anchor, expiry) if anchor != expiry
                    else None),
                "period_source": "county_stated",
                "redemption_expiry_date": expiry,
            })
            tax_rows += 1

    log(f"derived {len(rows)} rows "
        f"({tax_rows} tax-forfeiture; skipped: "
        f"{skipped_anchor_after_expiry} anchor-after-expiry, "
        f"{skipped_scheduled} anoka-pending, "
        f"{skipped_unheld_status} sale-not-held, "
        f"{skipped_future_anchor} future sale date, "
        f"{skipped_no_redemption} no-redemption-right, "
        f"{skipped_no_date} no sale date)")
    return rows


# A SELECT with no FROM clause yields EXACTLY ONE row, which is the
# invariant ON CONFLICT DO UPDATE requires: one proposed row per statement.
# The previous form was `FROM (SELECT 1) x LEFT JOIN core.parcels p ON
# p.parcel_id = %(parcel_id)s`, and a PIN held by two counties made that two
# rows. See the module docstring.
#
# county_code is cast to ::text because psycopg2 sends an untyped NULL for
# the 12 orphan events and COALESCE cannot infer a type from two unknowns.
UPSERT_SQL = """
INSERT INTO outcomes.redemption_tracker
    (county_code, parcel_id, source_table, source_id,
     anchor_date, anchor_type, redemption_period_months,
     period_source, redemption_expiry_date, check_stage, outcome,
     next_check_date)
SELECT COALESCE(%(county_code)s::text, 'unknown'),
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
-- CONFLICT TARGET CHANGED 2026-08-07 (task 542).
--
-- Was `ON CONFLICT (source_table, source_id)` — the identity of a redemption
-- window was the SCRAPER RECORD that reported it, not the window itself. So
-- one sheriff sale reported twice produced TWO clocks for one property.
-- Measured before the fix: 1,323 tracker rows describing only 1,261 real
-- windows. 62 duplicates, and NOT only Anoka pending-vs-completed pairs —
-- `RICE-FC-66-CV-25-1045` was an mnpublicnotice row duplicated against
-- itself, so the defect was already cross-source.
--
-- A redemption window is a fact about a PROPERTY AND A SALE: this parcel
-- had a sheriff sale on this date, so the owner has until this date to
-- redeem. It is not a fact about a feed. `outcomes.redemption_tracker` now
-- carries a unique index `redemption_tracker_fact_key` on
-- (county_code, parcel_id, anchor_date), and this target must match it —
-- ON CONFLICT on a DIFFERENT index does not catch a violation of that one,
-- so a mismatch here raises a unique violation and main() rolls back the
-- entire run.
--
-- Two sources reporting the same sale is now CONFIRMATION, not duplication:
-- the second one updates the existing row.
--
-- NOT YET SOLVED: a POSTPONED sale moves anchor_date, which is part of the
-- key, so the postponed sale inserts a NEW row and the old window survives
-- alongside it. Retiring windows whose sale date has moved is separate work.
ON CONFLICT (county_code, parcel_id, anchor_date) DO UPDATE
SET redemption_expiry_date   = EXCLUDED.redemption_expiry_date,
    redemption_period_months = EXCLUDED.redemption_period_months,
    period_source            = EXCLUDED.period_source,
    -- Provenance follows the winning row. When a completed sale supersedes
    -- the pending record of the same sale, the tracker should say it came
    -- from the completed event. anchor_date and county_code are NOT set
    -- here: both are part of the conflict key, so EXCLUDED already equals
    -- the stored value and assigning them would be misleading.
    source_table             = EXCLUDED.source_table,
    source_id                = EXCLUDED.source_id,
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

        future = sum(1 for r in rows if r["anchor_date"] > date.today())
        if future:
            log(f"FATAL: {future} derived rows still carry a future anchor_date "
                f"— the guard in build_rows did not hold; refusing to write")
            return 1

        no_county = sum(1 for r in rows if not r["county_code"])
        if no_county:
            log(f"WARNING: {no_county} rows have no county_code and will be "
                f"stored as 'unknown' (orphan events whose parcel is absent "
                f"from core.parcels)")

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
