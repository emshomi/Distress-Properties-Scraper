"""
Redemption notice promoter.

Promotes rows from ai.extracted_redemptions into signals.distress_events.

Run: python outcome_capture/redemption_promoter.py

=== WHY THIS FILE EXISTS (2026-08-09) ===
Foreclosure extractions are promoted ONE AT A TIME by a human, through
POST /extractions/approve in src/routes/admin.py. 417 of 434 were approved
that way. That gate is right for a few notices a day.

It does not survive this feed. A single Crow Wing publication staged 144
parcels; the 25 remaining zero-signal counties will stage thousands in their
first sweep. Nobody clicks approve 3,000 times. The realistic outcomes are
that promotion silently never happens (the hennepin_tax_roll failure: a
working source disabled for months with nobody deciding to disable it), or
that someone bulk-approves without looking — which is worse than no gate,
because it launders unreviewed rows as reviewed.

So: promote automatically on OBJECTIVE COMPLETENESS, hold everything else
for the human queue. The gate stays meaningful because it holds exactly the
rows that need a person, and the queue stays a size a person will actually
work.

=== WHY THE CONFIDENCE FLOOR IS 0.80 ===
Measured on the first 144 Crow Wing rows, not chosen for roundness:

  confidence >= 0.80 :  95 rows,  95 resolved to core.parcels,  0 failures
  confidence <  0.80 :  49 rows,  43 resolved,                  6 failures

EVERY parcel-resolution failure sat below 0.80. Every row missing a mailing
city sat below 0.80. From 0.75 down, extraction_notes is populated on
essentially every row (19/20, 12/12, 8/8, 7/7).

It is a real break in the data rather than an invented threshold.

Known imperfection, recorded so it is not rediscovered: the 0.88 band is 13
rows, 12 with notes, ALL 13 missing a property address — the model marking
itself down for a gap in the SOURCE (many parcels are vacant land with no
street address in the notice at all). The 0.92 band has the same gap with no
self-penalty. So the score is not perfectly calibrated about source gaps,
and the floor is deliberately set where RESOLUTION breaks, not where notes
appear.

=== WHAT THIS DELIBERATELY DOES NOT DO ===
It does not touch outcomes.redemption_tracker. redemption_builder.py owns
the precedence ladder (county-published expiry > notice-stated period >
statutory default) and rebuilds the tracker from distress_events. Writing
the tracker here would be a second copy of that logic and the two would
drift.

Note the builder currently filters `WHERE de.event_type = 'sheriff_sale'`
and hardcodes anchor_type='sheriff_sale', so it does NOT yet see these
events. Reaching the tracker needs (a) 'tax_judgment_sale' added to the
outcomes.redemption_tracker anchor_type CHECK constraint and (b) the builder
taught to read this event type. Both are deliberate follow-on work: that
file publishes the most consequential date Govire has, and has twice been
found mis-stating it.

=== IDEMPOTENCY ===
Two independent guards, because they fail in different places:

  1. ai.extracted_redemptions.promoted_at IS NULL — a row is promoted once.
  2. A pre-flight check against signals.distress_events on
     (source, source_id), so a staging row whose promoted_at was cleared by
     hand cannot create a duplicate event.
"""

from __future__ import annotations

import os
import sys
from typing import Any

import psycopg2
import psycopg2.extras


EVENT_SOURCE = "mnpn_redemption_notice"
EVENT_TYPE = "tax_forfeiture_redemption"
EVENT_SUBTYPE = "expiration_of_redemption"

# See the module docstring: measured, not chosen.
CONFIDENCE_FLOOR = 0.80

# severity is CHECK-constrained to low/medium/high/critical.
#
# 'high', not 'medium': olmsted_delq_list uses 'medium' for "appears on the
# delinquent tax list", which is the START of the ladder. These parcels have
# ALREADY been bid in for the state at the tax judgment sale and are inside
# the final statutory redemption window with a county-stated deadline. That
# is strictly worse.
#
# 'critical' is reserved for a future refinement keyed on days remaining.
EVENT_SEVERITY = "high"


def log(msg: str) -> None:
    print(f"[redemption-promoter] {msg}", flush=True)


SELECT_CANDIDATES = """
SELECT r.id,
       r.county_code,
       r.parcel_id,
       r.parcel_id_raw,
       r.owner_name,
       r.mailing_city_state_zip,
       r.do_not_mail,
       r.confidence,
       r.redemption_amount,
       r.bid_in_date,
       r.delinquent_tax_year,
       r.redemption_expiry,
       r.source_url,
       r.fetched_at
FROM ai.extracted_redemptions r
WHERE r.promoted_at IS NULL
  AND r.review_status <> 'rejected'
  -- Objective completeness. Every one of these is a fact about whether the
  -- row is USABLE, not a judgement about whether it is correct.
  AND r.parcel_id IS NOT NULL          -- resolved against core.parcels
  AND r.redemption_expiry IS NOT NULL  -- the deadline being published
  AND r.redemption_amount IS NOT NULL
  AND r.bid_in_date IS NOT NULL
  AND r.delinquent_tax_year IS NOT NULL
  AND r.confidence >= %(floor)s
ORDER BY r.id;
"""

# Rows that fail the gate, for the review queue readout. Counted, not
# promoted -- a row nobody looks at is a row nobody decided about, so the
# number is logged every run rather than left to be discovered.
SELECT_HELD = """
SELECT count(*)                                                AS held,
       count(*) FILTER (WHERE parcel_id IS NULL)               AS unresolved_parcel,
       count(*) FILTER (WHERE confidence < %(floor)s
                        AND parcel_id IS NOT NULL)             AS low_confidence,
       count(*) FILTER (WHERE redemption_expiry IS NULL)       AS no_expiry,
       count(*) FILTER (WHERE redemption_amount IS NULL)       AS no_amount
FROM ai.extracted_redemptions
WHERE promoted_at IS NULL
  AND review_status <> 'rejected';
"""

EXISTING_EVENT = """
SELECT 1
FROM signals.distress_events
WHERE source = %(source)s
  AND source_id = %(source_id)s
LIMIT 1;
"""

INSERT_EVENT = """
INSERT INTO signals.distress_events
    (county_code, parcel_id, event_type, event_subtype, severity,
     source, source_id, title, description, event_date, event_value,
     observed_at, raw_data)
VALUES
    (%(county_code)s, %(parcel_id)s, %(event_type)s, %(event_subtype)s,
     %(severity)s, %(source)s, %(source_id)s, %(title)s, %(description)s,
     %(event_date)s, %(event_value)s, %(observed_at)s, %(raw_data)s);
"""

MARK_PROMOTED = """
UPDATE ai.extracted_redemptions
   SET promoted_at = now(),
       review_status = 'promoted'
 WHERE id = %(id)s;
"""


def _money(value: Any) -> str:
    """Format an amount for display. Never returns 'None' in user-facing text."""
    if value is None:
        return "an amount stated in the notice"
    return f"${value:,.2f}"


def _fmt_date(value: Any) -> str:
    """'September 18, 2026'.

    Built from parts rather than strftime("%B %-d, %Y"): the %-d
    no-pad directive is glibc-only and raises ValueError on Windows, and
    this file may be run locally as well as on the Ubuntu runner.
    """
    if value is None:
        return "a date stated in the notice"
    if not hasattr(value, "strftime"):
        return str(value)
    return f"{value.strftime('%B')} {value.day}, {value.year}"


def build_event(row: dict[str, Any]) -> dict[str, Any]:
    """One staging row -> one signals.distress_events row.

    event_date is the EXPIRY, not the bid-in date. The platform sorts and
    filters on urgency and the deadline is the actionable fact; the bid-in
    date is preserved in raw_data.
    """
    expiry = _fmt_date(row["redemption_expiry"])
    amount = _money(row["redemption_amount"])
    bid_in = _fmt_date(row["bid_in_date"])

    title = f"Redemption expires {expiry} - {amount} to redeem"

    description = (
        f"Parcel was bid in for the State of Minnesota on {bid_in} at the tax "
        f"judgment sale for delinquent taxes of year "
        f"{row['delinquent_tax_year']}. Per the county auditor's Notice of "
        f"Expiration of Redemption (Minn. Stat. ch. 281), {amount} must be "
        f"paid on or before {expiry} or the land forfeits to the State. The "
        f"redemption expiry date is STATED BY THE COUNTY, not computed."
    )

    raw = {
        "bid_in_date": str(row["bid_in_date"]),
        "delinquent_tax_year": row["delinquent_tax_year"],
        "parcel_id_raw": row["parcel_id_raw"],
        "owner_name": row["owner_name"],
        "mailing_city_state_zip": row["mailing_city_state_zip"],
        # Carried onto the event so an outreach query never has to go back to
        # staging to find out whether this party may be contacted.
        "do_not_mail": bool(row["do_not_mail"]),
        "confidence": float(row["confidence"]) if row["confidence"] is not None else None,
        "staging_id": row["id"],
        "notice_url": row["source_url"],
    }

    return {
        "county_code": row["county_code"],
        "parcel_id": row["parcel_id"],
        "event_type": EVENT_TYPE,
        "event_subtype": EVENT_SUBTYPE,
        "severity": EVENT_SEVERITY,
        "source": EVENT_SOURCE,
        "source_id": str(row["id"]),
        "title": title,
        "description": description,
        "event_date": row["redemption_expiry"],
        "event_value": row["redemption_amount"],
        "observed_at": row["fetched_at"],
        "raw_data": psycopg2.extras.Json(raw),
    }


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1
    dry_run = os.environ.get("DRY_RUN") == "1"

    floor = float(os.environ.get("CONFIDENCE_FLOOR", CONFIDENCE_FLOOR))
    log(f"confidence floor: {floor}")

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SELECT_CANDIDATES, {"floor": floor})
            candidates = cur.fetchall()

            cur.execute(SELECT_HELD, {"floor": floor})
            held = cur.fetchone() or {}

        log(f"{len(candidates)} row(s) meet the promotion gate")
        log(f"review queue: {held.get('held', 0)} held "
            f"(unresolved_parcel={held.get('unresolved_parcel', 0)}, "
            f"low_confidence={held.get('low_confidence', 0)}, "
            f"no_expiry={held.get('no_expiry', 0)}, "
            f"no_amount={held.get('no_amount', 0)})")

        if not candidates:
            log("nothing to promote")
            return 0

        by_county: dict[str, int] = {}
        for r in candidates:
            by_county[r["county_code"]] = by_county.get(r["county_code"], 0) + 1
        log(f"by county: {by_county}")

        if dry_run:
            log("DRY_RUN=1 - no writes performed")
            return 0

        promoted = 0
        skipped_existing = 0
        with conn.cursor() as cur:
            for row in candidates:
                event = build_event(row)

                # Second idempotency guard. promoted_at alone is not enough:
                # if it were cleared by hand, a re-run would insert a
                # duplicate event with no constraint to stop it.
                cur.execute(EXISTING_EVENT,
                            {"source": EVENT_SOURCE,
                             "source_id": event["source_id"]})
                if cur.fetchone():
                    skipped_existing += 1
                    cur.execute(MARK_PROMOTED, {"id": row["id"]})
                    continue

                cur.execute(INSERT_EVENT, event)
                cur.execute(MARK_PROMOTED, {"id": row["id"]})
                promoted += 1

        conn.commit()
        log(f"promoted {promoted} event(s); "
            f"{skipped_existing} already had an event and were only stamped")
        return 0
    except Exception as e:
        conn.rollback()
        log(f"FAILED - {type(e).__name__}: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
