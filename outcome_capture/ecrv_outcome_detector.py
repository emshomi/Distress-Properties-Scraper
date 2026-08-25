"""
eCRV outcome detector. Run after every eCRV ingest.

    python -m outcome_capture.ecrv_outcome_detector
    python -m outcome_capture.ecrv_outcome_detector --dry-run
    python -m outcome_capture.ecrv_outcome_detector --source-file <name>

=== WHY THIS EXISTS ===

This logic existed in NO FILE. 140 tracker rows carry ecrv_* detection
sources; measured 2026-08-25, they were written across 17 DISTINCT MINUTES
over seven weeks -- 16:38, 20:28, 01:45, 02:15, 01:15. Two of the five
sources landed entirely within a single minute. That is a person running
statements in an editor, not a job.

It matters more than the other unautomated writers found today because it
is THE ONLY SOURCE OF redeemed_by_owner: 90 of the tracker's 108
redemptions. outcome_checker.py structurally cannot infer redemption --
its own docstring says negative-inference 'redeemed' requires eCRV or
recorder confirmation.

Its last run was 2026-08-17. Two eCRV ingests have landed since. On
2026-08-25 there were 45 pending tracker rows with an arms-length sale
inside their redemption window and no outcome. The published Premium
figure -- "36.5% redeemed, 252 outcomes" -- was computed while the
numerator was frozen and the denominator kept growing through the ArcGIS
checker. The rate is biased LOW, structurally.

=== THE RULE, RECONSTRUCTED FROM THE ROWS THEMSELVES ===

detection_notes preserved the MATCH but not the FILTER. Comparing the 90
published rows against the 45 candidates recovered the missing clause:

                        published 90    candidates 45
    quitclaim / OTHER            0            ~15
    warranty deed               62            ~20
    over $150k             65 (87%)           ~20
    under $50k              3  (4%)           ~10

ZERO quitclaims among the published rows, against a candidate pool full of
them. The operator applied a deed-type filter that appears in no note and
no file. This module makes it explicit.

=== WHY QUITCLAIMS ARE EXCLUDED: THE EQUITY STRIP ===

An individual quitclaiming during a redemption window for $5,000-$35,000
to an investor LLC has NOT redeemed. They sold their redemption RIGHT and
walked away; the investor redeems and flips. Real examples, all pending on
2026-08-25:

    QUITCLAIM  $5,000  Bridget Tooley        -> Renovation Group Inc
    QUITCLAIM  $7,500  Thomas L Bodin        -> NJE Holdings, LLC
    QUITCLAIM $20,000  Chase R. Downing      -> Blackstone 1, LLC
    QUITCLAIM $30,000  Marbue & Florence Watkins -> Blackstone 1, LLC

against genuine redemptions -- warranty deeds at market value to
individuals:

    WARRNTY  $341,000  Jennifer Beck; James Koonce -> Kyle Kyro
    WARRNTY  $360,000  Lency Clairmont             -> Lou Thao; Chou C Xiong

Calling the first group redeemed_by_owner would overstate redemptions
roughly 3x and would misdescribe the outcome for the household, which is
the population Govire's owner side exists to serve.

They are NOT stamped as anything here. 'outcome' is check-constrained to
eight values and none means "owner sold their redemption right". Inventing
a label is worse than leaving the row pending for the checker. Recording
the observation and the count is the honest middle, and it is what
STAGE-2 work would need if this ever becomes its own outcome value.

=== ONE SALE RESOLVES ONE WINDOW ===

Three of the published 90 stamped a SINGLE sale against TWO tracker rows.
One pair had anchors ONE DAY APART on the same parcel -- the same sheriff
sale recorded twice, which the builder's
ON CONFLICT (county_code, parcel_id, anchor_date) cannot catch because the
dates differ. Every double-count inflates the published redemption rate.

DISTINCT ON picks the window whose expiry is EARLIEST after the deed date:
the first window the sale could have resolved. Deliberately not "latest",
which would let one sale close a window opened months after it.

=== SAFETY ===

Writes only to rows that are still outcome='pending' and not superseded,
and re-asserts every guard condition inside the UPDATE. --dry-run prints
the full decision for each row and writes nothing.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

# Lender / GSE / servicer sellers. Kept in sync BY HAND with
# outcome_checker.REO_PATTERNS -- that list lives in Python and this one
# has to run inside Postgres. Retuned 2026-08-25 after reading
# outcomes.owner_checks: NATL, MTG and TRS abbreviations plus every
# spelling of Veterans Affairs had been missing, which cost 56 outcomes.
LENDER_RX = (
    "SEC(RETARY)? OF VETERANS AFFAIR|VETERANS AFFAIRS|"
    "FED(ERAL)? NAT(IONAL|L)? MORT|FEDERAL NATIONAL MORTGAGE|"
    "FED(ERAL)? HOME LOAN MT(G|GE)|FEDERAL HOME LOAN MORTGAGE|"
    "FANNIE MAE|FREDDIE MAC|SECRETARY OF HOUSING|"
    "MTG (LOAN|SERVICES|SERVICING|CORP|COMPANY|LLC|INC|TR)|"
    "MORTGAGE (LLC|CORP|INC|COMPANY|CORPORATION)|MORT\\.? (CO|CORP)|"
    "\\mTRS\\M|\\mTRUSTEE\\M|"
    "FUNDING (LLC|INC|CORP|COMPANY)|LENDING (LLC|INC|CORP|COMPANY)|"
    "\\mREO\\M|ACQUISITION TRUST|CREDIT OPPORTUNITIES|SELECT PORTFOLIO|"
    "SERVBANK|MIDFIRST|MCLP ASSET|\\mBANKS?\\M|CREDIT UNION|SAVINGS|"
    "WILMINGTON|DEUTSCHE|PENNYMAC|NEWREZ|NATIONSTAR|CARRINGTON|SELENE|"
    "LAKEVIEW LOAN|ROCKET MT(G|GE)|FREEDOM MORT|\\mMERS\\M"
)

# Deed types that convey real consideration from the party who held title.
# Derived from the 90 published rows: WARRNTY 62, PERREPDEED 8,
# SPECWARNTY 2, PROBATE 1, CONFORDEED 1. QUITCLAIM and OTHER appear ZERO
# times there and ~15 times among the candidates -- see the equity-strip
# note in the module docstring.
REDEMPTION_DEEDS = ("WARRNTY", "SPECWARNTY", "PERREPDEED", "PROBATE",
                    "CONFORDEED", "TRUSTEE")

# Sellers appearing on this many DISTINCT tracked foreclosures are
# certificate holders, not owners who redeemed.
#
# MEASURED 2026-08-25 rather than assumed. At 3+ distinct trackers, EVERY
# seller in the whole eCRV-to-tracker join is corporate, a trust or a
# lender: Realty Pros 8, Alabama 2 8, Federal Home Loan Mortgage 8,
# PA Hagen Legacy Trust 5, US Bank 5, Creative Real Estate 4, Mongoose 4,
# Freedom Mortgage 4, Blackstone 1 3, Slate 3, Mana Holdings 3, Pinewood 3,
# Renovo 3, Wings Financial 3, Lakeview Loan 3, Secretary of Veterans
# Affairs 3.
#
# NOT ONE INDIVIDUAL appears at 3+. Individuals top out at 2 -- Amy L.
# Revak and Patrick N. Andersen -- and both of those are known
# DOUBLE-COUNTS, one sale stamped against two windows, not repeat sellers.
# So 3 is where the population changes character, and 2 would catch the
# double-counts rather than flippers.
#
# The existing hand-written ecrv_repeat_seller_post_expiry rows used 5 and
# describe the same inference: "Corporate seller resolving that many
# foreclosed properties post-redemption is a certificate holder, not an
# owner who redeemed."
#
# THE KEY IS NORMALISED, AND IT HAS TO BE. The first version grouped on
# lower(btrim(sellers)) and the threshold was defeated by punctuation:
# 'Creative Real Estate Inc' and 'Creative Real Estate, Inc.' are two keys,
# so a seller on 5 tracked foreclosures counted as 4 and 1 and passed
# through as a redemption. Measured after normalising: Alabama 2 8->10,
# Realty Pros 8->9, Creative Real Estate 4->5, Renovo Holdings 3->4,
# Blackstone 1 3->4, Minnesota Renovations 2->3, Miramac 2->3.
#
# Same failure as carlton's PARCELID vs P_ID2 the same afternoon -- an
# identifier compared without normalising its punctuation.
#
# The threshold SURVIVED normalisation, which is the stronger result. At 3+
# all nineteen sellers are corporate, trusts, credit unions or GSEs, and
# not one individual appears. At 2 individuals appear immediately -- Amy L.
# Revak, Patrick N. Andersen, Samual S. Anderson -- and all three are known
# DOUBLE-COUNTS rather than repeat sellers.
REPEAT_SELLER_MIN_TRACKERS = 3

# Below this share of assessed value, a sale is flagged ambiguous rather
# than dropped.
#
# THERE IS NO PRICE FLOOR IN THE PUBLISHED ROWS, and that was measured
# after assuming otherwise. The 90 hand-written redemptions run down to
# 0.063 -- a WARRANTY deed, $20,000 against a $318,700 assessment, sold by
# individuals -- plus 0.137, 0.138 and 0.297. The operator applied a
# deed-type filter and nothing else.
#
# Dropping those rows would discard records the operator accepted, on a
# threshold this file invented. Flagging them keeps the row, keeps the
# count honest, and marks the four questionable published ones for review.
LOW_CONSIDERATION_RATIO = 0.4

# One row per SALE, not per tracker.
#
# The first version used DISTINCT ON (rt.id) and deduped NOTHING: duplicate
# windows on one parcel are DIFFERENT tracker ids, so every pair survived.
# The 2026-08-25 dry run stamped parcel 213124210030 twice from a single
# $248,000 sale, which is the exact defect this was meant to prevent.
#
# Keyed on (county_code, parcel_id, deed_date) and ordered so the window
# with the EARLIEST expiry after the deed wins -- the first window that
# sale could have resolved. Deliberately not the latest, which would let
# one sale close a window opened months afterwards.
CANDIDATE_SQL = """
WITH repeat_sellers AS (
  SELECT regexp_replace(lower(array_to_string(es.sellers, '; ')),
                        '[^a-z0-9]', '', 'g') AS seller_key,
         count(DISTINCT rt.id) AS trackers
  FROM outcomes.redemption_tracker rt
  JOIN outcomes.ecrv_county_map m ON m.county_slug = rt.county_code
  JOIN outcomes.ecrv_sales es
         ON es.county_cde = m.county_cde
        AND regexp_replace(rt.parcel_id, '\\D', '', 'g') = es.parcel_norm
  WHERE rt.superseded_by IS NULL
    AND es.primary_parcel
    AND es.purchase_amt > 0
  GROUP BY 1
  HAVING count(DISTINCT rt.id) >= %(repeat_min)s
)
SELECT DISTINCT ON (rt.county_code, rt.parcel_id, es.deed_date)
       rt.id                       AS tracker_id,
       rt.county_code,
       rt.parcel_id,
       rt.anchor_date,
       rt.redemption_expiry_date,
       rt.check_stage,
       es.deed_type,
       es.deed_date,
       es.purchase_amt,
       p.emv_total,
       CASE WHEN COALESCE(p.emv_total,0) > 0
            THEN round((es.purchase_amt / p.emv_total)::numeric, 3)
       END                                                       AS price_to_emv,
       array_to_string(es.sellers, '; ') AS sellers,
       array_to_string(es.buyers,  '; ') AS buyers,
       (upper(array_to_string(es.sellers, ' ')) ~ %(lender_rx)s)  AS seller_is_lender,
       (es.deed_type = ANY(%(deeds)s))                            AS deed_conveys,
       (rs.seller_key IS NOT NULL)                                AS seller_is_repeat,
       COALESCE(rs.trackers, 0)                                   AS seller_tracker_count
FROM outcomes.redemption_tracker rt
JOIN outcomes.ecrv_county_map m
       ON m.county_slug = rt.county_code
JOIN outcomes.ecrv_sales es
       ON es.county_cde = m.county_cde
      AND regexp_replace(rt.parcel_id, '\\D', '', 'g') = es.parcel_norm
LEFT JOIN core.parcels p
       ON p.county_code = rt.county_code
      AND p.parcel_id   = rt.parcel_id
LEFT JOIN repeat_sellers rs
       ON rs.seller_key = regexp_replace(
            lower(array_to_string(es.sellers, '; ')), '[^a-z0-9]', '', 'g')
WHERE rt.outcome = 'pending'
  AND rt.superseded_by IS NULL
  AND es.primary_parcel
  AND NOT COALESCE(es.related_ind, false)
  AND NOT COALESCE(es.non_market_price, false)
  AND es.purchase_amt > 0
  AND es.deed_date > rt.anchor_date
  AND es.deed_date <= rt.redemption_expiry_date
ORDER BY rt.county_code, rt.parcel_id, es.deed_date,
         rt.redemption_expiry_date, rt.id
"""

STAMP_SQL = """
UPDATE outcomes.redemption_tracker
SET outcome             = %(outcome)s,
    ambiguous           = %(ambiguous)s,
    outcome_detected_at = now(),
    outcome_event_date  = %(event_date)s,
    detection_source    = %(source)s,
    detection_notes     = %(notes)s,
    next_check_date     = NULL,
    updated_at          = now()
WHERE id = %(tracker_id)s
  AND outcome = 'pending'
  AND superseded_by IS NULL
"""


def log(msg):
    print("[ecrv-outcome] %s %s"
          % (datetime.now(timezone.utc).strftime("%H:%M:%S"), msg),
          flush=True)


def classify(row):
    """Return (outcome, source, note, ambiguous) or (None, reason, note,
    False) to skip.

    Four ways a sale inside the window can read, and only one is a
    redemption. Order matters: lender first, then repeat seller, then deed
    type, because a lender is also a repeat seller and the more specific
    reading should win.
    """
    seller = row["sellers"] or ""
    amt = float(row["purchase_amt"] or 0)
    ratio = row["price_to_emv"]
    base = ("eCRV: %s %s amt %s seller %s"
            % (row["deed_type"], row["deed_date"], amt, seller))

    low = (ratio is not None and float(ratio) < LOW_CONSIDERATION_RATIO)
    low_note = ""
    if low:
        low_note = (" | AMBIGUOUS: price is %s of assessed value (%s vs %s) "
                    "-- may be a sale of the redemption right rather than a "
                    "market sale. Kept because the 90 hand-written rows run "
                    "as low as 0.063 and applied no price test."
                    % (ratio, amt, row["emv_total"]))

    if row["seller_is_lender"]:
        # Title moved at the sheriff sale; this is the REO disposition.
        return ("foreclosed_sold", "ecrv_reo_sale_in_window",
                base + " | seller matches a lender/GSE pattern, so title had "
                       "already moved at the sheriff sale", False)

    if row["seller_is_repeat"]:
        # FLIPPER RULE. Same inference the hand-written
        # ecrv_repeat_seller_post_expiry rows record, at a threshold
        # measured rather than assumed -- see REPEAT_SELLER_MIN_TRACKERS.
        return ("foreclosed_sold", "ecrv_repeat_seller_post_expiry",
                base + " | seller appears on %d distinct tracked "
                       "foreclosures, so it is a certificate holder rather "
                       "than an owner who redeemed. INFERENCE from repeat "
                       "behaviour, not a lender match -- hence ambiguous."
                       % row["seller_tracker_count"], True)

    if not row["deed_conveys"]:
        # EQUITY STRIP. Not stamped -- see the module docstring.
        return (None, "equity_strip_not_stamped",
                base + " | buyers %s" % (row["buyers"] or ""), False)

    return ("redeemed_by_owner", "ecrv_owner_sale_in_window",
            base + low_note, low)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--source-file", default=None)
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1
    if args.dry_run:
        log("DRY RUN: no writes")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(CANDIDATE_SQL,
                        {"lender_rx": LENDER_RX,
                         "deeds": list(REDEMPTION_DEEDS),
                         "repeat_min": REPEAT_SELLER_MIN_TRACKERS})
            rows = cur.fetchall()
        log("candidates: %d pending trackers with a sale inside the window"
            % len(rows))

        counts = {"redeemed_by_owner": 0, "foreclosed_sold": 0,
                  "equity_strip_not_stamped": 0}
        flagged = 0
        stamped = 0
        for r in rows:
            outcome, source, note, ambiguous = classify(r)
            if outcome is None:
                counts[source] += 1
                log("  SKIP  %-11s %-14s %s %8.0f  %s -> %s"
                    % (r["county_code"], r["parcel_id"], r["deed_type"],
                       float(r["purchase_amt"] or 0),
                       (r["sellers"] or "")[:28], (r["buyers"] or "")[:28]))
                continue

            counts[outcome] += 1
            flagged += bool(ambiguous)
            log("  %-17s %-11s %-14s %-10s %8.0f r=%-6s %s%s"
                % (outcome, r["county_code"], r["parcel_id"], r["deed_type"],
                   float(r["purchase_amt"] or 0),
                   r["price_to_emv"] if r["price_to_emv"] is not None else "-",
                   (r["sellers"] or "")[:30],
                   "  [ambiguous]" if ambiguous else ""))
            if args.dry_run:
                continue
            with conn.cursor() as w:
                w.execute(STAMP_SQL, {
                    "outcome": outcome,
                    "event_date": r["deed_date"],
                    "source": source,
                    "notes": note[:2000],
                    "ambiguous": ambiguous,
                    "tracker_id": r["tracker_id"],
                })
                stamped += w.rowcount

        if not args.dry_run:
            conn.commit()

        log("redeemed_by_owner %d | foreclosed_sold %d | equity strips left "
            "pending %d | flagged ambiguous %d"
            % (counts["redeemed_by_owner"], counts["foreclosed_sold"],
               counts["equity_strip_not_stamped"], flagged))
        if args.dry_run:
            log("DRY RUN: nothing written")
        else:
            log("stamped %d rows" % stamped)
            if stamped != counts["redeemed_by_owner"] + counts["foreclosed_sold"]:
                log("NOTE: stamped count differs from decisions -- a row "
                    "changed state between the read and the write, and the "
                    "guard in STAMP_SQL correctly skipped it")
        return 0

    except Exception as exc:
        conn.rollback()
        log("FAILED -- %s: %s" % (type(exc).__name__, exc))
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
