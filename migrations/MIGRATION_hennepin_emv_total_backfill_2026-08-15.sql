-- MIGRATION_hennepin_emv_total_backfill_2026-08-15.sql
--
-- Repair core.parcels.emv_total for hennepin: 385,962 rows.
--
-- ============================================================
-- WHAT IS WRONG
-- ============================================================
-- Hennepin holds 448,719 parcels. emv_total -- the column the API, deal math
-- and every product surface read -- is populated on 39,631 of them (8.8%).
-- estimated_market_value, a LEGACY column nothing displays, is populated on
-- 443,610 (98.9%).
--
-- The values were loaded. They went to the wrong destination column.
--
-- This is the SAME defect fixed in washington_parcels.py on 2026-08-14 ("values
-- were going to the LEGACY column nothing displays"), still live in the
-- hennepin loader -- in the county holding by far the most distress inventory.
-- Compare: washington 100.0% with value, dakota 99.9%, chisago 99.5%,
-- anoka 98.7%, HENNEPIN 8.8%.
--
-- Visible symptom: a subscriber opens a Hennepin foreclosure and sees no market
-- value, no deal math, no owner mailing, no homestead. It reads as a parcel
-- matching failure and is not one -- no_parcel_row is ZERO across all 29
-- county/source combinations measured. Every event joins to a parcel row. The
-- parcel row is simply empty in the column that gets read.
--
-- ============================================================
-- WHICH COLUMN IS RIGHT -- DECIDED BY EVIDENCE, NOT PREFERENCE
-- ============================================================
-- 39,631 rows have BOTH columns set. 39,345 agree (99.3%). The 286 that
-- disagree were checked against the county's own raw payload:
--
--     raw_data->>'MKT_VAL_TOT' = estimated_market_value : 286
--     raw_data->>'MKT_VAL_TOT' = emv_total              :   0
--
-- Unanimous. The legacy column matches the county in every disagreement;
-- emv_total matches in none. 282 of the 286 are LOWER in emv_total, consistent
-- with a stale assessment year that stopped being refreshed while the legacy
-- column kept updating.
--
-- So this OVERWRITES rather than only filling gaps -- 285 stale values are
-- corrected. Every one is backed up first.
--
-- legacy_without_raw = 0: every legacy value is corroborated by MKT_VAL_TOT.
-- The column is trustworthy end to end, not merely more populated.
--
-- ============================================================
-- WHY 18,303 ZEROS ARE SKIPPED
-- ============================================================
-- 18,303 hennepin parcels carry MKT_VAL_TOT = 0. Some are genuine (government
-- land, right-of-way, church property); some are failed parses. We cannot tell
-- them apart, and the two cases need the same treatment anyway:
--
-- Writing 0 into emv_total would render "$0" where an em-dash belongs, and
-- would feed $0 into _compute_deal_math as a real market value -- producing an
-- equity spread equal to the entire payoff on 18,303 parcels. A fabricated
-- number is worse than a blank.
--
-- They stay NULL. Logged as an open question, not silently converted.
--
-- ============================================================
-- THIS IS DATA REPAIR ONLY
-- ============================================================
-- The hennepin parcel loader still writes to the wrong column and will undo
-- this on its next full refresh. The loader fix is a SEPARATE change and must
-- land, or this migration is a one-off patch on a recurring defect.

BEGIN;

-- 1. Prior state of every row this will change. 285 are overwrites, and an
--    overwrite is unrecoverable without this.
CREATE TABLE IF NOT EXISTS audit.hennepin_emv_backfill_20260815 AS
SELECT county_code,
       parcel_id,
       emv_total                  AS emv_total_before,
       estimated_market_value     AS legacy_value,
       (raw_data ->> 'MKT_VAL_TOT') AS raw_mkt_val_tot,
       now()                      AS captured_at
FROM   core.parcels
WHERE  county_code = 'hennepin'
  AND  estimated_market_value IS NOT NULL
  AND  estimated_market_value > 0
  AND  emv_total IS DISTINCT FROM estimated_market_value;

-- 2. Backfill. > 0 excludes the zeros; IS DISTINCT FROM avoids rewriting
--    385,677 rows that are already correct.
UPDATE core.parcels p
SET    emv_total = p.estimated_market_value,
       updated_at = now()
WHERE  p.county_code = 'hennepin'
  AND  p.estimated_market_value IS NOT NULL
  AND  p.estimated_market_value > 0
  AND  p.emv_total IS DISTINCT FROM p.estimated_market_value;

COMMIT;

-- NOT DONE HERE, DELIBERATELY:
--   * The hennepin parcel loader is unfixed. Until it writes MKT_VAL_TOT to
--     emv_total, the next refresh reverts this.
--   * 18,303 zero-value parcels stay NULL, pending a decision on whether a
--     county zero is real or a parse failure.
--   * mille_lacs (20,092 parcels, 20,085 with coordinates, ZERO with value),
--     polk (26.3%) and morrison (41.8%) show the same shape and were NOT
--     examined. This migration is hennepin only.
