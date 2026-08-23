-- sql/comp_ratios.sql
--
-- scoring.comp_ratios — the deal-math calibration layer.
--
-- Rebuilt 2026-08-23. This file exists because the rebuild was run directly
-- in the SQL editor and the definition lived only in the database: a
-- materialized view serving Premium deal math across 36 counties, one dropped
-- object away from unreconstructable, with every filter decision recorded
-- nowhere.
--
-- ============================================================
-- WHAT IT IS
-- ============================================================
-- The median ratio of sale price to assessed value, from confirmed eCRV
-- sales in the trailing 12 months, at three scopes. _recompute_deal_math in
-- src/routes/properties.py multiplies core.parcels.emv_total by it to produce
-- est_market_value, and names the sample size in the basis string shown to
-- the subscriber.
--
-- Consumed by _load_deal_calibration, which selects EXACTLY:
--     scope, county_code, city_norm, n, ratio
-- and buckets on scope IN ('city','county','metro').
--
-- *** THE FIVE-COLUMN CONTRACT IS NOT NEGOTIABLE. ***
-- _load_deal_calibration fails to a logger.warning and returns stale cache on
-- error, so dropping or renaming a column BREAKS DEAL MATH SILENTLY — no 500,
-- no empty response, just a number that quietly stops updating. Add columns
-- only after changing the consumer, never before.
--
-- ============================================================
-- WHY IT IS A RATE TABLE AND NOT A MODEL
-- ============================================================
-- Measured 2026-08-23 on 40,220 held-out 2026 sales, temporal split:
--
--     raw assessment (what equity spread used)      15.81%
--     LightGBM v1                                   12.80%
--     LightGBM v2 (medians as features)             12.79%
--     comp_ratios as previously deployed            12.97%
--     city median, pooled years                     12.11%
--     city median, 2025 only                        11.93%
--     county median, 2025 only                      11.73%
--     THIS DEFINITION, leak-free forecast           11.77%
--
-- Four approaches failed to beat a county-year median. v2 moved v1 by 0.01
-- points despite being handed the baseline as a feature — it barely used it
-- (county_med 818 splits against lot_sqft 7,167).
--
-- GOVIRE_AI_PREDICTION_STRATEGY §2.3 predicted this: "a calibrated rate IS a
-- prediction ... an unexplained 0.73 is the weaker product here." Building
-- the model was still necessary — without it there was no evidence the table
-- is a CEILING rather than a floor.
--
-- ============================================================
-- THE FIVE DEFECTS THIS REBUILD FIXED
-- ============================================================
-- Before: 60 rows, 3 counties, 57 cities. After: 241 rows, 36 counties,
-- 204 cities. Test rows matching a county: 13,873 -> 40,149 of 40,220.
--
-- 1. HARDCODED THREE-COUNTY MAP. The old definition carried
--    VALUES ('27','hennepin'), ('19','dakota'), ('82','washington') inline.
--    outcomes.ecrv_county_map now holds 57. Every property outside those
--    three fell through to the statewide ratio.
--
--    NOTE: eCRV county_cde is the ALPHABETICAL 1-87 sequence, NOT FIPS.
--    Hennepin is 27 here and 27053 in core.counties.fips_code, and no
--    arithmetic converts one to the other.
--
-- 2. UN-NORMALISED PARCEL JOIN. `p.parcel_id = es.parcel_norm` misses every
--    county whose PINs carry punctuation. regexp_replace to digits is what
--    reaches 182,020 rows in scoring.avm_training_set.
--
-- 3. READ estimated_market_value — the LEGACY column. The product reads
--    core.parcels.emv_total. On Ramsey the legacy column held values for
--    163,883 parcels while emv_total held 5,877, until the 2026-08-23
--    backfill.
--
-- 4. NO primary_parcel FILTER. eCRV writes the WHOLE transaction price
--    against EACH parcel of a multi-parcel sale. Rows with
--    primary_parcel = false have a median ratio of 13.6 and 69.9% above 5.
--    The 0.4-2.0 clamp was hiding them by clipping rather than excluding.
--    Without this filter the target's MEAN is 88.6.
--
-- 5. deed_type = 'WARRNTY' — dropped. related_ind and non_market_price cover
--    arm's-length verification and the deed-type filter was excluding
--    legitimate transfers.
--
-- ============================================================
-- WHAT IS DELIBERATELY UNCHANGED
-- ============================================================
-- * Rolling 12-month window. Recency is most of the value: county medians
--   from 2025 alone beat pooled 2024-25 by 0.43 points.
-- * The 0.4-2.0 ratio clamp and purchase_amt >= 30000 floor.
-- * city -> county -> metro fallback, city first. Two tests DISAGREED on
--   whether city beats county (county by 0.19 on one population, city by
--   0.036 on this one) — the effect is smaller than the difference between
--   test setups. A threshold sweep at 15/30/60/100/200/400 moved mdape by
--   under 0.03 at every step, so city is NOT sample-size limited; it is
--   genuinely near-neutral. City-first is retained because it is the
--   deployed behaviour and there is no evidence against it.
--
-- ============================================================
-- KNOWN LIMITATION: THIS MEASURES VINTAGE AS WELL AS PRACTICE
-- ============================================================
-- Every Ramsey city prices between 1.147 and 1.414 while every other county
-- sits between 1.116 and 1.195 — fifteen Ramsey rows holding the top fifteen
-- positions, with no overlap. That is not Saint Paul outperforming
-- Minneapolis. Ramsey publishes EMVYear 2021 on all 163,880 parcels, so its
-- ratio is five years of market movement, not assessment practice.
--
-- The correction is probably RIGHT. The JUSTIFICATION was wrong, and
-- _recompute_deal_math now appends a clause naming the vintage whenever
-- core.parcels.emv_year trails the calibrating sales by 2+ years.
--
-- *** WHEN RAMSEY REFRESHES TO 2026 VALUES, EVERY RAMSEY RATIO COLLAPSES
--     TOWARD 1.1 AND EVERY RAMSEY VALUATION MOVES ~20% IN ONE REFRESH. ***
-- That is expected behaviour, not a defect, and nothing else in the codebase
-- records it.
--
-- ============================================================
-- REFRESH
-- ============================================================
-- pg_cron job 1 'refresh-deal-math', Mondays 15:00 UTC, runs a bare
--   REFRESH MATERIALIZED VIEW scoring.comp_ratios;
--   REFRESH MATERIALIZED VIEW scoring.distress_multipliers;
-- so it re-executes THIS definition and picks up a fresh trailing window.
-- Verified 2026-08-23: no inline definition in the job command.
--
-- Re-running this file is safe and idempotent. It drops and recreates.
-- ============================================================

DROP MATERIALIZED VIEW IF EXISTS scoring.comp_ratios;

CREATE MATERIALIZED VIEW scoring.comp_ratios AS
WITH sales AS (
  SELECT p.county_code,
         lower(regexp_replace(p.city, '^City Of ', '', 'i')) AS city_norm,
         es.purchase_amt / NULLIF(p.emv_total, 0) AS ratio
  FROM outcomes.ecrv_sales es
  -- 57 counties, derived empirically 2026-08-23 rather than hardcoded.
  JOIN outcomes.ecrv_county_map cm
         ON cm.county_cde = es.county_cde
        AND cm.join_field = 'parcel_id'
  -- Composite (county_code, digits-only parcel_id). Minnesota PINs are not
  -- unique across counties: 51,662 nine-character PINs are shared.
  JOIN core.parcels p
         ON p.county_code = cm.county_slug
        AND regexp_replace(p.parcel_id, '\D', '', 'g') = es.parcel_norm
  WHERE COALESCE(es.non_market_price, false) = false
    AND COALESCE(es.related_ind, false) = false
    -- Defect 4. Without this the mean ratio is 88.6.
    AND es.primary_parcel = true
    AND es.deed_date >= (CURRENT_DATE - INTERVAL '1 year')
    AND es.purchase_amt >= 30000
    -- Nominal assessments ($100 placeholders) otherwise produce ratios in
    -- the thousands and survive the clamp below by being clipped, not cut.
    AND p.emv_total >= 10000
    AND (es.purchase_amt / NULLIF(p.emv_total, 0)) BETWEEN 0.4 AND 2.0
)
SELECT 'city'::text AS scope,
       county_code,
       city_norm,
       count(*) AS n,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio)::numeric, 3) AS ratio
FROM sales
WHERE city_norm IS NOT NULL
GROUP BY county_code, city_norm
HAVING count(*) >= 30
UNION ALL
SELECT 'county'::text,
       county_code,
       NULL::text,
       count(*),
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio)::numeric, 3)
FROM sales
GROUP BY county_code
HAVING count(*) >= 30
UNION ALL
SELECT 'metro'::text,
       NULL::text,
       NULL::text,
       count(*),
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio)::numeric, 3)
FROM sales;

-- ============================================================
-- VERIFY — a green CREATE is not evidence
-- ============================================================
-- Expected 2026-08-23: city 204 rows, county 36, metro 1 (n = 57,391).
-- A county count of 3 means the ecrv_county_map join did not take and the
-- rebuild silently reverted to the old coverage.
--
--   SELECT now() AS run_at, scope, count(*) AS rows,
--          min(n) AS min_n, max(n) AS max_n,
--          min(ratio) AS min_ratio, max(ratio) AS max_ratio
--   FROM scoring.comp_ratios GROUP BY scope ORDER BY scope;
--
-- And confirm the consumer still reads it — _load_deal_calibration failing
-- is invisible from the database side:
--
--   GET /properties/{id} with a premium key; deal_math.ratio_n should be in
--   the thousands and deal_math.ratio_scope one of city/county/metro.
