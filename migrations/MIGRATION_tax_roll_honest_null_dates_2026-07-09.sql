-- ============================================================
-- MIGRATION_tax_roll_honest_null_dates_2026-07-09.sql
-- Replace the 2000-01-01 sentinel event_date with honest NULL
-- ============================================================
-- The sentinel existed because event_date is part of the dedup key
-- (parcel_id, event_type, event_date, source) and NULL used to break
-- re-mining idempotency. The 2026-07-07 index fix made the key
-- NULLS NOT DISTINCT, so honest NULL now dedups exactly like a
-- constant — the sentinel's only justification is gone. The
-- no-fabrication rule applies: the county flag says THAT a parcel
-- forfeited / carries an assessment, not WHEN. Unknown is NULL.
--
-- Verified populations (Q3, 2026-07-09):
--   hennepin_tax_roll / tax_forfeit    : 139 rows
--   ramsey_tax_roll   / tax_assessment : 483 rows
-- hennepin tax_delinquent rows use Jan-1-of-delinquency-year (a real,
-- meaningful date) and are deliberately NOT touched.
--
-- NULL event_dates are proven-safe platform-wide: 9 already exist
-- (Q1), and both event_date sort paths in the API specify
-- nullsfirst=False, so NULL rows sort LAST under "Most recent first".
--
-- Interleaving-proof: if an old-code miner run re-inserts sentinel
-- rows between this migration and the miner deploy (ramsey runs
-- weekly), re-running this script converges to the correct state —
-- step 1 removes sentinel rows that already have a NULL twin (so the
-- UPDATE can never violate the dedup key), then step 2 converts the
-- rest. Safe to re-run any number of times.
-- ============================================================

-- ---- Step 1: remove sentinel rows that already have a NULL twin ----
-- (0 today; nonzero only if an old-code run interleaves. Prevents the
--  UPDATE below from ever colliding with an existing NULL-date row.)

DELETE FROM signals.distress_events de
WHERE de.event_date = DATE '2000-01-01'
  AND de.source IN ('hennepin_tax_roll', 'ramsey_tax_roll')
  AND de.event_type IN ('tax_forfeit', 'tax_assessment')
  AND EXISTS (
      SELECT 1 FROM signals.distress_events t
      WHERE t.parcel_id  = de.parcel_id
        AND t.event_type = de.event_type
        AND t.source     = de.source
        AND t.event_date IS NULL
  );
-- expect: DELETE 0

-- ---- Step 2: sentinel -> honest NULL ------------------------------

UPDATE signals.distress_events
SET event_date = NULL
WHERE event_date = DATE '2000-01-01'
  AND source IN ('hennepin_tax_roll', 'ramsey_tax_roll')
  AND event_type IN ('tax_forfeit', 'tax_assessment');
-- expect: UPDATE 622  (139 forfeit + 483 assessment)

-- ---- Step 3 (added after V3 caught it): century-pivot repair -------
-- V3 exposed one delinquent row dated 2084-01-01 — the miner's 2-digit
-- year expansion mapped '84' to 2084 instead of 1984, and the severity
-- math (today - year) went negative, mis-scoring the county's
-- LONGEST-delinquent parcel as LOW. Repair: shift future years back a
-- century, rescore high (any pivoted row is decades behind), and fix
-- the year inside raw_data and the description. Generic: repairs ALL
-- future-dated delinquent rows, not just the one known today.
-- (All SET expressions read the PRE-update row, so the arithmetic is
--  consistent. Idempotent: after the update no row matches the WHERE.)

UPDATE signals.distress_events
SET event_date = make_date(EXTRACT(YEAR FROM event_date)::int - 100, 1, 1),
    severity   = 'high',
    raw_data   = jsonb_set(
                   raw_data, '{earliest_delq_year}',
                   to_jsonb(EXTRACT(YEAR FROM event_date)::int - 100)),
    description = replace(
                   description,
                   EXTRACT(YEAR FROM event_date)::int::text,
                   (EXTRACT(YEAR FROM event_date)::int - 100)::text)
WHERE source = 'hennepin_tax_roll'
  AND event_type = 'tax_delinquent'
  AND event_date > CURRENT_DATE;
-- expect: UPDATE 1

-- ============================================================
-- VERIFICATION (run after; expected values in comments)
-- ============================================================
-- V1: no sentinel remains for these sources (expect 0)
-- SELECT COUNT(*) FROM signals.distress_events
-- WHERE event_date = DATE '2000-01-01'
--   AND source IN ('hennepin_tax_roll', 'ramsey_tax_roll');
--
-- V2: the honest-NULL populations (expect 139 and 483)
-- SELECT source, event_type, COUNT(*)
-- FROM signals.distress_events
-- WHERE event_date IS NULL
--   AND source IN ('hennepin_tax_roll', 'ramsey_tax_roll')
-- GROUP BY source, event_type;
--
-- V3: delinquent rows untouched (expect 0 NULLs; years intact)
-- SELECT COUNT(*) FILTER (WHERE event_date IS NULL) AS null_dates,
--        MIN(event_date) AS earliest, MAX(event_date) AS latest
-- FROM signals.distress_events
-- WHERE source = 'hennepin_tax_roll' AND event_type = 'tax_delinquent';
--
-- V4: no future-dated delinquencies remain; the 1984 row is high
-- SELECT COUNT(*) FILTER (WHERE event_date > CURRENT_DATE) AS future_rows,
--        COUNT(*) FILTER (WHERE event_date = '1984-01-01'
--                         AND severity = 'high') AS repaired_1984
-- FROM signals.distress_events
-- WHERE source = 'hennepin_tax_roll' AND event_type = 'tax_delinquent';
-- ============================================================

-- ============================================================
-- TASK LIST (do in this order)
-- ============================================================
-- [ ] 1. Run this migration (whole script, one paste).
--        Expected: DELETE 0, then UPDATE 622.
-- [ ] 2. Run V1, V2, V3 above (uncommented); paste to Claude.
-- [ ] 3. Stage the two miner files Claude provided
--        (src/scrapers/hennepin_tax_roll.py,
--         src/scrapers/ramsey_tax_roll.py) — one commit, Railway green.
-- [ ] 4. Tier check:
--        powershell -ExecutionPolicy Bypass -File $HOME\govire-scrapers\qa-tier-check.ps1
-- [ ] 5. Commit this file to migrations/ in the repo.
-- ============================================================
