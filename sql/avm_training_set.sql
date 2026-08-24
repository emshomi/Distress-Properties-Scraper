-- sql/avm_training_set.sql
--
-- The AVM training population: scoring.avm_training_set (view),
-- scoring.avm_training (materialised table + indexes), and the two pg_cron
-- jobs that fill them.
--
-- This file exists because all of it was built directly in the SQL editor on
-- 2026-08-23. 200,962 rows across 40 counties, a GiST-indexed spatial
-- feature, and every filter decision measured that night — none of it in
-- version control, and none of it reconstructable from the codebase.
--
-- ============================================================
-- READ THIS BEFORE CHANGING ANY FILTER
-- ============================================================
-- Four of the five WHERE clauses below were each worth measuring, and one of
-- them decides whether the target variable means anything at all.
--
-- primary_parcel = true    THE LOAD-BEARING FILTER. eCRV writes the WHOLE
--                          transaction price against EACH parcel of a
--                          multi-parcel sale. Without this clause the
--                          target's MEAN is 88.6 and its p95 is 27.3.
--                          Measured: rows with primary_parcel = false have a
--                          median ratio of 13.6 and 69.9% sit above 5.
--                          With it: median 1.076, p95 2.66.
--                          GOVIRE_AI_PREDICTION_STRATEGY does not mention
--                          this filter anywhere.
--
-- emv_total >= 10000       Nominal assessments ($100 placeholders on new
--                          construction and land) otherwise produce ratios
--                          in the thousands. 6,821 rows.
--
-- related_ind = false
-- non_market_price = false Arm's-length. The flags are already loaded; this
--                          is the only part the strategy document got
--                          exactly right about filtering.
--
-- deed_date >= 2015        Nearly cosmetic, and the reason is worth knowing:
--                          the REAL usable depth is 2024-2026.
--                              2015-2022      116 sales   (0.06%)
--                              2023         3,264
--                              2024        68,179
--                              2025        70,241
--                              2026        40,220  (through August)
--                          Old sales survive the join only where the parcel
--                          still carries a CURRENT assessment, and a 2015
--                          price over a 2026 assessment is not a meaningful
--                          ratio anyway. The strategy document describes
--                          eCRV as "1972-2026, 338,033 with a price" — true
--                          of outcomes.ecrv_sales, false of anything
--                          trainable.
--
-- ============================================================
-- THE TWO JOIN BRANCHES
-- ============================================================
-- outcomes.ecrv_county_map carries a join_field per county because Minnesota
-- publishes two parallel identifier systems. Dakota's eCRV records use the
-- 12-digit TAX PIN; core.parcels.parcel_id holds the 13-digit PLSS GIS PIN;
-- NO ARITHMETIC CONVERTS ONE TO THE OTHER.
--
-- The first version of this view filtered to join_field = 'parcel_id' and
-- silently excluded a full metro county — 22,165 arms-length sales since
-- 2015 on 167,354 parcels. Adding the TAXPIN branch took the training set
-- from 182,020 rows / 39 counties to 200,962 / 40.
--
-- The identical defect existed in scoring.comp_ratios and is recorded there
-- as defect 6. AFTER ANY CHANGE TO THESE JOINS, run the audit:
--
--   SELECT m.county_slug, m.join_field,
--          (t.county_code IS NOT NULL) AS in_training
--   FROM outcomes.ecrv_county_map m
--   LEFT JOIN (SELECT DISTINCT county_code FROM scoring.avm_training) t
--          ON t.county_code = m.county_slug
--   ORDER BY in_training, m.county_slug;
--
-- A county absent for a JOIN-FIELD reason is a defect. A county absent for
-- having no assessments loaded is correct — 17 counties are in that group.
--
-- ============================================================
-- WHY THE TABLE IS MATERIALISED, WITH A TIMING
-- ============================================================
-- The KNN feature computed THROUGH THE VIEW took 30,770 ms for 50 rows —
-- a top-N heapsort, because the join to ecrv_sales materialises ~17,000
-- candidate rows per outer row before the distance sort and the GiST index
-- never gets a chance.
--
-- Against the materialised table with avm_training_geom_idx: 143 ms for the
-- same 50 rows. `Index Scan using avm_training_geom_idx`, `Order By:
-- (geom <-> t.geom)`, `Rows Removed by Filter: 2`. 214x faster, 2.2 ms/row.
--
-- 31 hours vs 7 minutes across the full population. The materialisation is
-- not an optimisation, it is what makes the feature computable.
--
-- ============================================================
-- WHAT THIS POPULATION PROVED — no model shipped
-- ============================================================
-- Three training runs, MdAPE on held-out 2026 sales, temporal split:
--
--                              182,020 rows        200,962 rows
--     raw assessment              15.81%              15.14%
--     LightGBM v1                 12.80%                 —
--     LightGBM v2                 12.79%              12.53%
--     county median (train)       12.16%              11.70%
--     county median (2025)        11.73%  <-- won     11.32%  <-- won
--
-- Adding a fourth metro county moved the model 0.26 and the winning baseline
-- 0.41 — the gap WIDENED. Five approaches have failed to beat the county-year
-- median: LightGBM twice, city-pooled, city-recent, and city at six sample
-- thresholds (15/30/60/100/200/400, all within 0.03 of each other).
--
-- The rate table in sql/comp_ratios.sql is the product. This population is
-- the evidence for that, and the harness that will re-test it if the data
-- ever changes enough to matter.
--
-- ============================================================
-- HOW TO REBUILD FROM SCRATCH
-- ============================================================
-- 1. Run section A (view).
-- 2. Run section B (empty table + indexes).
-- 3. Run section C (fill job). Wait ~10 min for 57 counties. Unschedule.
-- 4. Run section D (KNN job). Wait ~22 min for 40 counties. Unschedule.
-- 5. Verify with section E.
--
-- Sections C and D are pg_cron jobs rather than single statements because a
-- single-statement fill times out, and because each needs a SEPARATE progress
-- ledger — a county that legitimately produces zero rows would otherwise
-- never be marked done and the job would re-select it forever. That happened
-- on 2026-08-23 (benton, no assessments) and the job sat on INSERT 0 0 every
-- ten seconds indefinitely.
--
-- UNSCHEDULE BOTH WHEN DONE. Neither self-terminates.
-- ============================================================


-- ============================================================
-- A. THE VIEW
-- ============================================================

CREATE OR REPLACE VIEW scoring.avm_training_set AS
SELECT s.id AS ecrv_id, s.crv_number_id, m.county_slug AS county_code,
       p.parcel_id, s.deed_date, s.purchase_amt, p.emv_total,
       (s.purchase_amt / p.emv_total) AS sale_to_assessment,
       p.lat, p.lng, p.geom, p.lot_sqft, p.sqft, p.year_built,
       p.property_type, p.homestead_status, s.deed_type, s.finance_type
FROM outcomes.ecrv_sales s
JOIN outcomes.ecrv_county_map m
       ON m.county_cde = s.county_cde
      AND m.join_field = 'parcel_id'
JOIN core.parcels p
       ON p.county_code = m.county_slug
      AND regexp_replace(p.parcel_id, '\D', '', 'g') = s.parcel_norm
WHERE s.related_ind = false
  AND s.non_market_price = false
  AND s.primary_parcel = true
  AND s.purchase_amt > 0
  AND p.emv_total >= 10000
  AND s.deed_date >= '2015-01-01'

UNION ALL

-- TAXPIN branch. Dakota only today; written against join_field rather than
-- against 'dakota' so the next one costs nothing. TAXPIN matches parcel_norm
-- directly with no digit normalisation, and parcels_dakota_taxpin_idx
-- (partial, WHERE county_code = 'dakota') serves it. Measured 2026-08-24:
-- 98.5% join, 21,532 with an assessment, 19,158 primary-parcel.
SELECT s.id AS ecrv_id, s.crv_number_id, m.county_slug AS county_code,
       p.parcel_id, s.deed_date, s.purchase_amt, p.emv_total,
       (s.purchase_amt / p.emv_total) AS sale_to_assessment,
       p.lat, p.lng, p.geom, p.lot_sqft, p.sqft, p.year_built,
       p.property_type, p.homestead_status, s.deed_type, s.finance_type
FROM outcomes.ecrv_sales s
JOIN outcomes.ecrv_county_map m
       ON m.county_cde = s.county_cde
      AND m.join_field = 'raw_data->>''TAXPIN'''
JOIN core.parcels p
       ON p.county_code = m.county_slug
      AND (p.raw_data->>'TAXPIN') = s.parcel_norm
WHERE s.related_ind = false
  AND s.non_market_price = false
  AND s.primary_parcel = true
  AND s.purchase_amt > 0
  AND p.emv_total >= 10000
  AND s.deed_date >= '2015-01-01';


-- ============================================================
-- B. THE TABLE
-- ============================================================
-- WHERE false copies the column types and no rows — instant. Creating it
-- from the populated view times out.

DROP TABLE IF EXISTS scoring.avm_training;

CREATE TABLE scoring.avm_training AS
SELECT * FROM scoring.avm_training_set WHERE false;

ALTER TABLE scoring.avm_training ADD PRIMARY KEY (ecrv_id);
CREATE INDEX avm_training_geom_idx ON scoring.avm_training USING gist (geom);
CREATE INDEX avm_training_cty_date ON scoring.avm_training (county_code, deed_date);

-- Spatial feature: median sale_to_assessment of the 10 nearest PRIOR sales
-- in the same county. PRIOR ONLY — using later sales leaks the future into
-- the past, the same error a random train/test split makes.
--
-- Weakly correlated and well calibrated, and both are true. Spearman 0.28 ->
-- 0.36 across 2024-2026, but the neighbour median tracks the actual annual
-- median to within 1-2%. It predicts the local market LEVEL, not the
-- individual negotiation, which is what an AVM needs.
--
-- corr on RAW ratio is 0.022 while corr on log(ratio) is 0.247 — same data.
-- The target is heavy-tailed, so anything fitting it must work in log space.
ALTER TABLE scoring.avm_training ADD COLUMN IF NOT EXISTS knn_ratio numeric;
ALTER TABLE scoring.avm_training ADD COLUMN IF NOT EXISTS knn_count integer;

CREATE TABLE IF NOT EXISTS scoring.avm_training_progress (
  county_code text PRIMARY KEY,
  rows_inserted integer,
  done_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scoring.avm_knn_progress (
  county_code text PRIMARY KEY,
  rows_updated integer,
  done_at timestamptz DEFAULT now()
);


-- ============================================================
-- C. FILL — one county per tick, ~10 minutes for 57
-- ============================================================
-- The NOT EXISTS check reads the PROGRESS LEDGER, never avm_training itself.
-- A county's own output cannot be its progress marker when zero output is a
-- valid result: 17 mapped counties have no assessments and insert nothing.

SELECT cron.schedule('avm_training_fill', '10 seconds', $$
  WITH nxt AS (
    SELECT s.county_slug
    FROM outcomes.ecrv_county_map s
    WHERE NOT EXISTS (
      SELECT 1 FROM scoring.avm_training_progress p
      WHERE p.county_code = s.county_slug
    )
    ORDER BY s.county_slug
    LIMIT 1
  ),
  ins AS (
    INSERT INTO scoring.avm_training
    SELECT v.* FROM scoring.avm_training_set v
    WHERE v.county_code = (SELECT county_slug FROM nxt)
    ON CONFLICT (ecrv_id) DO NOTHING
    RETURNING 1
  )
  INSERT INTO scoring.avm_training_progress (county_code, rows_inserted)
  SELECT (SELECT county_slug FROM nxt), (SELECT count(*) FROM ins)
  WHERE EXISTS (SELECT 1 FROM nxt);
$$);

-- SELECT cron.unschedule('avm_training_fill');


-- ============================================================
-- D. KNN — one county per tick, ~22 minutes for 40
-- ============================================================
-- 30s rather than 10s: Hennepin's 51,026 rows take longer than a tick.
-- Overlapping runs are harmless (the ecrv_id join cannot collide) but wasteful.

SELECT cron.schedule('avm_knn_fill', '30 seconds', $$
  WITH nxt AS (
    SELECT DISTINCT t.county_code
    FROM scoring.avm_training t
    WHERE NOT EXISTS (
      SELECT 1 FROM scoring.avm_knn_progress p
      WHERE p.county_code = t.county_code
    )
    ORDER BY t.county_code
    LIMIT 1
  ),
  upd AS (
    UPDATE scoring.avm_training t
    SET knn_ratio = k.med, knn_count = k.n
    FROM (
      SELECT t2.ecrv_id,
             (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY n.sale_to_assessment)
                FROM (SELECT x.sale_to_assessment
                        FROM scoring.avm_training x
                       WHERE x.deed_date < t2.deed_date
                         AND x.county_code = t2.county_code
                       ORDER BY x.geom <-> t2.geom
                       LIMIT 10) n) AS med,
             (SELECT count(*)
                FROM (SELECT 1
                        FROM scoring.avm_training x
                       WHERE x.deed_date < t2.deed_date
                         AND x.county_code = t2.county_code
                       ORDER BY x.geom <-> t2.geom
                       LIMIT 10) n) AS n
      FROM scoring.avm_training t2
      WHERE t2.county_code = (SELECT county_code FROM nxt)
    ) k
    WHERE t.ecrv_id = k.ecrv_id
    RETURNING 1
  )
  INSERT INTO scoring.avm_knn_progress (county_code, rows_updated)
  SELECT (SELECT county_code FROM nxt), (SELECT count(*) FROM upd)
  WHERE EXISTS (SELECT 1 FROM nxt);
$$);

-- SELECT cron.unschedule('avm_knn_fill');


-- ============================================================
-- E. VERIFY — a green CREATE is not evidence
-- ============================================================
-- Expected 2026-08-24: 57 processed, 200,962 rows, 40 counties,
-- 200,921 with knn_ratio (40 nulls = each county's earliest sale, which has
-- no prior neighbour), 28,577 with sqft (15.7% — Ramsey 17,065 at 92.7% and
-- Washington 11,512 at 86.3%; Hennepin has none and never will, its parcel
-- layer is a tax roll with no building area in any of its 104 attributes).
--
--   SELECT now() AS run_at,
--          (SELECT count(*) FROM scoring.avm_training_progress) AS processed,
--          (SELECT count(*) FROM scoring.avm_training)          AS rows_loaded,
--          (SELECT count(DISTINCT county_code) FROM scoring.avm_training) AS counties,
--          (SELECT count(knn_ratio) FROM scoring.avm_training) AS with_knn,
--          (SELECT count(sqft) FROM scoring.avm_training)      AS with_sqft;
--
-- Confirm the KNN index is actually used — a plain CREATE INDEX proves
-- nothing. The plan must show `Index Scan using avm_training_geom_idx` with
-- `Order By: (geom <-> t.geom)`. A Sort means it is not, and the feature
-- becomes a 31-hour job.
--
--   EXPLAIN ANALYZE
--   SELECT t.ecrv_id,
--          (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY n.sale_to_assessment)
--             FROM (SELECT x.sale_to_assessment FROM scoring.avm_training x
--                    WHERE x.deed_date < t.deed_date
--                      AND x.county_code = t.county_code
--                    ORDER BY x.geom <-> t.geom LIMIT 10) n)
--   FROM scoring.avm_training t
--   WHERE t.county_code = 'ramsey' AND t.deed_date >= '2026-06-01'
--   LIMIT 50;
--
-- Then re-run the comparison: Actions -> avm-train -> dry_run = 1.
-- The bar is the county-year median. It has never been beaten.
