-- sql/redemption_features.sql
--
-- scoring.redemption_features — one row per REDEMPTION WINDOW, shaped for
-- survival analysis.
--
-- Created 2026-08-24. This is the training surface for the model
-- GOVIRE_AI_PREDICTION_STRATEGY §3 calls "the primary prediction path".
--
-- ============================================================
-- WHY A NEW TABLE AND NOT scoring.parcel_features
-- ============================================================
-- scoring.parcel_features already exists with 29 columns and is keyed on
-- parcel_id. Those columns are DISTRESS SIGNALS — has_active_sheriff_sale,
-- years_tax_delinquent, is_on_vbr, violations_count_24mo — and they describe
-- a parcel's current state for a rule-based distress score.
--
-- A survival model needs one row per WINDOW, not per parcel. A parcel can
-- foreclose twice with different outcomes and different expiry dates.
-- Measured 2026-08-24: PIN 2902821130010 has two tracker rows expiring
-- 2025-09-11 and 2026-01-01, both foreclosed_sold; PIN 1902921440068 has one
-- unknown and one pending. Writing survival features into a parcel-keyed
-- table would silently collapse those pairs and lose half the events.
--
-- ============================================================
-- THE TARGET — READ THIS BEFORE TRAINING ANYTHING
-- ============================================================
-- *** DO NOT USE outcome_detected_at. ***
--
-- It records when the CHECKER RAN. Measured across resolved rows it spans
-- −177 to +413 days from expiry, and the negatives are the proof: a
-- redemption cannot be detected 177 days before its window closes. Every
-- resolved row was stamped on a handful of run dates. A model trained on it
-- learns the 30/60/90/180-day ladder offsets and nothing about redemption.
--
-- Use outcome_event_date. Added and backfilled 2026-08-24 from
-- detection_notes, which is where the date lived as English prose until then.
-- NO SINGLE REGEX COULD EXTRACT IT — the arcgis notes put the sale date
-- first of two, ecrv_repeat_seller_post_expiry puts a DETECTION date first
-- and the real event second, and hand-written notes carry three. Seven
-- source-specific branches were needed, each verified against its own text
-- before a row was written. outcome_checker.py now returns it as a value so
-- nothing has to parse a sentence again.
--
-- ============================================================
-- TWO EVENTS, NOT ONE
-- ============================================================
--     event                 n      median days vs expiry
--     owner exit           92      −65   (owner sold, in-window)
--     foreclosure sale    119     +100   (REO/third-party sale, post-expiry)
--     censored          2,209      —     (window still running)
--
-- These are COMPETING RISKS: a window ends one way or the other, and
-- observing one censors the other. Fit them separately or use a competing-
-- risks model; do not pool them into "time to any outcome", which would
-- answer a question nobody asked.
--
-- *** THE STRATEGY DOCUMENT HAS THE DIRECTION BACKWARDS. *** It promises
-- "62% chance of redeeming, and if it does, most likely IN THE FINAL THREE
-- WEEKS". Measured: owners who exit do so at a median of 65 days BEFORE the
-- deadline, up to 328 days early. Nobody waits for the last three weeks.
--
-- ============================================================
-- WHAT event_date MEANS, PRECISELY
-- ============================================================
-- For an owner exit it is the DEED date — when the owner SOLD. Redemption
-- necessarily happened at or before it, so this bounds redemption from
-- above. It is a right-censored observation of the redemption moment, not
-- the moment itself. Do not label a chart "date of redemption".
--
-- Pinning it exactly needs the county recorder's certificate of redemption
-- (Minn. Stat. § 580.24 requires one be recorded). No source in the platform
-- touches that document type. That is an improvement to a buildable model,
-- not a prerequisite for one.
--
-- ============================================================
-- THE 41 UNDATED foreclosed ROWS
-- ============================================================
-- outcome = 'foreclosed' is detected by matching a lender name in the owner
-- field. That says the lender HOLDS the property; it does not say when title
-- moved. All 41 are undated and always will be.
--
-- They are included with event_date NULL and censored = true, because "the
-- lender still holds it" is genuinely a window that has not resolved into a
-- sale. Excluding them would discard 41 real observations; treating them as
-- events at an invented date would be worse.
--
-- ============================================================
-- FEATURES, AND WHAT WAS MEASURED AND REJECTED
-- ============================================================
-- Included because they separated on the resolved population:
--
--   bid_to_value    72.0% -> 34.5% redemption. STRONGEST, and it has a
--                   mechanism: a lender bidding under half assessed value
--                   leaves an equity cushion worth saving; bidding at or
--                   above it means the debt exceeds the property.
--                   HENNEPIN ONLY — finalBidAmount is a hennepin_sheriff
--                   payload field.
--   county          49.6% / 29.6% / 20.8%
--   homestead       45.7% / 32.9%
--   buyer_type      64.7% third-party vs 47.1% lender credit-bid
--
-- Measured and NOT included:
--
--   paid_vs_value   Purchase price against current assessed value. NO
--                   GRADIENT: 44.6 / 42.4 / 32.7 / 41.2 across four buckets,
--                   non-monotonic, all near the 36.5% base. The bucketing is
--                   correct — avg_years_held runs 0.8 / 2.0 / 5.1 / 11.8 —
--                   so purchase vintage genuinely does not predict.
--                   Kept as a COLUMN anyway: it costs nothing, and a model
--                   may find an interaction a marginal rate cannot.
--
--   notice_of_intent  MEASURED THE OPPOSITE OF EXPECTED. Owners who FILE a
--                   formal intent to redeem redeem 14.3% of the time (n=14)
--                   against 54.2% for those who do not. Either that is real
--                   — filing is what a struggling owner does in a last
--                   attempt that usually fails — or the field does not mean
--                   what its name says. On n=14 those cannot be told apart.
--                   Kept as a COLUMN, excluded from the published rate
--                   table. REVISIT AT n>=50.
--
--   signal stacking   Only 12 of 225 resolved parcels ever carried a second
--                   signal type. Stacking is real (489 parcels platform-wide,
--                   every combination cross-source) but concentrated in
--                   hennepin and ramsey, and it barely intersects the
--                   outcome population. Not enough overlap to test.
--
-- ============================================================
-- A VIEW, NOT A TABLE
-- ============================================================
-- ~2,461 rows and it changes daily as the checker resolves windows. Being
-- current matters more than being fast. scoring.avm_training is materialized
-- because a KNN feature over 200,962 rows took 30,770ms through a view and
-- 143ms against a table; nothing here is remotely that expensive.
-- ============================================================

CREATE OR REPLACE VIEW scoring.redemption_features AS
SELECT
  t.id                                        AS tracker_id,
  t.county_code,
  t.parcel_id,

  -- ---- Timing ----
  t.anchor_date,
  t.redemption_expiry_date,
  t.redemption_period_months,
  t.period_source,
  t.anchor_type,
  t.outcome,
  t.outcome_event_date,

  -- Days from EXPIRY to the event. Negative = before the deadline, which is
  -- normal and is where owner exits live (median −65).
  (t.outcome_event_date - t.redemption_expiry_date)      AS days_expiry_to_event,

  -- Days from the SHERIFF SALE to the event — the duration a survival model
  -- actually wants, because the clock starts at the sale, not at expiry.
  (t.outcome_event_date - t.anchor_date)                 AS days_anchor_to_event,

  -- For censored rows: how long we have observed them without an event.
  (current_date - t.anchor_date)                         AS days_observed,

  -- ---- Survival framing ----
  -- Three states, and they are COMPETING RISKS. 'foreclosed' is censored:
  -- the lender holds it and no sale has been observed.
  CASE
    WHEN t.outcome = 'redeemed_by_owner' THEN 'owner_exit'
    WHEN t.outcome = 'foreclosed_sold'   THEN 'foreclosure_sale'
    ELSE NULL
  END                                                    AS event_type,
  (t.outcome NOT IN ('redeemed_by_owner','foreclosed_sold')
   OR t.outcome_event_date IS NULL)                      AS censored,

  -- ---- Property ----
  p.emv_total,
  p.emv_year,
  p.sqft,
  p.lot_sqft,
  p.year_built,
  p.property_type,
  p.last_sale_price,
  p.last_sale_date,
  CASE
    WHEN p.homestead_status IS NULL                        THEN NULL
    WHEN upper(p.homestead_status) IN ('Y','YES')          THEN 'homestead'
    WHEN upper(p.homestead_status) LIKE 'FULL HOMESTEAD%'  THEN 'homestead'
    WHEN upper(p.homestead_status) LIKE '%VET HOMESTEAD%'  THEN 'homestead'
    WHEN upper(p.homestead_status) = 'BLIND/DISABLED'      THEN 'homestead'
    WHEN upper(p.homestead_status) IN ('N','NO')           THEN 'non-homestead'
    WHEN upper(p.homestead_status) LIKE 'NON HOMESTEAD%'   THEN 'non-homestead'
    WHEN upper(p.homestead_status) IN ('P','FRACTIONAL')   THEN 'partial'
    ELSE NULL
  END                                                    AS homestead,

  -- ---- Sale economics ----
  e.event_value                                          AS amount_owed,
  (e.raw_data->>'finalBidAmount')::numeric               AS final_bid,
  CASE
    WHEN (e.raw_data->>'finalBidAmount')::numeric IS NULL
      OR p.emv_total IS NULL OR p.emv_total = 0 THEN NULL
    ELSE round((e.raw_data->>'finalBidAmount')::numeric / p.emv_total, 4)
  END                                                    AS bid_to_value,
  CASE
    WHEN p.last_sale_price IS NULL OR p.last_sale_price = 0
      OR p.emv_total IS NULL OR p.emv_total = 0 THEN NULL
    ELSE round(p.last_sale_price / p.emv_total, 4)
  END                                                    AS paid_vs_value,
  CASE
    WHEN e.raw_data->>'toWhomSold' IS NULL THEN NULL
    WHEN outcomes.normalize_party_name(e.raw_data->>'toWhomSold')
       = outcomes.normalize_party_name(e.raw_data->>'mortgagee')
      THEN 'lender_credit_bid'
    ELSE 'third_party_buyer'
  END                                                    AS buyer_type,
  (e.raw_data->>'noticeOfIntent')::boolean                AS notice_of_intent,
  e.raw_data->'mortgagors'->0->>'display'                AS mortgagor,
  e.postponement_count,
  e.source                                               AS anchor_source

FROM outcomes.redemption_tracker t
-- LEFT joins throughout. 27 washington rows are keyed
-- 'WASHINGTON-FC-0202821420118' — a valid PIN behind a stub prefix, which
-- outcome_checker reads through and core.parcels joins do not. They have
-- REAL outcomes and belong in the population; they simply cannot contribute
-- parcel-derived features. An inner join dropped them from
-- scoring.redemption_rates and moved the base rate 36.5% -> 40.9%, which was
-- caught the same day the view shipped.
LEFT JOIN core.parcels p
       ON p.county_code = t.county_code
      AND p.parcel_id   = t.parcel_id
LEFT JOIN signals.distress_events e ON e.id = t.source_id;

-- ============================================================
-- VERIFY — a green CREATE is not evidence
-- ============================================================
-- Expected 2026-08-24:
--   total rows            2,461
--   event_type owner_exit        92, median days_expiry_to_event  −65
--   event_type foreclosure_sale 119, median days_expiry_to_event +100
--   censored                  2,250  (2,209 pending + 41 foreclosed)
--   bid_to_value non-null     1,150  (hennepin only)
--   homestead non-null        ~2,400
--
--   SELECT now() AS run_at, event_type, censored, count(*) AS n,
--          round(percentile_cont(0.5) WITHIN GROUP (
--            ORDER BY days_expiry_to_event)::numeric, 0) AS median_days,
--          count(bid_to_value) AS with_bid,
--          count(homestead)    AS with_homestead
--   FROM scoring.redemption_features
--   GROUP BY event_type, censored ORDER BY n DESC;
--
-- If censored is TRUE on any row with an event_type, the CASE and the
-- censored flag have drifted apart and the model would treat events as
-- non-events.
