-- sql/redemption_curves.sql
--
-- scoring.redemption_curves — Kaplan-Meier survival, in SQL, with counts.
--
-- Created 2026-08-25. This is the TIMING half of the redemption product.
-- scoring.redemption_rates answers WHETHER an owner redeems; this answers
-- how long a window stays unresolved.
--
-- ============================================================
-- WHY A CURVE AND NOT THE COX MODEL
-- ============================================================
-- A Cox proportional-hazards model was built, and it works. Measured
-- 2026-08-25 on a time-ordered holdout, stratified by county:
--
--     target             full C   homestead-only C   the other 9 features
--     owner_exit          0.803        0.742              +0.061
--     foreclosure_sale    0.805        0.761              +0.044
--
-- It survived every check written to break it. A covariate-free control
-- scored exactly 0.500, so the county strata contribute no ranking.
-- Within-county concordance MATCHED or BEAT the pooled figure -- 0.866
-- against 0.805 for foreclosure_sale -- which is the opposite of what a
-- stratification artefact looks like.
--
-- *** BUT ONE BINARY COVARIATE CARRIES IT. *** homestead_yes alone scores
-- 0.74-0.76. Every other term is noise:
--
--     log_amount_owed   p=0.133
--     year_built        p=0.349
--     notice_of_intent  p=0.297, and it FLIPS SIGN between the two targets
--     bid_to_value      p=0.836 / 0.781, coefficient ~0
--
-- bid_to_value deserves a note, because it is the strongest cut in
-- scoring.redemption_rates -- 72.0% redemption under 50% of assessed value
-- against 34.5% at 80%+. It contributes NOTHING here. Either it is collinear
-- with a term already in the fit, or that gradient was a Hennepin-subset
-- effect that vanishes once county is stratified out. It separates as a
-- marginal rate and does not rank windows.
--
-- A per-property hazard whose ordering is 92% "is this owner-occupied" would
-- be a model-shaped wrapper around a fact scoring.redemption_rates already
-- publishes at 45.7% vs 32.9% with its sample size attached. The honest
-- product is the curve.
--
-- The Cox fit stays in ml/train_survival.py, gated. When the label set grows
-- past 92 and 119 events the other covariates may separate, and the script
-- will say so. It is the evidence that the curve is a ceiling, not a floor
-- -- the same role ml/train_avm.py plays for scoring.comp_ratios.
--
-- ============================================================
-- WHAT THE CURVE MEANS
-- ============================================================
-- survival = the share of comparable windows STILL UNRESOLVED that many days
-- after the SHERIFF SALE. Not after expiry: the clock starts at the sale,
-- which is when a subscriber first sees the property.
--
-- Two targets, and they are COMPETING RISKS. A window ends one way or the
-- other, and observing one censors the other. Read each curve as "still not
-- resolved THIS way", never as a probability of anything happening.
--
--     owner_exit        the owner sold inside the window    92 events
--     foreclosure_sale  an REO/third-party sale followed   119 events
--
-- Median days from expiry: owner exits run -65 (BEFORE the deadline, up to
-- 328 days early) and foreclosure sales +100. The strategy document promises
-- "most likely in the final three weeks" and has the direction backwards.
--
-- ============================================================
-- HOW KAPLAN-MEIER IS COMPUTED HERE
-- ============================================================
-- S(t) = product over event times i <= t of (1 - d_i / n_i), where d_i is
-- events at time i and n_i the number still at risk.
--
-- A product is a sum of logs: exp(sum(ln(1 - d/n))). n_i comes from a
-- descending running total of rows at each distinct duration, which is
-- exactly "how many were still being observed when this event happened".
--
-- GREATEST(..., 1e-12) guards the case where every remaining subject fails
-- at one time, which would put ln(0) in the sum.
--
-- Censoring is already handled upstream: scoring.redemption_features sets
-- observation_end to EXPIRY for the 109 ambiguous rows -- their ladder is
-- exhausted and observation stopped -- and to today for genuinely running
-- windows. A row that ended in the COMPETING way is censored at the moment
-- it resolved, not at today, so no curve claims a window was watched after
-- it had already closed.
--
-- ============================================================
-- THRESHOLDS
-- ============================================================
-- A stratum publishes at n >= 30 AND events >= 10. Below that the curve is
-- a step function of a handful of properties and the confidence interval
-- would be wider than the estimate. The event count is on every row so a
-- reader can judge it -- the same discipline as scoring.comp_ratios and
-- scoring.redemption_rates: publish the number and its n, and let a
-- subscriber argue with it.
-- ============================================================

CREATE OR REPLACE VIEW scoring.redemption_curves AS
WITH base AS (
  SELECT f.tracker_id,
         f.county_code,
         f.homestead,
         f.event_type,
         -- Duration to whatever ended the observation: the event itself, the
         -- competing event, or observation_end for a censored row.
         CASE
           WHEN f.event_type IS NOT NULL THEN f.days_anchor_to_event
           ELSE f.days_observed
         END AS duration
  FROM scoring.redemption_features f
),
expanded AS (
  -- One row per (target x stratum) membership. A window contributes to the
  -- 'all' curve, to its homestead curve and to its county curve.
  SELECT tgt.target,
         st.scope,
         st.bucket,
         b.duration,
         (b.event_type = tgt.target)::int AS event
  FROM base b
  CROSS JOIN (VALUES ('owner_exit'), ('foreclosure_sale')) AS tgt(target)
  -- === STRATA: WITHIN-COUNTY ONLY, AND WHY ===
  -- The first version of this view emitted a 'county' scope and a POOLED
  -- 'homestead' scope. Both were measured on 2026-08-25 and both mislead.
  --
  -- COUNTY comparisons are a coverage artefact. At 365 days:
  --     washington  84.4% resolved   31 events on   191 windows
  --     hennepin     9.8% resolved   35 events on 1,196 windows
  -- Two adjacent metro counties do not differ by 8x. Washington's tracker is
  -- small and heavily checked; Hennepin's is large and barely checked -- its
  -- ArcGIS batch has been failing with a 400 since 2026-08-23, and its sale
  -- date parsed to 1970-01-01 for every parcel until it was fixed the same
  -- day this was written. The curve measures HOW COMPLETELY WE HAVE CHECKED
  -- EACH COUNTY. Dropped.
  --
  -- POOLED HOMESTEAD washes the effect out entirely:
  --     homesteaded      40.5% resolved
  --     non-homesteaded  44.2% resolved
  -- which says homestead does not matter. Within Hennepin alone it plainly
  -- does:
  --     homesteaded      n=294  22 events  28.4% resolved
  --     non-homesteaded  n=122  13 events  51.5% resolved
  -- An absentee owner reaches a foreclosure sale at roughly TWICE the rate of
  -- one living in the house. Pooling across counties whose event rates run
  -- 2.9% to 16% swamps that completely.
  --
  -- The Cox model in ml/train_survival.py agreed all along -- homestead_yes
  -- was its only significant term, p=0.000, because it STRATIFIES BY COUNTY.
  -- The pooled curve disagreed because it did not. The model was right.
  --
  -- So the bucket is county AND homestead together. At n>=30 and events>=10
  -- that will leave Hennepin and little else, and that is the correct
  -- outcome: a comparison we cannot make honestly should not appear.
  CROSS JOIN LATERAL (VALUES
        ('all'::text,       'all windows'::text),
        ('county_homestead'::text,
         coalesce(b.county_code, '(unknown)') || ' / ' ||
         coalesce(b.homestead, '(unknown)'))
  ) AS st(scope, bucket)
  WHERE b.duration IS NOT NULL
    AND b.duration > 0
),
strata AS (
  SELECT target, scope, bucket,
         count(*)                          AS n,
         count(*) FILTER (WHERE event = 1) AS events
  FROM expanded
  GROUP BY target, scope, bucket
  HAVING count(*) >= 30
     AND count(*) FILTER (WHERE event = 1) >= 10
),
by_time AS (
  SELECT target, scope, bucket, duration AS t,
         count(*)                          AS n_here,
         count(*) FILTER (WHERE event = 1) AS d
  FROM expanded
  GROUP BY target, scope, bucket, duration
),
with_risk AS (
  -- Descending running total = how many were still at risk at time t.
  SELECT target, scope, bucket, t, d,
         sum(n_here) OVER (
           PARTITION BY target, scope, bucket ORDER BY t DESC
         ) AS n_risk
  FROM by_time
),
km AS (
  SELECT target, scope, bucket, t,
         ln(GREATEST(1::numeric - d::numeric / n_risk, 1e-12)) AS ln_q
  FROM with_risk
  WHERE d > 0 AND n_risk > 0
)
SELECT s.target,
       s.scope,
       s.bucket,
       s.n,
       s.events,
       h.horizon_days,
       -- exp(sum(ln q)). No event yet at this horizon -> sum is NULL ->
       -- survival is 1.0, which is correct rather than missing.
       round(coalesce(exp(sum(k.ln_q)), 1.0)::numeric, 4) AS survival,
       round((1 - coalesce(exp(sum(k.ln_q)), 1.0))::numeric * 100, 1)
         AS resolved_pct
FROM strata s
CROSS JOIN (VALUES (90), (180), (270), (365), (540)) AS h(horizon_days)
LEFT JOIN km k
       ON k.target = s.target
      AND k.scope  = s.scope
      AND k.bucket = s.bucket
      AND k.t     <= h.horizon_days
GROUP BY s.target, s.scope, s.bucket, s.n, s.events, h.horizon_days;

-- ============================================================
-- VERIFY — a green CREATE is not evidence
-- ============================================================
-- Expected 2026-08-25, the shape rather than exact values:
--
--   * survival is MONOTONE NON-INCREASING across horizons within a stratum.
--     If it rises, the running total is not ordered descending and n_risk is
--     wrong.
--   * every value is between 0 and 1.
--   * the 'all' stratum for foreclosure_sale sits at 0.8893 at 365 days,
--     matching the lifelines KM in the training log EXACTLY. That is the
--     cross-check that this SQL agrees with the library, and it has now held
--     twice -- before and after 137 duplicate rows were superseded.
--   * owner_exit 'all' 0.9394 at 365 days (lifelines: 0.9394).
--   * scope 'county' must NOT appear. If it does, the stratum list reverted.
--   * county_homestead should show hennepin / homestead and
--     hennepin / non-homestead, and probably nothing else. Fewer buckets than
--     expected is the threshold working, not a fault.
--
--   SELECT now() AS run_at, target, scope, bucket, n, events,
--          horizon_days, survival, resolved_pct
--   FROM scoring.redemption_curves
--   ORDER BY target, scope, bucket, horizon_days;
--
-- If the 365-day figures do not match the training log to within a rounding
-- step, the SQL and lifelines disagree about censoring and the SQL is wrong
-- -- lifelines is the reference implementation here, not this file.
