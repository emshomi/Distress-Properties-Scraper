-- sql/redemption_curves.sql
--
-- scoring.redemption_curves — Kaplan-Meier survival, in SQL, with counts.
--
-- Created 2026-08-25. This is the TIMING half of the redemption product.
-- scoring.redemption_rates answers WHETHER an owner redeems; this answers
-- how long a window stays unresolved.
--
-- REGENERATED 2026-08-25 evening after a defect was found that materially
-- changed every published figure. See THE STATUTE-MIXING DEFECT below.
-- Several facts recorded earlier the same day also moved; those are dated
-- in place rather than overwritten, because the earlier measurement was
-- correct when it was taken.
--
-- ============================================================
-- THE STATUTE-MIXING DEFECT — found and fixed 2026-08-25 evening
-- ============================================================
-- This view pooled TWO STATUTORY TRACKS into one Kaplan-Meier estimate.
--
--     anchor_type          rows   events   avg duration
--     sheriff_sale        1,336      218        159 days
--     tax_judgment_sale     335        0        921 days
--
-- Ch. 281 tax forfeiture runs three years from judgment. Ch. 580/582
-- mortgage redemption runs six months from the sheriff sale. They are not
-- the same process and they do not share a clock.
--
-- EVERY tax-forfeiture row is censored and always will be. outcome_checker's
-- REO and post-expiry-sale branches detect MORTGAGE foreclosure outcomes; a
-- forfeiture window cannot reach a foreclosure sale at all. So 315 rows
-- entered the risk set, sat in it for an average of 921 days, never failed,
-- and depressed the hazard at every event time.
--
-- Measured before the fix at the 1-year horizon: risk set 1,403 with 101
-- events; sheriff-sale only, 1,264 with the SAME 101 events. Identical
-- numerator, denominator inflated 10%.
--
-- THE EFFECT ON THE PUBLISHED FIGURES WAS FAR LARGER THAN 10%, and that is
-- worth understanding rather than just recording:
--
--     within 1 year    foreclosure sale   18.8%  ->  33.0%
--                      owner sold         11.1%  ->  15.0%
--     within 18 months foreclosure sale   21.9%  ->  51.0%
--
-- A single-horizon risk-set ratio does not predict the change, because KM
-- is a PRODUCT over event times. Those 315 long-duration rows sat in the
-- risk set at every event time including the latest ones, so removing them
-- lowers n_risk at each step, raises d/n_risk at each step, and the effect
-- compounds. A prediction of "roughly a tenth" was made before running it
-- and was wrong by a factor of seven.
--
-- redemption_builder.py already states the principle this violates: forcing
-- one statutory track onto the other "would make every row in this table
-- ambiguous about which law it describes".
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
-- scoring.redemption_rates. It contributes NOTHING here. Either it is
-- collinear with a term already in the fit, or that gradient was a Hennepin
-- subset effect that vanishes once county is stratified out. It separates as
-- a marginal rate and does not rank windows.
--
-- 2026-08-25 EVENING UPDATE: the bid_to_value gradient is now STEEPER and
-- better populated than when that was written, and it survives excluding
-- every inferred outcome -- 64.3% (n=28) / 49.3% (n=67) / 21.3% (n=47) on
-- confirmed rows only. That is new evidence on the disagreement, not a
-- resolution of it. The Cox p-value has not been re-measured on the larger
-- label set.
--
-- A per-property hazard whose ordering is 92% "is this owner-occupied" would
-- be a model-shaped wrapper around a fact scoring.redemption_rates already
-- publishes with its sample size attached. The honest product is the curve.
--
-- The Cox fit stays in ml/train_survival.py, gated. It is the evidence that
-- the curve is a ceiling, not a floor -- the same role ml/train_avm.py plays
-- for scoring.comp_ratios.
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
--     owner_exit        the owner sold inside the window
--     foreclosure_sale  an REO/third-party sale followed
--
-- EVENT COUNTS MOVED 2026-08-25. Written as 92 and 119; now 109 and 109.
-- Two changes that day: the REO pattern list was retuned after reading
-- outcomes.owner_checks (NATL, MTG and TRS abbreviations and every spelling
-- of Veterans Affairs had been missing, costing 56 outcomes), and the eCRV
-- outcome detector -- which had never been a scheduled job and last ran by
-- hand on 2026-08-17 -- was rebuilt and run, adding 17 redemptions.
--
-- Median days from expiry: owner exits run BEFORE the deadline and
-- foreclosure sales after. The strategy document promises "most likely in
-- the final three weeks" and has the direction backwards.
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
-- CENSORING IS HANDLED UPSTREAM and one part of it is a JUDGEMENT, not a
-- fact. scoring.redemption_features sets observation_end to EXPIRY for
-- 'unknown' rows and to today for running windows. Measured 2026-08-25: the
-- 86 unknown rows average 132 days observed under that rule and would
-- average 395 if censored at today.
--
-- The case for expiry: after it there is genuinely no post-expiry deed left
-- to find. The case for today: the checker laddered 30/60/90/180 days PAST
-- expiry and found nothing, which is real information about survival.
-- Censoring at 132 days removes those rows from the risk set before the
-- window where most events occur. UNRESOLVED -- changing it would move
-- published figures again and the original choice was deliberate.
--
-- ============================================================
-- ONE SCOPE ONLY, AND WHY
-- ============================================================
-- The first version emitted a 'county' scope and a POOLED 'homestead'
-- scope. Both were measured on 2026-08-25 and both mislead.
--
-- COUNTY comparisons are a coverage artefact. At 365 days:
--     washington  84.4% resolved   31 events on   191 windows
--     hennepin     9.8% resolved   35 events on 1,196 windows
-- Two adjacent metro counties do not differ by 8x. The curve measures HOW
-- COMPLETELY WE HAVE CHECKED EACH COUNTY. Dropped.
--
-- POOLED HOMESTEAD washed the effect out; COUNTY x HOMESTEAD then reversed
-- direction between neighbouring counties:
--
--     county      homesteaded   non-homesteaded   direction
--     dakota          37.1%          48.2%        non-hs faster
--     hennepin        28.4%          27.2%        NO DIFFERENCE
--     washington     100.0%          72.1%        homesteaded faster
--
-- (An earlier read appeared to show Hennepin separating 28.4% against
-- 51.5%. That query omitted the superseded filter and measured survival
-- over all time rather than at 365 days. The apparent finding was the
-- query, not the data.)
--
-- === 2026-08-25 EVENING: THE HOMESTEAD DISAGREEMENT HAS A MECHANISM ===
-- Re-measured on 218 events rather than 185, using a DIFFERENT metric --
-- share of RESOLVED events that were owner exits, not KM survival at 365
-- days. The two are not comparable figure-for-figure and are recorded
-- separately for that reason.
--
--     county      homesteaded   non-homesteaded   n (resolved)
--     hennepin        68.6%          48.3%        70 / 29
--     washington      38.5%          27.3%        26 / 22
--     dakota          36.4%          38.1%        22 / 21
--
-- Two of three now agree with the model. Dakota still runs the other way --
-- and Dakota is the ONLY county with an observation imbalance across
-- homestead:
--
--     county      homesteaded pending   non-homesteaded pending
--     hennepin           68.1%                   69.1%
--     washington         58.7%                   53.6%
--     dakota             52.7%                   29.3%      <- 23 points
--
-- Dakota's non-homesteaded rows have been checked far more completely, so
-- more of their outcomes are visible. That is a mechanism for the outlier
-- and a TESTABLE PREDICTION: work Dakota's 49 homesteaded pending rows down
-- to the non-homesteaded rate and the sign should flip.
--
-- The model still does not ship. Two counties agreeing and a third
-- confounded is not enough. But the blocker now has an explanation instead
-- of being an unexplained contradiction.
--
-- THE PATTERN, THREE TIMES OVER: checking coverage varies so much between
-- counties that almost any stratification inherits it. Stratifying to remove
-- the confound found the confound underneath.
--
-- (The earlier note here said Hennepin's ArcGIS batch "has been failing with
-- a 400 since 2026-08-23". Investigated 2026-08-25: it was ONE skipped batch
-- on one run, 5 rows, and the exact request returns HTTP 200 with all
-- features from a residential IP. Twelve hypotheses eliminated. Hennepin's
-- real problem was that 579 of its 1,035 pending rows were stub-keyed
-- duplicates the checker could not select; those were retired the same day
-- and its live pending is now 456.)
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
  -- SHERIFF SALES ONLY. ADDED 2026-08-25 evening -- see THE STATUTE-MIXING
  -- DEFECT in the header. 315 tax_judgment_sale rows were in this
  -- population with ZERO events and an average duration of 921 days,
  -- against 1,336 sheriff_sale rows averaging 159. Removing them moved
  -- "reached a foreclosure sale within 1 year" from 18.8% to 33.0%.
  WHERE f.anchor_type = 'sheriff_sale'
),
expanded AS (
  -- One row per (target x stratum) membership.
  SELECT tgt.target,
         st.scope,
         st.bucket,
         b.duration,
         (b.event_type = tgt.target)::int AS event
  FROM base b
  CROSS JOIN (VALUES ('owner_exit'), ('foreclosure_sale')) AS tgt(target)
  -- ONE SCOPE. Everything else measured as a coverage artefact -- see the
  -- header. Revisit when the counties are checked to comparable depth.
  CROSS JOIN LATERAL (VALUES
        ('all'::text, 'all windows'::text)
  ) AS st(scope, bucket)
  -- duration > 0 also excludes the tax-forfeiture rows whose anchor_date IS
  -- their expiry. That is NOT a data defect: redemption_builder falls back
  -- to the expiry as the anchor when a county publishes no bid-in date --
  -- only 151 of 288 events carry one -- so the row honestly says "this
  -- window ends on the date the county published" and its start is unknown.
  -- A duration computed from a placeholder anchor is meaningless. Those
  -- rows are now excluded by anchor_type as well, which is the better
  -- reason.
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
-- Expected 2026-08-25 EVENING, after the statute fix. TEN rows.
--
--   n = 1336 ON EVERY ROW. If it reads 1651 the anchor_type filter
--   reverted and 315 tax-forfeiture windows are back in the risk set.
--
--   events = 109 for both targets.
--
--     target             90    180    270    365    540
--     foreclosure_sale  1.2%   3.2%  11.0%  33.0%  51.0%
--     owner_exit        4.4%  11.7%  13.0%  15.0%  15.0%
--
--   Shape checks that hold regardless of the numbers:
--     * survival is MONOTONE NON-INCREASING across horizons.
--       If it rises, the running total is not ordered descending.
--     * every survival value is between 0 and 1.
--     * scope is 'all' and nothing else -- any other scope means the
--       stratum list reverted.
--
--   owner_exit is FLAT at 15.0% between 365 and 540 days: no owner sold
--   between one year and eighteen months in the tracked population. Either
--   real -- owners who sell do it inside the window or not at all -- or an
--   observation edge, since few rows have been tracked past a year. The n
--   is published beside it so a reader can judge.
--
--   SELECT now() AS run_at, target, scope, bucket, n, events,
--          horizon_days, survival, resolved_pct
--   FROM scoring.redemption_curves
--   ORDER BY target, scope, bucket, horizon_days;
--
-- THE LIFELINES CROSS-CHECK IS NOW STALE. The pre-fix figures matched the
-- lifelines KM in the training log exactly -- 0.8893 and 0.9394 at 365 days
-- -- and that agreement held twice, before and after 137 duplicate rows
-- were superseded. It was the strongest available evidence that this SQL is
-- correct.
--
-- ml/train_survival.py has NOT been re-run against the sheriff-sale-only
-- population. Until it is, this file has no independent implementation to
-- agree with. RE-ESTABLISH THAT CROSS-CHECK: two independent
-- implementations agreeing is the strongest check this project has, and it
-- is currently absent.
