-- sql/redemption_rates.sql
--
-- scoring.redemption_rates — observed redemption rates, with sample sizes.
--
-- Created 2026-08-24. The first prediction surface Govire has: not what a
-- property is worth, but what is likely to HAPPEN to it.
--
-- REGENERATED 2026-08-25 (task 2982) after two defects were found and fixed.
-- The prior version of this file recorded a definition that would silently
-- reintroduce the first of them. See DEFECTS FOUND 2026-08-25 below.
--
-- ============================================================
-- WHY THIS IS A VIEW AND NOT A MODEL
-- ============================================================
-- On 2026-08-23 an AVM was built, trained three times on growing data, and
-- LOST to a one-line county-year median every time — 12.53% against 11.32%,
-- with the gap WIDENING as data grew. Five approaches failed to beat a
-- GROUP BY.
--
-- GOVIRE_AI_PREDICTION_STRATEGY §2.3 predicted exactly that: "a calibrated
-- rate IS a prediction ... an unexplained 0.73 is the weaker product here —
-- an investor deciding whether to spend a Saturday wants to know why, and
-- '62 comparable properties, 47% redeemed' survives being questioned."
--
-- So this ships the rate, with its n, and a model is only worth reaching for
-- when a rate demonstrably cannot answer the question.
--
-- A VIEW rather than a materialized view: the population is a few hundred
-- rows and grows daily as outcome_checker and ecrv_outcome_detector resolve
-- windows. scoring.comp_ratios is materialized because it scans 338,038 eCRV
-- rows; this scans a few hundred, and being always-current matters more than
-- being fast. That property paid off on 2026-08-25 — every write that day
-- reached the page immediately with no refresh.
--
-- ============================================================
-- DEFECTS FOUND 2026-08-25
-- ============================================================
--
-- DEFECT 1 — IT COUNTED SUPERSEDED DUPLICATES.
-- The view had NO `superseded_by IS NULL` filter, so every retired duplicate
-- tracker row was counted in a PUBLISHED rate.
--
-- Found by measuring the same population two ways and getting different
-- answers: a hand query gave washington 66 resolved windows, the view gave
-- 93. The 27-row gap is exactly washington's 2026-08-25 12:38 supersede pass
-- — 1 foreclosed, 26 foreclosed_sold.
--
-- Effect on the headline: n 353 -> 326.
--
-- This matters more than it looks. 762 stub rows were retired on 2026-08-25
-- alone. ANY view over redemption_tracker that omits the superseded filter
-- counts windows twice, and its numbers move every time cleanup runs.
--
-- DEFECT 2 — INFERENCES WERE FOLDED INTO THE HEADLINE.
-- 127 of 412 resolved rows carry ambiguous = true: carlton's
-- forfeit-owner-name matches, the eCRV flipper rule, low-consideration eCRV
-- sales. The view reported one rate and said nothing about them.
--
-- They are NOT evenly spread. Almost all are foreclosed_sold (28) or
-- foreclosed (11) against only 2 redemptions, so they inflate the
-- DENOMINATOR and never the numerator. Excluding them raises every rate:
--
--     all          33.4% of 326  ->  37.5% of 285 confirmed
--     hennepin     39.2% of 158  ->  43.0% of 142
--     anoka        50.0% of  28  ->  68.4% of  19
--     bid <50%     58.1% of  31  ->  64.3% of  28
--
-- Three columns APPENDED — n_confirmed, redeemed_confirmed,
-- redeem_pct_confirmed. Appended, not reordered: CREATE OR REPLACE VIEW can
-- only add columns at the end, and src/routes/properties.py selects the
-- original five by name, so it keeps working untouched.
--
-- THE CONFIRMED FLOOR APPLIES TO BOTH COUNTS. Every HAVING now tests n AND
-- n_confirmed against the same threshold. Without it, `third-party buyer`
-- passed at n=18 and published a confirmed rate on 13 rows — under its own
-- floor of 15, and swinging 61.1% -> 84.6% between the two columns. A bucket
-- that cannot support a confirmed rate should not print one.
--
-- Two buckets dropped out when this was added, and both are honest losses:
--     third-party buyer   18 rows,  13 confirmed  (floor 15)
--     anoka               28 rows,  19 confirmed  (floor 20)
-- Anoka was the newest county on the page and the only one above 50%. It
-- returns at 20 confirmed. It is at 19.
--
-- ============================================================
-- THE BASE RATE
-- ============================================================
-- 2026-08-24:  36.5% of 252 resolved windows.
-- 2026-08-25:  33.4% of 326, or 37.5% of 285 confirmed.
--
-- The headline barely moved and the reasons CANCEL rather than agree:
--   -  56 foreclosures recovered by retuned REO patterns pushed it DOWN
--   -  17 redemptions recovered by the eCRV detector pushed it UP
--   -  27 superseded duplicates removed pushed it UP
--
-- A prediction was made during that day that the rate was "biased low,
-- structurally", reasoning from the idle redemption detector alone. True of
-- that mechanism, wrong about the net, because another was being changed at
-- the same time. Read the whole population before predicting a direction.
--
-- ============================================================
-- WHAT SEPARATES, AND WHAT DOES NOT
-- ============================================================
-- Cuts NOT published and why:
--
--   period_months   6mo dominates the population. The 12mo bucket is tiny.
--                   No contrast to publish yet, though the statutory
--                   difference is real and this should be revisited.
--
--   notice_of_intent  MEASURED THE OPPOSITE OF EXPECTED and is withheld
--                   deliberately. Owners who FILE a formal intent to redeem
--                   redeemed 14.3% of the time (n=14) against 54.2% for those
--                   who do not. Either that is a real finding — filing is
--                   what a struggling owner does in a last attempt that
--                   usually fails — or the field does not mean what its name
--                   suggests. On n=14 those cannot be told apart, and
--                   publishing a counter-intuitive rate on 14 observations
--                   would be indefensible. REVISIT AT n>=50.
--
-- ============================================================
-- bid_to_value — still the strongest cut
-- ============================================================
-- 2026-08-25, and it survives excluding every inference:
--
--                  all              confirmed
--   under 50%   58.1% (n=31)     64.3% (n=28)
--   50-80%      44.2% (n=77)     49.3% (n=67)
--   80%+        20.0% (n=50)     21.3% (n=47)
--
-- Monotonic on both. Mechanism: a lender bidding under half the assessed
-- value leaves a large equity cushion, so the owner has something worth
-- saving and can often borrow against it. A bid at or above assessed value
-- means the debt exceeds what the property is worth and there is nothing to
-- redeem FOR.
--
-- STAGE2_SURVIVAL_FINDINGS §3 calls bid_to_value "the sharpest
-- disagreement": strongest cut in this table, statistically nothing in the
-- Cox hazard model (p=0.836). That disagreement is UNRESOLVED. The gradient
-- here is now steeper and better-populated than when it was named, which is
-- new evidence on it but not a resolution.
--
-- *** THIS CUT EXISTS ONLY FOR HENNEPIN. *** finalBidAmount is a
-- hennepin_sheriff payload field. dakota_sheriff and washington_sheriff
-- publish a different field set and carry NO bid at all. Rows without a bid
-- are EXCLUDED from the scope, not bucketed — see the note on the
-- bid_to_value block below.
--
-- ============================================================
-- KNOWN LIMITATION: homestead coverage
-- ============================================================
-- Four county vocabularies exist and are normalised below:
--   dakota      FULL HOMESTEAD / NON HOMESTEAD / DISABLED VET HOMESTEAD /
--               FRACTIONAL / BLIND/DISABLED
--   ramsey      Y / N / P
--   hennepin    Yes / No
--   washington  Yes / No
--
-- ANOKA WRITES NULL FOR NOT-HOMESTEADED, not 'N' — enumerated 2026-08-25
-- over all 140,279 anoka parcels: 'Y' 105,887, null 34,392, no third value.
-- So an anoka null is a NEGATIVE, while a null elsewhere may mean unknown.
-- The CASE below maps null to NULL and the scope excludes it, which is
-- correct but means anoka contributes fewer homestead rows than it has.
--
-- FRACTIONAL and P are 'partial' and NOT folded into either side — a
-- fractional homestead is a genuinely different situation and collapsing it
-- would hide that.
--
-- ============================================================
-- WHAT THIS CANNOT ANSWER
-- ============================================================
-- WHEN a redemption happens. Redemption is inferred from the ABSENCE of a
-- post-expiry deed, and an absence has no date. See
-- STAGE2_SURVIVAL_FINDINGS_2026-08-23.md §6.
--
-- Time to FORECLOSURE SALE is recoverable and is the survival model's
-- target. This view answers WHETHER, not WHEN.
--
-- ============================================================
-- HOW TO REGENERATE THIS FILE
-- ============================================================
-- Never retype the body. Generate it:
--
--     SELECT pg_get_viewdef('scoring.redemption_rates'::regclass, true);
--
-- The database is authoritative for the definition; this file is the
-- reconstruction path and the record of WHY the filters are there, which
-- pg_get_viewdef cannot tell you.
-- ============================================================

CREATE OR REPLACE VIEW scoring.redemption_rates AS
WITH base AS (
  SELECT t.id,
         t.county_code,
         t.ambiguous,
         (t.outcome = 'redeemed_by_owner')                     AS redeemed,
         -- Normalise four county vocabularies to three states.
         CASE
           WHEN p.homestead_status IS NULL                     THEN NULL
           WHEN upper(p.homestead_status) IN ('Y','YES')       THEN 'homestead'
           WHEN upper(p.homestead_status) LIKE 'FULL HOMESTEAD%' THEN 'homestead'
           WHEN upper(p.homestead_status) LIKE '%VET HOMESTEAD%' THEN 'homestead'
           WHEN upper(p.homestead_status) = 'BLIND/DISABLED'   THEN 'homestead'
           WHEN upper(p.homestead_status) IN ('N','NO')        THEN 'non-homestead'
           WHEN upper(p.homestead_status) LIKE 'NON HOMESTEAD%' THEN 'non-homestead'
           WHEN upper(p.homestead_status) IN ('P','FRACTIONAL') THEN 'partial'
           ELSE NULL
         END                                                    AS homestead,
         CASE
           WHEN e.raw_data->>'toWhomSold' IS NULL THEN NULL
           WHEN outcomes.normalize_party_name(e.raw_data->>'toWhomSold')
              = outcomes.normalize_party_name(e.raw_data->>'mortgagee')
             THEN 'lender credit-bid'
           ELSE 'third-party buyer'
         END                                                    AS buyer_type,
         CASE
           WHEN (e.raw_data->>'finalBidAmount')::numeric IS NULL
             OR p.emv_total IS NULL OR p.emv_total = 0          THEN NULL
           WHEN (e.raw_data->>'finalBidAmount')::numeric / p.emv_total < 0.5
             THEN 'under 50%'
           WHEN (e.raw_data->>'finalBidAmount')::numeric / p.emv_total < 0.8
             THEN '50-80%'
           ELSE '80%+'
         END                                                    AS bid_to_value
  FROM outcomes.redemption_tracker t
  -- LEFT join, not inner (fixed 2026-08-24, the day this view shipped).
  --
  -- An inner join silently dropped 27 of 252 resolved rows — 11% of the
  -- population — and pulled the base rate from 36.5% to 40.9%. Every one was
  -- a washington row keyed 'WASHINGTON-FC-0202821420118': a valid 13-digit
  -- PIN behind a stub prefix, which outcome_checker.py learned to read
  -- through but core.parcels joins still cannot.
  --
  -- Those rows have a REAL outcome. They belong in the base rate and in the
  -- county cut. They simply cannot contribute to homestead or bid_to_value,
  -- which need parcel data, and a LEFT join expresses exactly that: present
  -- in the population, absent from the cuts that need a parcel.
  --
  -- STILL REQUIRED 2026-08-25. 762 stub rows were retired that day and 2
  -- re-keyed, but the retirement was for DUPLICATES — a stub whose twin
  -- already carries the real PIN. Stubs with no twin remain, and 58 tracker
  -- rows across hennepin and dakota have no PIN at source either. LEFT join
  -- stays.
  LEFT JOIN core.parcels p
         ON p.county_code = t.county_code
        AND p.parcel_id   = t.parcel_id
  LEFT JOIN signals.distress_events e ON e.id = t.source_id
  -- Resolved outcomes only. 'unknown' is an unresolved ladder, not a result,
  -- and 'pending' has not happened yet.
  WHERE t.outcome IN ('redeemed_by_owner','foreclosed_sold','foreclosed')
    -- ADDED 2026-08-25 — DEFECT 1. Without this the view counted retired
    -- duplicates. washington read 93 resolved windows against a true 66.
    AND t.superseded_by IS NULL
)
SELECT 'all'::text        AS scope,
       'all resolved windows'::text AS bucket,
       count(*)           AS n,
       count(*) FILTER (WHERE redeemed) AS redeemed,
       round(100.0 * count(*) FILTER (WHERE redeemed) / count(*), 1) AS redeem_pct,
       count(*) FILTER (WHERE NOT ambiguous) AS n_confirmed,
       count(*) FILTER (WHERE NOT ambiguous AND redeemed) AS redeemed_confirmed,
       round(100.0 * count(*) FILTER (WHERE NOT ambiguous AND redeemed)
             / NULLIF(count(*) FILTER (WHERE NOT ambiguous), 0), 1) AS redeem_pct_confirmed
FROM base

UNION ALL
SELECT 'county', county_code, count(*), count(*) FILTER (WHERE redeemed),
       round(100.0 * count(*) FILTER (WHERE redeemed) / count(*), 1),
       count(*) FILTER (WHERE NOT ambiguous),
       count(*) FILTER (WHERE NOT ambiguous AND redeemed),
       round(100.0 * count(*) FILTER (WHERE NOT ambiguous AND redeemed)
             / NULLIF(count(*) FILTER (WHERE NOT ambiguous), 0), 1)
FROM base GROUP BY county_code
HAVING count(*) >= 20 AND count(*) FILTER (WHERE NOT ambiguous) >= 20

UNION ALL
SELECT 'homestead', homestead, count(*), count(*) FILTER (WHERE redeemed),
       round(100.0 * count(*) FILTER (WHERE redeemed) / count(*), 1),
       count(*) FILTER (WHERE NOT ambiguous),
       count(*) FILTER (WHERE NOT ambiguous AND redeemed),
       round(100.0 * count(*) FILTER (WHERE NOT ambiguous AND redeemed)
             / NULLIF(count(*) FILTER (WHERE NOT ambiguous), 0), 1)
FROM base WHERE homestead IS NOT NULL
GROUP BY homestead
HAVING count(*) >= 20 AND count(*) FILTER (WHERE NOT ambiguous) >= 20

UNION ALL
SELECT 'buyer_type', buyer_type, count(*), count(*) FILTER (WHERE redeemed),
       round(100.0 * count(*) FILTER (WHERE redeemed) / count(*), 1),
       count(*) FILTER (WHERE NOT ambiguous),
       count(*) FILTER (WHERE NOT ambiguous AND redeemed),
       round(100.0 * count(*) FILTER (WHERE NOT ambiguous AND redeemed)
             / NULLIF(count(*) FILTER (WHERE NOT ambiguous), 0), 1)
FROM base WHERE buyer_type IS NOT NULL
GROUP BY buyer_type
HAVING count(*) >= 15 AND count(*) FILTER (WHERE NOT ambiguous) >= 15

UNION ALL
-- NULLS EXCLUDED, not bucketed (fixed 2026-08-24, hours after this view
-- shipped). This block used to coalesce them into a '(no bid data)' bucket,
-- which reached 131 rows at 24.4% once the LEFT join admitted washington.
--
-- That bucket was a lie in the shape of a statistic. It sits in the
-- bid_to_value scope and reads on a page as though MISSING INFORMATION
-- predicted a low redemption rate. It does not. finalBidAmount is a
-- hennepin_sheriff payload field; dakota_sheriff and washington_sheriff
-- publish a different set and carry no bid at all. '(no bid data): 24.4%'
-- means 'not Hennepin', and nothing else.
--
-- The three other scopes already do this with WHERE ... IS NOT NULL. This one
-- is now consistent with them: a property with no bid matches no bid bucket,
-- and the API's matcher drops the scope entirely for those rows rather than
-- showing an absence as a finding.
SELECT 'bid_to_value', bid_to_value, count(*), count(*) FILTER (WHERE redeemed),
       round(100.0 * count(*) FILTER (WHERE redeemed) / count(*), 1),
       count(*) FILTER (WHERE NOT ambiguous),
       count(*) FILTER (WHERE NOT ambiguous AND redeemed),
       round(100.0 * count(*) FILTER (WHERE NOT ambiguous AND redeemed)
             / NULLIF(count(*) FILTER (WHERE NOT ambiguous), 0), 1)
FROM base WHERE bid_to_value IS NOT NULL
GROUP BY bid_to_value
HAVING count(*) >= 20 AND count(*) FILTER (WHERE NOT ambiguous) >= 20;

-- ============================================================
-- VERIFY — a green CREATE is not evidence
-- ============================================================
-- Expected 2026-08-25 evening, TEN rows:
--
--   all            326 / 33.4%   confirmed 285 / 37.5%
--   county         hennepin 158 (39.2%, conf 142 / 43.0%)
--                  washington 66 (24.2%, conf 64 / 25.0%)
--                  dakota    61 (26.2%, conf 59 / 27.1%)
--   homestead      homestead 171 (38.6%, conf 158 / 41.1%)
--                  non-homestead 114 (24.6%, conf 107 / 26.2%)
--   buyer_type     lender credit-bid 140 (36.4%, conf 129 / 38.8%)
--   bid_to_value   under 50% 31 (58.1%, conf 28 / 64.3%)
--                  50-80%    77 (44.2%, conf 67 / 49.3%)
--                  80%+      50 (20.0%, conf 47 / 21.3%)
--
-- THE 'all' ROW MUST READ 326, NOT 353. If it reads 353 the
-- superseded_by filter reverted and retired duplicates are being published.
--
-- ANOKA AND third-party buyer MUST BE ABSENT. Both pass the n floor and fail
-- the n_confirmed floor (19 and 13). If either appears, the confirmed floor
-- reverted and a rate is being published on fewer rows than its own
-- threshold allows.
--
--   SELECT now() AS run_at, scope, bucket, n, redeemed, redeem_pct,
--          n_confirmed, redeemed_confirmed, redeem_pct_confirmed
--   FROM scoring.redemption_rates ORDER BY scope, n DESC;
