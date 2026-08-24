-- sql/redemption_rates.sql
--
-- scoring.redemption_rates — observed redemption rates, with sample sizes.
--
-- Created 2026-08-24. The first prediction surface Govire has: not what a
-- property is worth, but what is likely to HAPPEN to it.
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
-- A VIEW rather than a materialized view: the population is ~225 rows and
-- grows daily as outcome_checker resolves windows. scoring.comp_ratios is
-- materialized because it scans 338,038 eCRV rows; this scans a few hundred,
-- and being always-current matters more than being fast.
--
-- ============================================================
-- THE BASE RATE
-- ============================================================
-- 36.5% of resolved sheriff-sale redemption windows end in redemption
-- (92 of 252, measured 2026-08-24). First time this has been measured.
--
-- Read 40.9% (92 of 225) before the LEFT join fix below. The 27 rows an inner
-- join dropped are all washington and all foreclosed -- none of them redeemed
-- -- so excluding them inflated the rate by 4.4 points.
--
-- That is the number every bucket below should be read against. A bucket at
-- 47% is mildly above base; one at 72% is a different situation.
--
-- ============================================================
-- WHAT SEPARATES, AND WHAT DOES NOT
-- ============================================================
-- Seven cuts were measured before choosing these four:
--
--   bid_to_value    72.0% -> 34.5%   STRONGEST, and has a mechanism
--   county          49.6% -> 29.6%
--   homestead       47.0% -> 33.3%
--   buyer_type      64.7% -> 47.1%
--
-- Cuts NOT published and why:
--
--   period_months   6mo is 199 of 225 rows. The 12mo bucket is n=6. There is
--                   no contrast to publish yet, though the statutory
--                   difference is real and this should be revisited.
--
--   notice_of_intent  MEASURED THE OPPOSITE OF EXPECTED and is withheld
--                   deliberately. Owners who FILE a formal intent to redeem
--                   redeem 14.3% of the time (n=14) against 54.2% for those
--                   who do not. Either that is a real finding — filing is
--                   what a struggling owner does in a last attempt that
--                   usually fails — or the field does not mean what its name
--                   suggests. On n=14 those cannot be told apart, and
--                   publishing a counter-intuitive rate on 14 observations
--                   would be indefensible. REVISIT AT n>=50.
--
-- ============================================================
-- bid_to_value — the strongest cut, and Hennepin-only
-- ============================================================
--   under 50%   72.0%  (n=25)
--   50-80%      47.8%  (n=67)
--   80%+        34.5%  (n=29)
--
-- Mechanism: a lender bidding under half the assessed value leaves a large
-- equity cushion, so the owner has something worth saving and can often
-- borrow against it. A bid at or above assessed value means the debt exceeds
-- what the property is worth and there is nothing to redeem FOR.
--
-- Verified to be a real effect and not a county-mix artefact: the same
-- gradient holds with county held constant at hennepin.
--
-- 'over 100%' is COLLAPSED into '80%+'. On its own it is n=6, which is not
-- publishable, and it says the same thing as 80-100% (33.3% vs 34.8%).
--
-- *** THIS CUT EXISTS ONLY FOR HENNEPIN. *** finalBidAmount is a
-- hennepin_sheriff payload field. dakota_sheriff and washington_sheriff
-- publish a different field set and carry NO bid at all — 104 of 225 resolved
-- rows. The view labels those rows '(no bid data)' rather than bucketing them,
-- because a bucket that silently means "the other two counties" is worse than
-- an honest gap.
--
-- ============================================================
-- KNOWN LIMITATION: homestead coverage
-- ============================================================
-- homestead_status is populated on 167,104 dakota parcels and 163,880 ramsey
-- parcels, but only 5,578 of 448,266 hennepin (1.3%) and 207 of 118,430
-- washington (0.2%) as of 2026-08-24.
--
-- hennepin_parcels.py was fixed the same day to map HMSTD_CD1 (present on
-- 443,614 rows, clean two-state H/N), and a full run was in flight when this
-- view was written. Until that lands, the homestead cut is measuring a 1.3%
-- sample on the county holding half the tracker. THE n COLUMN MAKES THAT
-- VISIBLE, which is the point of publishing it.
--
-- Four vocabularies exist and are normalised below:
--   dakota      FULL HOMESTEAD / NON HOMESTEAD / DISABLED VET HOMESTEAD /
--               FRACTIONAL / BLIND/DISABLED
--   ramsey      Y / N / P
--   hennepin    Yes / No
--   washington  Yes / No
--
-- FRACTIONAL and P are 'partial' and NOT folded into either side — a
-- fractional homestead is a genuinely different situation and collapsing it
-- would hide that.
--
-- ============================================================
-- WHAT THIS CANNOT ANSWER
-- ============================================================
-- WHEN a redemption happens. Nothing in our data records it — redemption is
-- inferred from the ABSENCE of a post-expiry deed, and an absence has no
-- date. See STAGE2_SURVIVAL_FINDINGS_2026-08-23.md §6.
--
-- Time to FORECLOSURE SALE is recoverable (119 dated events) and is the
-- survival model's target. This view answers WHETHER, not WHEN.
-- ============================================================

CREATE OR REPLACE VIEW scoring.redemption_rates AS
WITH base AS (
  SELECT t.id,
         t.county_code,
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
  -- LEFT join, not inner (fixed 2026-08-24 the same day this view shipped).
  --
  -- An inner join silently dropped 27 of 252 resolved rows -- 11% of the
  -- population -- and pulled the base rate from 36.5% to 40.9%. Every one is
  -- a washington row keyed 'WASHINGTON-FC-0202821420118': a valid 13-digit
  -- PIN behind a stub prefix, which outcome_checker.py learned to read
  -- through on 2026-08-24 but core.parcels joins still cannot.
  --
  -- Those rows have a REAL outcome. They belong in the base rate and in the
  -- county cut. They simply cannot contribute to homestead or bid_to_value,
  -- which need parcel data, and a LEFT join expresses exactly that: present
  -- in the population, absent from the cuts that need a parcel.
  --
  -- The permanent fix is re-keying those rows to their bare PINs. That is
  -- destructive and needs its own guard -- 18 of the washington stubs have no
  -- twin row and would be destroyed by a careless retirement, while 66 have a
  -- resolved twin and would duplicate. Until then, LEFT join.
  LEFT JOIN core.parcels p
         ON p.county_code = t.county_code
        AND p.parcel_id   = t.parcel_id
  LEFT JOIN signals.distress_events e ON e.id = t.source_id
  -- Resolved outcomes only. 'unknown' is an unresolved ladder, not a result,
  -- and 'pending' has not happened yet.
  WHERE t.outcome IN ('redeemed_by_owner','foreclosed_sold','foreclosed')
)
SELECT 'all'::text        AS scope,
       'all resolved windows'::text AS bucket,
       count(*)           AS n,
       count(*) FILTER (WHERE redeemed) AS redeemed,
       round(100.0 * count(*) FILTER (WHERE redeemed) / count(*), 1) AS redeem_pct
FROM base

UNION ALL
SELECT 'county', county_code, count(*), count(*) FILTER (WHERE redeemed),
       round(100.0 * count(*) FILTER (WHERE redeemed) / count(*), 1)
FROM base GROUP BY county_code HAVING count(*) >= 20

UNION ALL
SELECT 'homestead', homestead, count(*), count(*) FILTER (WHERE redeemed),
       round(100.0 * count(*) FILTER (WHERE redeemed) / count(*), 1)
FROM base WHERE homestead IS NOT NULL
GROUP BY homestead HAVING count(*) >= 20

UNION ALL
SELECT 'buyer_type', buyer_type, count(*), count(*) FILTER (WHERE redeemed),
       round(100.0 * count(*) FILTER (WHERE redeemed) / count(*), 1)
FROM base WHERE buyer_type IS NOT NULL
GROUP BY buyer_type HAVING count(*) >= 15

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
       round(100.0 * count(*) FILTER (WHERE redeemed) / count(*), 1)
FROM base WHERE bid_to_value IS NOT NULL
GROUP BY bid_to_value HAVING count(*) >= 20;

-- ============================================================
-- VERIFY — a green CREATE is not evidence
-- ============================================================
-- Expected 2026-08-24 AFTER the LEFT join fix:
--   all           252 rows   36.5%   <- was 225 / 40.9% with an inner join
--   county        hennepin 121 (49.6%), washington 77, dakota 54 (29.6%)
--   homestead     homestead 140 (45.7%), non-homestead 85 (32.9%)
--   buyer_type    lender credit-bid 104 (47.1%), third-party buyer 17 (64.7%)
--   bid_to_value  50-80% 67 (47.8%), 80%+ 29 (34.5%), under 50% 25 (72.0%)
--                 THREE buckets, no '(no bid data)' row. If a fourth appears
--                 the null exclusion reverted.
--
-- The 'all' row MUST read 252. If it reads 225 the join reverted to inner and
-- 27 washington rows with real outcomes are being dropped from the base rate.
--
-- washington's county n rises from 50 to 77 for the same reason -- those 27
-- rows were always washington, they were just invisible.
--
--   SELECT now() AS run_at, scope, bucket, n, redeemed, redeem_pct
--   FROM scoring.redemption_rates ORDER BY scope, n DESC;
--
-- hennepin_parcels ran 2026-08-24 12:10 and took homestead from 5,578 to
-- 427,472 county-wide. The homestead cut here did NOT move: all 121 hennepin
-- tracker rows already carried a value. The gap was on the other 443,000
-- parcels nobody had foreclosed on.
