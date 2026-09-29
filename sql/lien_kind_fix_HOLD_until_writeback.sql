-- =====================================================================
-- lien_kind fix for scoring.redemption_features
-- Prepared 2026-09-29.  HOLD: apply together with the tracker write-back,
-- so the site's figures change once, with all corrections at the same time.
--
-- What changes: ONLY the lien_kind expression. Every other column is
-- copied unchanged from pg_get_viewdef() taken 2026-09-29, in the same
-- order, so CREATE OR REPLACE works and scoring.redemption_rates (which
-- depends on this view) keeps working.
--
-- New rule, first match wins:
--   1. Hennepin typeOfSale 'Assessment' or 'Association' -> association_lien
--   2. Hennepin typeOfSale 'Mortgage'                     -> mortgage
--   3. Washington sale.instrument_code AL/CIC/CID/DCR/AMD -> association_lien
--   4. Washington sale.instrument_code MTG                -> mortgage
--   5. the existing name pattern (unchanged)
--   6. NO type field at all (no typeOfSale, no instrument_code) and amount
--      owed under 12% of the property's value          -> association_lien
--   7. otherwise                                       -> mortgage
-- Judgment and Execution sales (Hennepin) fall to steps 5 and 7, as today.
--
-- Step 6 calibration (2026-09-29), on the 777 Hennepin + Washington rows whose
-- county labels the type: under 0.12 of value, 96 of 100 rows are association
-- liens (under 0.05: 52 of 53). 0.27 and over: 97% mortgage. 0.12-0.27 is
-- mixed (58% association) and is left as mortgage. The rule misses about a
-- third of association liens (those owing 12%+), so it under-detects.
-- Assumes amount_owed (event_value) means the same in counties without a type
-- field; Dakota 184 (deed-confirmed association at 0.113) supports this.
--
-- Expected effect (read-only preview, 2026-09-29):
--   Washington 38 rows mortgage -> association (AL 19, CIC 11, AMD 4, DCR 3, CID 1)
--   Hennepin   30 rows Assessment mortgage -> association
--   Hennepin    2 rows Association mortgage -> association
--   Hennepin    1 row  Mortgage association -> mortgage
--   Step 6: 49 more rows -> association (Dakota 36, Anoka 9, Washington 2
--           rows with no instrument, Scott 1, St. Louis 1)
--   association_lien total 99 -> 217; Hennepin 87 -> 118; Washington 4 -> 44;
--   Dakota 2 -> 38; Anoka 0 -> 9
-- =====================================================================

CREATE OR REPLACE VIEW scoring.redemption_features AS
 SELECT t.id AS tracker_id,
    t.county_code,
    t.parcel_id,
    t.anchor_date,
    t.redemption_expiry_date,
    t.redemption_period_months,
    t.period_source,
    t.anchor_type,
    t.outcome,
    t.outcome_event_date,
    t.outcome_event_date - t.redemption_expiry_date AS days_expiry_to_event,
    t.outcome_event_date - t.anchor_date AS days_anchor_to_event,
        CASE
            WHEN t.outcome = 'unknown'::text THEN t.redemption_expiry_date
            ELSE CURRENT_DATE
        END AS observation_end,
        CASE
            WHEN t.outcome = 'unknown'::text THEN t.redemption_expiry_date
            ELSE CURRENT_DATE
        END - t.anchor_date AS days_observed,
    t.outcome = 'unknown'::text AS outcome_ambiguous,
        CASE
            WHEN t.outcome = 'redeemed_by_owner'::text THEN 'owner_exit'::text
            WHEN t.outcome = 'foreclosed_sold'::text THEN 'foreclosure_sale'::text
            ELSE NULL::text
        END AS event_type,
    (t.outcome <> ALL (ARRAY['redeemed_by_owner'::text, 'foreclosed_sold'::text])) OR t.outcome_event_date IS NULL AS censored,
    p.emv_total,
    p.emv_year,
    p.sqft,
    p.lot_sqft,
    p.year_built,
    p.property_type,
    p.last_sale_price,
    p.last_sale_date,
        CASE
            WHEN p.homestead_status IS NULL THEN NULL::text
            WHEN upper(p.homestead_status) = ANY (ARRAY['Y'::text, 'YES'::text]) THEN 'homestead'::text
            WHEN upper(p.homestead_status) ~~ 'FULL HOMESTEAD%'::text THEN 'homestead'::text
            WHEN upper(p.homestead_status) ~~ '%VET HOMESTEAD%'::text THEN 'homestead'::text
            WHEN upper(p.homestead_status) = 'BLIND/DISABLED'::text THEN 'homestead'::text
            WHEN upper(p.homestead_status) = ANY (ARRAY['N'::text, 'NO'::text]) THEN 'non-homestead'::text
            WHEN upper(p.homestead_status) ~~ 'NON HOMESTEAD%'::text THEN 'non-homestead'::text
            WHEN upper(p.homestead_status) = ANY (ARRAY['P'::text, 'FRACTIONAL'::text]) THEN 'partial'::text
            ELSE NULL::text
        END AS homestead,
    e.event_value AS amount_owed,
    (e.raw_data ->> 'finalBidAmount'::text)::numeric AS final_bid,
        CASE
            WHEN ((e.raw_data ->> 'finalBidAmount'::text)::numeric) IS NULL OR p.emv_total IS NULL OR p.emv_total = 0::numeric THEN NULL::numeric
            ELSE round(((e.raw_data ->> 'finalBidAmount'::text)::numeric) / p.emv_total, 4)
        END AS bid_to_value,
        CASE
            WHEN p.last_sale_price IS NULL OR p.last_sale_price = 0::numeric OR p.emv_total IS NULL OR p.emv_total = 0::numeric THEN NULL::numeric
            ELSE round(p.last_sale_price / p.emv_total, 4)
        END AS paid_vs_value,
        CASE
            WHEN (e.raw_data ->> 'toWhomSold'::text) IS NULL THEN NULL::text
            WHEN outcomes.normalize_party_name(e.raw_data ->> 'toWhomSold'::text) = outcomes.normalize_party_name(e.raw_data ->> 'mortgagee'::text) THEN 'lender_credit_bid'::text
            ELSE 'third_party_buyer'::text
        END AS buyer_type,
    (e.raw_data ->> 'noticeOfIntent'::text)::boolean AS notice_of_intent,
    ((e.raw_data -> 'mortgagors'::text) -> 0) ->> 'display'::text AS mortgagor,
    e.source AS anchor_source,
        CASE
            -- 1-2. Hennepin: the county's own type of sale
            WHEN (e.raw_data ->> 'typeOfSale'::text) = ANY (ARRAY['Assessment'::text, 'Association'::text]) THEN 'association_lien'::text
            WHEN (e.raw_data ->> 'typeOfSale'::text) = 'Mortgage'::text THEN 'mortgage'::text
            -- 3-4. Washington: the instrument that was foreclosed
            WHEN ((e.raw_data -> 'sale'::text) ->> 'instrument_code'::text) = ANY (ARRAY['AL'::text, 'CIC'::text, 'CID'::text, 'DCR'::text, 'AMD'::text]) THEN 'association_lien'::text
            WHEN ((e.raw_data -> 'sale'::text) ->> 'instrument_code'::text) = 'MTG'::text THEN 'mortgage'::text
            -- 5. everything else: the name pattern, unchanged
            WHEN ((COALESCE(e.raw_data ->> 'mortgagee'::text, ''::text) || ' | '::text) || COALESCE(e.raw_data ->> 'toWhomSold'::text, ''::text)) ~* '(homeowner|home owner|condominium|townhome|villa|community association|owners association|master association)'::text AND ((COALESCE(e.raw_data ->> 'mortgagee'::text, ''::text) || ' | '::text) || COALESCE(e.raw_data ->> 'toWhomSold'::text, ''::text)) !~* '(national association|banking association|federal national|savings association)'::text THEN 'association_lien'::text
            -- 6. no type field at all: amount owed under 12% of value
            WHEN (e.raw_data ->> 'typeOfSale'::text) IS NULL
             AND ((e.raw_data -> 'sale'::text) ->> 'instrument_code'::text) IS NULL
             AND (e.event_value / NULLIF(p.emv_total, 0::numeric)) < 0.12 THEN 'association_lien'::text
            ELSE 'mortgage'::text
        END AS lien_kind
   FROM outcomes.redemption_tracker t
     LEFT JOIN core.parcels p ON p.county_code = t.county_code AND p.parcel_id = t.parcel_id
     LEFT JOIN signals.distress_events e ON e.id = t.source_id
  WHERE t.superseded_by IS NULL;

-- Check straight after applying. Expected: association_lien 217 in total;
-- hennepin 118, washington 44, dakota 38, anoka 9, ramsey 1, scott 1,
-- sherburne 1, st_louis 1, wright 4.
SELECT county_code, lien_kind, count(*) AS n
FROM scoring.redemption_features
WHERE anchor_type = 'sheriff_sale' AND lien_kind = 'association_lien'
GROUP BY 1, 2
UNION ALL
SELECT 'TOTAL', 'association_lien', count(*)
FROM scoring.redemption_features
WHERE anchor_type = 'sheriff_sale' AND lien_kind = 'association_lien'
ORDER BY 1;

-- If the numbers do not match, run lien_kind_fix_ROLLBACK.sql, which
-- restores the 2026-09-29 definition exactly.
