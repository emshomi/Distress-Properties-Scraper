-- ============================================================================
-- MIGRATION: Olmsted parcels — backfill typed columns from raw_data
-- Date: 2026-07-14 (Supabase project zdqwigbssxhqzlveisdz)
-- ============================================================================
-- Problem: the olmsted_parcels ArcGIS loader wrote attributes ONLY to
-- raw_data (jsonb); typed columns were empty for all 75,039 rows, so the UI
-- shows em-dashes for market value etc. and equity math is impossible.
--
-- Key mapping (from live jsonb_object_keys inspection 2026-07-14 — values
-- are jsonb numbers, not strings):
--   EMVTotal   -> emv_total      (NULLIF 0: a $0 "market value" would poison
--                                 equity-spread math; 0 here means unassessed
--                                 or exempt, not worthless)
--   EMVLand    -> emv_land       (0 kept — meaningful for some parcels)
--   EMVBldg    -> emv_building   (0 kept — bare land truly has no building)
--   DeededSqFt -> lot_sqft       (fallback DeedAcres * 43560 when 0)
--   LivngUnits -> num_units
--   Class      -> use_class      (e.g. '2a ACTIVELY FARMING')
--   SchoolDist -> school_district
--   Class -> property_type: 'single_family' where the class DESCRIPTION
--     says so — Olmsted's dominant class is literally
--     '1a/4bb(1) RESIDENTIAL SINGLE UNIT' (54,060 parcels). LESSON
--     (2026-07-14): the first attempt gated on LivngUnits = 1, but
--     LivngUnits is UNPOPULATED in this layer (0 for every class, even
--     apartments) — the rule matched 0 rows. Never gate on a field without
--     checking its population first. 'RESIDENTIAL 1-3 UNITS' (978) stays
--     NULL on purpose: could be a triplex; a wrong label is worse than a
--     missing one. Existing values preserved via COALESCE.
--
-- NOT in this ArcGIS layer (stay NULL, honest em-dash): year_built,
-- annual_tax, building sqft, legal_description. Consequences: the homescout
-- query (year_built >= 2000) still excludes Olmsted after this backfill;
-- lot_sqft covers only ~24% (DeededSqFt/DeedAcres are 0 for most platted
-- city lots — Shape.STArea() could fill the rest but is approximate GIS
-- polygon area; deliberate backlog decision, not a silent mix-in).
--
-- Idempotent: re-running re-derives the same values from raw_data.
-- ============================================================================

UPDATE core.parcels
SET
    emv_total    = NULLIF((raw_data ->> 'EMVTotal')::numeric, 0),
    emv_land     = (raw_data ->> 'EMVLand')::numeric,
    emv_building = (raw_data ->> 'EMVBldg')::numeric,
    lot_sqft     = CASE
                     WHEN COALESCE((raw_data ->> 'DeededSqFt')::numeric, 0) > 0
                       THEN (raw_data ->> 'DeededSqFt')::numeric
                     WHEN COALESCE((raw_data ->> 'DeedAcres')::numeric, 0) > 0
                       THEN (raw_data ->> 'DeedAcres')::numeric * 43560
                     ELSE NULL
                   END,
    num_units    = (raw_data ->> 'LivngUnits')::integer,
    use_class    = NULLIF(raw_data ->> 'Class', ''),
    school_district = NULLIF(raw_data ->> 'SchoolDist', ''),
    property_type = COALESCE(
        property_type,
        CASE
          WHEN raw_data ->> 'Class' ILIKE '%RESIDENTIAL SINGLE UNIT%'
            THEN 'single_family'
          ELSE NULL
        END
    )
WHERE county_code = 'olmsted'
  AND raw_data IS NOT NULL;

-- ============================================================================
-- Verification (ACTUAL results at apply time, 2026-07-14):
--   with_emv 72,070 (2,969 NULLIF'd zero/exempt — by design)
--   with_lot 17,735 (~24%; see lot_sqft note above)
--   with_class 75,038 of 75,039 (one row without raw_data)
--   single_family 54,068 (54,060 from the class rule + 705-overlap-adjusted
--   pre-existing values)
--   Spot check 511531032399 (Schoenfelder): 576600 / 576600 / 0 /
--   3484800 / '2a ACTIVELY FARMING' — exact.
-- ============================================================================
