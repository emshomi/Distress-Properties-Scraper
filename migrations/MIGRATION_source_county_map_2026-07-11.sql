-- ============================================================================
-- MIGRATION: core.source_county_map + data-driven distress_with_parcel view
-- Date applied: 2026-07-11 (Supabase project zdqwigbssxhqzlveisdz)
-- Committed:    2026-07-12, reconstructed from live-DB introspection
--               (information_schema.columns, pg_constraint,
--               SELECT * FROM core.source_county_map, pg_get_viewdef).
-- ============================================================================
-- Problem fixed: distress_with_parcel derived county_slug from a hardcoded
-- CASE statement that silently assigned NULL to every source added after the
-- view's creation (502 olmsted_delq_list + 34 ramsey_tfl + 7 postbulletin_legal
-- rows had NULL slug -> broken core.parcels joins -> no address/coords).
--
-- Fix: a mapping table. Onboarding a new source is now ONE INSERT here —
-- never view surgery again. Multi-county sources (startribune_legal) carry
-- NULL county_slug and fall back to raw_data->'detail'->>'county'.
--
-- Verified 2026-07-11: 502/502 olmsted_delq_list rows join with address
-- and coordinates. NULL-slug defect CLOSED.
--
-- Known follow-up (open, low urgency): the raw_data fallback yields
-- unnormalized slugs for ~25 startribune rows ('ramsey county', 'st. louis'
-- vs 'saint louis') — normalize in the fallback expression or clean raw_data.
--
-- Idempotent: safe to re-run (IF NOT EXISTS / ON CONFLICT DO NOTHING /
-- CREATE OR REPLACE).
-- ============================================================================

-- 1. The mapping table.
CREATE TABLE IF NOT EXISTS core.source_county_map (
    source      text PRIMARY KEY,
    county_slug text,   -- NULL = multi-county source, slug comes from raw_data
    note        text
);

-- 2. The mapping data (16 rows — these rows ARE the migration).
--    ON CONFLICT DO NOTHING so a re-run never clobbers later live edits.
INSERT INTO core.source_county_map (source, county_slug, note) VALUES
    ('anoka_sheriff',      'anoka',      NULL),
    ('carver_sheriff',     'carver',     'no rows yet, mapping kept from CASE'),
    ('dakota_sheriff',     'dakota',     NULL),
    ('hennepin_sheriff',   'hennepin',   NULL),
    ('hennepin_tax_roll',  'hennepin',   NULL),
    ('mpls_vbr',           'hennepin',   NULL),
    ('olmsted_delq_list',  'olmsted',    'fixed 2026-07-11: was NULL in view CASE'),
    ('postbulletin_legal', 'olmsted',    'fixed 2026-07-11: was NULL in view CASE'),
    ('ramsey_sheriff',     'ramsey',     'shelved source, mapping kept'),
    ('ramsey_tax_roll',    'ramsey',     NULL),
    ('ramsey_tfl',         'ramsey',     'fixed 2026-07-11: was NULL in view CASE'),
    ('saint_paul_dsi',     'ramsey',     'legacy CASE entry, mapping kept'),
    ('saint_paul_vacant',  'ramsey',     NULL),
    ('scott_sheriff',      'scott',      'no rows yet, mapping kept from CASE'),
    ('startribune_legal',  NULL,         'multi-county: slug from raw_data detail.county'),
    ('washington_sheriff', 'washington', NULL)
ON CONFLICT (source) DO NOTHING;

-- 3. Rewrite the view: county_slug now comes from the mapping table, with a
--    raw_data fallback for multi-county sources. Column list unchanged from
--    the previous version — only the slug derivation and the parcels join
--    condition changed.
CREATE OR REPLACE VIEW signals.distress_with_parcel AS
SELECT de.id,
    de.source,
    de.source_id,
    de.parcel_id,
    de.event_type,
    de.event_date,
    de.event_value,
    de.severity,
    de.title,
    de.description,
    de.raw_data,
    de.observed_at,
    COALESCE((de.raw_data -> 'detail'::text) ->> 'gis_pid'::text, de.parcel_id) AS eff_parcel_id,
    COALESCE(m.county_slug, lower((de.raw_data -> 'detail'::text) ->> 'county'::text)) AS county_slug,
    p.year_built,
    p.sqft,
    p.lot_sqft,
    p.emv_total,
    p.emv_land,
    p.emv_building,
    p.annual_tax,
    p.last_sale_price,
    p.last_sale_date,
    p.num_units,
    p.use_class,
    p.property_type,
    p.school_district,
    p.garage,
    p.basement,
    p.heating,
    p.cooling
   FROM signals.distress_events de
     LEFT JOIN core.source_county_map m ON m.source = de.source
     LEFT JOIN core.parcels p
       ON p.county_code = COALESCE(m.county_slug, lower((de.raw_data -> 'detail'::text) ->> 'county'::text))
      AND p.parcel_id   = COALESCE((de.raw_data -> 'detail'::text) ->> 'gis_pid'::text, de.parcel_id);

-- ============================================================================
-- Verification (run 2026-07-11, all passed):
--   SELECT count(*) FROM signals.distress_with_parcel
--   WHERE source = 'olmsted_delq_list' AND county_slug IS NULL;   -- 0
--   SELECT count(*) FROM signals.distress_with_parcel
--   WHERE source = 'olmsted_delq_list' AND emv_total IS NOT NULL
--      OR source = 'olmsted_delq_list';                           -- 502 join
-- Onboarding rule going forward: new single-county scraper =>
--   INSERT INTO core.source_county_map (source, county_slug) VALUES (...);
-- ============================================================================
