-- MIGRATION_distress_view_eff_pid_2026-08-15.sql
--
-- signals.distress_with_parcel: prefer a REAL stored parcel_id over
-- raw_data.detail.gis_pid when computing eff_parcel_id.
--
-- ============================================================
-- WHAT IS WRONG
-- ============================================================
-- The view computes
--     COALESCE((raw_data->'detail')->>'gis_pid', parcel_id) AS eff_parcel_id
-- so gis_pid wins UNCONDITIONALLY. That was correct while a sheriff row's
-- parcel_id was always a placeholder. It stopped being correct the moment we
-- began re-keying events onto real parcels.
--
-- MIGRATION_mnpublicnotice_pin_normalise_2026-08-15 normalised 151 events'
-- parcel_id from the notice's printed PIN to the county's own format
-- ('31.029.21.32.0142' -> '3102921320142'). raw_data.detail.gis_pid still holds
-- the punctuated form, so the view kept emitting it and every consumer kept
-- seeing a parcel id that does not exist:
--
--     event_parcel_id   3102921320142   (real, in core.parcels)
--     view eff_parcel_id 31.029.21.32.0142  (matches nothing)
--
-- The imagery resolver joins core.parcels ON eff_parcel_id, so all 151 were
-- dropped from the working set. A dry run reported "0 parcels would be
-- resolved" both BEFORE and AFTER the re-key -- the migration could not reach
-- it. That is why mnpublicnotice had a 27% photo rate against 85% elsewhere.
--
-- This is the SAME defect fixed in _effective_parcel_id() in properties.py
-- earlier today (2026-08-15): identical unconditional preference, identical
-- consequence. The view was not checked at the time.
--
-- ============================================================
-- BLAST RADIUS, MEASURED
-- ============================================================
-- Events where the view resolves differently from the events table:
--     mnpublicnotice     190, of which 151 have a REAL stored parcel_id
--     startribune_legal    1, stored is still a stub
--     dakota_sheriff       1, stored is still a stub
--
-- So this changes the answer for exactly 151 events and nothing else. Rows
-- still on a placeholder keep falling back to gis_pid, which remains correct
-- for them.
--
-- hennepin (418) and washington (131) do not appear because their re-keys wrote
-- parcel_id to the SAME value gis_pid already held.
--
-- ============================================================
-- THE RULE
-- ============================================================
-- A stored parcel_id that is NOT a synthetic placeholder is the most
-- authoritative thing we have: something resolved it and wrote it down.
-- gis_pid is the fallback for events still sitting on a placeholder.
--
-- '-FC-' is the placeholder marker: HENNEPIN-FC-2602022, ANOKA-FC-*,
-- DAKOTA-FC-*, WASHINGTON-FC-*.
--
-- The join predicate below MUST use the same expression as eff_parcel_id, or
-- the view would report one parcel and join to another.

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
    COALESCE(
      CASE WHEN de.parcel_id IS NOT NULL AND de.parcel_id !~ '-FC-'
           THEN de.parcel_id END,
      (de.raw_data -> 'detail'::text) ->> 'gis_pid'::text,
      de.parcel_id) AS eff_parcel_id,
    COALESCE(de.county_code, m.county_slug, regexp_replace(regexp_replace(regexp_replace(lower((de.raw_data -> 'detail'::text) ->> 'county'::text), '\s+count(y|ies)$'::text, ''::text), '[^a-z0-9]+'::text, '_'::text, 'g'::text), '^(st|saint)_louis$'::text, 'st_louis'::text)) AS county_slug,
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
    p.cooling,
    k.days_left AS redemption_days_left,
        CASE
            WHEN k.days_left IS NULL THEN 2
            WHEN k.days_left >= 0 THEN 0
            ELSE 1
        END AS redemption_sort_bucket,
    p.emv_total - de.event_value AS equity_spread
   FROM signals.distress_events de
     LEFT JOIN core.source_county_map m ON m.source = de.source
     LEFT JOIN core.parcels p ON p.county_code = COALESCE(de.county_code, m.county_slug, regexp_replace(regexp_replace(regexp_replace(lower((de.raw_data -> 'detail'::text) ->> 'county'::text), '\s+count(y|ies)$'::text, ''::text), '[^a-z0-9]+'::text, '_'::text, 'g'::text), '^(st|saint)_louis$'::text, 'st_louis'::text))
       AND p.parcel_id = COALESCE(
             CASE WHEN de.parcel_id IS NOT NULL AND de.parcel_id !~ '-FC-'
                  THEN de.parcel_id END,
             (de.raw_data -> 'detail'::text) ->> 'gis_pid'::text,
             de.parcel_id)
     LEFT JOIN signals.redemption_sort_key k ON k.id = de.id;
