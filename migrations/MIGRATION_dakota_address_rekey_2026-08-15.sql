-- MIGRATION_dakota_address_rekey_2026-08-15.sql
--
-- Re-key 86 dakota_sheriff events from a DAKOTA-FC-* placeholder onto the real
-- parcel, matched by normalised address and corroborated by the county's own
-- coordinates.
--
-- ============================================================
-- WHY EVERY DAKOTA EVENT IS ON A PLACEHOLDER
-- ============================================================
-- All 170 dakota_sheriff events sit on a stub, unlike hennepin (30%) or
-- washington. The reason is in the source: Dakota publishes its foreclosures
-- through an ArcGIS layer whose attributes are
--     CITYNAME, CRN, GeoAddress, GeoCity, Month, OBJECTID,
--     PersonIsBu, Recent, SaleAmount, SaleDate
-- There is NO parcel number anywhere in it. The scraper cannot key on what the
-- county does not publish, so it mints one and the event is orphaned by
-- construction.
--
-- ============================================================
-- WHAT WAS TRIED AND REJECTED
-- ============================================================
-- 1. POINT-IN-PARCEL. Dakota events DO carry geometry (a point, x/y in 4326),
--    so containment looked ideal. It is impossible: core.parcels.geom is
--    ST_Point on all 167,314 dakota rows -- the geom is DERIVED from lat/lng
--    by trg_parcels_set_geom, not imported as polygons. A point cannot contain
--    a point. This rules out point-in-parcel for the ENTIRE spine, not just
--    Dakota. (Note also that geom is geography, so ST_Contains needs a
--    ::geometry cast even to be callable.)
--
-- 2. NEAREST CENTROID. Measured on 10 random events: every distance was
--    4.7-29.6m and every STREET matched, which looks convincing. But only 3 of
--    10 had the right HOUSE NUMBER:
--        350 18TH AVENUE SOUTH -> nearest was 340
--        607 145TH ST E        -> nearest was 615
--        13838 GRANADA WAY     -> nearest was 13892
--        41 WILLOW ST          -> nearest was 37 WILLOW WAY
--    Suburban lots are 15-25m wide, so the nearest centroid is frequently NEXT
--    DOOR. Shipping this would have put a neighbour's market value and
--    photograph on roughly 70% of Dakota foreclosures, and it would have
--    looked right in every summary statistic.
--
-- ============================================================
-- WHAT IS USED: ADDRESS, WITH GEOMETRY AS CORROBORATION
-- ============================================================
-- Normalised exact address match (the same expression as
-- idx_parcels_norm_address, in the index's own operand order so it is
-- actually used):
--     86 of 170 match EXACTLY ONE parcel
--      0 ambiguous  -- no dakota address maps to two parcels
--     84 no match   -- abbreviation and street-type differences
--                      ('350 18TH AVENUE SOUTH' vs '350 18TH AVE S')
--
-- Geometry then CORROBORATES rather than decides: of the 86, 71 sit within 50m
-- of the county's own published point, average 33.6m.
--
-- The 15 beyond 50m were read individually rather than excluded by threshold.
-- ALL 15 have an identical address -- house number, street name and type.
-- Distance there is a centroid artefact (large lots, long driveways), not a
-- wrong property: 19754 MEADOWLARK WAY at 213.7m is the same address in the
-- same city. Two show a city difference (16919 EMBERS AVE Farmington/Lakeville,
-- 19722 CABRILLA WAY Farmington/Empire) which is postal city vs municipality on
-- adjoining boundaries, already a known issue in anoka.
--
-- So no distance gate is applied. A threshold would have discarded 15 correct
-- matches to guard against a failure mode that inspection shows is not present.
--
-- ============================================================
-- SAFETY
-- ============================================================
-- Collisions measured before writing, both ZERO:
--   * against existing rows on the new unique key
--     (county_code, source, source_id, event_date)          -- 0
--   * two re-keys landing on the same parcel+date+source     -- 0
-- Two events DO resolve to 16919 EMBERS AVE; they carry different source_ids
-- and stay distinct, which is correct -- two sales on one property.
--
-- signals.distress_events has no updated_at, so provenance is stamped per row
-- and the prior state preserved wholesale.

BEGIN;

CREATE TABLE IF NOT EXISTS audit.dakota_address_rekey_20260815 AS
SELECT e.*, m.parcel_id AS intended_new_parcel_id, now() AS captured_at
FROM   signals.distress_events e
JOIN   core.parcels s
       ON s.county_code = e.county_code AND s.parcel_id = e.parcel_id
JOIN   LATERAL (
    SELECT r.parcel_id
    FROM   core.parcels r
    WHERE  r.county_code = 'dakota'
      AND  upper(regexp_replace(r.address, '[^A-Za-z0-9]', '', 'g'))
           = upper(regexp_replace(s.address, '[^A-Za-z0-9]', '', 'g'))
      AND  r.parcel_id NOT LIKE 'DAKOTA-FC-%'
      AND  r.emv_total IS NOT NULL
    LIMIT  1
) m ON true
WHERE  e.source = 'dakota_sheriff'
  AND  e.parcel_id LIKE 'DAKOTA-FC-%'
  AND  s.address IS NOT NULL;

UPDATE signals.distress_events e
SET    parcel_id = b.intended_new_parcel_id,
       raw_data  = jsonb_set(
           coalesce(e.raw_data, '{}'::jsonb),
           '{_rekey}',
           jsonb_build_object(
               'migration', 'MIGRATION_dakota_address_rekey_2026-08-15',
               'at',        now(),
               'from',      e.parcel_id,
               'to',        b.intended_new_parcel_id,
               'basis',     'normalised address match, corroborated by county point geometry'
           ),
           true)
FROM   audit.dakota_address_rekey_20260815 b
WHERE  e.id = b.id
  AND  e.parcel_id LIKE 'DAKOTA-FC-%';

COMMIT;
