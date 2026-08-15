-- MIGRATION_dakota_street_type_rekey_2026-08-15.sql
--
-- Re-key 59 more dakota_sheriff events, matched on HOUSE NUMBER + STREET NAME
-- with the street TYPE ignored, bounded by the county's own point geometry.
--
-- Follows MIGRATION_dakota_address_rekey_2026-08-15.sql, which took the 86
-- that matched on a full normalised address. These are the ones that did not,
-- because the sheriff writes the street type out in full and the assessor
-- abbreviates it:
--     3724 BLACKHAWK LAKE COURT   -> 3724 BLACKHAWK LAKE CT
--     136 20TH AVENUE NORTH       -> 136 20TH AVE N
--     15767 FLACKWOOD TRAIL       -> 15767 FLACKWOOD TRL
--     7441 143RD ST COURT W       -> 7441 143RD STREET CT W
--
-- ============================================================
-- WHY NOT NEAREST CENTROID
-- ============================================================
-- Measured and rejected. Every distance looked convincing (4.7-29.6m) and
-- every STREET matched, but only 3 of 10 had the right HOUSE NUMBER:
--     34 MAPLE ISLAND ROAD -> nearest centroid was 36 (next door)
--     350 18TH AVENUE SOUTH -> nearest was 340
--     607 145TH ST E        -> nearest was 615
-- Suburban lots are 15-25m wide, so the nearest centroid is frequently the
-- neighbour. This match gets 34 MAPLE ISLAND ROAD -> 34 MAPLE ISLAND RD,
-- the correct house, because the HOUSE NUMBER decides and distance only
-- bounds.
--
-- Distance cannot be the discriminator in either direction: 34 MAPLE ISLAND
-- is 13.8m and WRONG by centroid; 19754 MEADOWLARK WAY is 213m and RIGHT by
-- address. Hence a loose 250m bound rather than a tight threshold.
--
-- ============================================================
-- CONDO PARCELS ARE EXCLUDED OUTRIGHT (r.address !~ '#')
-- ============================================================
-- Without that exclusion the match is meaningless for buildings: the sheriff
-- publishes only the building address, so '3155 COACHMAN ROAD' matches 112
-- unit parcels, '3425 GOLFVIEW DR' 72, '4110 RAHN RD' 72. Picking one would
-- attach a stranger's value -- the same units range $88,300 to $163,100.
--
-- Dakota's ArcGIS feed publishes no unit number and Dakota has no public
-- unit-level search endpoint of the kind Hennepin's PINS provides, so those
-- stay unresolved. That is the honest answer, not a gap to be filled.
--
-- (One exception spotted and NOT handled here: '1887 SILVER BELL ROAD 112'
-- carries its unit as a bare trailing number, and 1887 SILVER BELL RD #112
-- exists. A parser for that shape would recover a small number of rows.)
--
-- ============================================================
-- VERIFIED BEFORE WRITING
-- ============================================================
--     rows to write           59
--     distinct events         59   <- one candidate per event
--     ambiguous                0
--     collides with existing   0
--
-- 20 read individually: house number matches on every one, cities agree
-- (allowing 'South Saint Paul' vs 'South St Paul', the same municipality).
--
-- signals.distress_events has no updated_at, so provenance is stamped per row
-- and the prior state preserved wholesale.

BEGIN;

CREATE TABLE IF NOT EXISTS audit.dakota_street_type_rekey_20260815 AS
SELECT e.*, m.parcel_id AS intended_new_parcel_id, now() AS captured_at
FROM   signals.distress_events e
JOIN   core.parcels s
       ON s.county_code = e.county_code AND s.parcel_id = e.parcel_id
CROSS  JOIN LATERAL (
    SELECT r.parcel_id
    FROM   core.parcels r
    WHERE  r.county_code = 'dakota'
      AND  r.parcel_id NOT LIKE 'DAKOTA-FC-%'
      AND  r.emv_total IS NOT NULL
      AND  split_part(r.address, ' ', 1) = split_part(s.address, ' ', 1)
      AND  upper(split_part(r.address, ' ', 2)) = upper(split_part(s.address, ' ', 2))
      AND  r.address !~ '#'
      AND  ST_DWithin(r.geom,
             ST_SetSRID(ST_MakePoint(
               (e.raw_data #>> '{geometry,x}')::float8,
               (e.raw_data #>> '{geometry,y}')::float8), 4326)::geography, 250)
) m
WHERE  e.source = 'dakota_sheriff'
  AND  e.parcel_id LIKE 'DAKOTA-FC-%'
  AND  s.address IS NOT NULL;

UPDATE signals.distress_events e
SET    parcel_id = b.intended_new_parcel_id,
       raw_data  = jsonb_set(
           coalesce(e.raw_data, '{}'::jsonb),
           '{_rekey}',
           jsonb_build_object(
               'migration', 'MIGRATION_dakota_street_type_rekey_2026-08-15',
               'at',        now(),
               'from',      e.parcel_id,
               'to',        b.intended_new_parcel_id,
               'basis',     'house number + street name, street type ignored, within 250m of county point'
           ),
           true)
FROM   audit.dakota_street_type_rekey_20260815 b
WHERE  e.id = b.id
  AND  e.parcel_id LIKE 'DAKOTA-FC-%';

COMMIT;
