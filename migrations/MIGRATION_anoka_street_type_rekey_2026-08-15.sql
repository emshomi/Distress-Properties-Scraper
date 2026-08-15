-- MIGRATION_anoka_street_type_rekey_2026-08-15.sql
--
-- Re-key anoka_sheriff events on HOUSE NUMBER + STREET NAME, street type
-- ignored. Follows MIGRATION_anoka_embedded_pin_rekey_2026-08-15, which took
-- the 19 whose real PIN was embedded in the placeholder id.
--
-- ============================================================
-- WHY AN EXACT ADDRESS MATCH FAILS
-- ============================================================
-- The sheriff abbreviates, the assessor expands:
--     11136 Cottonwood Street NW  <->  11136 Cottonwood Street Northwest
--     15750 Lithium St NW         <->  15750 Lithium Street Northwest
--     171 Rice Creek Terrace NE   <->  171 Rice Creek Terrace Northeast
-- Only 12 of 34 matched on a full normalised address. House number + street
-- name recovers the rest.
--
-- The HOUSE NUMBER is what keeps this honest. Nearest-centroid was measured on
-- dakota and picks the NEIGHBOUR (34 MAPLE ISLAND ROAD -> 36) on 7 of 10.
--
-- ============================================================
-- NO GEOMETRY TO CORROBORATE WITH
-- ============================================================
-- Dakota's equivalent bounded the match with ST_DWithin against the county's
-- own published point. ANOKA EVENTS CARRY NO POINT GEOMETRY, so house number +
-- street name + directional is the only evidence. That is thinner, so the 20
-- single-candidate matches were read individually: house number, street name
-- and directional agree on every one.
--
-- City differences are the postal-city-vs-municipality artefact already
-- recorded for anoka (Coon Rapids/Minneapolis, Ramsey/Anoka, Oak Grove/Bethel),
-- not different properties.
--
-- idx_parcels_house_street was created for this
-- (MIGRATION_parcels_house_street_index_2026-08-15). Without it the query
-- TIMED OUT: 34 events x 139,425 parcels, split_part() defeating every index.
--
-- ============================================================
-- SINGLE-CANDIDATE EVENTS ONLY -- THE FILTER IS NOT OPTIONAL
-- ============================================================
-- Measured: 63 candidate rows for 29 events. The fan-out is apartment
-- buildings, where a stub carrying the BUILDING address matches every unit:
--     12371 Alamo Circle NE Unit A -> county has Unit C and Unit D, no Unit A
--     5425 144th Way NW #20        -> county has Apartment 12 and 16, no #20
-- Those stay unresolved. Picking one would attach a neighbour's value.
--
-- Junk addresses are excluded too: one anoka stub reads ',' against a
-- $1,853,552.89 sale.
--
-- ============================================================
-- VERIFIED BEFORE WRITING
-- ============================================================
--     candidate rows           63
--     distinct events          29
--     single-candidate events  24   <- what this writes
--     ambiguous                 5   <- deliberately left alone
--     collides with existing    0

BEGIN;

CREATE TABLE IF NOT EXISTS audit.anoka_street_type_rekey_20260815 AS
SELECT e.*, m.parcel_id AS intended_new_parcel_id, now() AS captured_at
FROM   signals.distress_events e
JOIN   core.parcels s
       ON s.county_code = e.county_code AND s.parcel_id = e.parcel_id
CROSS  JOIN LATERAL (
    SELECT r.parcel_id
    FROM   core.parcels r
    WHERE  r.county_code = 'anoka'
      AND  split_part(r.address, ' ', 1) = split_part(s.address, ' ', 1)
      AND  upper(split_part(r.address, ' ', 2)) = upper(split_part(s.address, ' ', 2))
      AND  r.parcel_id NOT LIKE 'ANOKA-FC-%'
      AND  r.emv_total IS NOT NULL
      AND  r.address !~ '#'
) m
WHERE  e.source = 'anoka_sheriff'
  AND  e.parcel_id LIKE 'ANOKA-FC-%'
  AND  s.address IS NOT NULL
  AND  s.address ~ '[A-Za-z]'
  --
