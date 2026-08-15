-- MIGRATION_anoka_embedded_pin_rekey_2026-08-15.sql
--
-- Re-key 19 anoka_sheriff events onto the real parcel whose id is EMBEDDED IN
-- THE STUB ID ITSELF.
--
-- ============================================================
-- THE SCRAPER ALREADY KNEW THE ANSWER
-- ============================================================
-- anoka_sheriff mints 'ANOKA-FC-<source_id>'. For some notices the source_id
-- is the sheriff's case number (23672, 23849) -- nothing to recover. For 19 of
-- them it is the county's own 12-digit PIN:
--
--     ANOKA-FC-263124420041  ->  263124420041  ->  1052 95th Lane Northwest
--     ANOKA-FC-123225340010  ->  123225340010  ->  16526 Argon Street Northwest
--
-- All 19 resolve to a real anoka parcel, and all 19 of those carry BOTH
-- coordinates and a market value. Same shape as
-- MIGRATION_washington_stub_rekey_2026-08-15, where the real id was likewise
-- embedded in the placeholder the scraper generated.
--
-- ============================================================
-- WHY address_agrees READS false ON ALL 19 AND IS NOT EVIDENCE
-- ============================================================
-- A character-exact normalised comparison of the stub address against the
-- parcel address fails on every row, because the stub ABBREVIATES and the
-- assessor EXPANDS:
--
--     8015 4TH AVE              <->  8015 4th Avenue
--     16526 ARGON ST NW         <->  16526 Argon Street Northwest
--     20042 WILD RICE DR NE     <->  20042 Wild Rice Drive Northeast
--     14731 COBALT ST NW UNIT 19<->  14731 Cobalt Street Northwest Apartment 19
--
-- Read as pairs, house number, street name and directional agree on all 19.
-- The exact test was too strict to corroborate anything; it is not a signal
-- against the match. The PIN is the authority here and the addresses were
-- inspected individually.
--
-- One of the 19 is a UNIT (14731 Cobalt St NW #19) that the address route
-- could never have resolved -- the embedded PIN carries the unit.
--
-- ============================================================
-- WHAT THIS DOES NOT COVER
-- ============================================================
-- 34 anoka stubs carry a case number rather than a PIN. Of those, 12 resolve on
-- an exact normalised address and 22 do not, differing by street type
-- ('Street Northeast' vs 'ST NE'). Those 22 need the looser house-number +
-- street-name match used for dakota -- which TIMED OUT here, because Anoka
-- events carry NO point geometry, so there is no ST_DWithin to narrow the
-- candidate set and split_part() defeats every index. That needs its own
-- index before it can run.
--
-- ============================================================
-- VERIFIED BEFORE WRITING
-- ============================================================
--     rows to write            19
--     distinct events          19   <- one target per event
--     collides with existing    0

BEGIN;

CREATE TABLE IF NOT EXISTS audit.anoka_embedded_pin_rekey_20260815 AS
SELECT e.*, replace(e.parcel_id, 'ANOKA-FC-', '') AS intended_new_parcel_id,
       now() AS captured_at
FROM   signals.distress_events e
WHERE  e.source = 'anoka_sheriff'
  AND  e.parcel_id LIKE 'ANOKA-FC-%'
  AND  replace(e.parcel_id, 'ANOKA-FC-', '') ~ '^\d{12}$'
  AND  EXISTS (SELECT 1 FROM core.parcels r
                WHERE r.county_code = 'anoka'
                  AND r.parcel_id = replace(e.parcel_id, 'ANOKA-FC-', ''));

UPDATE signals.distress_events e
SET    parcel_id = b.intended_new_parcel_id,
       raw_data  = jsonb_set(
           coalesce(e.raw_data, '{}'::jsonb),
           '{_rekey}',
           jsonb_build_object(
               'migration', 'MIGRATION_anoka_embedded_pin_rekey_2026-08-15',
               'at',        now(),
               'from',      e.parcel_id,
               'to',        b.intended_new_parcel_id,
               'basis',     'real 12-digit PIN embedded in the stub id, verified against core.parcels'
           ),
           true)
FROM   audit.anoka_embedded_pin_rekey_20260815 b
WHERE  e.id = b.id;

COMMIT;
