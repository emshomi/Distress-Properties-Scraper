-- MIGRATION_mnpublicnotice_pin_normalise_2026-08-15.sql
--
-- Re-key 151 mnpublicnotice sheriff_sale events whose parcel id differs from
-- the spine ONLY BY PUNCTUATION.
--
-- ============================================================
-- WHAT IS WRONG
-- ============================================================
-- signals.distress_with_parcel.eff_parcel_id for mnpublicnotice carries the PIN
-- AS THE NOTICE PRINTS IT. core.parcels stores each county's own format. 192 of
-- 247 mnpublicnotice sheriff_sale events pointed at a parcel id that does not
-- exist:
--     washington  02.028.21.41.0032   (118,595 parcels loaded)
--     st_louis    010-0735-00115      (184,571 loaded)
--     dakota      01-18064-03-030     (167,504 loaded)
--     anoka       06-31-24-21-0026    (139,425 loaded)
--
-- The counties ARE loaded. This is punctuation, not a spine gap.
--
-- Visible symptom: mnpublicnotice had 184 complete events but only 50 with a
-- photograph -- a 27% photo rate against 85% for hennepin and dakota. The
-- imagery resolver joins core.parcels on eff_parcel_id, so a non-existent id
-- drops the row from the working set entirely and it is never even considered.
-- A dry run reported "0 parcels would be resolved" while 148 sat waiting.
--
-- ============================================================
-- THE DIGIT LENGTHS DIFFER BY COUNTY -- DO NOT VALIDATE ON LENGTH
-- ============================================================
--     washington 13   st_louis 12   cass 9   steele 9   itasca 9
-- This is not one format with punctuation; it is each county's own scheme.
-- Stripping non-digits is the only safe normalisation.
--
-- ============================================================
-- CORROBORATED BY ADDRESS, WHICH THE MATCH NEVER USES
-- ============================================================
-- 14 of 15 sampled matched the notice's own address independently:
--     7774 Rimbley Road    -> 7774 RIMBLEY RD, CITY OF WOODBURY
--     1422 Forest Lane     -> 1422 Forest Lane          (rice)
--     5718 Wyoming Street  -> 5718 Wyoming Street       (st_louis)
--     911 Conway Street    -> 911 CONWAY ST             (ramsey)
--     2025 - 26th Avenue SW-> 2025 26th Avenue Southwest (cass)
-- The 15th (itasca 98-410-0830) has an incomplete parcel address ('Bridge
-- Street', no house number) so it cannot corroborate either way; the PIN is the
-- authority there.
--
-- ============================================================
-- OUR OWN STUBS MUST BE EXCLUDED
-- ============================================================
-- r.parcel_id NOT LIKE '%-FC-%' is NOT cosmetic. Sheriff stubs are minted FROM
-- the real PIN, so 'WASHINGTON-FC-1603221330060' strips to the same digits as
-- the real parcel '1603221330060'. Without the exclusion, source_id 25-118995
-- matched BOTH, the event would have been written twice, and the last write --
-- possibly the empty stub -- would have won. Caught because 'rows to write'
-- (152) exceeded 'distinct events' (151).
--
-- ============================================================
-- NOT FIXED HERE
-- ============================================================
-- 41 events remain unmatched, 12 of them MULTI-PIN notices where one notice
-- lists several parcels:
--     '2803220420005; 2803220340014; 2803220340015'
--     '32-412-0510 and 32-412-0503'
-- Stripping turns those into one 39-digit string that matches nothing. They
-- need a split on ';' / ',' / ' and ' AND a decision about which parcel the
-- event attaches to. Do not silently take the first.
--
-- One is genuinely short: washington '34.028.21.0064' -> 11 digits where that
-- county uses 13. The notice omits a segment.
--
-- THE SOURCE IS ALSO UNFIXED. The mnpublicnotice extractor still stores the
-- printed PIN, so every new notice repeats this. This migration is a repair,
-- not a fix.
--
-- ============================================================
-- VERIFIED BEFORE WRITING
-- ============================================================
--     rows to write           151
--     distinct events         151   <- one candidate per event
--     target has coords       151   -> 151 photographs become possible
--     target has value        134   -> 134 gain deal math
--     collides with existing    0

BEGIN;

CREATE TABLE IF NOT EXISTS audit.mnpn_pin_normalise_20260815 AS
SELECT e.*, m.parcel_id AS intended_new_parcel_id, now() AS captured_at
FROM   signals.distress_events e
JOIN   signals.distress_with_parcel d
       ON d.source = e.source AND d.source_id = e.source_id
LEFT   JOIN core.parcels p
       ON p.county_code = d.county_slug AND p.parcel_id = d.eff_parcel_id
CROSS  JOIN LATERAL (
    SELECT r.parcel_id
    FROM   core.parcels r
    WHERE  r.county_code = d.county_slug
      AND  regexp_replace(r.parcel_id, '[^0-9]', '', 'g')
           = regexp_replace(d.eff_parcel_id, '[^0-9]', '', 'g')
      AND  r.parcel_id NOT LIKE '%-FC-%'
) m
WHERE  e.source = 'mnpublicnotice'
  AND  e.event_type = 'sheriff_sale'
  AND  p.parcel_id IS NULL;

UPDATE signals.distress_events e
SET    parcel_id = b.intended_new_parcel_id,
       raw_data  = jsonb_set(
           coalesce(e.raw_data, '{}'::jsonb),
           '{_rekey}',
           jsonb_build_object(
               'migration', 'MIGRATION_mnpublicnotice_pin_normalise_2026-08-15',
               'at',        now(),
               'from',      e.parcel_id,
               'to',        b.intended_new_parcel_id,
               'basis',     'notice PIN normalised to digits only, matched against county spine'
           ),
           true)
FROM   audit.mnpn_pin_normalise_20260815 b
WHERE  e.id = b.id;

COMMIT;
