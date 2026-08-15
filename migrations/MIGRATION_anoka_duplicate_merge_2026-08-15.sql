-- MIGRATION_anoka_duplicate_merge_2026-08-15.sql
--
-- Delete 23 duplicate anoka_sheriff events sitting on ANOKA-FC-* stubs.
--
-- ============================================================
-- SAME MECHANISM AS HENNEPIN -- WE CAUSED IT
-- ============================================================
-- write_events_dedup keys ON CONFLICT on
--     (county_code, parcel_id, event_type, event_date, source)
-- which CONTAINS parcel_id, a column we deliberately rewrite.
--
-- The 2026-08-14 anoka re-key moved events off their ANOKA-FC-* placeholders
-- onto real parcels. The anoka scraper then ran at 12:00 on 2026-08-15,
-- regenerated the placeholder id for the same sales, found no conflict (the
-- original now carries a REAL parcel_id) and inserted 27 second copies.
--
-- Anoka runs DAILY at ~13:00 UTC. Ordinary days produce 1-4 events with zero
-- stubs; the two exceptions are 08-07 (20 stubs) and 08-15 (27 stubs), both
-- immediately after a re-key. Whatever is deleted here returns tomorrow unless
-- the dedup key changes first -- see step 3.
--
-- ============================================================
-- VERIFIED FIELD BY FIELD BEFORE DELETING
-- ============================================================
-- Across all 23 pairs:
--     event_value differs     0
--     event_subtype differs   0
--     severity differs        0
--     title differs           0
--     keys only in the stub   NONE (checked both directions, per pair)
--
-- 17 stubs have a LARGER raw_data than their twin, which looked like unique
-- data and was checked rather than assumed. The delta is EXACTLY 55 bytes on
-- every one -- a constant, so one shared field holding a longer value, not
-- extra content. jsonb_object_keys confirms identical key sets both ways.
--
-- This matters because on 2026-08-14, 2 of 60 anoka "duplicates" held the
-- BETTER data, including one where the survivor read $0.00. Same county, same
-- shape, opposite conclusion -- reached by measuring, not by pattern.
--
-- Unlike hennepin (71 stale severities and one county-corrected sale amount),
-- nothing here needs carrying across. A plain delete is correct.
--
-- ============================================================
-- STEP 2 OF 3
-- ============================================================
--   1. hennepin merge (done), anoka delete (this), mpls_311 (NOT resolved)
--   2. UNIQUE INDEX on (county_code, source, source_id, event_date)
--   3. event_writer.py on_conflict -> the new key
-- 3 is inert without 2; 2 cannot be created while violations remain.

BEGIN;

CREATE TABLE IF NOT EXISTS audit.anoka_duplicate_merge_20260815 AS
SELECT s.*, r.id AS surviving_id, now() AS captured_at
FROM   signals.distress_events s
JOIN   signals.distress_events r
       ON  r.county_code = s.county_code
       AND r.source      = s.source
       AND r.source_id   = s.source_id
       AND r.event_date IS NOT DISTINCT FROM s.event_date
       AND r.id <> s.id
       AND r.parcel_id NOT LIKE 'ANOKA-FC-%'
WHERE  s.source = 'anoka_sheriff'
  AND  s.parcel_id LIKE 'ANOKA-FC-%';

DELETE FROM signals.distress_events s
WHERE  s.source = 'anoka_sheriff'
  AND  s.parcel_id LIKE 'ANOKA-FC-%'
  AND  EXISTS (SELECT 1 FROM signals.distress_events r
                WHERE r.county_code = s.county_code
                  AND r.source      = s.source
                  AND r.source_id   = s.source_id
                  AND r.event_date IS NOT DISTINCT FROM s.event_date
                  AND r.id <> s.id
                  AND r.parcel_id NOT LIKE 'ANOKA-FC-%');

COMMIT;
