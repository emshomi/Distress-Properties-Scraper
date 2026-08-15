-- MIGRATION_hennepin_duplicate_merge_2026-08-15.sql
--
-- Merge 373 duplicate hennepin_sheriff events, then delete the duplicates.
--
-- ============================================================
-- HOW THEY GOT HERE -- WE CAUSED THIS
-- ============================================================
-- write_events_dedup keys ON CONFLICT on
--     (county_code, parcel_id, event_type, event_date, source)
-- -- a key that CONTAINS parcel_id, the column we deliberately rewrite.
--
-- On 2026-08-15 a migration re-keyed 418 hennepin events off their
-- HENNEPIN-FC-* placeholders onto real parcels. The sheriff scraper then ran
-- at 11:18:42, regenerated the placeholder id for the same sales, found no
-- conflict (the original now carries a REAL parcel_id), and inserted 373
-- second copies.
--
-- Measured: all 373 have a twin with the same (county_code, source,
-- source_id) already on a real parcel. genuinely_new = 0.
--
-- This is why they render blank on screen: the duplicate sits on a stub with
-- no lat, no emv_total, no owner -- beside its complete twin.
--
-- ============================================================
-- WHY A MERGE AND NOT A DELETE
-- ============================================================
-- The newer rows are NOT strictly poorer. Field-by-field across all 373:
--     event_date differs      0
--     event_subtype differs   0
--     title differs           0
--     raw_data bigger on stub 0
--     severity differs       71   <-- newer is CURRENT, older is STALE
--     event_value differs     1   <-- the COUNTY changed the figure
--
-- SEVERITY: computed from the redemption window (open = high, expired = low).
-- All 71 are high -> low, sales from 2025-09-09 to 2026-07-10: windows that
-- were open in May have since closed. Zero go low -> high, which would have
-- meant a window reopening and needed explaining before being trusted.
--
-- EVENT_VALUE: sale 2605017, 2638 Queen Ave N. Old row $1,242,500, new row
-- $124,250 -- and BOTH match their own raw_data.finalBidAmount. Neither is a
-- parse error: HENNEPIN REVISED THE PUBLISHED FIGURE between 2026-05-30 and
-- 2026-08-15. $1.24M for a house on Queen Ave N was never plausible.
--
-- That is a finding beyond this row. write_events_dedup is ON CONFLICT DO
-- NOTHING, so a corrected amount can NEVER overwrite the original. The only
-- reason this one is visible is that the duplicate landed on a different
-- parcel_id and slipped past the dedup. Every other correction the county has
-- ever issued was silently discarded. Recorded as an open defect.
--
-- Deleting on a pattern would have thrown away 71 current severities and a
-- corrected sale amount. On 2026-08-14, 2 of 60 anoka "duplicates" held the
-- BETTER data, including one where the survivor read $0.00. Never delete on a
-- pattern; verify field by field first.
--
-- ============================================================
-- THIS IS STEP 2 OF 3
-- ============================================================
--   1. this merge                        (removes the key violations)
--   2. UNIQUE INDEX on (county_code, source, source_id, event_date)
--      -- source_id is the sheriff's own sale record number and does NOT
--         change when we re-key parcel_id. event_date stays in the key
--         because DAKOTA POSTPONEMENTS are real: 60 dakota violations, 120
--         rows, ALL with differing event_date. Dropping it would collapse a
--         rescheduled sale into its original and lose the new date.
--   3. event_writer.py on_conflict -> the new key
--
-- Order matters: 3 is inert without 2, and 2 cannot be created before 1.

BEGIN;

CREATE TABLE IF NOT EXISTS audit.hennepin_duplicate_merge_20260815 AS
SELECT s.*, r.id AS surviving_id, now() AS captured_at
FROM   signals.distress_events s
JOIN   signals.distress_events r
       ON  r.county_code = s.county_code
       AND r.source      = s.source
       AND r.source_id   = s.source_id
       AND r.id <> s.id
       AND r.parcel_id NOT LIKE 'HENNEPIN-FC-%'
WHERE  s.source = 'hennepin_sheriff'
  AND  s.parcel_id LIKE 'HENNEPIN-FC-%';

-- Carry the newer row's CURRENT values onto the survivor before deleting it.
UPDATE signals.distress_events r
SET    event_value = s.event_value,
       severity    = s.severity
FROM   signals.distress_events s
WHERE  r.county_code = s.county_code
  AND  r.source      = s.source
  AND  r.source_id   = s.source_id
  AND  r.id <> s.id
  AND  r.parcel_id NOT LIKE 'HENNEPIN-FC-%'
  AND  s.source = 'hennepin_sheriff'
  AND  s.parcel_id LIKE 'HENNEPIN-FC-%'
  AND  (r.event_value IS DISTINCT FROM s.event_value
        OR r.severity IS DISTINCT FROM s.severity);

DELETE FROM signals.distress_events s
WHERE  s.source = 'hennepin_sheriff'
  AND  s.parcel_id LIKE 'HENNEPIN-FC-%'
  AND  EXISTS (SELECT 1 FROM signals.distress_events r
                WHERE r.county_code = s.county_code
                  AND r.source      = s.source
                  AND r.source_id   = s.source_id
                  AND r.id <> s.id
                  AND r.parcel_id NOT LIKE 'HENNEPIN-FC-%');

COMMIT;
