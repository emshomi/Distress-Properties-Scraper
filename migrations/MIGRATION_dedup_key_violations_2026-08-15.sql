-- MIGRATION_dedup_key_violations_2026-08-15.sql
--
-- Clear the last 46 rows blocking a unique index on
--     (county_code, source, source_id, event_date)
--
-- ============================================================
-- WHY THIS KEY
-- ============================================================
-- The current dedup key is
--     (county_code, parcel_id, event_type, event_date, source)
-- and it CONTAINS parcel_id -- a value WE generate and WE rewrite.
--
-- Every duplicate found today has the same cause: we re-key an event onto its
-- real parcel, the scraper regenerates the placeholder id, finds no conflict,
-- and inserts a second copy. Measured across three sources:
--     hennepin_sheriff  373 copies, inserted 2026-08-15 11:18 (merged)
--     anoka_sheriff      23 copies, inserted 2026-08-15 12:00 (deleted)
--     mnpublicnotice      1 copy  -- the SCRAPER's OWN id format changed
--         between 2026-08-08 and 2026-08-13: 'DAKOTA-FC-MN25506' became
--         'DAKOTA-FC-025372701220-2026-09-29'. Same notice, same $284,929.33,
--         same date, different generated string, no conflict.
--
-- source_id is the PUBLISHER's identifier -- a sheriff sale record number, a
-- 311 case number, a notice id. We do not mint it and we never rewrite it.
--
-- event_date STAYS in the key. Dakota postponements are real: measured
-- 2026-08-15, 60 dakota violations across 120 rows, ALL with differing
-- event_date. Dropping it would collapse a rescheduled sale into its original
-- and silently lose the new date -- the most time-sensitive field we publish.
--
-- ============================================================
-- mpls_311: 22 PAIRS, SURVIVOR CHOSEN ON EVIDENCE
-- ============================================================
-- Same case number, same date, same Violation_Case_Inspection_ID -- the city's
-- id for ONE physical inspection -- but two different APNs, in several cases
-- 5km apart. One inspection did not happen in two places.
--
-- The scraper is NOT at fault: it honours the city's own APN on all 1,343
-- events, and every assignment sits within 100m of the city's own published
-- coordinates (avg 1m, max 51m). The city published contradictory APNs for the
-- same inspection on different days.
--
-- Survivor = the row whose PARCEL address matches the violation's own address.
-- Independent of scrape date, and it splits all 22 pairs exactly one-and-one.
--
-- The comparison NORMALISES away non-alphanumerics. Without that, '1061 23RD
-- AVE SE' does not equal parcel '1061 23RD AVE S E' and the pair looks
-- undecidable -- the THIRD time 'S E' vs 'SE' has produced a false reading in
-- this codebase (see the 164 false mpls_311 violations of 2026-08-14).
--
-- SEPARATELY VERIFIED, NOT TOUCHED HERE: 203 mpls_311 events have an address
-- string disagreeing with their parcel. ALL 203 use the city's APN and ALL 203
-- sit within 100m of the city's coordinates. The assignment is right and the
-- address string is unreliable. Nothing to repair -- confirmed twice, once on
-- 2026-08-14 and again today.

BEGIN;

CREATE TABLE IF NOT EXISTS audit.dedup_key_violations_20260815 AS
SELECT e.*, now() AS captured_at
FROM   signals.distress_events e
WHERE  e.id IN (
    -- mpls_311: the copy whose parcel address does NOT corroborate
    SELECT x.id
    FROM   signals.distress_events x
    JOIN   core.parcels q
           ON q.county_code = x.county_code AND q.parcel_id = x.parcel_id
    WHERE  x.source = 'mpls_311'
      AND  regexp_replace(upper(q.address), '[^A-Z0-9]', '', 'g')
           IS DISTINCT FROM
           regexp_replace(upper(x.raw_data ->> 'address'), '[^A-Z0-9]', '', 'g')
      AND  EXISTS (SELECT 1 FROM signals.distress_events y
                    WHERE y.county_code = x.county_code
                      AND y.source      = x.source
                      AND y.source_id   = x.source_id
                      AND y.event_date IS NOT DISTINCT FROM x.event_date
                      AND y.id <> x.id)
    UNION ALL
    -- mnpublicnotice: the older copy, whose stub carries no real parcel id
    SELECT 192556
);

DELETE FROM signals.distress_events e
WHERE  e.id IN (SELECT id FROM audit.dedup_key_violations_20260815);

COMMIT;
