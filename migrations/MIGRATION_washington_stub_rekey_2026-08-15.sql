-- MIGRATION_washington_stub_rekey_2026-08-15.sql
--
-- Re-key 131 washington sheriff-sale events from a SYNTHETIC parcel stub to
-- the real parcel -- whose id is EMBEDDED IN THE STUB ID ITSELF.
--
-- ============================================================
-- WHAT A SYNTHETIC STUB IS, AND WHY IT IS WORSE THAN A NULL
-- ============================================================
-- core.parcels is the spine and a distress event must point at a parcel row
-- (foreign key). When a sheriff notice publishes no usable parcel number the
-- scraper MINTS a fake parcel -- 'WASHINGTON-FC-0103221230009' -- carrying an
-- address and nothing else, so the event can be stored at all. Dropping the
-- foreclosure would be worse.
--
-- But a stub fails SILENTLY where a null fails loudly:
--   * the join SUCCEEDS -- the row exists
--   * it returns no lat, no emv_total
--   * nothing errors, nothing logs, nothing alerts
--   * the product renders em-dashes as though the county published nothing
--
-- Measured: 138 washington sheriff_sale events sit on stubs with NULL lat.
-- That is 63% of all 209 coordinate-less parcels in a county that is 99.8%
-- geocoded -- distress does not cluster like that by chance.
--
-- ============================================================
-- THE SCRAPER ALREADY KNEW THE ANSWER
-- ============================================================
-- 'WASHINGTON-FC-0103221230009' minus the prefix is '0103221230009', a real
-- washington parcel id. The scraper HAD the id -- it built the stub out of it
-- -- and then wrote a fake parcel instead of pointing at the real one.
--
-- Same shape as hennepin, where the real PIN sat unused in
-- raw_data.detail.gis_pid (MIGRATION_hennepin_gis_pid_rekey_2026-08-15).
--
-- Measured before writing:
--   138 stub events, 131 resolve to a real parcel
--   131 of those carry BOTH coordinates AND a market value
--     -> 131 properties gain a photograph AND deal math
--   7 do not resolve and are left alone
--   tax_parcel_no is NULL on all 138 -- the embedded id is the only source
--
-- Collisions on (county_code, parcel_id, event_type, event_date, source),
-- both directions, both ZERO. Pure UPDATE: nothing merges, nothing deleted.
--
-- ============================================================
-- THIS IS HALF THE FIX
-- ============================================================
-- The washington sheriff scraper still mints stubs when it already holds the
-- real id. Without a scraper change the next run creates fresh ones -- exactly
-- as the emv_total backfill would have expired without the loader fix.
--
-- signals.distress_events has no updated_at, so provenance is stamped into
-- raw_data._rekey per row and the prior state is preserved wholesale.

BEGIN;

CREATE TABLE IF NOT EXISTS audit.washington_stub_rekey_20260815 AS
SELECT e.*,
       replace(e.parcel_id, 'WASHINGTON-FC-', '') AS intended_new_parcel_id,
       now() AS captured_at
FROM   signals.distress_events e
WHERE  e.county_code = 'washington'
  AND  e.parcel_id LIKE 'WASHINGTON-FC-%'
  AND  EXISTS (SELECT 1 FROM core.parcels r
                WHERE r.county_code = 'washington'
                  AND r.parcel_id = replace(e.parcel_id, 'WASHINGTON-FC-', ''));

UPDATE signals.distress_events e
SET    parcel_id = replace(e.parcel_id, 'WASHINGTON-FC-', ''),
       raw_data  = jsonb_set(
           coalesce(e.raw_data, '{}'::jsonb),
           '{_rekey}',
           jsonb_build_object(
               'migration', 'MIGRATION_washington_stub_rekey_2026-08-15',
               'at',        now(),
               'from',      e.parcel_id,
               'to',        replace(e.parcel_id, 'WASHINGTON-FC-', ''),
               'basis',     'real parcel id embedded in the stub id, verified against core.parcels'
           ),
           true)
WHERE  e.county_code = 'washington'
  AND  e.parcel_id LIKE 'WASHINGTON-FC-%'
  AND  EXISTS (SELECT 1 FROM core.parcels r
                WHERE r.county_code = 'washington'
                  AND r.parcel_id = replace(e.parcel_id, 'WASHINGTON-FC-', ''));

COMMIT;
