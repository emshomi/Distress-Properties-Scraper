-- MIGRATION_hennepin_gis_pid_rekey_2026-08-15.sql
--
-- Re-key 418 Hennepin sheriff-sale events from a SYNTHETIC parcel stub to the
-- real parcel id the scraper already captured.
--
-- ============================================================
-- WHAT IS WRONG
-- ============================================================
-- 607 hennepin sheriff_sale rows carry parcel_id 'HENNEPIN-FC-<source_id>',
-- minted when the sheriff notice published no PIN. 418 of them ALREADY hold
-- the real parcel id at raw_data->'detail'->>'gis_pid'; the scraper looked it
-- up, stored it, and nothing ever wrote it back to parcel_id.
--
-- A stub is worse than a null. It EXISTS in core.parcels, so every join
-- succeeds and returns a row with no lat, no lng and no value. Nothing fails
-- and nothing logs.
--
-- Measured 2026-08-15: all 418 gis_pids match a real core.parcels row, all 418
-- of those carry coordinates, 363 carry a market value.
--
-- ============================================================
-- WHY RE-KEY RATHER THAN KEEP RESOLVING AT READ TIME
-- ============================================================
-- _effective_parcel_id() in properties.py already translates these per
-- request, which is why the detail panel shows a photograph for a property the
-- table said had none. Correctness that depends on every reader remembering to
-- translate is not correctness — _apply_imagery_flags did not, and no Hennepin
-- row showed a camera glyph for as long as the feature existed.
--
-- The table should state the truth once.
--
-- ============================================================
-- SAFETY
-- ============================================================
-- Collisions measured BEFORE writing, both directions, both ZERO:
--   * against existing rows on (county_code, parcel_id, event_type,
--     event_date, source) -- 0
--   * within the re-key set itself                                 -- 0
-- So this is a pure UPDATE. Nothing merges, nothing is deleted, no event_value
-- is at risk. Contrast anoka, where 30 events cannot be re-keyed because a
-- pending and a completed sale share a date and the dedup key has no
-- event_subtype.
--
-- Addresses verified on a RANDOM sample of 15 (ORDER BY random(), never a
-- top-N slice -- that returns planner order, not a sample): 15 of 15 matched
-- the parcel address exactly, case and abbreviation aside.
--
-- ============================================================
-- PROVENANCE, BECAUSE THIS TABLE HAS NO updated_at
-- ============================================================
-- signals.distress_events records only observed_at, which means "when the
-- scraper saw it at source" and MUST NOT be touched here -- this migration did
-- not observe anything. So each row carries its own record of the change in
-- raw_data._rekey, and the prior state is preserved wholesale in a backup
-- table. 418 rows is small; the backup is the only way back.

BEGIN;

-- 1. Full prior state. Not a list of ids -- the whole row, so a revert needs
--    no reconstruction.
CREATE TABLE IF NOT EXISTS audit.hennepin_gis_pid_rekey_20260815 AS
SELECT e.*,
       e.raw_data #>> '{detail,gis_pid}' AS intended_new_parcel_id,
       now()                             AS captured_at
FROM   signals.distress_events e
WHERE  e.county_code = 'hennepin'
  AND  e.event_type  = 'sheriff_sale'
  AND  e.parcel_id LIKE 'HENNEPIN-FC-%'
  AND  e.raw_data #>> '{detail,gis_pid}' IS NOT NULL;

-- 2. Re-key, and stamp each row with what happened to it.
UPDATE signals.distress_events e
SET    parcel_id = e.raw_data #>> '{detail,gis_pid}',
       raw_data  = jsonb_set(
           e.raw_data,
           '{_rekey}',
           jsonb_build_object(
               'migration', 'MIGRATION_hennepin_gis_pid_rekey_2026-08-15',
               'at',        now(),
               'from',      e.parcel_id,
               'to',        e.raw_data #>> '{detail,gis_pid}',
               'basis',     'raw_data.detail.gis_pid, verified against core.parcels'
           ),
           true)
WHERE  e.county_code = 'hennepin'
  AND  e.event_type  = 'sheriff_sale'
  AND  e.parcel_id LIKE 'HENNEPIN-FC-%'
  AND  e.raw_data #>> '{detail,gis_pid}' IS NOT NULL
  -- Belt and braces: only re-key onto a parcel that actually exists. If the
  -- spine changed between the measurement and this run, those rows stay
  -- synthetic rather than pointing at nothing.
  AND  EXISTS (
         SELECT 1 FROM core.parcels p
         WHERE  p.county_code = e.county_code
           AND  p.parcel_id   = e.raw_data #>> '{detail,gis_pid}'
       );

COMMIT;

-- NOT DONE HERE, DELIBERATELY:
--   * The 418 HENNEPIN-FC-* rows in core.parcels are now orphans. Deleting
--     parcel rows is destructive and belongs in its own migration, verified on
--     its own. (ANOKA-FC-* orphans from 2026-08-14 are in the same state.)
--   * 189 hennepin sheriff_sale rows have NO gis_pid and stay synthetic.
--     Those need address matching -- the anoka problem, 59 of 107 last time.
