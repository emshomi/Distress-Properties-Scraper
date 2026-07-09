-- ============================================================
-- MIGRATION_vbr_synthetic_cleanup_2026-07-09.sql
-- Retire MPLS-VBR-* synthetic identities after the leading-zero fix
-- ============================================================
-- Context: parcel_id_normalizer now left-pads 12-digit Hennepin APNs
-- (deployed 2026-07-08). Run 260 (2026-07-09 12:00 UTC) inserted
-- brand-new correctly-keyed rows for all previously-synthetic records:
-- distress_events now has 309 real-PIN mpls_vbr parcels. The old
-- synthetic rows are stale duplicates — DELETE, not re-key (design
-- decision 2026-07-08: no UPDATE of signal identity).
--
-- Verified ground truth this migration relies on:
--   - 118 synthetic MPLS-VBR-* ids in distress_events (118 rows)
--   - 121 synthetic rows in vacant_registrations (118 + 3 NULL-date
--     echo pairs on parcels 159/162/164 — echoes exist because this
--     table's dedup index lacks NULLS NOT DISTINCT; the July-7
--     distress_events fix was never applied here)
--   - 118 stub rows in core.parcels (raw_data IS NULL)
--   - ZERO references in watchlists/alerts/scoring/outcomes/ai/
--     marketplace/owners/transactions (verified 2026-07-08)
--   - Data preserved: the new real-PIN rows carry the same snapshot
--     payload (event_date / date_entered_registry come from the data)
--
-- Step order matters: dedupe (step 3) MUST precede the unique index
-- rebuild (step 4) or CREATE UNIQUE INDEX fails on existing dupes.
-- Idempotent: every statement is safe to re-run.
-- ============================================================

-- ---- Step 1: delete synthetic signal rows -------------------------

DELETE FROM signals.distress_events
WHERE source = 'mpls_vbr'
  AND parcel_id LIKE 'MPLS-VBR-%';
-- expect: DELETE 118

DELETE FROM signals.vacant_registrations
WHERE parcel_id LIKE 'MPLS-VBR-%'
  AND raw_data->>'_source' = 'mpls_vbr';
-- expect: DELETE 121 (118 + the 3 echo duplicates)

-- ---- Step 2: retire the core.parcels stubs ------------------------

DELETE FROM core.parcels
WHERE county_code = 'hennepin'
  AND parcel_id LIKE 'MPLS-VBR-%'
  AND raw_data IS NULL;          -- belt and suspenders
-- expect: DELETE 118

-- ---- Step 3: dedupe vacant_registrations table-wide ---------------
-- Removes any (parcel_id, date_entered_registry) duplicates that
-- NULL-date rows may have created (keeps the lowest id per group).
-- Required before the NULLS NOT DISTINCT index can be built; also
-- covers the 3 formerly-synthetic parcels whose new real-PIN NULL-date
-- rows would otherwise start duplicating on tomorrow's 12:00 UTC run.

DELETE FROM signals.vacant_registrations v
USING signals.vacant_registrations keep
WHERE keep.parcel_id = v.parcel_id
  AND keep.date_entered_registry IS NOT DISTINCT FROM v.date_entered_registry
  AND keep.id < v.id;
-- expect: DELETE 0 or a small number (NULL-date dupes, if any)

-- ---- Step 4: harden the dedup rule (the missed July-7 fix) --------
-- vacant_registrations_dedup is a UNIQUE CONSTRAINT (it owns its
-- index), so it must be dropped/recreated as a constraint. Recreating
-- it as a constraint (not a bare index) also keeps PostgREST's
-- on_conflict="parcel_id,date_entered_registry" working unchanged.

ALTER TABLE signals.vacant_registrations
    DROP CONSTRAINT IF EXISTS vacant_registrations_dedup;

ALTER TABLE signals.vacant_registrations
    ADD CONSTRAINT vacant_registrations_dedup
    UNIQUE NULLS NOT DISTINCT (parcel_id, date_entered_registry);

-- ============================================================
-- VERIFICATION (run after; expected values in comments)
-- ============================================================
-- V1: synthetic ids fully gone everywhere
-- SELECT
--   (SELECT COUNT(*) FROM signals.distress_events
--     WHERE parcel_id LIKE 'MPLS-VBR-%')  AS de_synthetic,   -- 0
--   (SELECT COUNT(*) FROM signals.vacant_registrations
--     WHERE parcel_id LIKE 'MPLS-VBR-%')  AS vr_synthetic,   -- 0
--   (SELECT COUNT(*) FROM core.parcels
--     WHERE parcel_id LIKE 'MPLS-VBR-%')  AS parcel_stubs;   -- 0
--
-- V2: Minneapolis fully real, joining, owner-covered
-- SELECT COUNT(*) AS mpls_parcels,                            -- 309
--        COUNT(p.parcel_id) AS join_core_parcels,             -- 308
--        COUNT(o.owner_name) AS with_assessor_owner           -- ~308
-- FROM (SELECT DISTINCT parcel_id FROM signals.distress_events
--       WHERE source = 'mpls_vbr') v
-- LEFT JOIN core.parcels p
--   ON p.parcel_id = v.parcel_id AND p.county_code = 'hennepin'
-- LEFT JOIN core.owners o
--   ON o.parcel_id = v.parcel_id AND o.source = 'hennepin_parcels';
--
-- V3: index is hardened
-- SELECT indexdef FROM pg_indexes
-- WHERE schemaname='signals' AND tablename='vacant_registrations'
--   AND indexname='vacant_registrations_dedup';
--   -- must contain: NULLS NOT DISTINCT
-- ============================================================

-- ============================================================
-- TASK LIST (2026-07-09 session — do in this order)
-- ============================================================
-- [ ] 1. Run this migration in the Supabase SQL Editor (one paste,
--        whole script; watch for the file-viewer title line).
--        Expected deletes: 118 / 121 / 118, then 0-ish, then index.
-- [ ] 2. Run V1, V2, V3 above (uncommented) and paste results to
--        Claude. Expect: 0/0/0 synthetics; 309 parcels, 308 joined,
--        ~308 with owner; index contains NULLS NOT DISTINCT.
-- [ ] 3. Commit this file to the repo as
--        migrations/MIGRATION_vbr_synthetic_cleanup_2026-07-09.sql
--        (and confirm yesterday's
--        migrations/MIGRATION_hennepin_owners_2026-07-09.sql is there).
-- [ ] 4. Upload to Claude: the backend file containing
--        _apply_assessor_owners (from the API repo) — Half B needs it
--        current-file-first before any code is written.
-- [ ] 5. Half B (Claude drafts after upload): assessor-current owner
--        beats the 2023 VBR snapshot for owner fields only ->
--        executed tests -> deploy green -> tier check:
--        powershell -ExecutionPolicy Bypass -File $HOME\govire-scrapers\qa-tier-check.ps1
--        -> vacant tab spot-check: 3511 LOGAN AVE N shows a CURRENT
--        owner (not 2023 "Santino J DeRose").
-- [ ] 6. Then: forfeit-land surfacing + forfeit date fix (scoped in
--        VBR_CLEANUP_BRIEFING_2026-07-09.md section 4).
-- ============================================================
