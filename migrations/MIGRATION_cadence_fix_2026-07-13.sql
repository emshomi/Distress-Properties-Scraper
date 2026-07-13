-- ============================================================================
-- MIGRATION: source_health cadence corrections (monthly workflows + Tyler)
-- Date applied: 2026-07-13 (Supabase project zdqwigbssxhqzlveisdz)
-- ============================================================================
-- Root cause: three sources were registered with expected_interval_days = 7
-- while their GitHub Actions workflows are DELIBERATELY monthly (verified in
-- the workflow files themselves, both headed "Manual + monthly ... monthly is
-- plenty"):
--   * ramsey_parcels + ramsey_tax_roll — ramsey-tax.yml, cron '30 13 1 * *'
--     (1st of month). Jul 1 run #5 SUCCEEDED; the Jul 12/13 digests' "stale"
--     alerts were this misconfiguration, not failures.
--   * dakota_parcels — dakota-enrichment.yml, cron '0 14 2 * *' (2nd of
--     month). Jul 2 run #5 SUCCEEDED; same false alert from Jul 13.
--
-- Additionally olmsted_tax_detail had NO cadence row value (NULL -> falls
-- back to the flat HEALTH_STALE_DAYS default of 3), which would false-flag
-- it stale on days 4-7 of its weekly cycle (cron Tue 16:00 UTC). Set to 7.
--
-- Precedent: the hennepin_parcels=92 cadence row, treated as part of that
-- source's fix (see scripts/run_hennepin_parcels.py header).
--
-- Idempotent: plain UPDATE by source_name; re-running re-asserts the values.
-- ============================================================================

UPDATE audit.source_health
SET expected_interval_days = CASE source_name
      WHEN 'dakota_parcels'     THEN 31
      WHEN 'ramsey_parcels'     THEN 31
      WHEN 'ramsey_tax_roll'    THEN 31
      WHEN 'olmsted_tax_detail' THEN 7
    END,
    updated_at = now()
WHERE source_name IN ('dakota_parcels', 'ramsey_parcels',
                      'ramsey_tax_roll', 'olmsted_tax_detail');

-- ============================================================================
-- Verification (RETURNING output at apply time, 2026-07-13):
--   olmsted_tax_detail  7
--   dakota_parcels      31
--   ramsey_tax_roll     31
--   ramsey_parcels      31
-- Expected effect: next daily digest reads 18/18 healthy; monthlies stop
-- false-flagging mid-cycle; olmsted_tax_detail stops false-flagging on
-- days 4-7 after each weekly Tuesday run.
-- Known remaining gap (separate code fix, queued): washington_sheriff has
-- NO source_health row at all — its runner bypasses BaseScraper.run(), so
-- record_success never fires.
-- ============================================================================
