-- =====================================================================
-- audit.integrity_findings + audit.run_integrity_checks()
--
-- Written 2026-08-18 out of the 2026-08-17 session, in which four
-- scrapers were found minting ~534 synthetic parcel stubs A DAY against
-- a spine where almost all of them already existed. The accumulation had
-- been running for months and was invisible: the health digest counts
-- records_new, which counts the stubs themselves, so a scraper writing
-- 381 junk rows every morning reported as healthy.
--
-- === WHAT THIS IS AND IS NOT ===
--
-- DETECTION ONLY. This function writes to audit.integrity_findings and
-- touches nothing else. It never deletes, never re-points, never
-- "cleans up".
--
-- That is a deliberate limit, not an unfinished feature. On 2026-08-17
-- six stub populations were migrated by hand and the correct action
-- differed EVERY TIME on something no rule could have known in advance:
--
--   hennepin  14 imagery rows -> DELETE   (status=no_location, no pano)
--   dakota     2 imagery rows -> REPOINT  (status=ok, real pano_id)
--   anoka    172 imagery rows -> DELETE   (predicted REPOINT, was wrong)
--   washington 133 stubs      -> matched by PIN, not address
--   beltrami   1 stub         -> CORRECT, county genuinely has no spine
--   red lake / beltrami dups  -> keep the NEWER one / keep the OLDER one
--
-- An automated cleaner would have destroyed Dakota's Street View panos
-- and "fixed" Beltrami by deleting a legitimate row. core.parcel_imagery
-- is ON DELETE CASCADE, so that failure is silent: from the job's side
-- everything succeeded.
--
-- The second reason is subtler. Automating cleanup treats the symptom.
-- Had a nightly cleaner been running these past months the table would
-- have looked fine every morning and the 527-a-day minting would never
-- have been found. The stubs ACCUMULATING is what made the defect
-- visible. Keep it visible.
--
-- === CORRECTED TWICE ON 2026-08-18, THE SAME NIGHT IT SHIPPED ===
--
-- v1  grouped on the notice's gis_pid -> flagged every PACKAGE notice
--     as a duplicate. 9 false alerts on run 1, and it would have missed
--     both real duplicates.
-- v2  grouped on the parcel -> flagged the AUCTION LIFECYCLE (a pending
--     sale and its completed sale are two real facts about one
--     property). 57 groups, 39 of them false.
-- v3  grouped on parcel + event_subtype -> 18 groups, all genuine.
--
-- Recorded because the shape of the error repeated: each version was
-- checked against a population that did not contain the case it got
-- wrong. See check 3.
--
-- === HOW TO READ THE OUTPUT ===
--
-- Every check reports a count. The healthy state for all of them is
-- ZERO or a stable baseline. A number that MOVES is the signal; a number
-- that is merely large may be a known population (see check 2).
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. The findings table
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.integrity_findings (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_at       timestamptz NOT NULL DEFAULT now(),
    check_name   text        NOT NULL,
    severity     text        NOT NULL,
    county_code  text,
    n            bigint      NOT NULL,
    detail       jsonb,
    CONSTRAINT integrity_findings_severity_chk
        CHECK (severity IN ('info', 'warn', 'alert'))
);

CREATE INDEX IF NOT EXISTS idx_integrity_findings_run
    ON audit.integrity_findings (run_at DESC, check_name);

CREATE INDEX IF NOT EXISTS idx_integrity_findings_check
    ON audit.integrity_findings (check_name, run_at DESC);

COMMENT ON TABLE audit.integrity_findings IS
    'Nightly data-integrity detection. Written by audit.run_integrity_checks(). '
    'Detection only -- nothing here deletes or repairs. See the migration '
    'header for why cleanup is deliberately manual.';


-- ---------------------------------------------------------------------
-- 2. The check function
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.run_integrity_checks()
RETURNS TABLE (check_name text, severity text, n bigint, note text)
LANGUAGE plpgsql
AS $$
DECLARE
    v_run timestamptz := now();
BEGIN

-- === CHECK 1: STUBS MINTED TODAY, BY COUNTY AND WRITER =============
--
-- THE headline check. On 2026-08-17 this would have read
-- hennepin 381, dakota 129, anoka 17 -- every day, for months.
-- After the fix, four scrapers were re-run manually and all four
-- reported zero.
--
-- A stub is a core.parcels row whose parcel_id is a synthetic
-- '{COUNTY}-FC-...' key. It carries no market value, no coordinates,
-- no owner and no lot size -- the em-dash rows.
--
-- NOT every stub is a defect. Counties with no parcel spine at all
-- (beltrami holds exactly one row, and it is a stub) can only ever
-- produce synthetic keys. Watch the TREND per writer, not the total.
INSERT INTO audit.integrity_findings (run_at, check_name, severity, county_code, n, detail)
SELECT v_run,
       'stubs_minted_today',
       CASE WHEN COUNT(*) >= 20 THEN 'alert'
            WHEN COUNT(*) >= 5  THEN 'warn'
            ELSE 'info' END,
       p.county_code,
       COUNT(*),
       jsonb_build_object(
         'data_sources', p.data_sources,
         'sample_pid',   MIN(p.parcel_id),
         'first_at',     MIN(p.created_at),
         'last_at',      MAX(p.created_at))
FROM core.parcels p
WHERE p.parcel_id LIKE '%-FC-%'
  AND p.created_at >= CURRENT_DATE
GROUP BY p.county_code, p.data_sources
HAVING COUNT(*) > 0;


-- === CHECK 2: STUBS THAT WOULD NOW RESOLVE ==========================
--
-- A stub whose address matches exactly one real parcel is a migration
-- candidate: the property IS in the spine and the row is pointing at a
-- placeholder instead. 998 such rows were migrated on 2026-08-17.
--
-- This is a BASELINE check, not a zero check. A steady number means
-- known leftovers; a RISING number means something started minting
-- resolvable stubs again, which is the failure mode this whole system
-- exists to catch.
--
-- Capped at the counties with stubs to keep the scan bounded --
-- core.resolve_parcel_by_address does an unbounded candidate scan per
-- call, and in hennepin a house number can match 500+ parcels.
INSERT INTO audit.integrity_findings (run_at, check_name, severity, county_code, n, detail)
SELECT v_run,
       'stubs_resolvable',
       CASE WHEN COUNT(*) >= 50 THEN 'alert'
            WHEN COUNT(*) >= 10 THEN 'warn'
            ELSE 'info' END,
       s.county_code,
       COUNT(*),
       jsonb_build_object('sample_pid', MIN(s.parcel_id))
FROM (
    SELECT p.county_code, p.parcel_id, p.address
    FROM core.parcels p
    WHERE p.parcel_id LIKE '%-FC-%'
      AND p.address IS NOT NULL
) s
CROSS JOIN LATERAL (
    SELECT COUNT(*) AS hits
    FROM core.resolve_parcel_by_address(s.county_code, s.address)
) r
WHERE r.hits = 1
GROUP BY s.county_code;


-- === CHECK 3: TWO EVENTS, ONE PARCEL, ONE DATE, ONE SUBTYPE =========
--
-- A genuine duplicate is the SAME PROPERTY, SAME SALE, twice. Three
-- causes seen, all real:
--
--  (a) A PostgREST connection dropped mid-lookup, the resolver was
--      swallowing the exception, and the fallback minted a stub -- so
--      the idempotency guard (which keys on the EFFECTIVE parcel id)
--      checked a stub, found no events, and inserted a second row.
--      Red Lake 129 Bottineau: events 217046 and 217047, six seconds
--      apart, one on the real parcel and one on a stub.
--
--  (b) THE SYNTHETIC PID FORMAT CHANGED ON 2026-08-10, from
--      '{COUNTY}-FC-{source_id}' to '{COUNTY}-FC-{digits}-{sale_date}'.
--      Any stub minted BEFORE that date is invisible to the guard.
--      Beltrami extraction 528 duplicated event 123546 on 2026-08-18 for
--      exactly this reason -- while extraction 397, promoted 08-02 under
--      the old format, correctly matched and wrote nothing.
--      STILL LIVE for every pre-08-10 stub in the table.
--
--  (c) A county reissues a notice under a NEW detail id. Anoka parcel
--      313123440024, sale 2026-10-13: source_id 23665 observed 07-29 and
--      23849 observed 08-07. source_id is part of
--      distress_events_source_identity_key, so the constraint permits it.
--      A cross-source variant exists too -- anoka 063124210026 pairs
--      anoka_sheriff's '22677' with mnpublicnotice's '19-25-004802'.
--
-- === WHY event_subtype IS IN THE GROUPING ===
--
-- Without it this check flags the AUCTION LIFECYCLE as a duplicate.
-- Anoka publishes a sale in Pending Sales before it happens and in
-- Completed Sales after:
--
--   45353   pending_sale    source_id 23679         $124,880.44  high
--   187159  completed_sale  source_id 033324220004  NULL         low
--
-- Same parcel, same date -- and TWO REAL FACTS. The pending row carries
-- the amount due; the completed row starts the redemption clock. Losing
-- either is worse than holding both. (Note the completed list publishes
-- no detail id, so source_id falls back to the PIN.)
--
-- Measured 2026-08-18: 57 groups without event_subtype, 18 with it.
-- The 39 difference was entirely lifecycle pairs.
--
-- This is the same field that is load-bearing in the supersession
-- trigger, where omitting it collapsed dakota completed_sale_2025 and
-- completed_sale_2026 -- 60 chains, two different real sales of one
-- property.
--
-- === AND WHY NOT GROUPED ON THE NOTICE PID ===
--
-- An earlier version grouped on raw_data.detail.gis_pid and flagged
-- differing parcel_ids. That is the PACKAGE shape: washington
-- 26-003536FC lists THIRTEEN Forest Lake properties under one bid, each
-- correctly its own event. It fired 9 times on run 1, every one a false
-- positive, and would have missed both real duplicates -- Red Lake and
-- Beltrami each carried the same notice pid on both rows.
INSERT INTO audit.integrity_findings (run_at, check_name, severity, county_code, n, detail)
SELECT v_run,
       'duplicate_events_same_parcel',
       'alert',
       e.county_code,
       COUNT(*),
       jsonb_build_object(
         'parcel_id',     e.parcel_id,
         'event_date',    e.event_date,
         'event_subtype', e.event_subtype,
         'on_stub',       e.parcel_id LIKE '%-FC-%',
         'event_ids',     array_agg(e.id        ORDER BY e.id),
         'source_ids',    array_agg(e.source_id ORDER BY e.id),
         'sources',       array_agg(DISTINCT e.source))
FROM signals.distress_events e
WHERE e.event_type = 'sheriff_sale'
GROUP BY e.county_code, e.parcel_id, e.event_date, e.event_subtype
HAVING COUNT(*) > 1;


-- === CHECK 4: SHERIFF SALES WITH NO MATCHING EVENT ==================
--
-- A signals.sheriff_sales row whose parcel carries no sheriff_sale
-- event. The approve path writes BOTH inside one transaction, so a
-- mismatch means one leg was written or moved without the other --
-- historically, an event re-pointed to a real parcel while its sale
-- stayed on the stub.
--
-- Confirmed on 2026-08-17: 26 stubs across 13 counties held a sale and
-- ZERO events, left behind by an earlier migration. Previously logged
-- as '213 sheriff_sales vs 115 distress_events on stubs, unexplained'.
INSERT INTO audit.integrity_findings (run_at, check_name, severity, county_code, n, detail)
SELECT v_run,
       'sheriff_sales_without_event',
       CASE WHEN COUNT(*) >= 20 THEN 'alert' ELSE 'warn' END,
       s.county_code,
       COUNT(*),
       jsonb_build_object(
         'sample_parcel', MIN(s.parcel_id),
         'on_stub',       COUNT(*) FILTER (WHERE s.parcel_id LIKE '%-FC-%'))
FROM signals.sheriff_sales s
WHERE NOT EXISTS (
    SELECT 1 FROM signals.distress_events e
    WHERE e.county_code = s.county_code
      AND e.parcel_id   = s.parcel_id
      AND e.event_type  = 'sheriff_sale'
)
GROUP BY s.county_code;


-- === CHECK 5: IMAGERY FAILURE ROWS ON STUBS =========================
--
-- The imagery resolver runs daily against every parcel. A stub by
-- definition has no lat/lng, so it records 'no_location' /
-- 'parcel has no lat/lng' -- one row per source, forever, guaranteed
-- to fail. 204 such rows were deleted on 2026-08-17.
--
-- Pure waste, not corruption. But it rises in lockstep with stub
-- minting, so it is a second independent witness to check 1.
--
-- DO NOT let anything delete these automatically. Dakota's stubs DO
-- carry lat/lng from ArcGIS geometry, and two of its rows held a real
-- google_streetview pano_id. status='ok' rows are genuine images.
INSERT INTO audit.integrity_findings (run_at, check_name, severity, county_code, n, detail)
SELECT v_run,
       'imagery_failures_on_stubs',
       'info',
       i.county_code,
       COUNT(*),
       jsonb_build_object(
         'with_real_pano', COUNT(*) FILTER (WHERE i.pano_id IS NOT NULL),
         'status_ok',      COUNT(*) FILTER (WHERE i.status = 'ok'))
FROM core.parcel_imagery i
WHERE i.parcel_id LIKE '%-FC-%'
GROUP BY i.county_code;


-- === CHECK 6: EVENTS ON A PARCEL THAT DOES NOT EXIST ================
--
-- The composite FK (county_code, parcel_id) -> core.parcels should make
-- this impossible. It is checked anyway because a NULL county_code
-- leaves the FK UNENFORCED -- measured 2026-08-10, when 8 mnpublicnotice
-- rows carried NULL and pointed at nothing.
--
-- Any row here is a genuine referential break. Never expected.
INSERT INTO audit.integrity_findings (run_at, check_name, severity, county_code, n, detail)
SELECT v_run,
       'events_orphaned_from_parcels',
       'alert',
       COALESCE(e.county_code, '(null county_code)'),
       COUNT(*),
       jsonb_build_object('sample_event_id', MIN(e.id))
FROM signals.distress_events e
WHERE e.county_code IS NULL
   OR NOT EXISTS (
        SELECT 1 FROM core.parcels p
        WHERE p.county_code = e.county_code
          AND p.parcel_id   = e.parcel_id)
GROUP BY COALESCE(e.county_code, '(null county_code)');


-- === CHECK 7: EXTRACTIONS APPROVED BUT NEVER PROMOTED ===============
--
-- Before 2026-08-17 the approve path issued four independent PostgREST
-- writes with no transaction. A failure at the last one left the event
-- committed and the extraction still 'pending' -- the card reappeared,
-- the banner said "Try again", and the retry hit 23505 because the row
-- it was inserting already existed. The UI was driving the duplicate.
--
-- signals.promote_extraction() now does it in one plpgsql transaction,
-- so an approved row missing promoted_at should no longer occur.
INSERT INTO audit.integrity_findings (run_at, check_name, severity, county_code, n, detail)
SELECT v_run,
       'approved_never_promoted',
       'warn',
       NULL,
       COUNT(*),
       jsonb_build_object('sample_id', MIN(x.id))
FROM ai.extracted_foreclosures x
WHERE x.review_status = 'approved'
  AND x.promoted_at IS NULL
HAVING COUNT(*) > 0;


-- === CHECK 8: SOURCES THAT HAVE STOPPED PRODUCING ===================
--
-- Deliberately NOT the health digest's freshness rule, which as of
-- 2026-08-17 reached only 7 of 78 sources, counted FROZEN sources
-- inside its HEALTHY total, and reported saint_paul_vacant as FROZEN
-- at 392 days on a day it wrote 16 events.
--
-- This measures the thing that actually matters: when did this source
-- last WRITE AN EVENT. dakota_sheriff last wrote 2026-06-11 while
-- minting 129 parcels a day -- dead by this measure, healthy by
-- records_new.
INSERT INTO audit.integrity_findings (run_at, check_name, severity, county_code, n, detail)
SELECT v_run,
       'source_silent_days',
       CASE WHEN MAX(e.observed_at) < now() - interval '30 days' THEN 'alert'
            WHEN MAX(e.observed_at) < now() - interval '14 days' THEN 'warn'
            ELSE 'info' END,
       NULL,
       EXTRACT(DAY FROM now() - MAX(e.observed_at))::bigint,
       jsonb_build_object(
         'source',           e.source,
         'last_event_at',    MAX(e.observed_at),
         'latest_event_date',MAX(e.event_date),
         'events_total',     COUNT(*))
FROM signals.distress_events e
WHERE e.observed_at IS NOT NULL
GROUP BY e.source
HAVING MAX(e.observed_at) < now() - interval '14 days';


-- ---------------------------------------------------------------------
-- Return this run's findings, worst first.
-- ---------------------------------------------------------------------
RETURN QUERY
SELECT f.check_name,
       f.severity,
       f.n,
       COALESCE(f.county_code, f.detail->>'source', '') AS note
FROM audit.integrity_findings f
WHERE f.run_at = v_run
ORDER BY CASE f.severity
           WHEN 'alert' THEN 1
           WHEN 'warn'  THEN 2
           ELSE 3 END,
         f.n DESC,
         f.check_name;

END;
$$;

GRANT EXECUTE ON FUNCTION audit.run_integrity_checks() TO service_role;

COMMENT ON FUNCTION audit.run_integrity_checks() IS
    'Read-only integrity detection. Writes findings to '
    'audit.integrity_findings and returns this run''s rows. Never '
    'deletes or repairs anything -- see the migration header for why.';
