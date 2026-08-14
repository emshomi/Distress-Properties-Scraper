-- MIGRATION_anoka_synthetic_parcels_2026-08-14.sql
--
-- Repairs Anoka sheriff-sale events that carry a SYNTHETIC parcel_id
-- (ANOKA-FC-{file_number}) instead of a real Anoka PIN.
--
-- ============================================================
-- WHY THEY EXIST
-- ============================================================
-- Anoka's sheriff publishes each foreclosure across two pages. The LIST page
-- carries a parcel number (propid) only for COMPLETED sales. The DETAIL page
-- carries the same parcel number as `tax_parcel_no` — dashed, e.g.
-- "08-31-24-22-0220" — for PENDING sales.
--
-- src/scrapers/anoka_sheriff.py read only the list page's propid. A comment in
-- it asserted that "Pending Sales rows expose NO propid ... the PIN only
-- appears once a sale completes" and treated that as a limitation of the
-- source. The data disproves it: the scraper was ALREADY fetching and storing
-- tax_parcel_no, and the enrichment step was ALREADY using it to pull
-- gis_owner, gis_market_value and gis_homestead off the county parcel layer.
-- One inspected row carried gis_market_value 307700 — fetched via that PIN —
-- while its parcel_id stayed ANOKA-FC-23832.
--
-- So the PIN was present, stored, and proven to resolve. It simply never
-- reached the field that decides whether a property is linked to core.parcels.
-- Fixed in the scraper 2026-08-14; this migration repairs the rows already
-- written.
--
-- ============================================================
-- WHY THE DUPLICATES WERE INVISIBLE
-- ============================================================
-- distress_events_dedup_key is
--     (county_code, parcel_id, event_type, event_date, source)
-- A synthetic parcel_id NEVER collides with a real one, so the same
-- foreclosure could be stored twice — once resolved, once floating — and the
-- unique index had no way to object. source_id (the sheriff's own file
-- number) is NOT in the key.
--
-- ============================================================
-- WHAT THIS DOES, MEASURED 2026-08-14
-- ============================================================
-- 181 Anoka events carry a synthetic parcel_id. 74 of them have a
-- tax_parcel_no; all 74 are exactly 12 digits once punctuation is stripped;
-- 73 match an existing (anoka, parcel_id) in core.parcels.
--
--   58  duplicate of an already-resolved row, IDENTICAL amount  -> DELETE
--    2  duplicate, but the synthetic holds the BETTER amount    -> COPY then DELETE
--   11  no duplicate, PIN resolves, unique target               -> RE-KEY
--    2  two file numbers for ONE foreclosure                    -> RE-KEY one, DELETE other
--    1  PIN is well-formed but matches no parcel                -> LEAVE synthetic
--  107  no tax_parcel_no anywhere in the row                    -> OUT OF SCOPE
--
-- The 61 deletions are all provably the same foreclosure as a surviving row:
-- same address, same sale date, same subtype, and same amount to the cent on
-- 58 of them. Verified field by field, not by rule.
--
-- ============================================================
-- WHY THE 2 AMOUNT MISMATCHES ARE COPIED, NOT IGNORED
-- ============================================================
-- In BOTH cases the synthetic row holds the better figure:
--   15689 Yellow Pine St NW, Andover — synthetic $377,579.87 vs resolved $0.00
--   4328 4th St NE, Columbia Heights — $257,691.40 vs $257,691.41
-- A foreclosure with $0 due is a failed parse, not a fact. Deleting the
-- synthetic first would leave a live Andover foreclosure showing zero owed and
-- an equity spread computed from it. So the value moves across FIRST.
--
-- ============================================================
-- WHAT IS DELIBERATELY LEFT ALONE
-- ============================================================
-- The 1 unmatched PIN stays synthetic. A real-looking parcel_id that matches
-- nothing is WORSE than a synthetic one: the synthetic is honestly
-- unresolvable, the other is a silent dead end.
--
-- The 107 without a PIN are untouched. Their fix is unknown — the detail page
-- may not have been fetched, or the county may not publish one — and nothing
-- gets changed here that has not been diagnosed.
--
-- ============================================================
-- SAFETY
-- ============================================================
-- No foreign keys reference signals.distress_events (checked: zero rows in
-- pg_constraint contype='f' confrelid=distress_events). No table anywhere
-- carries a distress_event_id / event_id column (checked across all schemas;
-- the only match was billing.webhook_events.stripe_event_id, unrelated).
-- So these deletes orphan nothing.
--
-- Reversible in practice: the scraper rebuilds events from the source on every
-- run, and the scraper fix is already deployed — so anything wrongly removed
-- returns on the next Anoka scrape, this time with a real parcel_id.

-- ============================================================
-- STEP 0 — PRE-FLIGHT. Run this FIRST, on its own, and read it.
-- Nothing below should run until these numbers match the header:
--   to_delete_identical 58 | to_delete_better_amount 2 |
--   to_rekey 11 | converging_pair 2
-- ============================================================
WITH candidate AS (
  SELECT e.id,
         e.parcel_id                                            AS old_pid,
         regexp_replace(e.raw_data->'detail'->>'tax_parcel_no',
                        '\D', '', 'g')                          AS new_pid,
         e.event_type, e.event_date, e.source, e.event_value
  FROM   signals.distress_events e
  WHERE  e.county_code = 'anoka'
    AND  e.parcel_id LIKE '%-FC-%'
    AND  length(regexp_replace(
           coalesce(e.raw_data->'detail'->>'tax_parcel_no',''),
           '\D', '', 'g')) = 12
)
SELECT now()                                                     AS run_at,
       count(*)                                                  AS candidates,
       count(*) FILTER (WHERE tw.id IS NOT NULL
                          AND c.event_value IS NOT DISTINCT FROM tw.event_value)
                                                                 AS to_delete_identical,
       count(*) FILTER (WHERE tw.id IS NOT NULL
                          AND c.event_value IS DISTINCT FROM tw.event_value)
                                                                 AS to_delete_better_amount,
       count(*) FILTER (WHERE tw.id IS NULL AND sp.parcel_id IS NOT NULL)
                                                                 AS to_rekey_incl_pair,
       count(*) FILTER (WHERE tw.id IS NULL AND sp.parcel_id IS NULL)
                                                                 AS leave_alone_no_spine
FROM   candidate c
LEFT   JOIN signals.distress_events tw
       ON  tw.county_code = 'anoka'
       AND tw.parcel_id   = c.new_pid
       AND tw.event_type  = c.event_type
       AND tw.event_date  = c.event_date
       AND tw.source      = c.source
       AND tw.id         <> c.id
LEFT   JOIN core.parcels sp
       ON  sp.county_code = 'anoka'
       AND sp.parcel_id   = c.new_pid;


-- ============================================================
-- STEP 1 — THE REPAIR. One transaction; all of it or none of it.
-- ============================================================
BEGIN;

-- 1a. Move the better amount onto the surviving row BEFORE its duplicate is
--     deleted. Two rows. Without this, deleting first loses $377,579.87 and
--     leaves $0.00 in its place.
UPDATE signals.distress_events tw
SET    event_value = c.event_value,
       updated_at  = now()
FROM  (SELECT e.id, e.parcel_id,
              regexp_replace(e.raw_data->'detail'->>'tax_parcel_no',
                             '\D', '', 'g')                      AS new_pid,
              e.event_type, e.event_date, e.source, e.event_value
       FROM   signals.distress_events e
       WHERE  e.county_code = 'anoka'
         AND  e.parcel_id LIKE '%-FC-%'
         AND  length(regexp_replace(
                coalesce(e.raw_data->'detail'->>'tax_parcel_no',''),
                '\D', '', 'g')) = 12) c
WHERE  tw.county_code = 'anoka'
  AND  tw.parcel_id   = c.new_pid
  AND  tw.event_type  = c.event_type
  AND  tw.event_date  = c.event_date
  AND  tw.source      = c.source
  AND  tw.id         <> c.id
  AND  tw.event_value IS DISTINCT FROM c.event_value;

-- 1b. Delete the converging duplicate FIRST, before the re-key in 1d.
--     ANOKA-FC-23704 and ANOKA-FC-23862 are one foreclosure published under
--     two file numbers: same property, same date 2026-11-05, same amount
--     $85,090.81, same "Postponed" status, observed 24 MICROSECONDS apart.
--     Both would re-key to 123124120041 and collide with each other — the
--     twin check cannot see two CANDIDATES converging, only a candidate
--     hitting an existing row. Keep the higher/later file number.
DELETE FROM signals.distress_events
WHERE  county_code = 'anoka'
  AND  parcel_id   = 'ANOKA-FC-23704';

-- 1c. Delete every synthetic row whose foreclosure already exists resolved.
--     60 rows (58 identical + the 2 whose amount was just copied across).
DELETE FROM signals.distress_events d
USING (SELECT e.id,
              regexp_replace(e.raw_data->'detail'->>'tax_parcel_no',
                             '\D', '', 'g')                      AS new_pid,
              e.event_type, e.event_date, e.source
       FROM   signals.distress_events e
       WHERE  e.county_code = 'anoka'
         AND  e.parcel_id LIKE '%-FC-%'
         AND  length(regexp_replace(
                coalesce(e.raw_data->'detail'->>'tax_parcel_no',''),
                '\D', '', 'g')) = 12) c
WHERE  d.id = c.id
  AND  EXISTS (SELECT 1 FROM signals.distress_events tw
               WHERE  tw.county_code = 'anoka'
                 AND  tw.parcel_id   = c.new_pid
                 AND  tw.event_type  = c.event_type
                 AND  tw.event_date  = c.event_date
                 AND  tw.source      = c.source
                 AND  tw.id         <> c.id);

-- 1d. Re-key the survivors that have no duplicate. 12 rows (11 + the kept
--     half of the converging pair). Each gains a real parcel link, and with
--     it the address, market value, deal math, map and imagery that a
--     synthetic id can never reach.
--
--     The core.parcels join is the guard: a PIN that matches no parcel is
--     left synthetic on purpose. A real-looking parcel_id pointing at nothing
--     is a silent dead end; a synthetic one is honestly unresolvable.
UPDATE signals.distress_events d
SET    parcel_id  = c.new_pid,
       updated_at = now()
FROM  (SELECT e.id,
              regexp_replace(e.raw_data->'detail'->>'tax_parcel_no',
                             '\D', '', 'g')                      AS new_pid,
              e.event_type, e.event_date, e.source
       FROM   signals.distress_events e
       WHERE  e.county_code = 'anoka'
         AND  e.parcel_id LIKE '%-FC-%'
         AND  length(regexp_replace(
                coalesce(e.raw_data->'detail'->>'tax_parcel_no',''),
                '\D', '', 'g')) = 12) c
JOIN   core.parcels sp
       ON  sp.county_code = 'anoka'
       AND sp.parcel_id   = c.new_pid
WHERE  d.id = c.id;

COMMIT;


-- ============================================================
-- STEP 2 — VERIFY. Run separately. The transaction reporting success is
-- not evidence; the row counts are.
-- ============================================================
SELECT now()                                                     AS run_at,
       count(*)                                                  AS anoka_events,
       count(*) FILTER (WHERE parcel_id LIKE '%-FC-%')            AS still_synthetic,
       count(*) FILTER (WHERE parcel_id LIKE '%-FC-%'
                          AND raw_data->'detail'->>'tax_parcel_no' IS NOT NULL)
                                                                 AS synthetic_with_pin,
       count(*) FILTER (WHERE parcel_id NOT LIKE '%-FC-%')        AS resolved
FROM   signals.distress_events
WHERE  county_code = 'anoka';

-- EXPECTED after the repair:
--   anoka_events        401  (462 - 61)
--   still_synthetic     120  (181 - 61)
--   synthetic_with_pin    1  (the one whose PIN matches no parcel)
--   resolved            281  + 12 re-keyed = 293
--
-- If synthetic_with_pin is anything other than 1, stop: a row that should
-- have been re-keyed or deleted was not, and the reason matters more than
-- the count.
