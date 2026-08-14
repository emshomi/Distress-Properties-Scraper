-- MIGRATION_anoka_address_rekey_2026-08-14.sql
--
-- Re-keys Anoka sheriff-sale events from a synthetic parcel_id
-- (ANOKA-FC-{file_number}) to the real Anoka parcel, matched by NORMALISED
-- ADDRESS. Deletes nothing. Changes one column on ~62 rows.
--
-- ============================================================
-- WHY THESE ARE SEPARATE FROM THE PIN REPAIR
-- ============================================================
-- MIGRATION_anoka_synthetic_parcels_2026-08-14.sql repaired 74 events that
-- carried a tax_parcel_no the scraper had fetched, stored, and never used.
-- These 108 are the remainder: the detail page WAS fetched for every one of
-- them (0 have a missing detail block, 88 carry an owner name), and Anoka
-- simply published no PIN. There is no unused identifier to recover here.
--
-- 107 of the 108 carry an address, so address matching is the only route —
-- and these are UPCOMING foreclosures currently showing no map, no imagery,
-- no valuation and no deal math.
--
-- ============================================================
-- WHY A NORMALISER, AFTER I SAID ONE WAS NOT NEEDED
-- ============================================================
-- A punctuation-and-case strip alone appeared to match 107 of 107. That was
-- an artifact: core.parcels CONTAINS the synthetic stubs these events created,
-- standing at the same addresses. Excluding stubs, only 44 matched. The other
-- 63 failed on abbreviation alone:
--
--   notice  10162 IBIS ST NW           spine  10162 Ibis Street Northwest
--   notice  1036 95TH LN NW            spine  1036 95th Lane Northwest
--   notice  1072 HACKMANN CIR NE       spine  1072 Hackmann Circle Northeast
--
-- The house numbers all existed in the spine (7-52 parcels share each), so it
-- was never a coverage gap. Normalising both sides took 44 -> 97 matches, with
-- worst_case = 1: no address matched two parcels, so no judgement call exists
-- anywhere in the set.
--
-- ============================================================
-- WHAT THE NORMALISER DELIBERATELY WILL NOT DO
-- ============================================================
-- Directions are stripped ONLY at end-of-string. Anoka has streets genuinely
-- named "North Park Drive" and "West River Road"; an unanchored replacement
-- would mangle them and attach foreclosures to the wrong houses.
--
-- Multi-address notices are excluded, not guessed. "101 CHARLES STREET NE,
-- 179 CHARLES STREET NE, 180 CHARLES STREET NE" is ONE foreclosure over THREE
-- parcels. No normalisation makes that a single match.
--
-- Unit-bearing addresses are excluded. Several condo units share one street
-- address; a unit-stripped match is a coin flip between them. Measured: 0 in
-- this batch, but the guard stays.
--
-- ============================================================
-- WHAT IS EXCLUDED FROM THE RE-KEY, AND WHY
-- ============================================================
-- 30 COLLIDE with an existing event on the dedup key. Inspected field by
-- field: these are NOT duplicates. Every one is a pending_sale whose target
-- parcel already carries a completed_sale for the SAME DATE — the sale was
-- scheduled and then happened, two genuine records of one foreclosure's
-- lifecycle. Only 1 of 30 shares a title, 1 a subtype.
--
--   1020 95TH LANE NW      pending $173,140.73  |  completed  amount NULL
--   171 RICE CREEK TER NE  pending $388,215.93  |  completed  amount NULL
--   15750 LITHIUM ST NW    pending $353,210.01  |  completed  amount NULL
--
-- distress_events_dedup_key is (county_code, parcel_id, event_type,
-- event_date, source) — event_subtype is NOT in it, so a pending and a
-- completed sale on the same date cannot both exist against one parcel.
-- Re-keying these would either fail the unique index or, if forced, destroy a
-- real event. They stay synthetic.
--
-- KNOWN COST, RECORDED RATHER THAN HIDDEN: those 30 properties display the
-- completed sale (which carries NO amount) while the amount due sits in the
-- synthetic pending row, invisible. Fixing that means changing the dedup key
-- to include event_subtype — a unique index change on a live table, and its
-- own piece of work.
--
-- 5 CONVERGE within the batch: two or more re-keys landing on the same parcel
-- and date. The per-event collision check cannot see candidates converging on
-- each other, only a candidate hitting an existing row. Excluded here and
-- left for individual inspection.
--
-- 9 STILL DO NOT MATCH after normalisation, including the three-address
-- notice. Left synthetic.
--
-- ============================================================
-- SAFETY
-- ============================================================
-- No foreign keys reference signals.distress_events (pg_constraint contype='f'
-- confrelid=distress_events returns zero rows). No table anywhere carries a
-- distress_event_id column. This migration DELETES NOTHING — it changes
-- parcel_id on rows that currently point at a stub, to point at a real parcel.
--
-- The core.parcels join is the guard: an address that resolves to no real
-- parcel leaves the event synthetic. A synthetic id is honestly unresolvable;
-- a real-looking parcel_id pointing at the wrong house is a silent lie, and
-- that distinction has decided every judgement call in this repair.

-- ============================================================
-- STEP 0 — PRE-FLIGHT. Run alone. Expected: to_rekey 62,
-- collides 0, converging 0. Anything else, stop and read it.
-- ============================================================
WITH ev AS (
  SELECT e.id, e.event_type, e.event_date, e.source,
         regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
           upper(btrim(e.raw_data->'list'->>'address')),
           '\y(ST|AVE|LN|CIR|BLVD|DR|CT|PL|TRL|PKWY|TER|RD|WAY)\y','','g'),
           '\y(NW|NE|SW|SE|N|S|E|W|NORTHWEST|NORTHEAST|SOUTHWEST|SOUTHEAST|NORTH|SOUTH|EAST|WEST)\s*$','','g'),
           '\y(STREET|AVENUE|LANE|CIRCLE|BOULEVARD|DRIVE|COURT|PLACE|TRAIL|PARKWAY|TERRACE|ROAD)\y','','g'),
           '\y(UNIT|APT|APARTMENT|STE|SUITE)\s*[A-Z0-9-]*\s*$','','g'),
           '[^A-Z0-9]','','g')                                     AS key
  FROM   signals.distress_events e
  WHERE  e.county_code = 'anoka'
    AND  e.parcel_id LIKE '%-FC-%'
    AND  e.raw_data->'list'->>'address' IS NOT NULL
    AND  e.raw_data->'list'->>'address' NOT LIKE '%,%'
    AND  upper(e.raw_data->'list'->>'address') !~ '\y(UNIT|APT|APARTMENT|STE|SUITE|#)\y'
),
sp AS (
  SELECT p.parcel_id,
         regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
           upper(btrim(p.address)),
           '\y(ST|AVE|LN|CIR|BLVD|DR|CT|PL|TRL|PKWY|TER|RD|WAY)\y','','g'),
           '\y(NW|NE|SW|SE|N|S|E|W|NORTHWEST|NORTHEAST|SOUTHWEST|SOUTHEAST|NORTH|SOUTH|EAST|WEST)\s*$','','g'),
           '\y(STREET|AVENUE|LANE|CIRCLE|BOULEVARD|DRIVE|COURT|PLACE|TRAIL|PARKWAY|TERRACE|ROAD)\y','','g'),
           '\y(UNIT|APT|APARTMENT|STE|SUITE)\s*[A-Z0-9-]*\s*$','','g'),
           '[^A-Z0-9]','','g')                                     AS key
  FROM   core.parcels p
  WHERE  p.county_code = 'anoka' AND p.address IS NOT NULL
    AND  p.parcel_id NOT LIKE '%-FC-%'
),
matched AS (
  SELECT ev.id, ev.event_type, ev.event_date, ev.source,
         min(sp.parcel_id) AS new_pid
  FROM   ev JOIN sp ON sp.key = ev.key
  GROUP  BY 1,2,3,4
  HAVING count(DISTINCT sp.parcel_id) = 1
),
clean AS (
  SELECT m.*
  FROM   matched m
  WHERE  NOT EXISTS (SELECT 1 FROM signals.distress_events ex
                     WHERE ex.county_code='anoka' AND ex.parcel_id=m.new_pid
                       AND ex.event_type=m.event_type AND ex.event_date=m.event_date
                       AND ex.source=m.source AND ex.id<>m.id)
    AND  NOT EXISTS (SELECT 1 FROM matched m2
                     WHERE m2.id<>m.id AND m2.new_pid=m.new_pid
                       AND m2.event_type=m.event_type AND m2.event_date=m.event_date
                       AND m2.source=m.source)
)
SELECT now() AS run_at,
       (SELECT count(*) FROM matched)                              AS matched_total,
       (SELECT count(*) FROM clean)                                AS to_rekey,
       (SELECT count(*) FROM matched) - (SELECT count(*) FROM clean) AS excluded;


-- ============================================================
-- STEP 1 — THE RE-KEY. One statement, one column, no deletes.
-- ============================================================
BEGIN;

WITH ev AS (
  SELECT e.id, e.event_type, e.event_date, e.source,
         regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
           upper(btrim(e.raw_data->'list'->>'address')),
           '\y(ST|AVE|LN|CIR|BLVD|DR|CT|PL|TRL|PKWY|TER|RD|WAY)\y','','g'),
           '\y(NW|NE|SW|SE|N|S|E|W|NORTHWEST|NORTHEAST|SOUTHWEST|SOUTHEAST|NORTH|SOUTH|EAST|WEST)\s*$','','g'),
           '\y(STREET|AVENUE|LANE|CIRCLE|BOULEVARD|DRIVE|COURT|PLACE|TRAIL|PARKWAY|TERRACE|ROAD)\y','','g'),
           '\y(UNIT|APT|APARTMENT|STE|SUITE)\s*[A-Z0-9-]*\s*$','','g'),
           '[^A-Z0-9]','','g')                                     AS key
  FROM   signals.distress_events e
  WHERE  e.county_code = 'anoka'
    AND  e.parcel_id LIKE '%-FC-%'
    AND  e.raw_data->'list'->>'address' IS NOT NULL
    AND  e.raw_data->'list'->>'address' NOT LIKE '%,%'
    AND  upper(e.raw_data->'list'->>'address') !~ '\y(UNIT|APT|APARTMENT|STE|SUITE|#)\y'
),
sp AS (
  SELECT p.parcel_id,
         regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
           upper(btrim(p.address)),
           '\y(ST|AVE|LN|CIR|BLVD|DR|CT|PL|TRL|PKWY|TER|RD|WAY)\y','','g'),
           '\y(NW|NE|SW|SE|N|S|E|W|NORTHWEST|NORTHEAST|SOUTHWEST|SOUTHEAST|NORTH|SOUTH|EAST|WEST)\s*$','','g'),
           '\y(STREET|AVENUE|LANE|CIRCLE|BOULEVARD|DRIVE|COURT|PLACE|TRAIL|PARKWAY|TERRACE|ROAD)\y','','g'),
           '\y(UNIT|APT|APARTMENT|STE|SUITE)\s*[A-Z0-9-]*\s*$','','g'),
           '[^A-Z0-9]','','g')                                     AS key
  FROM   core.parcels p
  WHERE  p.county_code = 'anoka' AND p.address IS NOT NULL
    AND  p.parcel_id NOT LIKE '%-FC-%'
),
matched AS (
  SELECT ev.id, ev.event_type, ev.event_date, ev.source,
         min(sp.parcel_id) AS new_pid
  FROM   ev JOIN sp ON sp.key = ev.key
  GROUP  BY 1,2,3,4
  HAVING count(DISTINCT sp.parcel_id) = 1
),
clean AS (
  SELECT m.*
  FROM   matched m
  WHERE  NOT EXISTS (SELECT 1 FROM signals.distress_events ex
                     WHERE ex.county_code='anoka' AND ex.parcel_id=m.new_pid
                       AND ex.event_type=m.event_type AND ex.event_date=m.event_date
                       AND ex.source=m.source AND ex.id<>m.id)
    AND  NOT EXISTS (SELECT 1 FROM matched m2
                     WHERE m2.id<>m.id AND m2.new_pid=m.new_pid
                       AND m2.event_type=m.event_type AND m2.event_date=m.event_date
                       AND m2.source=m.source)
)
UPDATE signals.distress_events d
SET    parcel_id = c.new_pid
FROM   clean c
WHERE  d.id = c.id;

COMMIT;


-- ============================================================
-- STEP 2 — VERIFY. Run separately. The commit succeeding is not
-- evidence; the counts are.
-- ============================================================
SELECT now()                                                     AS run_at,
       count(*)                                                  AS anoka_events,
       count(*) FILTER (WHERE parcel_id LIKE '%-FC-%')            AS still_synthetic,
       count(*) FILTER (WHERE parcel_id NOT LIKE '%-FC-%')        AS resolved
FROM   signals.distress_events
WHERE  county_code = 'anoka';

-- EXPECTED: anoka_events unchanged at 401 (nothing is deleted).
--           still_synthetic 108 -> ~46
--           resolved        293 -> ~355
--
-- The ~46 remaining are the 30 pending/completed collisions, the 5 converging,
-- the 9 unmatched, and the 1 whose PIN matched no parcel. Every one is a
-- deliberate exclusion with a stated reason, not a silent skip.
