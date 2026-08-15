-- MIGRATION_mnpn_package_split_2026-08-15.sql
--
-- Split 9 mnpublicnotice package notices into 32 per-parcel events, and delete
-- the 9 originals they supersede.
--
-- ============================================================
-- WHAT IS WRONG
-- ============================================================
-- One Minnesota foreclosure notice can cover MANY parcels. The PID field is
-- printed as a list, and _pid_digits() ran over the WHOLE field, so
-- '150063915; 150063922' became one 18-digit string that matched nothing and
-- the notice landed on a single synthetic stub.
--
-- The worst case, washington 26-003536FC: THIRTEEN parcels -- twelve addresses
-- on Keibler Ct and 211th St, Forest Lake -- sold under one bid of
-- $261,140.77, stored as ONE row. Twelve distressed properties were invisible
-- to every subscriber searching Forest Lake.
--
-- ============================================================
-- WHY event_value IS NULL ON EVERY MEMBER
-- ============================================================
-- The bid is ONE figure for the WHOLE package. Copying $261,140.77 onto each
-- of thirteen parcels would fabricate thirteen equity spreads -- the same class
-- of error as writing a county's published $0 into emv_total. The total is
-- carried in raw_data._package.total_bid instead.
--
-- Deal math needs event_value, so it correctly declines to compute. Market
-- value, coordinates, owner and imagery are all real per-parcel facts from the
-- assessor and are all still shown. The member also carries a note saying it is
-- part of a package of N sold together for the total.
--
-- ============================================================
-- WHY source_id GAINS A '#<pid digits>' SUFFIX
-- ============================================================
-- distress_events_source_identity_key is
--     (county_code, source, source_id, event_date) NULLS NOT DISTINCT
-- and every member of a package shares all four. Without a suffix the first
-- insert succeeds and the second raises 23505.
--
-- The suffix is the PARCEL'S OWN DIGITS, never a position in the list.
-- Minn. Stat. 580.03 requires six consecutive weekly publications, and a
-- republication may list the parcels in a DIFFERENT ORDER -- an index-based
-- suffix would shift each week and mint a fresh event every time, which is the
-- 24% inflation measured on 2026-08-10.
--
-- This does not weaken the key: for a package notice the publisher's identity
-- for a GIVEN PARCEL genuinely is notice-plus-parcel, and the suffix is derived
-- from what the county published, not from anything we rewrite.
--
-- Matches src/llm/foreclosure_promotion.py exactly, so notices arriving from
-- now on are split at promotion and this migration never needs repeating.
--
-- ============================================================
-- COT# / CERT# NOISE IS STRIPPED
-- ============================================================
-- Torrens registrations print as '08.032.21.11.0036 COT# 77608'. The trailing
-- 77608 is a certificate number, not part of the PID -- digits over the whole
-- string yields '080322111003677608', matching nothing.
--
-- ============================================================
-- ONE ORIGINAL IS DELIBERATELY KEPT
-- ============================================================
-- martin 058273-F1 ('1228 & 1224 N Prairie Ave') resolves 0 of 2 pins: martin
-- holds 3 parcels in the spine. Deleting it would leave NOTHING behind, so it
-- stays exactly as it is. An original is deleted only when ALL its members
-- resolved.
--
-- Note washington 26-003550FC is TWO originals (4 pins in July, 3 in
-- September, three parcels in both). Different event_date, so no collision --
-- a package sale partly sold or partly postponed. Both are kept.
--
-- ============================================================
-- VERIFIED BEFORE WRITING
-- ============================================================
--     member rows                34
--     usable (>=6 digits)        34
--     resolve to a real parcel   32   <- rows inserted
--     distinct new keys          32   <- no member collides with another
--     collides with existing      0
--     originals fully resolved    9   <- rows deleted
--
-- Net event count: +32 -9 = +23.

BEGIN;

CREATE TEMP TABLE _pkg_members ON COMMIT DROP AS
SELECT e.id                                    AS orig_id,
       d.county_slug,
       e.source_id                             AS base_source_id,
       e.event_date,
       e.event_value                           AS total_bid,
       e.raw_data                              AS orig_raw,
       e.severity,
       t.ord,
       r.parcel_id                             AS new_parcel_id,
       count(*)    OVER (PARTITION BY e.id)    AS total_pins,
       count(r.parcel_id) OVER (PARTITION BY e.id) AS resolved_pins,
       regexp_replace(
         regexp_replace(t.pin, '\m(COT|CERT|DOC|TORRENS)\M\s*#?\s*\d+', '', 'gi'),
         '\D', '', 'g')                        AS pin_digits
FROM   signals.distress_events e
JOIN   signals.distress_with_parcel d ON d.id = e.id
LEFT   JOIN core.parcels p
       ON p.county_code = d.county_slug AND p.parcel_id = d.eff_parcel_id
CROSS  JOIN LATERAL unnest(
         string_to_array(regexp_replace(d.eff_parcel_id, '\s+and\s+', ';', 'g'), ';')
       ) WITH ORDINALITY AS t(pin, ord)
LEFT   JOIN core.parcels r
       ON r.county_code = d.county_slug
      AND regexp_replace(r.parcel_id, '\D', '', 'g') =
          regexp_replace(
            regexp_replace(t.pin, '\m(COT|CERT|DOC|TORRENS)\M\s*#?\s*\d+', '', 'gi'),
            '\D', '', 'g')
WHERE  e.source = 'mnpublicnotice'
  AND  e.event_type = 'sheriff_sale'
  AND  p.parcel_id IS NULL
  AND  d.eff_parcel_id ~ '[;,]| and ';

CREATE TABLE IF NOT EXISTS audit.mnpn_package_split_20260815 AS
SELECT e.*, now() AS captured_at
FROM   signals.distress_events e
WHERE  e.id IN (SELECT DISTINCT orig_id FROM _pkg_members);

INSERT INTO signals.distress_events
       (parcel_id, county_code, event_type, event_subtype, event_date,
        event_value, source, source_id, severity, title, description, raw_data,
        observed_at)
SELECT m.new_parcel_id,
       m.county_slug,
       'sheriff_sale',
       'scheduled',
       m.event_date,
       NULL,                                   -- see the event_value note above
       'mnpublicnotice',
       m.base_source_id || '#' || m.pin_digits,
       m.severity,
       'Scheduled sheriff foreclosure sale — ' || coalesce(p.address, m.new_parcel_id),
       'Part of a package sale of ' || m.total_pins || ' properties sold '
         || 'together. No individual price was published for this parcel.',
       jsonb_set(
         m.orig_raw,
         '{_package}',
         jsonb_build_object(
           'size',      m.total_pins,
           'index',     m.ord,
           'total_bid', m.total_bid,
           'migration', 'MIGRATION_mnpn_package_split_2026-08-15',
           'from_event', m.orig_id,
           'note', 'Part of a package sale of ' || m.total_pins
                   || ' properties sold together. No individual price was '
                   || 'published for this parcel.'),
         true),
       now()
FROM   _pkg_members m
LEFT   JOIN core.parcels p
       ON p.county_code = m.county_slug AND p.parcel_id = m.new_parcel_id
WHERE  m.new_parcel_id IS NOT NULL;

DELETE FROM signals.distress_events e
WHERE  e.id IN (SELECT DISTINCT orig_id FROM _pkg_members
                 WHERE resolved_pins = total_pins);

COMMIT;
