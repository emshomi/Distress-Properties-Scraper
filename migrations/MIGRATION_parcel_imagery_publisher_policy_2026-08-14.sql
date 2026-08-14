-- MIGRATION_parcel_imagery_publisher_policy_2026-08-14.sql
--
-- Adds publisher evidence and a resolution-policy fingerprint to
-- core.parcel_imagery, and admits a sixth status.
--
-- ============================================================
-- WHAT WAS MEASURED (2026-08-14, all 6,899 ok rows, zero cost)
-- ============================================================
-- The resolver sent only `location` and `key`. Google's default panorama
-- search includes INDOOR collections and third-party photospheres, so
-- 8300 NORMAN CENTER DR, Bloomington — a $48.8M commercial parcel — rendered
-- a member of the public's photo of a cafe interior, captioned as the
-- property, with nothing indicating otherwise.
--
-- A census of every ok parcel under `source=outdoor` (audit
-- .streetview_source_probe, probe_run outdoor_v1_2026-08-14):
--     206 panoramas CHANGED       (187 of them off a third-party photosphere)
--      13 LOST coverage entirely  (only third-party imagery existed)
--      18 third-party photospheres SURVIVED source=outdoor
--   6,662 unchanged
--
-- Two signals identified the class and AGREED ON ALL 6,899 ROWS: a CAoS-
-- prefixed pano_id, and a copyright string other than '© Google'. Neither
-- disagreed with the other once.
--
-- source=google was tried and REJECTED BY GOOGLE: HTTP 400, "Invalid request.
-- Invalid 'source' parameter." That value exists only in the Maps JavaScript
-- API. `outdoor` is the only source filter this endpoint accepts, so the
-- residue is a CLASSIFICATION problem and is handled here.
--
-- ============================================================
-- pano_copyright — EVIDENCE, NOT DECORATION
-- ============================================================
-- Of the 18 survivors, 7 are '© WSB' (a Minnesota municipal engineering firm
-- whose panoramas sit 23-40m out on the roadway across six different cities)
-- and 11 are named individuals and virtual-tour companies, two of which have
-- the words "Virtual Tour" in their own name — the same business-interior
-- class as the cafe.
--
-- Storing the publisher is what makes that judgeable at all. Without it, a
-- wrong row can only be diagnosed by re-querying Google, and the table
-- records that resolution SUCCEEDED while saying nothing about what it
-- resolved to. That is the same shape as distress_events having no
-- updated_at.
--
-- ============================================================
-- resolver_policy — THE COLUMN THAT STOPS THE NEXT ROT
-- ============================================================
-- geom sat wrong on 18,415 rows for eleven days because nothing could tell a
-- row resolved under old rules from one resolved under current rules. Every
-- imagery row now records the policy it was decided under, and the resolver's
-- working set re-opens any row whose policy is not the current one.
--
-- The string is COMPUTED FROM THE CONSTANTS, not hand-written, so changing a
-- threshold, the source parameter, or the verified-publisher list cannot fail
-- to trigger re-resolution. NULL on every existing row, which is correct:
-- they were resolved under a policy that had no name and they must all be
-- re-decided.
--
-- ============================================================
-- unverified_source — THE SIXTH STATUS
-- ============================================================
-- A panorama from a publisher we have not verified is NOT no_imagery (Google
-- has something), NOT too_far (distance is fine), and NOT error (the lookup
-- worked). It is a fourth fact: we found an image and will not vouch for what
-- it depicts. Collapsing it into any existing status would make a false
-- statement, which is the same argument that kept no_location separate.
--
-- The verified list starts as '© Google' ALONE. WSB is probably fine and
-- nobody has yet looked at one of its images; "probably fine" is not a
-- standard for asserting that a photograph is of someone's property. Seven
-- parcels go dark until it is verified, and adding it changes the policy
-- fingerprint, which re-resolves them automatically.
--
-- parcel_imagery_notok_has_no_pano_ck already forces pano_id NULL on any
-- non-ok row, so a rejected panorama leaves no stale locator behind.

BEGIN;

ALTER TABLE core.parcel_imagery
    ADD COLUMN IF NOT EXISTS pano_copyright text,
    ADD COLUMN IF NOT EXISTS resolver_policy text;

ALTER TABLE core.parcel_imagery
    DROP CONSTRAINT parcel_imagery_status_ck;

ALTER TABLE core.parcel_imagery
    ADD CONSTRAINT parcel_imagery_status_ck
    CHECK (status IN ('ok', 'no_imagery', 'too_far',
                      'no_location', 'error', 'unverified_source'));

-- The resolver's new working-set predicate is
-- "resolver_policy IS DISTINCT FROM <current>", asked once per batch over
-- the whole table. Without this it is a sequential scan of every imagery row
-- on every batch.
CREATE INDEX IF NOT EXISTS idx_parcel_imagery_policy
    ON core.parcel_imagery (source, resolver_policy);

COMMENT ON COLUMN core.parcel_imagery.pano_copyright IS
    'Google''s copyright string for the panorama, verbatim. Evidence, not '
    'decoration: it is how a row can be judged wrong later without paying '
    'for another Google call. Agreed with the CAoS pano_id prefix on all '
    '6,899 rows measured 2026-08-14.';

COMMENT ON COLUMN core.parcel_imagery.resolver_policy IS
    'Fingerprint of the rules this row was resolved under, computed from the '
    'resolver''s constants. The working set re-opens any row whose policy is '
    'not the current one, so a threshold or publisher change is self-healing '
    'rather than needing a manual sweep. NULL means pre-policy.';

COMMENT ON COLUMN core.parcel_imagery.status IS
    'ok | no_imagery | too_far | no_location | error | unverified_source. '
    'SIX different facts that must never collapse: no_location is a '
    'geocoding gap, too_far is coverage pointed elsewhere, error is the '
    'absence of an answer, and unverified_source is an image we found but '
    'will not vouch for.';

COMMIT;
