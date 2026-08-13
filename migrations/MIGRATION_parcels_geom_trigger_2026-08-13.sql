-- MIGRATION_parcels_geom_trigger_2026-08-13.sql
--
-- core.parcels.geom becomes derived from lat/lng, enforced on every write.
--
-- WHY
-- geom had no live writer. outcome_capture/backfill_parcel_geom.py populated it
-- once on 2026-08-02 with predicate `geom IS NULL`, so it never revisits a row.
-- Measured 2026-08-13:
--   * 18,415 rows across 12 MnGeo counties hold ANOTHER COUNTY'S geometry. The
--     2026-08-06 composite-key incident left those rows carrying Fillmore and
--     Wabasha lat/lng; the backfill converted them faithfully on 08-02; the
--     08-07 recovery corrected lat/lng and could not touch geom.
--   * ~1.8M rows have coordinates and no geometry at all - everything written
--     since 08-02.
--
-- WHY A TRIGGER AND NOT A GENERATED COLUMN
-- A generated column is the stronger guarantee and was the first choice. It was
-- withdrawn on measurement: core.parcels is 2,663,928 rows / 7,763 MB with
-- 3,769 MB of indexes, so ADD COLUMN ... GENERATED ... STORED means a full
-- rewrite under ACCESS EXCLUSIVE - reads AND writes blocked for an unbounded
-- window on a live product with paying subscribers - plus ~4.2 GB of transient
-- free space. A BEFORE trigger that ALWAYS assigns NEW.geom overwrites whatever
-- the caller supplied, so no write path can produce a divergent value. Same
-- runtime invariant, no lock, no rewrite.
--
-- EXPRESSION is byte-identical to the script's BATCH_SQL:
--   ST_SetSRID(ST_MakePoint(lng::float8, lat::float8), 4326)::geography
-- The CASE reproduces the script's Minnesota bounding-box GUARD: a transposed or
-- projected coordinate stays NULL rather than becoming a plausible point in the
-- wrong hemisphere. Verified 2026-08-13 on a temp table - the known-good sample
-- POINT(-93.066465 44.977053) reproduces exactly; transposed and null-island
-- inputs both return NULL.
--
-- PostGIS 3.3.7 is installed in `public`, NOT `extensions`. Calls are schema-
-- qualified and search_path is pinned: a trigger body does not inherit the SQL
-- editor's path, and an unqualified call would fail on every insert from a role
-- whose path differs.
--
-- This migration does NOT repair existing rows. Backlog repair is batched
-- separately - see the runbook note at the foot of this file.

CREATE OR REPLACE FUNCTION core.parcels_set_geom()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
BEGIN
    IF NEW.lat IS NOT NULL
       AND NEW.lng IS NOT NULL
       AND NEW.lat BETWEEN 43.0 AND 49.5
       AND NEW.lng BETWEEN -97.5 AND -89.0
    THEN
        NEW.geom := public.ST_SetSRID(
                        public.ST_MakePoint(NEW.lng::float8, NEW.lat::float8),
                        4326
                    )::public.geography;
    ELSE
        -- Outside the Minnesota box, or no coordinates: leave it EMPTY rather
        -- than converting garbage into a well-formed point somewhere wrong.
        NEW.geom := NULL;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_parcels_set_geom ON core.parcels;

CREATE TRIGGER trg_parcels_set_geom
    BEFORE INSERT OR UPDATE OF lat, lng ON core.parcels
    FOR EACH ROW
    EXECUTE FUNCTION core.parcels_set_geom();

-- RUNBOOK
-- 1. This file. Seconds. Every future write carries correct geometry.
-- 2. Backlog: ~1.8M rows need geom, 18,415 need it CORRECTED. Batched, via a
--    direct psycopg2 connection - the Supabase SQL editor's gateway times out
--    on whole-table UPDATEs and you cannot tell a slow success from a failure
--    (backfill_parcel_geom.py docstring, 2026-08-02).
-- 3. RETIRE outcome_capture/backfill_parcel_geom.py once the backlog clears.
--    Its `geom IS NULL` predicate cannot fix a WRONG value, only a missing one.
