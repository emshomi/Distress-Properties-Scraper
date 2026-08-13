-- MIGRATION_parcels_geom_generated_2026-08-13.sql
--
-- core.parcels.geom becomes a GENERATED column derived from lat/lng.
--
-- WHY
-- geom had no live writer. outcome_capture/backfill_parcel_geom.py populated
-- it once on 2026-08-02 with predicate `geom IS NULL`, so it never revisits a
-- row. Consequences measured 2026-08-13:
--   * 18,415 rows across 12 MnGeo counties hold ANOTHER COUNTY'S geometry.
--     The 2026-08-06 composite-key incident left those rows carrying Fillmore
--     and Wabasha lat/lng; the backfill converted them faithfully on 08-02;
--     the 08-07 recovery corrected lat/lng and could not touch geom.
--   * 464,803+ rows written since 08-02 have good coordinates and NO geometry.
-- Generation makes divergence structurally impossible and repairs both on the
-- rewrite. The backfill script is retired in the same change.
--
-- EXPRESSION is byte-identical to the script's BATCH_SQL:
--   ST_SetSRID(ST_MakePoint(lng::float8, lat::float8), 4326)::geography
-- The CASE reproduces the script's Minnesota bounding-box GUARD. A transposed
-- or projected coordinate stays NULL rather than becoming a plausible point in
-- the wrong hemisphere. Verified 2026-08-13 on a temp table: the known-good
-- sample POINT(-93.066465 44.977053) reproduces exactly, and transposed and
-- null-island inputs both return NULL.
--
-- LOCK: ADD COLUMN ... GENERATED ... STORED rewrites the table. DROP COLUMN
-- first so idx_parcels_geom is not rebuilt inside the exclusive lock; the GIST
-- index is recreated afterwards under a SHARE lock (blocks writes, not reads).
-- No view, constraint or FK depends on geom - pg_depend returned zero rows.

SET statement_timeout = '30min';

BEGIN;

ALTER TABLE core.parcels DROP COLUMN geom;

ALTER TABLE core.parcels
  ADD COLUMN geom geography(Point,4326) GENERATED ALWAYS AS (
    CASE WHEN lat BETWEEN 43.0 AND 49.5
          AND lng BETWEEN -97.5 AND -89.0
         THEN ST_SetSRID(ST_MakePoint(lng::float8, lat::float8), 4326)::geography
    END
  ) STORED;

COMMIT;
