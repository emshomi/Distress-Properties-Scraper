-- MIGRATION_parcels_house_street_index_2026-08-15.sql
--
-- Expression index on (house number, street name) so an address can be matched
-- with the STREET TYPE ignored, without a sequential scan.
--
-- ============================================================
-- WHY
-- ============================================================
-- The sheriff abbreviates and the assessor expands:
--     16526 ARGON ST NW      <->  16526 Argon Street Northwest
--     20042 WILD RICE DR NE  <->  20042 Wild Rice Drive Northeast
-- An exact normalised match fails on every one of those. Matching on house
-- number + street name (word 2) and ignoring the type recovers them, and the
-- HOUSE NUMBER is what keeps it honest -- nearest-centroid picks the neighbour
-- (34 MAPLE ISLAND ROAD -> 36) while this picks the right house.
--
-- Dakota's version of this ran because ST_DWithin narrowed the candidate set
-- first. ANOKA EVENTS CARRY NO POINT GEOMETRY, so there is nothing to narrow
-- with and split_part() on the left of a comparison defeats every index: the
-- query timed out on 34 events x 139,425 anoka parcels.
--
-- 22 anoka events need this. It also serves dakota and any future county whose
-- notices abbreviate.
--
-- THE OPERAND ORDER AND ARGUMENTS MUST MATCH THE QUERY EXACTLY. Postgres
-- matches expression indexes by exact expression. On 2026-08-15
-- idx_parcels_norm_address was silently unused because queries wrote
-- regexp_replace(upper(address), ...) while the index held
-- upper(regexp_replace(address, ...)) -- 13,130ms vs 18ms. Queries using this
-- index must write EXACTLY:
--     split_part(address, ' ', 1)
--     upper(split_part(address, ' ', 2))
--
-- CONCURRENTLY so the daily scrapers are not blocked. Cannot run inside a
-- transaction block, so no BEGIN/COMMIT and it must be the only statement run.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_parcels_house_street
    ON core.parcels (county_code,
                     split_part(address, ' ', 1),
                     upper(split_part(address, ' ', 2)))
    WHERE address IS NOT NULL;
