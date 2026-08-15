-- MIGRATION_parcel_pid_lookup_address_2026-08-15.sql
--
-- Add address and city to core.parcel_pid_lookup.
--
-- ============================================================
-- WHY
-- ============================================================
-- _resolve_spine_parcel() in admin.py reads this view to turn a notice's
-- printed PID into a real core.parcels.parcel_id. It now also needs the
-- parcel's ADDRESS.
--
-- A PACKAGE notice's extracted['property_address'] is the notice's FULL LIST:
-- washington 26-003536FC prints twelve addresses for thirteen parcels. Written
-- to raw_data.address it makes every member display all twelve of its
-- siblings' addresses -- which is exactly what the 2026-08-15 package-split
-- migration produced before it was repaired.
--
-- Each member needs ITS OWN address, and only a DB lookup can supply it:
-- build_promotion_rows is deliberately pure, so the caller passes it in the
-- same way it already passes resolved_parcel_id.
--
-- The view's existing shape is preserved and the two columns are APPENDED, so
-- nothing selecting the current columns changes. Verified before this edit:
--     county_code, parcel_id, pid_digits
--
-- The digits expression is unchanged and still matches
-- idx_parcels_county_pid_digits exactly ('\D', not '[^0-9]'). Postgres matches
-- expression indexes by exact expression -- writing the other form silently
-- drops to a sequential scan over 2.66M rows.

CREATE OR REPLACE VIEW core.parcel_pid_lookup AS
SELECT p.county_code,
       p.parcel_id,
       regexp_replace(p.parcel_id, '\D'::text, ''::text, 'g'::text) AS pid_digits,
       p.address,
       p.city
FROM   core.parcels p;
