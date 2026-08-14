-- MIGRATION_streetview_probe_google_variant_2026-08-14.sql
--
-- Widen audit.streetview_source_probe.variant to admit 'google'.
--
-- WHY
-- The outdoor census (probe_run outdoor_v1_2026-08-14, 6,899 parcels) fixed
-- 206 panoramas and dropped 13, but 18 third-party photospheres SURVIVED
-- source=outdoor. Seven are municipal street imagery (© WSB) and are correct.
-- Eleven are named individuals and virtual-tour companies, including two whose
-- own names say "Virtual Tour" — the same business-interior class as the cafe
-- panorama at 8300 Norman Center Dr that started this.
--
-- The Maps JavaScript API exposes a GOOGLE street-view source meaning official
-- Google collections only. Whether the REST metadata endpoint honours
-- source=google is UNDOCUMENTED, so it gets measured, not assumed — the same
-- discipline that caught a hand-guessed witness coordinate 3km off target.
--
-- The alternative was filtering on the CAoS pano_id prefix. Rejected: it is a
-- heuristic on an undocumented id format, and it would discard the WSB
-- imagery, which is genuinely a photograph of the property.

ALTER TABLE audit.streetview_source_probe
    DROP CONSTRAINT streetview_probe_variant_ck;

ALTER TABLE audit.streetview_source_probe
    ADD CONSTRAINT streetview_probe_variant_ck
    CHECK (variant IN ('baseline', 'outdoor', 'google'));
