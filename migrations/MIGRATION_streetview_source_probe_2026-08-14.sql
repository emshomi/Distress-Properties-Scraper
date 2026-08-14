-- MIGRATION_streetview_source_probe_2026-08-14.sql
--
-- Measurement table for the Street View `source=outdoor` probe.
--
-- WHY A TABLE AND NOT A LOG
-- The probe queries 6,899 parcels twice and produces 13,798 answers. That is
-- not something you read; it is something you GROUP BY. It also has to be
-- re-run after the resolver is fixed, against the same frame, to prove the fix
-- worked — which means the BEFORE numbers must still exist when the AFTER
-- numbers arrive. A workflow log cannot be joined to core.parcels and does not
-- survive retention.
--
-- WHY BOTH VARIANTS ARE STORED, NOT JUST THE DIFFERENCE
-- The question is not only "does the panorama change". It is also how far the
-- outdoor panorama is (it may be further, and may cross a too_far threshold),
-- what its copyright says, and whether the location loses coverage entirely.
-- Storing only the delta would answer the first question and destroy the rest.
--
-- NOT a permanent product table. It records an experiment, is keyed by the run
-- that produced it, and is safe to drop once the resolver fix is verified.

CREATE TABLE IF NOT EXISTS audit.streetview_source_probe (
    probe_run       text              NOT NULL,
    county_code     text              NOT NULL,
    parcel_id       text              NOT NULL,

    -- 'baseline' = the request resolve_parcel_imagery.py sends today
    --              (location + key, nothing else)
    -- 'outdoor'  = the same request plus source=outdoor
    variant         text              NOT NULL,

    -- The coordinate ASKED about, stored so a later parcel move cannot
    -- silently change what this row appears to have measured.
    req_lat         double precision  NOT NULL,
    req_lng         double precision  NOT NULL,

    -- Google's own status string, verbatim: OK / ZERO_RESULTS /
    -- OVER_QUERY_LIMIT / REQUEST_DENIED / INVALID_REQUEST / UNKNOWN_ERROR,
    -- or 'transport_error' when the request itself failed.
    status          text              NOT NULL,

    pano_id         text,
    pano_date       text,

    -- The reason this column exists. Google's copyright string is believed to
    -- distinguish official Street View captures from user and business
    -- photospheres. This probe is what turns that belief into a measurement.
    -- It is EVIDENCE, not a filter — the filter is source=outdoor.
    pano_copyright  text,

    pano_lat        double precision,
    pano_lng        double precision,
    distance_m      numeric(8,1),

    error_detail    text,
    probed_at       timestamptz       NOT NULL DEFAULT now(),

    PRIMARY KEY (probe_run, county_code, parcel_id, variant),

    CONSTRAINT streetview_probe_variant_ck
        CHECK (variant IN ('baseline', 'outdoor')),

    -- Same principle as parcel_imagery's constraints: a row that says OK
    -- without an id is a row that lies about what Google answered.
    CONSTRAINT streetview_probe_ok_has_pano_ck
        CHECK (status <> 'OK' OR pano_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS streetview_source_probe_run_variant_idx
    ON audit.streetview_source_probe (probe_run, variant);

CREATE INDEX IF NOT EXISTS streetview_source_probe_parcel_idx
    ON audit.streetview_source_probe (county_code, parcel_id);
