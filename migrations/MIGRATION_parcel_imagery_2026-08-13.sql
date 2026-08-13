-- MIGRATION_parcel_imagery_2026-08-13.sql
--
-- core.parcel_imagery — one row per parcel, holding REFERENCES to Google
-- imagery. Never the imagery itself.
--
-- ============================================================
-- WHY ITS OWN TABLE, NOT COLUMNS ON core.parcels
-- ============================================================
-- The obvious shape is a pano_id column on core.parcels. It is the wrong one,
-- and 2026-08-13 produced the evidence twice over.
--
-- core.parcels has TWO writer classes that already disagree. The county
-- parcel loaders upsert direct with exclude_none=True; the signal scrapers go
-- through parcel_resolver's per-field merge. A column on that table is
-- therefore subject to: exclude_none dropping it from the UPDATE (which left
-- 6,268 rows carrying ANOTHER county's property_type), and fill-in semantics
-- that only write when the existing value is null (so a wrong value can never
-- be corrected by any scraper).
--
-- And geom is the cautionary tale in full: a column on core.parcels with an
-- index, no writer, no trigger and no owner. Populated once by a backfill on
-- 2026-08-02, then stale for 1,515,168 rows and WRONG for 18,415 — carrying
-- Fillmore and Wabasha geometry on Morrison, Polk and Chisago parcels —
-- through a recovery that was recorded as complete. A field nobody owns rots.
--
-- So imagery gets its own table with EXACTLY ONE WRITER: the resolver.
-- No loader touches it, no enrichment path can clobber it, and no upsert
-- anywhere else in the codebase can null a value it knows nothing about.
--
-- ============================================================
-- COMPOSITE KEY, NON-NEGOTIABLE
-- ============================================================
-- (county_code, parcel_id). Minnesota PINs are NOT globally unique — 51,662
-- nine-character PINs are shared across counties. A parcel_id-only key is how
-- ~191,600 rows were destroyed on 2026-08-06. The FK is composite for the
-- same reason, and it CASCADEs: an imagery row for a parcel that no longer
-- exists is a reference to nothing.
--
-- ============================================================
-- WHAT IS STORED, AND WHAT MUST NOT BE
-- ============================================================
-- Google's terms prohibit pre-fetching, indexing, storing, resharing or
-- rehosting Maps Content, and call out bulk download of Street View images
-- specifically. The PANORAMA ID is expressly exempt from the caching
-- restriction and may be stored indefinitely.
--
-- So: pano_id yes, pixels never. The browser fetches each image from Google
-- directly, by pano_id, at render time. Nothing in this schema is an image,
-- an image URL, or a path to one.
--
-- ============================================================
-- MEASURED, NOT ASSUMED (2026-08-13)
-- ============================================================
-- Street View near-coverage (panorama within 60m of the parcel), sampled
-- randomly per county, n=100 metro / all-available rural:
--     ramsey    92%     crow_wing  45%
--     hennepin  92%     carlton    37%
--     olmsted   79%     fillmore   37%
--     dakota    74%
-- Weighted by actual distress population (~7,196 of 8,299 parcels are in the
-- four metro counties) that is ~87% overall — but rural is genuinely half.
--
-- Aerial (Maps Static, maptype=satellite) measured by tile byte size, pixel
-- stddev and colour count: median 97-135 KB against a 12 KB low-detail floor,
-- low% = 0 in EVERY county except fillmore at z19 (5%, flat farmland). Rural
-- aerial is as good as metro — carlton, the WORST Street View county at 37%,
-- has the HIGHEST aerial detail at 135 KB.
--
-- Consequence: both sources ship on every parcel that has them. Aerial is not
-- a fallback — on acreage it is the better image. A 40-acre Fillmore parcel
-- from the road is a mailbox and a treeline; from above it is the house, the
-- outbuildings, the access track and how much is tilled.
--
-- Zoom was expected to be county-dependent and is NOT: crow_wing and carlton
-- are equally detailed at z17/18/19. One global zoom, no per-county table.
--
-- ============================================================
-- IMAGERY IS A LOCATOR
-- ============================================================
-- A pano_id resolves to exact coordinates with one call to Google's metadata
-- endpoint. It is a machine-readable, bulk-harvestable locator — stronger
-- than the address string, not weaker. src/utils/redaction.py already places
-- lat/lng in _LOCATOR_FIELDS alongside address, city and parcel_id, locked
-- below STANDARD. pano_id belongs in the same tuple by the same reasoning.
-- Below Standard the API must send a bare boolean and nothing else.

BEGIN;

CREATE TABLE IF NOT EXISTS core.parcel_imagery (
    county_code     text        NOT NULL,
    parcel_id       text        NOT NULL,

    -- 'google_streetview' | 'google_aerial'. Aerial has no per-parcel
    -- identifier to store (Maps Static is generated per request), so an
    -- aerial row records only that the parcel was checked and at what zoom.
    -- Kept in the same table because both answer "what can we show here",
    -- and a single lookup per parcel beats two.
    source          text        NOT NULL,

    -- ok          a usable image exists
    -- no_imagery  checked, Google has nothing here (ZERO_RESULTS)
    -- too_far     a panorama exists but stands beyond near_metres — a
    --             picture of a field, not of the property. DISTINCT from
    --             no_imagery on purpose: it is a different fact, and the
    --             detail panel must not claim "no coverage" when the truth
    --             is "coverage, pointed at something else".
    -- no_location the parcel has no lat/lng. NOT an imagery problem — a
    --             geocoding one. 287,081 parcels are in this state and
    --             collapsing it into no_imagery would make a false
    --             statement about our own coverage.
    -- error       the lookup itself failed. Never conflate with no_imagery:
    --             one is an answer, the other is the absence of one.
    status          text        NOT NULL,

    -- Google's panorama identifier. Exempt from the caching restriction and
    -- storable indefinitely; the imagery it points at is not.
    pano_id         text,

    -- 'YYYY-MM' exactly as Google returns it. NOT decoration: rendered
    -- against first_delq_year and redemption_ends_at, this says whether the
    -- picture predates the distress or falls inside it. Competitors warn
    -- about staleness in blog posts; we can place the image on the
    -- property's own timeline.
    pano_date       text,

    -- Where the camera stood, and how far that is from the parcel.
    pano_lat        numeric(9,6),
    pano_lng        numeric(9,6),
    distance_m      numeric(8,1),

    -- Bearing FROM the camera TO the parcel. Street View defaults the camera
    -- to the panorama's own heading, not toward the requested point, which
    -- on a residential street regularly frames the house OPPOSITE. Computing
    -- and storing this is the difference between a photo of the property and
    -- a photo of its neighbour.
    heading_deg     numeric(5,2),

    -- Aerial only. Measured 2026-08-13: z18 is correct statewide — equivalent
    -- detail to z19, frames more of the lot, and avoids fillmore's only soft
    -- spot (5% low at z19).
    zoom            smallint,

    -- The column whose absence let geom rot for eleven days.
    --
    -- Google re-drives roads and refreshes aerials. Without a resolved_at
    -- there is no way to tell "checked recently, genuinely no coverage" from
    -- "checked once and never again" — the exact ambiguity that made
    -- records_new useless for distinguishing a dead source from a healthy
    -- one. A re-resolution job finds stale rows by this column.
    resolved_at     timestamptz NOT NULL DEFAULT now(),

    error_detail    text,

    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT parcel_imagery_pkey
        PRIMARY KEY (county_code, parcel_id, source),

    CONSTRAINT parcel_imagery_parcel_fk
        FOREIGN KEY (county_code, parcel_id)
        REFERENCES core.parcels (county_code, parcel_id)
        ON DELETE CASCADE,

    CONSTRAINT parcel_imagery_source_ck
        CHECK (source IN ('google_streetview', 'google_aerial')),

    CONSTRAINT parcel_imagery_status_ck
        CHECK (status IN ('ok', 'no_imagery', 'too_far',
                          'no_location', 'error')),

    -- status='ok' on a Street View row MUST carry a pano_id, or the frontend
    -- has nothing to render and would show an empty slot while claiming
    -- coverage. Aerial rows legitimately have no pano_id — Maps Static
    -- generates per request — so the constraint is scoped to the source that
    -- has one. Enforced in the schema rather than in the resolver: a bug in
    -- one writer should not be able to produce a row that lies.
    CONSTRAINT parcel_imagery_ok_has_pano_ck
        CHECK (
            source <> 'google_streetview'
            OR status <> 'ok'
            OR pano_id IS NOT NULL
        ),

    -- Symmetrically: a non-ok row must not carry a pano_id. A leftover ID on
    -- a row that later resolved to no_imagery is exactly the stale-value
    -- problem this whole session has been about.
    CONSTRAINT parcel_imagery_notok_has_no_pano_ck
        CHECK (status = 'ok' OR pano_id IS NULL)
);

-- The frontend's cheapest question, asked once per table page: "does this
-- parcel have imagery?" — answered from our own row, never from Google, and
-- free. Partial because only 'ok' rows are ever looked up this way.
CREATE INDEX IF NOT EXISTS idx_parcel_imagery_ok
    ON core.parcel_imagery (county_code, parcel_id)
    WHERE status = 'ok';

-- The re-resolution job's working set: oldest first, within a source.
CREATE INDEX IF NOT EXISTS idx_parcel_imagery_stale
    ON core.parcel_imagery (source, resolved_at);

-- Coverage reporting per county without a sequential scan of the table.
CREATE INDEX IF NOT EXISTS idx_parcel_imagery_county_status
    ON core.parcel_imagery (county_code, source, status);

COMMENT ON TABLE core.parcel_imagery IS
    'Imagery REFERENCES per parcel (Google pano IDs, never pixels). One '
    'writer: the resolver. Composite-keyed (county_code, parcel_id) — MN '
    'PINs are not unique across counties. pano_id is a LOCATOR and gates at '
    'STANDARD alongside address and lat/lng.';

COMMENT ON COLUMN core.parcel_imagery.status IS
    'ok | no_imagery | too_far | no_location | error. These are FIVE '
    'different facts and must never collapse: no_location is a geocoding '
    'gap, too_far is coverage pointed elsewhere, error is the absence of an '
    'answer rather than a negative one.';

COMMENT ON COLUMN core.parcel_imagery.resolved_at IS
    'When Google was last asked. Without it, "no coverage" and "never '
    'rechecked" are indistinguishable — the failure mode that left geom '
    'stale on 1.5M rows and wrong on 18,415.';

COMMIT;
