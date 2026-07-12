-- ============================================================================
-- MIGRATION: Tyler-portal tax delinquency tables
-- Date applied: 2026-07-10 (Supabase project zdqwigbssxhqzlveisdz)
-- Committed:    2026-07-12, reconstructed from live-DB introspection
--               (information_schema.columns, pg_constraint, pg_indexes,
--               pg_attribute.attidentity) — NOT from memory.
-- ============================================================================
-- Written by the olmsted_tax_detail scraper (v2.2, weekly cron Tue 16:00 UTC):
--   * tax_delinquency_status — one row per parcel: the current verdict
--     (redeemed vs. true delinquent), forfeiture clock, flags, owner mailing.
--     on_conflict target: (parcel_id, county_slug)  [composite PK]
--   * tax_delinquency_detail — one row per parcel × pay_year × row_kind:
--     the per-year money breakdown behind the status row.
--     on_conflict target: (parcel_id, county_slug, pay_year, row_kind)  [UNIQUE]
--
-- forfeiture_basis note: estimated_judgment_date / estimated_forfeiture_date
-- are COMPUTED estimates (judgment = 2nd Monday of May of first_delq_year+1,
-- forfeiture = judgment + 3yr), never county-stated. Any surface showing the
-- date must ship the basis with it.
--
-- Idempotent: safe to re-run (IF NOT EXISTS throughout).
-- ============================================================================

-- 1. Per-parcel status (current verdict). Composite natural PK, no surrogate.
CREATE TABLE IF NOT EXISTS signals.tax_delinquency_status (
    parcel_id                    text    NOT NULL,
    county_slug                  text    NOT NULL DEFAULT 'olmsted',
    first_delinquent_year        integer,          -- NULL = redeemed since list
    years_delinquent             integer,
    total_delinquent_due         numeric,
    current_year_due             numeric,
    redeemed_since_list          boolean,
    estimated_judgment_date      date,             -- computed, see header note
    estimated_forfeiture_date    date,             -- computed, see header note
    forfeiture_basis             text,             -- ALWAYS ship with the date
    in_forfeiture                boolean,
    coj                          boolean,          -- confession of judgment
    in_bankruptcy                boolean,
    homestead                    boolean,
    owner_name                   text,             -- PREMIUM-gated at the API
    owner_name_2                 text,             -- PREMIUM-gated at the API
    owner_mailing_address        text,             -- PREMIUM-gated at the API
    owner_mailing_city_state_zip text,             -- PREMIUM-gated at the API
    raw_data                     jsonb,
    observed_at                  timestamptz DEFAULT now(),
    scraper_run_id               bigint,
    CONSTRAINT tax_delinquency_status_pkey PRIMARY KEY (parcel_id, county_slug)
);

-- 2. Per-year detail rows. Surrogate identity PK + natural UNIQUE key
--    (the scraper's on_conflict target).
CREATE TABLE IF NOT EXISTS signals.tax_delinquency_detail (
    id             bigint  GENERATED ALWAYS AS IDENTITY,
    parcel_id      text    NOT NULL,
    county_slug    text    NOT NULL DEFAULT 'olmsted',
    pay_year       integer NOT NULL,
    row_kind       text    NOT NULL DEFAULT 'delinquent',  -- 'delinquent' | 'current'
    base_taxes     numeric,
    penalty_due    numeric,
    fees_due       numeric,
    interest_due   numeric,
    total_amt_paid numeric,
    date_last_paid date,
    total_due      numeric,
    raw_data       jsonb,
    observed_at    timestamptz DEFAULT now(),
    scraper_run_id bigint,
    CONSTRAINT tax_delinquency_detail_pkey PRIMARY KEY (id),
    UNIQUE (parcel_id, county_slug, pay_year, row_kind)
    -- live auto-generated constraint name:
    -- tax_delinquency_detail_parcel_id_county_slug_pay_year_row_k_key
);

-- 3. Retire the old, always-empty predecessor table.
--    Executed 2026-07-10 against the live DB (0 rows at time of drop).
DROP TABLE IF EXISTS signals.tax_delinquencies;

-- ============================================================================
-- Verification (state at first full census, 2026-07-10/11):
--   SELECT count(*) FROM signals.tax_delinquency_status;   -- 502
--   SELECT count(*) FILTER (WHERE redeemed_since_list),
--          count(*) FILTER (WHERE NOT redeemed_since_list)
--   FROM signals.tax_delinquency_status;                   -- 254 / 248
--   Reference parcel: 641013084421 (Apache) — delinquent 2025,
--   total $155,062.04, estimated forfeiture 2029-05-11.
-- ============================================================================
