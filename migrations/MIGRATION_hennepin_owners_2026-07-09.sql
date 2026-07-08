-- ============================================================
-- MIGRATION_hennepin_owners_2026-07-09.sql
-- Hennepin owner backfill: core.parcels.raw_data -> core.owners
-- ============================================================
-- Mirrors the Ramsey owner load (2026-07-08) and the production
-- projection in src/scrapers/ramsey_parcels.py:
--   - owner precedence: OWNER_NM, fallback TAXPAYER_NM
--     (mirrors Ramsey OwnerName -> TaxName1)
--   - mailing lines are POSITIONAL FROM THE END:
--       TAXPAYER_NM_3 matches CITY ST ZIP  -> csz=l3, street=l2
--       else TAXPAYER_NM_2 matches         -> csz=l2, street=l1
--       else                               -> honest NULL mailing
--   - CSZ regex identical to ramsey_parcels._CSZ_RE
--     (zip stored as 5 digits; +4 dropped)
--   - owner_type CASE ordered exactly like _classify_owner()
--     in ramsey_parcels.py (gov -> bank1 -> bank2 -> llc ->
--     lender-trust -> individual), so backfill == refresh.
--   - is_absentee: mailing street vs core.parcels.address, both
--     normalized (strip trailing "#unit" / "UNIT|APT|STE x",
--     collapse whitespace, upper). NULL when either side missing.
--     Hennepin-specific: mailing lines carry units, site never
--     does; exact compare would false-flag owner-occupied condos.
--   - is_out_of_state: mailing_state <> 'MN'; NULL when unparsed.
--   - observed_at inherits parcels.last_observed_at (convention
--     verified against existing Ramsey rows in core.owners).
--   - upsert on (parcel_id, source): idempotent, re-runnable.
-- Excluded automatically by the WHERE clause:
--   - 118 MPLS-VBR-* stub parcels (raw_data IS NULL)
--   - ~5,014 parcels whose feed row has no owner name
-- Expected result: ~443,603 rows, source='hennepin_parcels'.
-- ============================================================

SET statement_timeout = '600s';

WITH src AS (
    SELECT
        p.parcel_id,
        COALESCE(
            NULLIF(trim(p.raw_data->>'OWNER_NM'), ''),
            NULLIF(trim(p.raw_data->>'TAXPAYER_NM'), '')
        )                                            AS owner_name,
        NULLIF(trim(p.raw_data->>'TAXPAYER_NM_1'), '') AS l1,
        NULLIF(trim(p.raw_data->>'TAXPAYER_NM_2'), '') AS l2,
        NULLIF(trim(p.raw_data->>'TAXPAYER_NM_3'), '') AS l3,
        NULLIF(trim(p.address), '')                  AS site_address,
        p.last_observed_at
    FROM core.parcels p
    WHERE p.county_code = 'hennepin'
      AND p.raw_data IS NOT NULL
),
matched AS (
    SELECT
        s.*,
        regexp_match(s.l3,
          '^(.*?)\s+([A-Z]{2})\s+(\d{5})(?:-\d{4})?\s*$') AS m3,
        regexp_match(s.l2,
          '^(.*?)\s+([A-Z]{2})\s+(\d{5})(?:-\d{4})?\s*$') AS m2
    FROM src s
    WHERE s.owner_name IS NOT NULL
),
parsed AS (
    SELECT
        m.parcel_id,
        m.owner_name,
        m.last_observed_at,
        m.site_address,
        CASE WHEN m.m3 IS NOT NULL THEN m.l2
             WHEN m.m2 IS NOT NULL THEN m.l1
             ELSE NULL END                           AS mailing_address,
        CASE WHEN m.m3 IS NOT NULL THEN trim(m.m3[1])
             WHEN m.m2 IS NOT NULL THEN trim(m.m2[1])
             ELSE NULL END                           AS mailing_city,
        CASE WHEN m.m3 IS NOT NULL THEN m.m3[2]
             WHEN m.m2 IS NOT NULL THEN m.m2[2]
             ELSE NULL END                           AS mailing_state,
        CASE WHEN m.m3 IS NOT NULL THEN m.m3[3]
             WHEN m.m2 IS NOT NULL THEN m.m2[3]
             ELSE NULL END                           AS mailing_zip
    FROM matched m
),
classified AS (
    SELECT
        p.*,
        -- Ordering mirrors ramsey_parcels._classify_owner() exactly.
        CASE
            WHEN upper(p.owner_name) ~ '(SECRETARY OF|VETERANS AFFAIRS|\mHUD\M|HOUSING & URBAN|HOUSING AND URBAN|COUNTY OF|STATE OF MINNESOTA|CITY OF)'
                THEN 'government'
            WHEN upper(p.owner_name) ~ '(BANK|MORTGAGE|\mMTGE\M|\mMTG\M|LENDING|FINANCIAL|CREDIT UNION|NATIONSTAR|FREDDIE|FANNIE|MIDFIRST|BANKUNITED|FEDERAL HOME LOAN|FEDERAL NAT|SERVBANK|CITIMORTGAGE)'
                THEN 'bank_lender'
            WHEN upper(p.owner_name) ~ '(\mLOAN\M|NATIONAL ASSOC|\mNA\M|\mN A\M|\mN\.A\.|TRUSTEE)'
                THEN 'bank_lender'
            WHEN upper(p.owner_name) ~ '(\mLLC\M|L\.?L\.?C|\mINC\M|\mLTD\M|HOLDINGS|VENTURES|PROPERTIES|RENOVATION|REALTY|GROUP|COMPANY|\mCO\M)'
                THEN 'llc_business'
            WHEN upper(p.owner_name) ~ 'TRUST'
             AND upper(p.owner_name) ~ '(MORTGAGE|\mMTG\M|\mLOAN\M|PARTIC|POINT|FUNDING|CAPITAL|MASTER|TITLE TRUST|TRUST [0-9])'
                THEN 'bank_lender'
            ELSE 'individual'
        END AS owner_type,
        -- Normalized street strings for the absentee comparison.
        NULLIF(btrim(regexp_replace(regexp_replace(regexp_replace(
            upper(p.mailing_address),
            '\s*#\s*\S+$', ''),
            '\s+(UNIT|APT|STE|SUITE)\s+\S+$', ''),
            '\s+', ' ', 'g')), '')                   AS mail_norm,
        NULLIF(btrim(regexp_replace(regexp_replace(regexp_replace(
            upper(p.site_address),
            '\s*#\s*\S+$', ''),
            '\s+(UNIT|APT|STE|SUITE)\s+\S+$', ''),
            '\s+', ' ', 'g')), '')                   AS site_norm
    FROM parsed p
)
INSERT INTO core.owners (
    parcel_id, owner_name, owner_type,
    mailing_address, mailing_city, mailing_state, mailing_zip,
    is_absentee, is_out_of_state, is_current, source, observed_at
)
SELECT
    c.parcel_id,
    c.owner_name,
    c.owner_type,
    c.mailing_address,
    c.mailing_city,
    c.mailing_state,
    c.mailing_zip,
    CASE WHEN c.mail_norm IS NOT NULL AND c.site_norm IS NOT NULL
         THEN c.mail_norm <> c.site_norm
         ELSE NULL END                               AS is_absentee,
    CASE WHEN c.mailing_state IS NOT NULL
         THEN c.mailing_state <> 'MN'
         ELSE NULL END                               AS is_out_of_state,
    TRUE                                             AS is_current,
    'hennepin_parcels'                               AS source,
    c.last_observed_at                               AS observed_at
FROM classified c
ON CONFLICT (parcel_id, source) DO UPDATE SET
    owner_name       = EXCLUDED.owner_name,
    owner_type       = EXCLUDED.owner_type,
    mailing_address  = EXCLUDED.mailing_address,
    mailing_city     = EXCLUDED.mailing_city,
    mailing_state    = EXCLUDED.mailing_state,
    mailing_zip      = EXCLUDED.mailing_zip,
    is_absentee      = EXCLUDED.is_absentee,
    is_out_of_state  = EXCLUDED.is_out_of_state,
    is_current       = EXCLUDED.is_current,
    observed_at      = EXCLUDED.observed_at;
