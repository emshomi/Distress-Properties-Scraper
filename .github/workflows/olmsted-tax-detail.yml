name: Olmsted Tax Detail (Tyler portal)
# Weekly + manual. Reads the county iasWorld portal per parcel on the
# annual delinquent list (~502): per-year delinquency detail, computed
# forfeiture clock, owner mailing addresses. Delinquency moves slowly —
# weekly is plenty.
#
# v2 (2026-07-10): DIRECT deep links (datalet.aspx?UseSearch=no&pin=..
# &jur=055&taxyr=..) — 3 plain HTTP GETs per parcel, no Playwright, no
# session, no disclaimer. Datacenter access verified live the same day.
# ~502 parcels x ~2s ≈ 20-25 min.
#
# v2.1 (2026-07-10): the portal enforces a rolling request quota — after
# ~100 rapid parcels it redirects to OverLimit.aspx. The scraper now
# treats that as a cooldown signal (90s wait + retry, 4 attempts), not a
# parcel failure. Full run: ~45-75 min depending on how often the quota
# trips.
#
# The manual dispatch takes an optional comma-separated PIN list — the
# 5-PIN verification path. ALWAYS run that first after any change.
on:
  workflow_dispatch:
    inputs:
      pins:
        description: >-
          Optional comma-separated PARIDs (test mode). Leave empty for
          the full delinquent-list run.
        required: false
        default: ''
  schedule:
    # Tuesdays 16:00 UTC = 10am Central (DST) / 9am (standard). Offset
    # from the dailies (12:00-12:20 UTC), monthly tax jobs (13:30 UTC),
    # and postbulletin (Mon+Thu 14:45 UTC) so nothing overlaps.
    - cron: '0 16 * * 2'
jobs:
  scrape:
    name: Run Olmsted Tyler-portal tax detail
    runs-on: ubuntu-latest
    # Full list ≈ 45-75 min with rate-limit cooldowns.
    timeout-minutes: 150
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'
      - name: Install Python dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
      - name: Run Olmsted tax-detail scraper
        env:
          # --- Required: database ---
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}
          # --- Required: app config (mirror Railway's values) ---
          ENVIRONMENT: production
          LOG_LEVEL: INFO
          ADMIN_API_KEY: ${{ secrets.ADMIN_API_KEY }}
          # --- Geocoding (config may require these) ---
          GEOCODING_ENABLED: 'true'
          GEOCODING_CACHE_DAYS: '30'
          NOMINATIM_USER_AGENT: ${{ secrets.NOMINATIM_USER_AGENT }}
          MAPBOX_TOKEN: ${{ secrets.MAPBOX_TOKEN }}
          # --- Scheduler (only used by Railway, but config may require it) ---
          SCHEDULER_TIMEZONE: America/Chicago
          # --- Feature flags: enable ONLY this job ---
          SCRAPER_ANOKA_SHERIFF_ENABLED: 'false'
          SCRAPER_DAKOTA_SHERIFF_ENABLED: 'false'
          SCRAPER_HENNEPIN_SHERIFF_ENABLED: 'false'
          SCRAPER_HENNEPIN_PARCELS_ENABLED: 'false'
          SCRAPER_HENNEPIN_TAX_ROLL_ENABLED: 'false'
          SCRAPER_MCRO_PROBATE_ENABLED: 'false'
          SCRAPER_MPLS_311_ENABLED: 'false'
          SCRAPER_MPLS_VBR_ENABLED: 'false'
          SCRAPER_RAMSEY_SHERIFF_ENABLED: 'false'
          SCRAPER_RAMSEY_PARCELS_ENABLED: 'false'
          SCRAPER_RAMSEY_TAX_ROLL_ENABLED: 'false'
          SCRAPER_RAMSEY_TFL_ENABLED: 'false'
          SCRAPER_SAINT_PAUL_VACANT_ENABLED: 'false'
          SCRAPER_TAX_FORFEIT_ENABLED: 'false'
          SCRAPER_USPS_VACANCY_ENABLED: 'false'
          SCRAPER_POSTBULLETIN_LEGAL_ENABLED: 'false'
          SCRAPER_OLMSTED_PARCELS_ENABLED: 'false'
          SCRAPER_OLMSTED_TAX_DETAIL_ENABLED: 'true'
        run: |
          python -m scripts.run_olmsted_tax_detail github_actions "${{ github.event.inputs.pins }}"
