name: Stub Geocode Backfill
# Geocodes core.parcels rows that carry a real street address and no lat/lng.
#
# ADDED 2026-08-18. 222 parcels on synthetic '{COUNTY}-FC-...' keys have an
# address a person could drive to -- '20088 FERRET ST, NOWTHEN' -- and no
# coordinates, so the product shows no map pin and no Street View for any of
# them. Writing lat/lng also writes geom, which has been a GENERATED column
# since MIGRATION_parcels_geom_generated_2026-08-13.sql, and
# outcome_capture/resolve_parcel_imagery.py then re-opens their
# status='no_location' imagery rows on its next run ("that verdict was true
# when written and is not any more"). One write, three consequences.
#
# MANUAL ONLY, NO SCHEDULE. The script is self-resuming on `lat IS NULL`, so
# once the 222 are done a second run does nothing. A cron would spend Mapbox
# calls on an empty working set forever and give a scheduled job with nothing
# to do, which is how a dead feed hides.
#
# dry_run DEFAULTS TO '1'. redemption-promoter.yml run #1 failed because a
# blank input is an EMPTY env var, not an absent one, so os.environ.get
# returned '' rather than the default. backfill_stub_geocode.py tests
# `DRY_RUN == "1"`, so '' means LIVE -- the safe value has to be the default
# here rather than in the script, and writing for real is a deliberate change
# in the dispatch form.
on:
  workflow_dispatch:
    inputs:
      dry_run:
        description: "1 = print the composed queries and stop (default). 0 = call Mapbox and write lat/lng."
        required: false
        default: '1'
jobs:
  geocode:
    name: Geocode stub parcels with an address
    runs-on: ubuntu-latest
    # 222 addresses at ~0.1s each plus request time. Minutes, not tens of
    # minutes; the ceiling is generous so a slow Mapbox does not truncate a
    # partially-written run.
    timeout-minutes: 20
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'
      # Same reasoning as redemption-promoter.yml: outcome_capture/ scripts
      # talk to Postgres directly via psycopg2 and do not use the FastAPI
      # service's dependency set. `requests` is listed for the same reason --
      # this script is the first in outcome_capture/ to make an outbound HTTP
      # call, so it cannot rely on the service's httpx being present either.
      - name: Install Python dependencies
        run: pip install psycopg2-binary requests
      - name: Geocode stub parcels
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
          MAPBOX_TOKEN: ${{ secrets.MAPBOX_TOKEN }}
          DRY_RUN: ${{ inputs.dry_run }}
        run: |
          python outcome_capture/backfill_stub_geocode.py
