"""
Measure what `source=outdoor` changes about Street View resolution.

Run: python outcome_capture/probe_streetview_source.py

=== WHAT THIS IS FOR ===
resolve_parcel_imagery.py asks Google's metadata endpoint for the nearest
panorama and sends nothing but `location` and `key`. Google's default search
includes INDOOR collections and user photospheres, so for a commercial parcel
in a retail area the nearest panorama is frequently the inside of a shop.
Measured 2026-08-14: 8300 Norman Center Dr, Bloomington — a $48.8M commercial
parcel — renders the interior of a cafe, with nothing in the product saying so.

The metadata endpoint accepts `source=outdoor`, which excludes indoor
collections and excludes photospheres whose indoor/outdoor status cannot be
determined. That is the fix. This script measures its cost BEFORE it ships.

=== WHY EVERY ROW, NOT A SAMPLE ===
All 6,899 `ok` rows were resolved under the unrestricted request, so all 6,899
are suspect. Metadata calls are FREE and consume no image quota, so a census
costs a runner's time and nothing else. A sample would give a confidence
interval where a complete answer is available for the same money.

=== WHY BOTH VARIANTS ARE QUERIED ===
Not "does the panorama change" alone. Also: how far away the outdoor panorama
is (it may be further, and may cross a too_far threshold that the baseline
never reached), what the copyright string says on each, and whether the
location loses coverage entirely. Querying only `outdoor` and diffing against
core.parcel_imagery would compare TODAY's answer to YESTERDAY's, mixing the
source change together with anything Google published in between. The two
variants are asked seconds apart so the only difference is the parameter.

=== THE CONTROL POINT VERIFIES THE PARAMETER IS HONOURED ===
An unrecognised parameter is the dangerous failure here: Google would ignore
it, both variants would return identical panoramas, and the run would look like
proof that no indoor problem exists. So the control point is a coordinate with
KNOWN indoor coverage, and the run ABORTS unless outdoor and baseline actually
differ there. A probe that cannot detect its own instrument being broken is not
a measurement.

=== SELF-RESUMING ===
The frame is keyed the same way the table is: (probe_run, county_code,
parcel_id). Both variant rows for a parcel are written in ONE transaction, so
the 'outdoor' row existing means the parcel is done. A runner cut off half way
costs a re-run, not a restart.
"""

from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from math import atan2, cos, radians, sin, sqrt
from typing import Any

import psycopg2
import psycopg2.extras
import requests


META_URL = "https://maps.googleapis.com/maps/api/streetview/metadata"

BATCH_SIZE = 250
PACE_SECONDS = 0.05
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
MAX_BATCHES = 200

# 810 Maryland Ave E, Saint Paul. The same point resolve_parcel_imagery.py and
# backfill_parcel_geom.py verify against, so a coordinate-order mistake here
# would show up as a difference from two scripts already known good.
CONTROL_LAT, CONTROL_LNG = 44.977053, -93.066465

# hennepin / 1611621310014 — 8300 NORMAN CENTER DR, Bloomington. The parcel
# that renders a cafe interior. Coordinate and pano_id are READ FROM
# core.parcels and core.parcel_imagery (2026-08-14), not estimated: the first
# version of this probe used a hand-guessed coordinate ~3km away, the witness
# assertion correctly aborted the run, and that abort is the only reason a
# 13,798-request census was not spent on a coordinate that was never the
# defect.
WITNESS_LAT, WITNESS_LNG = 44.853085, -93.353105
WITNESS_LABEL = "hennepin/1611621310014 — 8300 NORMAN CENTER DR, Bloomington"

# The panorama core.parcel_imagery currently serves for that parcel, resolved
# 2026-08-13 at 34.4m. The assertion is that the BASELINE request still returns
# exactly this, and that outdoor returns something else.
#
# Asserting on the specific id rather than on "the two answers differ" is the
# whole point. Two answers differ at plenty of coordinates for reasons that
# have nothing to do with indoor imagery; only this id proves the probe is
# pointed at the panorama that produced the defect.
#
# NOTE THE SHAPE. Official Street View captures carry a 22-character opaque id
# (Z0D1wuEgE3qg4bj_E_LuzA, 8IJl6bYtnWVX5NQqysznqg) and report copyright
# "© Google". This one is CAoS-prefixed and dot-terminated — the form Google
# uses for photo ENTITIES, which is what user and business photospheres are.
# The census records both copyright and id so that shape can be counted rather
# than assumed.
WITNESS_EXPECTED_PANO = "CAoSHENJQUJJaEJZZFdITGVjTFJRWmlwQVc4cXlVdzI."


# Every `ok` Street View row, with the coordinate the resolver would use today.
# Ordered so a capped run and the full run cover the same parcels in the same
# sequence — a capped run is then a true prefix of the full one, not a
# different population.
FRAME_SQL = """
SELECT i.county_code                          AS county_code,
       i.parcel_id                            AS parcel_id,
       p.lat::float8                          AS lat,
       p.lng::float8                          AS lng
FROM   core.parcel_imagery i
JOIN   core.parcels p
       ON  p.county_code = i.county_code
       AND p.parcel_id   = i.parcel_id
LEFT   JOIN audit.streetview_source_probe s
       ON  s.probe_run   = %(run)s
       AND s.county_code = i.county_code
       AND s.parcel_id   = i.parcel_id
       AND s.variant     = 'outdoor'
WHERE  i.source   = 'google_streetview'
  AND  i.status   = 'ok'
  AND  p.lat IS NOT NULL
  AND  p.lng IS NOT NULL
  AND  s.parcel_id IS NULL
ORDER  BY i.county_code, i.parcel_id
LIMIT  %(batch)s;
"""

INSERT_SQL = """
INSERT INTO audit.streetview_source_probe
    (probe_run, county_code, parcel_id, variant, req_lat, req_lng,
     status, pano_id, pano_date, pano_copyright, pano_lat, pano_lng,
     distance_m, error_detail)
VALUES %s
ON CONFLICT (probe_run, county_code, parcel_id, variant) DO NOTHING;
"""


def log(msg: str) -> None:
    print(f"[probe] {msg}", flush=True)


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371000.0
    p1, p2 = radians(lat1), radians(lat2)
    dp = radians(lat2 - lat1)
    dl = radians(lng2 - lng1)
    a = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 2 * r * atan2(sqrt(a), sqrt(1 - a))


def fetch_metadata(key: str, lat: float, lng: float,
                   source: str | None = None) -> dict[str, Any] | None:
    """One metadata lookup. Free — does not consume image quota.

    `source=None` reproduces exactly what resolve_parcel_imagery.py sends
    today: location and key, nothing else. Anything added here that is not
    added there would make the baseline a fiction.

    A transport failure is retried. ZERO_RESULTS is NOT: it is an answer, and
    retrying it turns a real finding into a slow one.
    """
    params: dict[str, Any] = {"location": f"{lat},{lng}", "key": key}
    if source:
        params["source"] = source

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(META_URL, params=params,
                                timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            log(f"HTTP {resp.status_code}: {resp.text[:200]}")
            return None
        except requests.RequestException:
            if attempt == MAX_RETRIES:
                return None
            time.sleep(2 ** attempt)
    return None


def probe_row(run: str, cc: str, pid: str, lat: float, lng: float,
              variant: str, meta: dict[str, Any] | None) -> tuple:
    """Flatten one metadata answer into one probe row."""
    if meta is None:
        return (run, cc, pid, variant, lat, lng, "transport_error",
                None, None, None, None, None, None,
                "metadata request failed after retries")

    status = meta.get("status") or "UNKNOWN"
    pano = meta.get("pano_id")
    loc = meta.get("location") or {}
    plat, plng = loc.get("lat"), loc.get("lng")

    if status == "OK" and not pano:
        # streetview_probe_ok_has_pano_ck would reject this row and take the
        # whole batch with it. Record it as its own finding rather than
        # losing 249 good rows to it.
        return (run, cc, pid, variant, lat, lng, "OK_NO_PANO",
                None, meta.get("date"), meta.get("copyright"),
                plat, plng, None,
                "status OK with no pano_id")

    dist = None
    if plat is not None and plng is not None:
        dist = round(haversine_m(lat, lng, plat, plng), 1)

    return (run, cc, pid, variant, lat, lng, status,
            pano, meta.get("date"), meta.get("copyright"),
            plat, plng, dist,
            meta.get("error_message"))


def describe(meta: dict[str, Any] | None) -> str:
    if meta is None:
        return "transport_error"
    pano = meta.get("pano_id")
    # Official captures are a 22-char opaque id; CAoS-prefixed ids are photo
    # entities (user / business photospheres). Printed so the shape is visible
    # in the log without going to the table.
    shape = "photo_entity" if (pano or "").startswith("CAoS") else "streetview"
    return (f"status={meta.get('status')} "
            f"pano={pano} [{shape}] "
            f"date={meta.get('date')} "
            f"copyright={meta.get('copyright')!r}")


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        return 1
    if not key:
        log("FATAL: GOOGLE_MAPS_API_KEY is not set")
        return 1

    run = os.environ.get("PROBE_RUN", "outdoor_v1_2026-08-14")
    max_parcels = int(os.environ.get("MAX_PARCELS", "0"))  # 0 = no cap
    log(f"probe_run={run} max_parcels={max_parcels or 'uncapped'}")

    # --- Control point 1: is the key usable at all? -----------------------
    ctl = fetch_metadata(key, CONTROL_LAT, CONTROL_LNG)
    if ctl is None:
        log("FATAL: metadata endpoint unreachable")
        return 1
    if ctl.get("status") == "REQUEST_DENIED":
        log(f"FATAL: REQUEST_DENIED — {ctl.get('error_message', 'no detail')}")
        return 1
    log(f"control point baseline: {describe(ctl)}")

    ctl_out = fetch_metadata(key, CONTROL_LAT, CONTROL_LNG, source="outdoor")
    if ctl_out is None:
        log("FATAL: metadata endpoint unreachable with source=outdoor")
        return 1
    if ctl_out.get("status") in ("REQUEST_DENIED", "INVALID_REQUEST"):
        log(f"FATAL: source=outdoor rejected — status="
            f"{ctl_out.get('status')} {ctl_out.get('error_message', '')}")
        return 1
    log(f"control point outdoor:  {describe(ctl_out)}")

    # --- Control point 2: is the parameter actually HONOURED? -------------
    # The witness is a coordinate whose nearest panorama is known to be a
    # business interior. If outdoor returns the SAME pano_id there, Google is
    # ignoring the parameter and every row this run writes would be a pair of
    # identical answers misread as "no indoor problem exists".
    w_base = fetch_metadata(key, WITNESS_LAT, WITNESS_LNG)
    w_out = fetch_metadata(key, WITNESS_LAT, WITNESS_LNG, source="outdoor")
    log(f"witness ({WITNESS_LABEL})")
    log(f"  baseline: {describe(w_base)}")
    log(f"  outdoor:  {describe(w_out)}")

    if w_base is None or w_out is None:
        log("FATAL: witness lookup failed — cannot verify the parameter")
        return 1

    if w_base.get("pano_id") != WITNESS_EXPECTED_PANO:
        log(f"FATAL: baseline at the witness returned "
            f"{w_base.get('pano_id')!r}, expected {WITNESS_EXPECTED_PANO!r} "
            f"— the panorama core.parcel_imagery is serving for "
            f"{WITNESS_LABEL}. The defect this probe measures no longer "
            f"reproduces at this coordinate, so the probe cannot claim to "
            f"measure it. Re-read the stored pano_id before re-running.")
        return 1

    if w_out.get("pano_id") == WITNESS_EXPECTED_PANO:
        log("FATAL: source=outdoor returned the SAME panorama as baseline at "
            "the witness — the known indoor case. The parameter is not being "
            "honoured, and a census run would write 13,798 identical pairs "
            "that would read as proof no indoor problem exists. A probe whose "
            "instrument cannot move must not be trusted when it reports no "
            "movement.")
        return 1

    log(f"source=outdoor is honoured — baseline returns the known indoor "
        f"panorama, outdoor returns status={w_out.get('status')} "
        f"pano={w_out.get('pano_id')}")

    conn = psycopg2.connect(dsn)
    counts: dict[str, int] = defaultdict(int)
    done = 0
    batches = 0
    started = time.monotonic()

    try:
        while batches < MAX_BATCHES:
            remaining = BATCH_SIZE
            if max_parcels:
                remaining = min(BATCH_SIZE, max_parcels - done)
                if remaining <= 0:
                    log(f"MAX_PARCELS={max_parcels} reached")
                    break

            with conn.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(FRAME_SQL, {"run": run, "batch": remaining})
                work = cur.fetchall()

            if not work:
                if batches == 0:
                    # A measurement job that finds nothing to measure and
                    # exits 0 is the Washington failure shape: written=118,418
                    # failed=0 with lat NULL on every row. On 2026-08-14 this
                    # script returned green having probed nothing, and the
                    # empty table was read as a query defect for two steps.
                    # An empty frame is now a FAILURE, and it names what it
                    # looked for.
                    log("FATAL: the frame returned ZERO parcels. Expected "
                        "core.parcel_imagery rows with source="
                        "'google_streetview' AND status='ok' whose parcel has "
                        "lat/lng and which have no 'outdoor' row for "
                        f"probe_run={run!r}. Either the frame is genuinely "
                        "exhausted for this label — use a new PROBE_RUN — or "
                        "DATABASE_URL points somewhere without that data.")
                    return 1
                break

            rows: list[tuple] = []
            for r in work:
                cc, pid = r["county_code"], r["parcel_id"]
                lat, lng = r["lat"], r["lng"]

                base = fetch_metadata(key, lat, lng)
                time.sleep(PACE_SECONDS)
                out = fetch_metadata(key, lat, lng, source="outdoor")
                time.sleep(PACE_SECONDS)

                rows.append(probe_row(run, cc, pid, lat, lng, "baseline", base))
                rows.append(probe_row(run, cc, pid, lat, lng, "outdoor", out))

                b_pano = (base or {}).get("pano_id")
                o_pano = (out or {}).get("pano_id")
                o_status = (out or {}).get("status")
                if o_status != "OK":
                    counts["outdoor_lost_coverage"] += 1
                elif b_pano != o_pano:
                    counts["panorama_changed"] += 1
                else:
                    counts["unchanged"] += 1

            # Both variants for every parcel in ONE transaction, so the
            # 'outdoor' row the frame resumes on can never exist without its
            # 'baseline' partner.
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, INSERT_SQL, rows)
            conn.commit()

            done += len(work)
            batches += 1
            elapsed = time.monotonic() - started
            log(f"{done} parcels probed ({batches} batches, "
                f"{done / elapsed:.1f}/sec)")

        if batches >= MAX_BATCHES:
            log(f"WARNING: hit MAX_BATCHES ({MAX_BATCHES}) — re-run to finish")

        print()
        log(f"done: {done} parcels probed, {done * 2} rows written")
        for k, n in sorted(counts.items(), key=lambda kv: -kv[1]):
            log(f"  {k}: {n}")
        log("These counts are a convenience. The measurement is the table — "
            "query audit.streetview_source_probe, do not read this log.")
        return 0
    except Exception as e:
        conn.rollback()
        log(f"FAILED — {type(e).__name__}: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
