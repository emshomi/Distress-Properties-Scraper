"""
Govire outcome checker, v1 (Hennepin County ArcGIS pass).

Pulls due rows from outcomes.redemption_tracker, queries the Hennepin
County Parcels ArcGIS REST layer for current owner / last sale, logs every
check to outcomes.owner_checks, and stamps deterministic outcomes:

  - 'foreclosed'       OWNER_NM/TAXPAYER_NM matches a lender/REO pattern
  - 'foreclosed_sold'  county-recorded SALE_DATE is after redemption expiry
  - 'unknown'          re-check ladder exhausted with no signal (honest label;
                       negative-inference 'redeemed' requires eCRV/recorder
                       confirmation, added in a later stage)

No signal and ladder not exhausted -> advance to the next ladder stage.

Environment:
  DATABASE_URL   Supabase Postgres connection string (required)
  DRY_RUN        if set to '1', perform all reads and print decisions but
                 write nothing to the database

Run: python outcome_capture/outcome_checker.py
"""

import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import requests

HENNEPIN_LAYER_URL = (
    "https://gis.hennepin.us/arcgis/rest/services/"
    "HennepinData/LAND_PROPERTY/MapServer/1/query"
)

OUT_FIELDS = ",".join([
    "PID",
    "OWNER_NM",
    "TAXPAYER_NM",
    "SALE_DATE",
    "SALE_PRICE",
    "SALE_CODE_NAME",
    "FORFEIT_LAND_IND",
    "TORRENS_TYP",
    "ABSTR_TORRENS_CD",
])

BATCH_SIZE = 100          # PIDs per ArcGIS request
REQUEST_TIMEOUT = 60      # seconds
THROTTLE_SECONDS = 2.0    # pause between ArcGIS requests

# Re-check ladder: days after redemption_expiry_date. check_stage N means
# the check at LADDER_OFFSETS[N-1] has been completed.
LADDER_OFFSETS = [30, 60, 90, 180]

# Lender / REO name patterns from the outcome-capture research. Matched
# case-insensitively against OWNER_NM and TAXPAYER_NM.
REO_PATTERNS = [
    r"FEDERAL NATIONAL MORTGAGE",     # Fannie Mae
    r"\bFANNIE\s*MAE\b",
    r"FEDERAL HOME LOAN MORTGAGE",    # Freddie Mac
    r"\bFREDDIE\s*MAC\b",
    r"SECRETARY OF HOUSING",          # HUD
    r"\bHUD\b",
    r"U\.?\s*S\.?\s*BANK.*TRUST",
    r"WILMINGTON TRUST",
    r"WILMINGTON SAVINGS",
    r"DEUTSCHE BANK",
    r"PENNYMAC",
    r"WELLS FARGO.*TRUST",
    r"BANK OF NEW YORK",
    r"HSBC BANK.*TRUST",
    r"CITIBANK.*TRUST",
    r"\bAS TRUSTEE\b",
    r"MORTGAGE\s+(LLC|CORP|INC|COMPANY)",
    r"\bLOAN\s+(TRUST|SERVICING|SERVICES)\b",
    r"\bMERS\b",
    r"MORTGAGE ELECTRONIC REGISTRATION",
    r"\bLAKEVIEW LOAN\b",
    r"\bNEWREZ\b",
    r"\bNATIONSTAR\b",
    r"\bMR\.?\s*COOPER\b",
    r"\bFREEDOM MORTGAGE\b",
    r"\bROCKET MORTGAGE\b",
    r"\bCARRINGTON MORTGAGE\b",
    r"\bSELENE FINANCE\b",
    r"\bUS BANK NATIONAL ASSOC",
    r"\bCREDIT UNION\b",
    r"SAVINGS BANK",
]
REO_REGEX = re.compile("|".join(REO_PATTERNS), re.IGNORECASE)

PID_REGEX = re.compile(r"^\d{13}$")


def log(msg):
    print("[%s] %s" % (datetime.now(timezone.utc).strftime("%H:%M:%S"), msg))


def esri_ms_to_date(value):
    """Convert an Esri epoch-milliseconds field to a date, or None."""
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc).date()
    except (ValueError, TypeError, OSError):
        return None


def match_reo(*names):
    """Return the matched REO pattern text if any name matches, else None."""
    for name in names:
        if not name:
            continue
        m = REO_REGEX.search(name)
        if m:
            return m.group(0)
    return None


def fetch_hennepin_batch(pids):
    """Query the Hennepin parcels layer for a batch of PIDs.

    Returns dict pid -> attributes (first feature wins for stacked parcels).
    """
    where = "PID IN (%s)" % ",".join("'%s'" % p for p in pids)
    payload = {
        "where": where,
        "outFields": OUT_FIELDS,
        "returnGeometry": "false",
        "f": "json",
    }
    resp = requests.post(HENNEPIN_LAYER_URL, data=payload, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    if "error" in body:
        raise RuntimeError("ArcGIS error: %s" % json.dumps(body["error"]))
    result = {}
    for feature in body.get("features", []):
        attrs = feature.get("attributes", {})
        pid = attrs.get("PID")
        if pid and pid not in result:
            result[pid] = attrs
    return result


def next_ladder_date(expiry, today):
    """Smallest ladder date strictly after today, or None if exhausted."""
    for offset in LADDER_OFFSETS:
        candidate = expiry + timedelta(days=offset)
        if candidate > today:
            return candidate
    return None


def decide(row, attrs, today):
    """Return (outcome, ambiguous, detection_source, notes, next_check, stage).

    outcome is None when the record stays pending.
    """
    expiry = row["redemption_expiry_date"]

    if attrs is None:
        nxt = next_ladder_date(expiry, today)
        if nxt is None:
            return ("unknown", True, "hennepin_arcgis",
                    "PID not found in county parcels layer after full ladder "
                    "(possible split/reformat)", None, len(LADDER_OFFSETS))
        return (None, False, None, "PID not found in this pull", nxt,
                min(row["check_stage"] + 1, len(LADDER_OFFSETS)))

    owner = attrs.get("OWNER_NM") or ""
    taxpayer = attrs.get("TAXPAYER_NM") or ""
    sale_dt = esri_ms_to_date(attrs.get("SALE_DATE"))
    forfeit = (attrs.get("FORFEIT_LAND_IND") or "").strip().upper()

    reo_hit = match_reo(owner, taxpayer)
    if reo_hit:
        return ("foreclosed", False, "arcgis_owner_reo_match",
                "REO/lender pattern '%s' matched owner '%s' / taxpayer '%s'"
                % (reo_hit, owner, taxpayer), None, row["check_stage"])

    if forfeit in ("Y", "T", "1"):
        return ("foreclosed", True, "arcgis_forfeit_flag",
                "FORFEIT_LAND_IND=%s (tax forfeiture path, review)" % forfeit,
                None, row["check_stage"])

    if sale_dt and sale_dt > expiry:
        return ("foreclosed_sold", False, "arcgis_sale_after_expiry",
                "County last-sale %s (price %s, code %s) is after redemption "
                "expiry %s" % (sale_dt, attrs.get("SALE_PRICE"),
                               attrs.get("SALE_CODE_NAME"), expiry),
                None, row["check_stage"])

    if sale_dt and row["anchor_date"] < sale_dt <= expiry:
        # Sale recorded inside the redemption window: could be an
        # arm's-length pre-expiry sale or data lag. Flag, keep checking.
        nxt = next_ladder_date(expiry, today)
        if nxt is None:
            return ("unknown", True, "arcgis_sale_in_window",
                    "Sale %s recorded inside redemption window; ladder "
                    "exhausted without REO signal" % sale_dt,
                    None, len(LADDER_OFFSETS))
        return (None, True, None,
                "Sale %s inside redemption window, re-checking" % sale_dt,
                nxt, min(row["check_stage"] + 1, len(LADDER_OFFSETS)))

    # No signal.
    nxt = next_ladder_date(expiry, today)
    if nxt is None:
        return ("unknown", True, "hennepin_arcgis",
                "No REO match, no post-expiry sale after full ladder. "
                "Possible redemption; needs eCRV/recorder confirmation.",
                None, len(LADDER_OFFSETS))
    return (None, False, None, "No signal yet", nxt,
            min(row["check_stage"] + 1, len(LADDER_OFFSETS)))


def main():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        sys.exit(1)
    dry_run = os.environ.get("DRY_RUN") == "1"
    if dry_run:
        log("DRY RUN: no database writes will be made")

    today = date.today()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, county_code, parcel_id, anchor_date,
                   redemption_expiry_date, check_stage
            FROM outcomes.redemption_tracker
            WHERE outcome = 'pending'
              AND next_check_date <= %s
              AND county_code = 'hennepin'
              AND parcel_id ~ '^[0-9]{13}$'
            ORDER BY redemption_expiry_date
            """,
            (today,),
        )
        due = cur.fetchall()

    log("Due Hennepin records with real PINs: %d" % len(due))
    if not due:
        conn.close()
        return

    by_pid = {}
    for row in due:
        by_pid.setdefault(row["parcel_id"], []).append(row)

    pids = sorted(by_pid.keys())
    stamped = {"foreclosed": 0, "foreclosed_sold": 0, "unknown": 0}
    advanced = 0

    for i in range(0, len(pids), BATCH_SIZE):
        batch = pids[i:i + BATCH_SIZE]
        log("ArcGIS batch %d-%d of %d PIDs"
            % (i + 1, i + len(batch), len(pids)))
        try:
            attrs_by_pid = fetch_hennepin_batch(batch)
        except Exception as exc:
            log("Batch failed, skipping (will retry next run): %s" % exc)
            time.sleep(THROTTLE_SECONDS)
            continue

        with conn.cursor() as cur:
            for pid in batch:
                attrs = attrs_by_pid.get(pid)
                for row in by_pid[pid]:
                    outcome, ambiguous, source, notes, nxt, stage = decide(
                        row, attrs, today)

                    owner = (attrs or {}).get("OWNER_NM")
                    reo_hit = None
                    if attrs is not None:
                        reo_hit = match_reo(owner,
                                            (attrs or {}).get("TAXPAYER_NM"))

                    if dry_run:
                        log("  PID %s tracker %d -> %s (%s)"
                            % (pid, row["id"],
                               outcome or "pending/stage %d" % stage, notes))
                        continue

                    cur.execute(
                        """
                        INSERT INTO outcomes.owner_checks
                          (tracker_id, source, owner_name_raw,
                           owner_changed, reo_pattern_matched, raw)
                        VALUES (%s, 'hennepin_arcgis', %s, NULL, %s, %s)
                        """,
                        (row["id"], owner, reo_hit,
                         json.dumps(attrs) if attrs else None),
                    )
                    if outcome:
                        cur.execute(
                            """
                            UPDATE outcomes.redemption_tracker
                            SET outcome = %s,
                                ambiguous = %s,
                                outcome_detected_at = now(),
                                detection_source = %s,
                                detection_notes = %s,
                                check_stage = %s,
                                next_check_date = NULL,
                                updated_at = now()
                            WHERE id = %s
                            """,
                            (outcome, ambiguous, source, notes, stage,
                             row["id"]),
                        )
                        stamped[outcome] += 1
                    else:
                        cur.execute(
                            """
                            UPDATE outcomes.redemption_tracker
                            SET check_stage = %s,
                                next_check_date = %s,
                                ambiguous = %s,
                                detection_notes = %s,
                                updated_at = now()
                            WHERE id = %s
                            """,
                            (stage, nxt, ambiguous, notes, row["id"]),
                        )
                        advanced += 1
        if not dry_run:
            conn.commit()
        time.sleep(THROTTLE_SECONDS)

    log("Done. Stamped: foreclosed=%d foreclosed_sold=%d unknown=%d; "
        "advanced=%d" % (stamped["foreclosed"], stamped["foreclosed_sold"],
                         stamped["unknown"], advanced))
    conn.close()


if __name__ == "__main__":
    main()
