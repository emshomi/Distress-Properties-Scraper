"""
Govire outcome checker, v2 (Hennepin + Dakota + Washington).

Pulls due rows from outcomes.redemption_tracker, queries each county's
parcel ArcGIS REST layer for current owner / last sale, logs every check to
outcomes.owner_checks, and stamps deterministic outcomes:

  - 'foreclosed'       owner name matches a lender/REO pattern
  - 'foreclosed_sold'  county-recorded SALE_DATE is after redemption expiry
  - 'unknown'          re-check ladder exhausted with no signal (honest label;
                       negative-inference 'redeemed' requires eCRV/recorder
                       confirmation, added in a later stage)

No signal and ladder not exhausted -> advance to the next ladder stage.

Reporting (v2):
  - outcomes.checker_runs: one heartbeat row per execution, ALWAYS written
    (including dry runs and zero-due runs), with counts and status.
  - audit.source_health: upserted as source 'outcome_checker' on live runs,
    so the existing daily health-digest email covers this job.

Environment:
  DATABASE_URL   Supabase Postgres connection string (required; Session
                 pooler string, plain postgresql:// scheme)
  DRY_RUN        '1' = read-only for outcome data; still writes the
                 checker_runs heartbeat row (flagged dry_run=true), never
                 touches source_health

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

# ---------------------------------------------------------------------------
# County endpoint configuration (all verified live 2026-07-06)
# ---------------------------------------------------------------------------
COUNTY_CONFIG = {
    "hennepin": {
        "url": ("https://gis.hennepin.us/arcgis/rest/services/"
                "HennepinData/LAND_PROPERTY/MapServer/1/query"),
        "pin_field": "PID",
        "owner_fields": ["OWNER_NM", "TAXPAYER_NM"],
        "sale_date_field": "SALE_DATE",
        "sale_price_field": "SALE_PRICE",
        "forfeit_field": "FORFEIT_LAND_IND",
        "extra_fields": ["SALE_CODE_NAME", "TORRENS_TYP", "ABSTR_TORRENS_CD"],
        "pin_variants": lambda pin: [pin],
        "check_source": "hennepin_arcgis",
    },
    "dakota": {
        "url": ("https://gis2.co.dakota.mn.us/arcgis/rest/services/"
                "DCGIS_OL_PropertyInformation/MapServer/71/query"),
        "pin_field": "PIN",
        "owner_fields": ["FULLNAME", "JOINT_OWNER"],
        "sale_date_field": "SALE_DATE",
        "sale_price_field": "SALE_VALUE",
        "forfeit_field": None,
        "extra_fields": ["TAXPIN", "HOMESTEAD", "Update_Date"],
        "pin_variants": lambda pin: [pin],
        "check_source": "dakota_arcgis",
    },
    "washington": {
        "url": ("https://maps.co.washington.mn.us/arcgis/rest/services/"
                "GISViewer/Parcels/MapServer/0/query"),
        "pin_field": "PIN",
        "owner_fields": ["OWNER_NAME", "OWNER_MORE"],
        "sale_date_field": "SALE_DATE",
        "sale_price_field": "SALE_VALUE",
        "forfeit_field": None,
        "extra_fields": ["TAXPIN", "HOMESTEAD", "EMV_TOTAL"],
        # Washington's PIN field is 17 chars: likely dotted 2.3.2.2.4
        # (e.g. 21.030.20.33.0102). We query BOTH raw and dotted variants
        # and map results back by stripping non-digits.
        "pin_variants": lambda pin: [
            pin,
            "%s.%s.%s.%s.%s" % (pin[0:2], pin[2:5], pin[5:7],
                                pin[7:9], pin[9:13]),
        ],
        "check_source": "washington_arcgis",
    },
}

BATCH_SIZE = 100          # tracker PIDs per ArcGIS request
REQUEST_TIMEOUT = 60      # seconds
THROTTLE_SECONDS = 2.0    # pause between ArcGIS requests

# Re-check ladder: days after redemption_expiry_date. check_stage N means
# the check at LADDER_OFFSETS[N-1] has been completed.
LADDER_OFFSETS = [30, 60, 90, 180]

# Lender / REO name patterns. Matched case-insensitively against all
# configured owner fields. Broad by design; every hit is logged with the
# exact pattern for audit and tuning.
REO_PATTERNS = [
    r"FEDERAL NATIONAL MORTGAGE",     # Fannie Mae
    r"\bFANNIE\s*MAE\b",
    r"FEDERAL HOME LOAN MORTGAGE",    # Freddie Mac
    r"FED HOME LOAN MORTGAGE",
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
    r"\bTRUSTEE\b",
    r"MORTGAGE\s+(LLC|CORP|INC|COMPANY|CORPORATION)",
    r"\bLOAN\s+(TRUST|SERVICING|SERVICES)\b",
    r"\bMTG\s+LOAN\s+TRUST\b",
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

DIGITS_ONLY = re.compile(r"\D+")


def log(msg):
    print("[%s] %s" % (datetime.now(timezone.utc).strftime("%H:%M:%S"), msg))


def norm_pin(value):
    """Normalize any PIN representation to its digits-only form."""
    if value is None:
        return None
    return DIGITS_ONLY.sub("", str(value))


def esri_ms_to_date(value):
    """Convert an Esri epoch-milliseconds field to a date, or None."""
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc).date()
    except (ValueError, TypeError, OSError):
        return None


def match_reo(names):
    """Return the matched REO pattern text if any name matches, else None."""
    for name in names:
        if not name:
            continue
        m = REO_REGEX.search(name)
        if m:
            return m.group(0)
    return None


def fetch_county_batch(cfg, pins):
    """Query one county's parcel layer for a batch of digits-only PINs.

    Returns dict digits_only_pin -> attributes (first feature wins).
    """
    variants = []
    for pin in pins:
        variants.extend(cfg["pin_variants"](pin))
    where = "%s IN (%s)" % (cfg["pin_field"],
                            ",".join("'%s'" % v for v in variants))
    out_fields = [cfg["pin_field"]] + cfg["owner_fields"]
    for f in (cfg["sale_date_field"], cfg["sale_price_field"],
              cfg["forfeit_field"]):
        if f:
            out_fields.append(f)
    out_fields.extend(cfg["extra_fields"])
    payload = {
        "where": where,
        "outFields": ",".join(out_fields),
        "returnGeometry": "false",
        "f": "json",
    }
    resp = requests.post(cfg["url"], data=payload, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    if "error" in body:
        raise RuntimeError("ArcGIS error: %s" % json.dumps(body["error"]))
    result = {}
    for feature in body.get("features", []):
        attrs = feature.get("attributes", {})
        key = norm_pin(attrs.get(cfg["pin_field"]))
        if key and key not in result:
            result[key] = attrs
    return result


def next_ladder_date(expiry, today):
    """Smallest ladder date strictly after today, or None if exhausted."""
    for offset in LADDER_OFFSETS:
        candidate = expiry + timedelta(days=offset)
        if candidate > today:
            return candidate
    return None


def decide(cfg, row, attrs, today):
    """Return (outcome, ambiguous, detection_source, notes, next_check, stage).

    outcome is None when the record stays pending.
    """
    expiry = row["redemption_expiry_date"]
    src = cfg["check_source"]

    if attrs is None:
        nxt = next_ladder_date(expiry, today)
        if nxt is None:
            return ("unknown", True, src,
                    "PID not found in county parcels layer after full ladder "
                    "(possible split/reformat)", None, len(LADDER_OFFSETS))
        return (None, False, None, "PID not found in this pull", nxt,
                min(row["check_stage"] + 1, len(LADDER_OFFSETS)))

    owner_values = [attrs.get(f) or "" for f in cfg["owner_fields"]]
    sale_dt = esri_ms_to_date(attrs.get(cfg["sale_date_field"]))
    forfeit = ""
    if cfg["forfeit_field"]:
        forfeit = (attrs.get(cfg["forfeit_field"]) or "").strip().upper()

    reo_hit = match_reo(owner_values)
    if reo_hit:
        return ("foreclosed", False, src + "_owner_reo_match",
                "REO/lender pattern '%s' matched owner(s) %s"
                % (reo_hit, " / ".join("'%s'" % v for v in owner_values)),
                None, row["check_stage"])

    if forfeit in ("Y", "T", "1"):
        return ("foreclosed", True, src + "_forfeit_flag",
                "Forfeit flag=%s (tax forfeiture path, review)" % forfeit,
                None, row["check_stage"])

    if sale_dt and sale_dt > expiry:
        return ("foreclosed_sold", False, src + "_sale_after_expiry",
                "County last-sale %s (value %s) is after redemption expiry %s"
                % (sale_dt, attrs.get(cfg["sale_price_field"]), expiry),
                None, row["check_stage"])

    if sale_dt and row["anchor_date"] < sale_dt <= expiry:
        # Sale recorded inside the redemption window: could be an
        # arm's-length pre-expiry sale or data lag. Flag, keep checking.
        nxt = next_ladder_date(expiry, today)
        if nxt is None:
            return ("unknown", True, src + "_sale_in_window",
                    "Sale %s recorded inside redemption window; ladder "
                    "exhausted without REO signal" % sale_dt,
                    None, len(LADDER_OFFSETS))
        return (None, True, None,
                "Sale %s inside redemption window, re-checking" % sale_dt,
                nxt, min(row["check_stage"] + 1, len(LADDER_OFFSETS)))

    # No signal.
    nxt = next_ladder_date(expiry, today)
    if nxt is None:
        return ("unknown", True, src,
                "No REO match, no post-expiry sale after full ladder. "
                "Possible redemption; needs eCRV/recorder confirmation.",
                None, len(LADDER_OFFSETS))
    return (None, False, None, "No signal yet", nxt,
            min(row["check_stage"] + 1, len(LADDER_OFFSETS)))


# ---------------------------------------------------------------------------
# Run heartbeat (outcomes.checker_runs) and health (audit.source_health)
# ---------------------------------------------------------------------------

def heartbeat_start(conn, dry_run):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO outcomes.checker_runs (dry_run) VALUES (%s) "
            "RETURNING id",
            (dry_run,),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def heartbeat_finish(conn, run_id, due_count, stamped, advanced,
                     status, error_detail=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outcomes.checker_runs
            SET finished_at = now(),
                due_count = %s,
                stamped_foreclosed = %s,
                stamped_foreclosed_sold = %s,
                stamped_unknown = %s,
                advanced = %s,
                status = %s,
                error_detail = %s
            WHERE id = %s
            """,
            (due_count, stamped.get("foreclosed", 0),
             stamped.get("foreclosed_sold", 0), stamped.get("unknown", 0),
             advanced, status, error_detail, run_id),
        )
    conn.commit()


def report_source_health(conn, ok, note=""):
    """Upsert audit.source_health row for 'outcome_checker'.

    Follows the same conventions as the scraper health tracker: success
    clears notes and resets consecutive_failures; failure increments the
    counter and records the error note. Update-then-insert so no unique
    constraint is required.
    """
    with conn.cursor() as cur:
        if ok:
            cur.execute(
                """
                UPDATE audit.source_health
                SET last_successful_run_at = now(),
                    consecutive_failures = 0,
                    is_healthy = true,
                    notes = '',
                    updated_at = now()
                WHERE source_name = 'outcome_checker'
                """
            )
        else:
            cur.execute(
                """
                UPDATE audit.source_health
                SET last_failed_run_at = now(),
                    consecutive_failures = COALESCE(consecutive_failures,0)+1,
                    is_healthy = false,
                    notes = %s,
                    updated_at = now()
                WHERE source_name = 'outcome_checker'
                """,
                (note[:500],),
            )
        if cur.rowcount == 0:
            cur.execute(
                """
                INSERT INTO audit.source_health
                  (source_name, last_successful_run_at, last_failed_run_at,
                   consecutive_failures, is_healthy, notes, updated_at)
                VALUES ('outcome_checker',
                        CASE WHEN %s THEN now() END,
                        CASE WHEN NOT %s THEN now() END,
                        CASE WHEN %s THEN 0 ELSE 1 END,
                        %s, %s, now())
                """,
                (ok, ok, ok, ok, "" if ok else note[:500]),
            )
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process(conn, dry_run, today):
    """Run all county checks. Returns (due_count, stamped, advanced)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, county_code, parcel_id, anchor_date,
                   redemption_expiry_date, check_stage
            FROM outcomes.redemption_tracker
            WHERE outcome = 'pending'
              AND next_check_date <= %s
              AND county_code = ANY(%s)
              AND parcel_id ~ '^[0-9]{13}$'
            ORDER BY county_code, redemption_expiry_date
            """,
            (today, list(COUNTY_CONFIG.keys())),
        )
        due = cur.fetchall()

    log("Due records with real PINs: %d" % len(due))
    stamped = {"foreclosed": 0, "foreclosed_sold": 0, "unknown": 0}
    advanced = 0
    if not due:
        return 0, stamped, advanced

    by_county = {}
    for row in due:
        by_county.setdefault(row["county_code"], {}) \
                 .setdefault(row["parcel_id"], []).append(row)

    for county, by_pid in by_county.items():
        cfg = COUNTY_CONFIG[county]
        pids = sorted(by_pid.keys())
        log("%s: %d due records, %d unique PINs"
            % (county, sum(len(v) for v in by_pid.values()), len(pids)))

        for i in range(0, len(pids), BATCH_SIZE):
            batch = pids[i:i + BATCH_SIZE]
            log("%s ArcGIS batch %d-%d of %d PINs"
                % (county, i + 1, i + len(batch), len(pids)))
            try:
                attrs_by_pin = fetch_county_batch(cfg, batch)
            except Exception as exc:
                log("%s batch failed, skipping (will retry next run): %s"
                    % (county, exc))
                time.sleep(THROTTLE_SECONDS)
                continue
            log("%s: %d of %d PINs matched in county layer"
                % (county, len(attrs_by_pin), len(batch)))

            with conn.cursor() as cur:
                for pid in batch:
                    attrs = attrs_by_pin.get(pid)
                    for row in by_pid[pid]:
                        outcome, ambiguous, source, notes, nxt, stage = \
                            decide(cfg, row, attrs, today)

                        owner = None
                        reo_hit = None
                        if attrs is not None:
                            owner_vals = [attrs.get(f) or ""
                                          for f in cfg["owner_fields"]]
                            owner = owner_vals[0] or None
                            reo_hit = match_reo(owner_vals)

                        if dry_run:
                            log("  %s PIN %s tracker %d -> %s (%s)"
                                % (county, pid, row["id"],
                                   outcome or "pending/stage %d" % stage,
                                   notes))
                            continue

                        cur.execute(
                            """
                            INSERT INTO outcomes.owner_checks
                              (tracker_id, source, owner_name_raw,
                               owner_changed, reo_pattern_matched, raw)
                            VALUES (%s, %s, %s, NULL, %s, %s)
                            """,
                            (row["id"], cfg["check_source"], owner, reo_hit,
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

    return len(due), stamped, advanced


def main():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("FATAL: DATABASE_URL is not set")
        sys.exit(1)
    dry_run = os.environ.get("DRY_RUN") == "1"
    if dry_run:
        log("DRY RUN: outcome data is read-only "
            "(heartbeat row still written)")

    today = date.today()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    run_id = heartbeat_start(conn, dry_run)
    log("Heartbeat run id: %d" % run_id)

    try:
        due_count, stamped, advanced = process(conn, dry_run, today)
    except Exception as exc:
        conn.rollback()
        err = "%s: %s" % (type(exc).__name__, exc)
        log("FAILED: %s" % err)
        heartbeat_finish(conn, run_id, None, {}, 0, "error", err)
        if not dry_run:
            report_source_health(conn, ok=False, note=err)
        conn.close()
        sys.exit(1)

    heartbeat_finish(conn, run_id, due_count, stamped, advanced, "ok")
    if not dry_run:
        report_source_health(conn, ok=True)

    log("Done. Stamped: foreclosed=%d foreclosed_sold=%d unknown=%d; "
        "advanced=%d" % (stamped["foreclosed"], stamped["foreclosed_sold"],
                         stamped["unknown"], advanced))
    conn.close()


if __name__ == "__main__":
    main()
