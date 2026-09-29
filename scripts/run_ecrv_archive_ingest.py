"""Load one year of the MN DOR eCRV archive into outcomes.ecrv_sales_history.

WHAT THIS IS FOR (2026-09-29)
The Department of Revenue opened its "eCRV Extracts - Archives" room to us for
14 days: 482 weekly extracts, 2014-10-06 to 2023-12-25, the same format as the
weekly files. All 482 were uploaded to the 'ecrv-extracts' bucket the same day
(uploaded=482 failed=0). This loads them, one year per run.

WHY A SEPARATE TABLE
outcomes.ecrv_sales feeds live deal math (scoring.comp_ratios), the AVM
training set, and the buyer/investor activity views. Ten years of old prices
there would move all of them. The archive goes to outcomes.ecrv_sales_history,
which nothing downstream reads. Its one job: owner-at-a-point-in-time lookups
for the redemption checker ("who owned this house when it was foreclosed").

WHAT IT DOES NOT DO
No matview refresh, no AVM rebuild, no unmapped-county report. Those steps in
the weekly workflow are about ecrv_sales and would be wrong here.

GUARDS
- Year must be 2014..2023. 2024+ files belong in ecrv_sales, not here.
- Only objects named YYYY-MM-DD-HH-MM-SS_eCRVExtract.zip for that year.
- --dry-run parses every file and writes nothing. Run it first for 2014,
  the oldest format.

PARSE HEALTH
For each file it prints xml_files (XML documents in the zip) next to
certificates (those the parser could key). A gap means certificates the
parser dropped — no CRV id, no parcels, or unparseable XML. Current weekly
files show a gap of 0; an older format would show up here first.

Idempotent: re-running a year upserts on (crv_number_id, parcel_id_raw).

Usage:
    python -m scripts.run_ecrv_archive_ingest 2014 --dry-run
    python -m scripts.run_ecrv_archive_ingest 2014
    python -m scripts.run_ecrv_archive_ingest 2015 --limit 2      # first 2 files

Exits 0 when every file loaded with no failed rows, 1 otherwise.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import traceback
import zipfile

from src.db.supabase_client import get_client
from src.scrapers.ecrv_extract import (
    STORAGE_BUCKET,
    download_from_storage,
    ingest_zip,
    iter_zip_rows,
)

TARGET_TABLE = "ecrv_sales_history"
FIRST_YEAR, LAST_YEAR = 2014, 2023
NAME_RE = re.compile(r"^(\d{4})-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}_eCRVExtract\.zip$")


def list_year(year: int) -> list[str]:
    """Every archive object for one year, sorted by name (= by date).

    PAGINATED: Supabase Storage list() returns 100 objects by default, and the
    bucket now holds 500+. An unpaginated list would silently miss most years.
    """
    client = get_client()
    prefix = f"{year}-"
    names: list[str] = []
    offset = 0
    page = 1000
    while True:
        res = client.storage.from_(STORAGE_BUCKET).list(
            "",
            {
                "limit": page,
                "offset": offset,
                "search": prefix,
                "sortBy": {"column": "name", "order": "asc"},
            },
        )
        batch = [o["name"] for o in (res or []) if o.get("name")]
        names.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return sorted(n for n in names if n.startswith(prefix) and NAME_RE.match(n))


def count_xml(zip_path: str) -> int:
    with zipfile.ZipFile(zip_path) as zf:
        return sum(1 for n in zf.namelist() if n.lower().endswith(".xml"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("year", type=int, help="archive year, 2014..2023")
    ap.add_argument("--dry-run", action="store_true",
                    help="parse every file, write nothing")
    ap.add_argument("--limit", type=int, default=0,
                    help="only the first N files of the year (0 = all)")
    args = ap.parse_args()

    if not (FIRST_YEAR <= args.year <= LAST_YEAR):
        print(f"[archive] year {args.year} out of range "
              f"{FIRST_YEAR}..{LAST_YEAR} — 2024+ belongs in ecrv_sales",
              flush=True)
        return 1

    mode = "DRY RUN (no writes)" if args.dry_run else f"WRITE -> outcomes.{TARGET_TABLE}"
    print(f"[archive] year={args.year} mode={mode}", flush=True)

    names = list_year(args.year)
    print(f"[archive] {len(names)} objects found for {args.year} in "
          f"bucket '{STORAGE_BUCKET}'", flush=True)
    if not names:
        return 1
    if args.limit:
        names = names[: args.limit]
        print(f"[archive] --limit {args.limit}: processing {len(names)}",
              flush=True)

    tot = {"files": 0, "xml": 0, "certs": 0, "rows": 0,
           "written": 0, "failed": 0, "dropped": 0}
    bad_files: list[str] = []

    for i, name in enumerate(names, 1):
        path = None
        try:
            path = download_from_storage(name)
            xml_files = count_xml(path)
            if args.dry_run:
                rows = list(iter_zip_rows(path, name))
                certs = len({r["crv_number_id"] for r in rows})
                stats = {"parcel_rows": len(rows), "certificates": certs,
                         "written": 0, "failed": 0}
                dates = sorted(r["deed_date"] for r in rows if r.get("deed_date"))
                span = f" deeds {dates[0]}..{dates[-1]}" if dates else ""
            else:
                stats = ingest_zip(path, source_file=name,
                                   target_table=TARGET_TABLE)
                span = ""
            dropped = max(xml_files - stats["certificates"], 0)
            print(
                f"[archive] {i:>2}/{len(names)} {name} xml_files={xml_files} "
                f"certificates={stats['certificates']} dropped={dropped} "
                f"parcel_rows={stats['parcel_rows']} written={stats['written']} "
                f"failed={stats['failed']}{span}",
                flush=True,
            )
            tot["files"] += 1
            tot["xml"] += xml_files
            tot["certs"] += stats["certificates"]
            tot["rows"] += stats["parcel_rows"]
            tot["written"] += stats["written"]
            tot["failed"] += stats["failed"]
            tot["dropped"] += dropped
            if stats["parcel_rows"] == 0 or stats["failed"] > 0:
                bad_files.append(name)
        except Exception as e:
            print(f"[archive] {i:>2}/{len(names)} {name} ERROR "
                  f"{type(e).__name__}: {e}", flush=True)
            traceback.print_exc()
            bad_files.append(name)
        finally:
            if path:
                try:
                    os.unlink(path)
                except OSError:
                    pass

    pct = (100.0 * tot["dropped"] / tot["xml"]) if tot["xml"] else 0.0
    print(
        f"[archive] TOTAL year={args.year} files={tot['files']}/{len(names)} "
        f"xml_files={tot['xml']} certificates={tot['certs']} "
        f"dropped={tot['dropped']} ({pct:.2f}%) parcel_rows={tot['rows']} "
        f"written={tot['written']} failed={tot['failed']}",
        flush=True,
    )
    if bad_files:
        print(f"[archive] {len(bad_files)} file(s) need attention: "
              + ", ".join(bad_files), flush=True)
        return 1
    print("[archive] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
