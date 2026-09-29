"""
eCRV (Electronic Certificate of Real Estate Value) weekly-extract ingest.

Source: Minnesota Department of Revenue — Weekly Sales Extract
        Access requested via ecrv.support@state.mn.us. The state emails an
        alert when a new extract is ready; the zip is downloaded manually.
        There is NO fetchable URL — the eCRV SOAP Web Services API exists
        but production credentials require being the eCRV contact person at
        a county or city office, which Govire is not.

An eCRV must be filed for every Minnesota real property transfer with
consideration over $3,000, so this is the state's authoritative record of
what property ACTUALLY SOLD FOR — as opposed to the assessor's estimate in
core.parcels.emv_total. It is the evidence behind the portal's claim that
outcomes are "confirmed from county records and state deed filings, not
inferred," and it feeds the scoring.comp_ratios / distress_multipliers
calibration views.

=== TWO TARGET TABLES (added 2026-09-29) ===
  outcomes.ecrv_sales          the live table, 2024 onward. Everything
                               downstream reads it: comp_ratios,
                               distress_multipliers, avm_training,
                               ecrv_buyer_activity, investor_activity,
                               reo_seller_patterns, distressed_exit_sales.
                               DEFAULT — the weekly run is unchanged.
  outcomes.ecrv_sales_history  the MN DOR archive room, 2014-10 to 2023-12
                               (482 weekly files). Same columns, same unique
                               key, deliberately SEPARATE so ten years of old
                               prices cannot move live deal math, the AVM or
                               the investor lists. Used for owner-at-a-point-
                               in-time lookups (the redemption checker).
                               Written only by scripts/run_ecrv_archive_ingest.

The target is an allow-list, not free text: a typo must fail loudly rather
than create writes against an unintended table.

=== TWO XML LAYOUTS (found 2026-09-29, 2014 archive dry run) ===
The archive's early files (2014, possibly later years) are an OLDER export:
no <?xml?> declaration, root <us.mn.state.mdor.ecrv.extract.form.EcrvForm>,
and repeated items wrapped one level deeper:

    modern:  <parcels><parcelId>..</parcelId></parcels>            (one per parcel)
    legacy:  <parcels><...ParcelForm><parcelId>..</parcelId></...ParcelForm>
                      <...ParcelForm>...</...ParcelForm></parcels>
    modern:  <individuals><firstName>..</firstName>..</individuals>  (one per person)
    legacy:  <individuals><...StandardBuyerSellerForm>..</...>
                          <...StandardBuyerSellerForm>..</...></individuals>

The first 2014 dry run read 0 of 31,649 certificates: the CRV id was found,
but parcelId sat one level deeper, so every certificate looked parcel-less
and was skipped. _items() now reads both layouts. The modern path is
unchanged: an element that carries the fields directly is used as-is.

Legacy files also carry invalid XML character references (e.g. &#11;) that
even the cp1252 repair cannot parse; a second repair strips them.

=== HOW THE ZIP REACHES THIS CODE ===
Uploaded to the PRIVATE Supabase Storage bucket 'ecrv-extracts' via the
dashboard, then this loader downloads it by object name. That is deliberate:
GitHub Actions cannot read a file on a home PC, and the previous arrangement
(a local script, never committed) left 322,728 rows of history with no
loader in version control and two extracts unloaded. The local-scrapers
README already documents what that costs — a deleted local copy took the
mnpublicnotice pipeline down for a week.

Only mnpublicnotice genuinely requires a home IP (Railway's datacenter IP is
blocked). eCRV has no such constraint; it was local only because that is
where the download landed.

=== WHY THIS IS NOT A BaseScraper SUBCLASS ===
There is nothing to fetch on a schedule. The run is triggered by hand after
the state's email, so there is no cron and no feature flag — the same
reasoning that keeps startribune_legal a standalone module.

=== WHAT THE EXTRACT CARRIES BEYOND PRICES ===
Counts from the 2026-07-27 extract (n=2700 certificates):
  nonListedInd  true    395 (14.6%)  never hit the market
  deedTypeCde   PERREPDEED  56       personal representative = ESTATE sale
                TRUSTEE    195       trustee conveyance
                CONFORDEED  60       contract for deed
  financeType   CASH       743       investor / non-financed activity
  relatedInd    true      123        non-arm's-length — MUST be excluded
                                     from any comps calculation

=== ONE ROW PER PARCEL ===
A certificate can list several parcels, so each ParcelSubmission becomes its
own row and the table's UNIQUE (crv_number_id, parcel_id_raw) makes
re-ingesting a file idempotent. Weekly extracts overlap and corrected
certificates are re-delivered, so re-running is expected and safe.

parcel_norm is a plain digits-only normalization of parcel_id_raw
('19-32100-04-050' -> '193210004050'), matching the 322,728 rows already
loaded. It is NOT resolved against core.parcels here — see below.

=== A NOTE ON JOINING TO core.parcels (verified live 2026-07-28) ===
Do NOT assume parcel_norm joins to core.parcels.parcel_id for every county.
Measured against the full history:

    hennepin    97.2%   dakota       0.0%  <-- on parcel_norm
    ramsey      96.5%   dakota      98.4%  <-- on raw_data->>'TAXPIN'
    washington  97.1%   anoka       97.2%
    olmsted     94.2%   wabasha     94.1%
    fillmore    97.3%

Dakota County publishes TWO parallel identifier systems: its GIS layer uses
a PLSS-derived 13-digit PIN (36-115-20-77-0066), which is what core.parcels
stores, while eCRV records the 12-digit tax PIN (01-18050-01-010). No
arithmetic converts one to the other. The bridge is the TAXPIN column on
Dakota's layer 71, carried in core.parcels.raw_data. Proven end to end on
15706 Diamond Way: eCRV 01-18050-01-010 -> TAXPIN 011805001010 ->
PIN 3611520770066 -> $685,000 on 2026-07-16.

That belongs in the CONSUMER, not here — this loader stores what the state
published and nothing more.

=== PII: DELIBERATELY DROPPED ===
The extract carries buyer/seller daytimePhone (2,324 of 2,700) and email
(265). Names are kept — they feed owner matching and the estate-sale signal.
Phone and email are NOT stored: they add nothing to the product and would
create a data-handling obligation that exists nowhere else in this system.
FEINs appearing in certificates of real estate value are classified as
private/nonpublic under Minn. Stat. ch. 13, so the file is not uniformly
public data. Parse them, drop them on the floor.

Usage:
    python -m scripts.run_ecrv_ingest 2026-07-27-02-10-41_eCRVExtract.zip
    python -m scripts.run_ecrv_ingest --local /path/to/extract.zip
    python -m scripts.run_ecrv_archive_ingest 2014 [--dry-run]
"""

from __future__ import annotations

import os
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator, Optional
import xml.etree.ElementTree as ET

from src.db.supabase_client import get_client, outcomes_table
from src.utils.logger import logger


STORAGE_BUCKET = "ecrv-extracts"

DEFAULT_TARGET_TABLE = "ecrv_sales"
ALLOWED_TARGET_TABLES = frozenset({"ecrv_sales", "ecrv_sales_history"})

_DB_BATCH_SIZE = 500

# Count of XML documents that needed a repair to parse, per ingest. Logged once
# per file as a number instead of once per certificate: the 2014 dry run
# printed ~31,000 identical "repaired" lines.
_REPAIRS = {"cp1252": 0, "charref": 0, "ctrl": 0}


def reset_repair_counts() -> None:
    for k in _REPAIRS:
        _REPAIRS[k] = 0


def repair_counts() -> dict[str, int]:
    return dict(_REPAIRS)


# XML 1.0 forbids character references to control characters other than
# tab (9), LF (10) and CR (13).
_BAD_CHARREF = re.compile(
    rb"&#(?:x0*(?:[0-8bBcCeEfF]|1[0-9a-fA-F])|0*(?:[0-8]|1[124-9]|2[0-9]|3[01]));"
)

# THIRD REPAIR (2026-09-29): from 2021 on, ~30-40 certificates a year fail
# with "not well-formed (invalid token)" even after the two repairs above.
# Two causes produce exactly that error from expat:
#   - raw control bytes (0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F) pasted into a text
#     field, illegal anywhere in XML 1.0;
#   - a bare "&" in text that is not the start of an entity (e.g. "A & B").
# Strip the first, escape the second. Tab, LF and CR are left alone.
_RAW_CTRL = re.compile(rb"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_BARE_AMP = re.compile(rb"&(?!#[0-9]+;|#x[0-9a-fA-F]+;|[A-Za-z_][A-Za-z0-9._-]*;)")


def _deep_repair(data: bytes) -> bytes:
    data = _BAD_CHARREF.sub(b"", data)
    data = _RAW_CTRL.sub(b"", data)
    return _BARE_AMP.sub(b"&amp;", data)


def _items(parent: Optional[ET.Element], tag: str, marker_fields: tuple[str, ...]) -> list[ET.Element]:
    """Return the item elements under parent/<tag>, for either XML layout.

    modern: each <tag> element IS an item (it carries a marker field directly)
    legacy: each <tag> element WRAPS items (its children carry the fields)
    """
    if parent is None:
        return []
    out: list[ET.Element] = []
    for el in parent.findall(tag):
        if any(el.find(f) is not None for f in marker_fields):
            out.append(el)
        else:
            out.extend(child for child in el
                       if any(child.find(f) is not None for f in marker_fields))
    return out


def _txt(node: Optional[ET.Element], path: str) -> Optional[str]:
    if node is None:
        return None
    el = node.find(path)
    if el is None or el.text is None:
        return None
    return el.text.strip() or None


def _decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
        return d if d >= 0 else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    s = str(value).strip().lower()
    if s == "true":
        return True
    if s == "false":
        return False
    return None


def _date_only(value: Optional[str]) -> Optional[str]:
    """eCRV dates arrive as '2026-06-30T00:00:00-05:00'. deed_date is a DATE
    column, so keep the calendar day and drop the offset — converting to UTC
    would shift a late-evening Central deed onto the following day."""
    if not value:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", value.strip())
    return m.group(1) if m else None


def _party_names(form: Optional[ET.Element]) -> list[str]:
    """Collect buyer or seller names as a list (the column is a text ARRAY).

    Handles BOTH <individuals> and <organizations>: the 2026-07-27 extract
    carried 3,831 individual and 388 organization buyers, and reading only
    individuals would silently drop every LLC, builder and bank purchase —
    exactly the buyer class that matters most for investor-activity signals.

    Names only. daytimePhone and email are deliberately not read.
    """
    if form is None:
        return []
    names: list[str] = []
    for ind in _items(form, "individuals", ("firstName", "lastName", "middleName")):
        parts = [
            _txt(ind, "firstName"),
            _txt(ind, "middleName"),
            _txt(ind, "lastName"),
            _txt(ind, "nameSuffix"),
        ]
        joined = " ".join(p for p in parts if p)
        if joined:
            names.append(joined)
    for org in _items(form, "organizations", ("organizationName",)):
        name = _txt(org, "organizationName")
        if name:
            names.append(name)
    return names


def parse_ecrv_xml(xml_bytes: bytes, source_file: str) -> list[dict[str, Any]]:
    """Parse one eCRV XML into ONE ROW PER PARCEL.

    Returns [] when the certificate has no CRV id or lists no parcels —
    there would be nothing to key on.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        # ENCODING FALLBACK (2026-07-28): the 2026-07-13 extract declared
        # encoding="UTF-8" but wrote Windows-1252 bytes, so 198 of its 2,932
        # certificates (6.8%) failed strict parsing and were silently
        # dropped. The offenders are exactly the characters legal
        # descriptions are full of: ° (414), ¼ (310), curly quotes (211),
        # ½ (77) — quarter-quarter section calls and bearings.
        # The 2014 archive files have no declaration at all and need this
        # on almost every certificate.
        repaired = xml_bytes.decode("cp1252", errors="replace").encode("utf-8")
        try:
            root = ET.fromstring(repaired)
            _REPAIRS["cp1252"] += 1
        except ET.ParseError:
            # SECOND REPAIR (2026-09-29): invalid character references such
            # as &#11; appear in legacy files and are illegal in XML 1.0.
            try:
                root = ET.fromstring(_BAD_CHARREF.sub(b"", repaired))
                _REPAIRS["charref"] += 1
            except ET.ParseError:
                # THIRD REPAIR: raw control bytes and bare ampersands.
                try:
                    root = ET.fromstring(_deep_repair(repaired))
                    _REPAIRS["ctrl"] += 1
                except ET.ParseError as e:
                    logger.warning("eCRV XML parse failed", file=source_file,
                                   error=str(e)[:200])
                    return []

    crv_raw = _txt(root, "headerForm/crvNumberId")
    if not crv_raw:
        return []
    try:
        crv_id = int(crv_raw)
    except ValueError:
        return []

    prop = root.find("propertyForm")
    sale = root.find("salesAgreementForm")
    supp = root.find("supplementaryForm")
    if prop is None:
        return []

    shared = {
        "crv_number_id": crv_id,
        "county_cde": _txt(root, "headerForm/countyCde"),
        "deed_type": _txt(sale, "deedTypeCde") if sale is not None else None,
        "deed_date": _date_only(
            _txt(sale, "deedContractDate") if sale is not None else None),
        "finance_type": _txt(sale, "financeType") if sale is not None else None,
        "buyers": _party_names(root.find("buyersForm")),
        "sellers": _party_names(root.find("sellersForm")),
        "legal_action": _bool(_txt(supp, "legalActionInd")) if supp is not None else None,
        "government_ind": _bool(_txt(supp, "governmentInd")) if supp is not None else None,
        "related_ind": _bool(_txt(supp, "relatedInd")) if supp is not None else None,
        "non_market_price": _bool(_txt(supp, "nonMarketPriceInd")) if supp is not None else None,
        "source_file": source_file,
    }
    amt = _decimal(_txt(sale, "totPurchaseAmt")) if sale is not None else None
    shared["purchase_amt"] = float(amt) if amt is not None else None

    rows: list[dict[str, Any]] = []
    seen_raw: set[str] = set()
    for p in _items(prop, "parcels", ("parcelId",)):
        raw_pin = _txt(p, "parcelId")
        if not raw_pin or raw_pin in seen_raw:
            continue          # UNIQUE (crv_number_id, parcel_id_raw)
        seen_raw.add(raw_pin)
        row = dict(shared)
        row["parcel_id_raw"] = raw_pin
        row["parcel_norm"] = re.sub(r"\D", "", raw_pin) or None
        row["primary_parcel"] = _bool(_txt(p, "primary")) or False
        rows.append(row)

    # If the state marked no parcel primary, treat the first as primary so
    # downstream "the parcel this sale is about" queries still work.
    if rows and not any(r["primary_parcel"] for r in rows):
        rows[0]["primary_parcel"] = True
    return rows


def iter_zip_rows(zip_path: str, source_file: str) -> Iterator[dict[str, Any]]:
    """Yield one row per parcel for every XML in the extract zip."""
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".xml"):
                continue
            for row in parse_ecrv_xml(zf.read(name), source_file):
                yield row


def download_from_storage(object_name: str) -> str:
    """Download an extract from the private Supabase Storage bucket to a temp
    file and return its path. Raises on failure — a missing object should
    stop the run loudly, not silently ingest nothing."""
    client = get_client()
    logger.info("Downloading eCRV extract from storage",
                bucket=STORAGE_BUCKET, object=object_name)
    data = client.storage.from_(STORAGE_BUCKET).download(object_name)
    if not data:
        raise RuntimeError(
            f"Storage object '{object_name}' in bucket "
            f"'{STORAGE_BUCKET}' was empty or missing"
        )
    fd, path = tempfile.mkstemp(suffix=".zip", prefix="ecrv_")
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
    logger.info("eCRV extract downloaded", bytes=len(data), path=path)
    return path


def _chunks(seq: list[Any], n: int) -> Iterator[list[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _check_target(target_table: str) -> None:
    if target_table not in ALLOWED_TARGET_TABLES:
        raise ValueError(
            f"target_table {target_table!r} is not allowed; "
            f"expected one of {sorted(ALLOWED_TARGET_TABLES)}"
        )


def ingest_zip(
    zip_path: str,
    source_file: Optional[str] = None,
    target_table: str = DEFAULT_TARGET_TABLE,
) -> dict[str, Any]:
    """Parse an extract zip and upsert into outcomes.<target_table>.

    target_table defaults to 'ecrv_sales' (the weekly behaviour). The archive
    runner passes 'ecrv_sales_history'. Anything else raises.

    Idempotent on (crv_number_id, parcel_id_raw): re-running the same file
    updates rather than duplicates, and a corrected certificate overwrites
    the original.
    """
    _check_target(target_table)
    started = datetime.now(timezone.utc)
    source_file = source_file or os.path.basename(zip_path)

    reset_repair_counts()
    rows = list(iter_zip_rows(zip_path, source_file))
    certs = len({r["crv_number_id"] for r in rows})
    repairs = repair_counts()
    logger.info("eCRV extract parsed", source_file=source_file,
                certificates=certs, parcel_rows=len(rows),
                target_table=target_table, repaired_cp1252=repairs["cp1252"],
                repaired_charref=repairs["charref"],
                repaired_ctrl=repairs["ctrl"])
    if not rows:
        return {"source_file": source_file, "target_table": target_table,
                "certificates": 0, "parcel_rows": 0, "written": 0,
                "failed": 0, "repaired": repairs}

    now_iso = started.isoformat()
    for r in rows:
        r["loaded_at"] = now_iso

    written = 0
    failed = 0
    for batch in _chunks(rows, _DB_BATCH_SIZE):
        try:
            res = (
                outcomes_table(target_table)
                .upsert(batch, on_conflict="crv_number_id,parcel_id_raw")
                .execute()
            )
            written += len(res.data) if res.data else len(batch)
        except Exception as e:
            failed += len(batch)
            logger.warning("eCRV upsert failed", batch_size=len(batch),
                           target_table=target_table, error=str(e)[:500])

    stats = {
        "source_file": source_file,
        "target_table": target_table,
        "certificates": certs,
        "parcel_rows": len(rows),
        "written": written,
        "failed": failed,
        "repaired": repairs,
        "duration_seconds": round(
            (datetime.now(timezone.utc) - started).total_seconds(), 1),
    }
    logger.info("eCRV ingest complete", **stats)
    return stats


def ingest_from_storage(
    object_name: str,
    target_table: str = DEFAULT_TARGET_TABLE,
) -> dict[str, Any]:
    """Download an extract from the bucket, ingest it, clean up the temp file."""
    _check_target(target_table)
    path = download_from_storage(object_name)
    try:
        return ingest_zip(path, source_file=object_name,
                          target_table=target_table)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


__all__ = [
    "STORAGE_BUCKET",
    "DEFAULT_TARGET_TABLE",
    "ALLOWED_TARGET_TABLES",
    "parse_ecrv_xml",
    "iter_zip_rows",
    "reset_repair_counts",
    "repair_counts",
    "download_from_storage",
    "ingest_zip",
    "ingest_from_storage",
]
