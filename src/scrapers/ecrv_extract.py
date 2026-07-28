"""
eCRV (Electronic Certificate of Real Estate Value) weekly-extract ingest.

Source: Minnesota Department of Revenue — Weekly Sales Extract
        Access requested via ecrv.support@state.mn.us; the state emails an
        alert and the zip is downloaded manually. There is NO fetchable URL:
        the SOAP Web Services API exists but production credentials require
        being the eCRV contact person at a county or city office, which
        Govire is not. So this is an UPLOAD-DRIVEN ingest, not a scheduled
        scraper — hence no BaseScraper subclass, no cron, no feature flag.

An eCRV must be filed for every Minnesota real property transfer with
consideration over $3,000, so this is the state's authoritative record of
what property ACTUALLY SOLD FOR — as opposed to the assessor's estimate in
core.parcels.emv_total. It is the evidence behind the portal's claim that
outcomes are "confirmed from county records and state deed filings, not
inferred."

=== WHY THIS MATTERS BEYOND PRICES ===
The supplementary + sales-agreement blocks carry signals no MLS feed has
(counts from the 2026-07-27 extract, n=2700):
  nonListedInd  = true   395 (14.6%)  sales that never hit the market
  deedTypeCde   PERREPDEED  56        personal representative = ESTATE sale
                TRUSTEE    195        trustee conveyance
                CONFORDEED  60        contract for deed
  financeType   CASH       743        investor / non-financed activity
  relatedInd    = true   123         non-arm's-length — MUST be excluded
                                      from any comps calculation
  giftInd / governmentInd / legalActionInd — further non-market transfers

=== PARCEL RESOLUTION (verified live 2026-07-28) ===
eCRV publishes each county's parcel id in that county's own format, and the
digits differ per county. Match rates measured against 1,445 real eCRV PINs:

    hennepin    100.0%   parcel_id   13 digits
    dakota      100.0%   TAXPIN      12 digits  <-- NOT parcel_id
    wabasha     100.0%   parcel_id    9 digits
    anoka       100.0%   parcel_id   12 digits
    olmsted      98.9%   parcel_id   12 digits
    ramsey       97.8%   parcel_id   12 digits
    washington   96.7%   parcel_id   13 digits
    fillmore     90.9%   parcel_id    9 digits

DAKOTA IS THE EXCEPTION and the reason this table is a dict, not a rule.
Dakota County publishes TWO parallel identifier systems: its GIS layer uses
a PLSS-derived 13-digit PIN (36-115-20-77-0066) which is what core.parcels
stores, while eCRV records the 12-digit tax PIN (01-18050-01-010). No
arithmetic converts one to the other — they are different systems. The
bridge is the TAXPIN column on Dakota's layer 71, carried in raw_data.
Proven end to end on 15706 Diamond Way: eCRV 01-18050-01-010 ->
TAXPIN 011805001010 -> PIN 3611520770066 -> $685,000 on 2026-07-16.

Non-matching rows are stored with parcel_id = NULL. core.transactions
allows it, and an honest NULL beats a fabricated key — roughly 5% of sales
are new splits, replats, or parcels created since the last spine refresh.

=== PII: DELIBERATELY DROPPED ===
The extract carries buyer/seller daytimePhone (2,324 of 2,700) and email
(265). Names are kept — they feed owner matching and the estate-sale
signal. Phone and email are NOT stored: they add nothing to the product and
would create a data-handling obligation that exists nowhere else in this
system. Note also that FEINs appearing in certificates of real estate value
are classified as private/nonpublic under Minn. Stat. ch. 13, so the file
is not uniformly public data. Parse them, drop them on the floor.

=== IDEMPOTENCE ===
Upsert key is (source, source_id) where source_id is the crvNumberId.
Weekly extracts overlap and corrected certificates are re-delivered, so
re-ingesting the same zip is harmless and corrections overwrite cleanly.
Keying on parcel would be wrong: one certificate can list many parcels.

Usage:
    python -m scripts.run_ecrv_ingest /path/to/2026-07-27_eCRVExtract.zip
"""

from __future__ import annotations

import re
import zipfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator, Optional
import xml.etree.ElementTree as ET

from src.db.supabase_client import core_table
from src.utils.logger import logger


SOURCE_NAME = "ecrv_extract"

_DB_BATCH_SIZE = 500
_LOOKUP_CHUNK = 200

# Minnesota county codes are assigned alphabetically, 01-87. Mapped to the
# core.counties slug. Only counties actually seeded in core.counties get
# written to transactions.county_code (it carries an FK); everything else
# stores NULL rather than failing the row.
_COUNTY_CODE_TO_SLUG: dict[str, str] = {
    "01": "aitkin", "02": "anoka", "03": "becker", "04": "beltrami",
    "05": "benton", "06": "big_stone", "07": "blue_earth", "08": "brown",
    "09": "carlton", "10": "carver", "11": "cass", "12": "chippewa",
    "13": "chisago", "14": "clay", "15": "clearwater", "16": "cook",
    "17": "cottonwood", "18": "crow_wing", "19": "dakota", "20": "dodge",
    "21": "douglas", "22": "faribault", "23": "fillmore", "24": "freeborn",
    "25": "goodhue", "26": "grant", "27": "hennepin", "28": "houston",
    "29": "hubbard", "30": "isanti", "31": "itasca", "32": "jackson",
    "33": "kanabec", "34": "kandiyohi", "35": "kittson", "36": "koochiching",
    "37": "lac_qui_parle", "38": "lake", "39": "lake_of_the_woods",
    "40": "le_sueur", "41": "lincoln", "42": "lyon", "43": "mcleod",
    "44": "mahnomen", "45": "marshall", "46": "martin", "47": "meeker",
    "48": "mille_lacs", "49": "morrison", "50": "mower", "51": "murray",
    "52": "nicollet", "53": "nobles", "54": "norman", "55": "olmsted",
    "56": "otter_tail", "57": "pennington", "58": "pine", "59": "pipestone",
    "60": "polk", "61": "pope", "62": "ramsey", "63": "red_lake",
    "64": "redwood", "65": "renville", "66": "rice", "67": "rock",
    "68": "roseau", "69": "st_louis", "70": "scott", "71": "sherburne",
    "72": "sibley", "73": "stearns", "74": "steele", "75": "stevens",
    "76": "swift", "77": "todd", "78": "traverse", "79": "wabasha",
    "80": "wadena", "81": "waseca", "82": "washington", "83": "watonwan",
    "84": "wilkin", "85": "winona", "86": "wright", "87": "yellow_medicine",
}

# How each county's parcels are matched. Default is 'parcel_id' (compare the
# digits-only eCRV pin against core.parcels.parcel_id). Dakota must use the
# TAXPIN carried in raw_data — see the module docstring.
_PARCEL_MATCH_MODE: dict[str, str] = {
    "dakota": "taxpin",
}

# tier1Cde -> a readable transaction_type. Unmapped codes fall through to the
# raw code itself rather than being dropped, so a new code shows up in the
# data instead of vanishing.
_USE_TO_TYPE: dict[str, str] = {
    "RESID": "residential_sale",
    "VACANT": "vacant_land_sale",
    "AG2A": "agricultural_sale",
    "AGRI": "agricultural_sale",
    "WHSE": "industrial_sale",
    "RETAIL": "commercial_sale",
    "OFFICE": "commercial_sale",
    "RESTBC": "commercial_sale",
    "APART": "multifamily_sale",
    "OTHER": "other_sale",
}


def _txt(node: Optional[ET.Element], path: str) -> Optional[str]:
    if node is None:
        return None
    el = node.find(path)
    if el is None or el.text is None:
        return None
    s = el.text.strip()
    return s or None


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
    """eCRV dates arrive as '2026-06-30T00:00:00-05:00'. transaction_date is
    a DATE column, so keep the calendar day and drop the offset — converting
    to UTC would shift a late-evening Central deed to the following day."""
    if not value:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", value.strip())
    return m.group(1) if m else None


def _party_names(form: Optional[ET.Element]) -> Optional[str]:
    """Collect buyer or seller names.

    Handles BOTH <individuals> and <organizations>: the 2026-07-27 extract
    carried 3,831 individual and 388 organization buyers, and reading only
    individuals would silently drop every LLC, builder and bank purchase —
    exactly the buyer class that matters most for investor-activity signals.

    Names only. daytimePhone and email are deliberately not read.
    """
    if form is None:
        return None
    names: list[str] = []
    for ind in form.findall("individuals"):
        parts = [
            _txt(ind, "firstName"),
            _txt(ind, "middleName"),
            _txt(ind, "lastName"),
            _txt(ind, "nameSuffix"),
        ]
        joined = " ".join(p for p in parts if p)
        if joined:
            names.append(joined)
    for org in form.findall("organizations"):
        name = _txt(org, "organizationName")
        if name:
            names.append(name)
    if not names:
        return None
    return " & ".join(names).upper()[:500]


def parse_ecrv_xml(xml_bytes: bytes) -> Optional[dict[str, Any]]:
    """Parse one eCRV XML document into a core.transactions-shaped dict.

    Returns None when the record has no CRV id (nothing to key on).
    Every parcel id on the certificate is retained in raw_data.all_parcel_ids;
    the PRIMARY parcel is the one used for the FK.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.warning("eCRV XML parse failed", error=str(e)[:200])
        return None

    crv_id = _txt(root, "headerForm/crvNumberId")
    if not crv_id:
        return None
    county_cde = _txt(root, "headerForm/countyCde")
    county_slug = _COUNTY_CODE_TO_SLUG.get(county_cde or "")

    prop = root.find("propertyForm")
    sale = root.find("salesAgreementForm")
    supp = root.find("supplementaryForm")

    # Parcels: keep them all, mark the primary.
    primary_pin: Optional[str] = None
    all_pins: list[str] = []
    if prop is not None:
        for p in prop.findall("parcels"):
            pin = _txt(p, "parcelId")
            if not pin:
                continue
            all_pins.append(pin)
            if _bool(_txt(p, "primary")) and primary_pin is None:
                primary_pin = pin
        if primary_pin is None and all_pins:
            primary_pin = all_pins[0]

    use1 = _txt(prop, "plannedUses/tier1Cde") if prop is not None else None
    use2 = _txt(prop, "plannedUses/tier2Cde") if prop is not None else None
    txn_type = _USE_TO_TYPE.get((use1 or "").upper(), (use1 or None))

    raw: dict[str, Any] = {
        "crv_number_id": crv_id,
        "county_cde": county_cde,
        "parcel_ids": all_pins,
        "primary_parcel_id_raw": primary_pin,
        "planned_use_tier1": use1,
        "planned_use_tier2": use2,
        "use_before_sale_tier1": _txt(prop, "usesBeforeSale/tier1Cde") if prop is not None else None,
        "principal_residence": _bool(_txt(prop, "principalResidence")) if prop is not None else None,
        "legal_description": _txt(prop, "legalDescription") if prop is not None else None,
        "site_address": _txt(prop, "mnPropertyAddresses/street1") if prop is not None else None,
        "site_zip": _txt(prop, "mnPropertyAddresses/zip") if prop is not None else None,
        # --- sales agreement ---
        "deed_type": _txt(sale, "deedTypeCde") if sale is not None else None,
        "finance_type": _txt(sale, "financeType") if sale is not None else None,
        "down_payment_equity": str(_decimal(_txt(sale, "downPmtEquity"))) if sale is not None and _decimal(_txt(sale, "downPmtEquity")) is not None else None,
        "special_assessment_amt": str(_decimal(_txt(sale, "specialAssesmtAmt"))) if sale is not None and _decimal(_txt(sale, "specialAssesmtAmt")) is not None else None,
        "personal_property_included": _bool(_txt(sale, "personalPropertyIncludedInTotal")) if sale is not None else None,
        "like_kind_exchange": _bool(_txt(sale, "likeKindExchange")) if sale is not None else None,
        # --- supplementary: the signal flags ---
        "non_listed": _bool(_txt(supp, "nonListedInd")) if supp is not None else None,
        "related_parties": _bool(_txt(supp, "relatedInd")) if supp is not None else None,
        "gift": _bool(_txt(supp, "giftInd")) if supp is not None else None,
        "government": _bool(_txt(supp, "governmentInd")) if supp is not None else None,
        "legal_action": _bool(_txt(supp, "legalActionInd")) if supp is not None else None,
        "non_market_price": _bool(_txt(supp, "nonMarketPriceInd")) if supp is not None else None,
        "tax_exempt": _bool(_txt(supp, "taxExemptInd")) if supp is not None else None,
        "foreclosure_or_repossession": _bool(_txt(supp, "foreclosureInd")) if supp is not None else None,
    }
    raw = {k: v for k, v in raw.items() if v is not None and v != []}

    price = _decimal(_txt(sale, "totPurchaseAmt")) if sale is not None else None

    return {
        "source": SOURCE_NAME,
        "source_id": crv_id,
        "county_code": county_slug,
        "county_slug_unverified": county_slug,
        "transaction_type": txn_type,
        "transaction_date": _date_only(_txt(sale, "deedContractDate") if sale is not None else None),
        "sale_price": str(price) if price is not None else None,
        "buyer_name": _party_names(root.find("buyersForm")),
        "seller_name": _party_names(root.find("sellersForm")),
        "document_number": None,   # eCRV carries no recorded document number
        "raw_data": raw,
        "_pin_digits": re.sub(r"\D", "", primary_pin) if primary_pin else None,
    }


def iter_zip_records(zip_path: str) -> Iterator[dict[str, Any]]:
    """Yield a parsed record for each XML in the extract zip."""
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".xml"):
                continue
            rec = parse_ecrv_xml(zf.read(name))
            if rec is not None:
                yield rec


def _seeded_county_slugs() -> set[str]:
    """core.transactions.county_code has an FK to core.counties, so a slug we
    haven't seeded would fail the whole batch. Load what exists and write NULL
    for the rest — a sale in an un-onboarded county is still worth storing."""
    try:
        res = core_table("counties").select("county_code").execute()
        return {r["county_code"] for r in (res.data or [])}
    except Exception as e:
        logger.warning("Could not load core.counties; county_code left NULL",
                       error=str(e)[:200])
        return set()


def _chunks(seq: list[Any], n: int) -> Iterator[list[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _resolve_parcels(records: list[dict[str, Any]]) -> tuple[int, int]:
    """Attach parcel_id to each record, in place.

    Batched per county: one lookup per chunk of PINs rather than per record.
    Match mode comes from _PARCEL_MATCH_MODE — 'parcel_id' for every county
    except Dakota, which matches raw_data->>'TAXPIN' (see module docstring).

    Records whose county has no spine, or whose PIN doesn't match, keep
    parcel_id = None. That is the honest outcome, not a failure.
    """
    by_county: dict[str, list[dict[str, Any]]] = {}
    for rec in records:
        slug = rec.get("county_slug_unverified")
        pin = rec.get("_pin_digits")
        if slug and pin:
            by_county.setdefault(slug, []).append(rec)

    matched = 0
    unmatched = 0
    for slug, recs in by_county.items():
        mode = _PARCEL_MATCH_MODE.get(slug, "parcel_id")
        pins = sorted({r["_pin_digits"] for r in recs})
        found: dict[str, str] = {}
        for chunk in _chunks(pins, _LOOKUP_CHUNK):
            try:
                q = core_table("parcels").select("parcel_id,raw_data").eq(
                    "county_code", slug
                )
                if mode == "taxpin":
                    q = q.filter(
                        "raw_data->>TAXPIN", "in",
                        "(" + ",".join(chunk) + ")",
                    )
                else:
                    q = q.in_("parcel_id", chunk)
                res = q.execute()
            except Exception as e:
                logger.warning("eCRV parcel lookup failed",
                               county=slug, mode=mode, error=str(e)[:200])
                continue
            for row in (res.data or []):
                if mode == "taxpin":
                    key = ((row.get("raw_data") or {}).get("TAXPIN")) or ""
                else:
                    key = row.get("parcel_id") or ""
                if key:
                    found[key] = row["parcel_id"]
        for r in recs:
            pid = found.get(r["_pin_digits"])
            if pid:
                r["parcel_id"] = pid
                matched += 1
            else:
                unmatched += 1
        logger.info("eCRV parcels resolved", county=slug, mode=mode,
                    pins=len(pins), matched=len(found))
    return matched, unmatched


def ingest_zip(zip_path: str) -> dict[str, Any]:
    """Parse an eCRV weekly extract and upsert into core.transactions.

    Idempotent on (source, source_id): re-running the same zip updates rather
    than duplicates, and a corrected certificate overwrites the original.
    """
    started = datetime.now(timezone.utc)
    records = list(iter_zip_records(zip_path))
    logger.info("eCRV extract parsed", zip=zip_path, records=len(records))
    if not records:
        return {"records": 0, "written": 0, "failed": 0,
                "matched": 0, "unmatched": 0}

    matched, unmatched = _resolve_parcels(records)

    seeded = _seeded_county_slugs()
    now_iso = started.isoformat()
    rows: list[dict[str, Any]] = []
    for r in records:
        slug = r.get("county_slug_unverified")
        rows.append({
            "parcel_id": r.get("parcel_id"),
            "county_code": slug if slug in seeded else None,
            "transaction_type": r.get("transaction_type"),
            "transaction_date": r.get("transaction_date"),
            "sale_price": r.get("sale_price"),
            "buyer_name": r.get("buyer_name"),
            "seller_name": r.get("seller_name"),
            "document_number": r.get("document_number"),
            "source": r["source"],
            "source_id": r["source_id"],
            "raw_data": r.get("raw_data"),
            "observed_at": now_iso,
        })

    written = 0
    failed = 0
    for batch in _chunks(rows, _DB_BATCH_SIZE):
        try:
            res = (
                core_table("transactions")
                .upsert(batch, on_conflict="source,source_id")
                .execute()
            )
            written += len(res.data) if res.data else len(batch)
        except Exception as e:
            failed += len(batch)
            logger.warning("eCRV transactions upsert failed",
                           batch_size=len(batch), error=str(e)[:500])

    stats = {
        "records": len(records),
        "written": written,
        "failed": failed,
        "matched": matched,
        "unmatched": unmatched,
        "duration_seconds": round(
            (datetime.now(timezone.utc) - started).total_seconds(), 1),
    }
    logger.info("eCRV ingest complete", **stats)
    return stats


__all__ = [
    "SOURCE_NAME",
    "parse_ecrv_xml",
    "iter_zip_records",
    "ingest_zip",
    "_COUNTY_CODE_TO_SLUG",
    "_PARCEL_MATCH_MODE",
]
