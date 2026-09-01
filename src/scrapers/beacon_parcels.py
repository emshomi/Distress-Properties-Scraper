"""
Beacon / Schneider Geospatial per-parcel report scraper — parcel spine loader.

ONE class per VENDOR, not per county. Blue Earth is the first county through
it; every subsequent Beacon county is an INSERT into core.vendor_portals, not
a new file. Same shape as TylerTaxDetailScraper (src/scrapers/
olmsted_tax_detail.py) and MNGACParcelsScraper, and for the same reason: a
file per county re-creates the artefact multiplication the registries exist
to remove.

=== WHY THIS EXISTS (measured 2026-09-01) ===
28 Minnesota counties have NO parcel loader. Their only core.parcels rows are
by-products of the notice scrapers, and with_emv = 0 across all 28. Those 28
are exactly the counties with no `gac_open_approval` in MnGeo's
plan_parcels_open layer (measured 2026-08-04, PLAN_mngeo_statewide_spine.md
section 2) — the layer enforces the opt-in list itself, so no amount of work
reaches them there.

Checked the same day and all closed for Blue Earth:
  - MnGeo opt-in compilation      absent, by county policy
  - enterprise.gisdata.mn.gov aghost REST   only aitkin/itasca/lake/wabasha
    publish county folders; no us_mn_co_blueearth
  - Minnesota Geospatial Commons  1,049 catalogue rows, 101 mention parcels
    or cadastre, ZERO for any of the 28 (control: the same filter finds
    Houston, Waseca, Steele and Hennepin)
  - county ArcGIS server          none published; the county sells its
    property-tax GIS data under a licence agreement

What Blue Earth DOES publish is a Beacon parcel report, and it carries more
than the MnGeo spine ever did: owner + mailing address, EMV improvement /
land / total across four assessment years, the assessment year itself,
building sqft, year built, tax district, acreage, sale price and the eCRV
certificate number.

The immediate payoff is the eCRV join. outcomes.ecrv_sales holds 3,286 Blue
Earth rows (county_cde '07') with a non-null parcel_norm and NOTHING to join
to. 2,991 distinct parcels is this loader's work list.

=== SOURCE — DIRECT DEEP LINKS, NO SESSION, NO BROWSER ===
Verified live 2026-09-01 with plain GETs:

  https://beacon.schneidercorp.com/Application.aspx
      ?AppID=<AppID>&LayerID=<LayerID>&PageTypeID=4&PageID=<PageID>
      &KeyValue=<PIN>

The interactive UI adds a `Q=<token>` cached-query handle. It is NOT
required: a second, different parcel (R010431300013, Brick Commercial
Properties, 600 Summit Ave) rendered correctly from a fresh tab with Q
removed. So this is one plain GET per parcel — no Playwright, no session
priming, no token minting.

Blue Earth ids, read off the county's own URL: AppID=387, LayerID=5678,
PageID=15097 (PageTypeID=4 is the Report page; 2 is Search, 3 is Results).

=== NETWORK: RESIDENTIAL IP ONLY ===
beacon.schneidercorp.com returns bot detection to datacenter egress. Verified
BOTH directions on 2026-09-01: an automated fetch from a datacenter address
was blocked, while the same URL rendered normally in a residential browser
minutes apart.

So this runs from the Windows box, like govire_mnpn_browser.py, NOT from
Railway and NOT from GitHub Actions. Every avoided request is therefore real
cost, which is why the work list is eCRV-driven (2,991 parcels) rather than a
full enumeration of the county's ~34,000.

The full enumeration is a later run of the SAME loader with a different work
list, not a second scraper — see _load_pins.

=== PHANTOM-PIN BEHAVIOUR IS PER-INSTALL ===
core.vendor_portals' anoka note records the rule the hard way: Carver's Tyler
install served a complete, plausible, WRONG parcel for pin=999999999 (a live
NORTHERN STATES POWER transmission parcel, with owner and mailing address),
while Anoka returned "-- No Data --" for the same phantom. Unknown-PIN
handling is a property of the INSTALL, never of the vendor.

Blue Earth was tested 2026-09-01 with KeyValue=R019999999999 — structurally
valid, R plus 12 digits, no such parcel. It returned "No results match your
search criteria." with no Summary block and no fabricated parcel.

The guard is written anyway and is NOT optional. Every response must echo its
own punctuated Parcel ID in sdw1_summary_OriginalParcelNumber, and that value
must re-flatten to the KeyValue we asked for. A mismatch is logged, counted
toward records_failed and NEVER written. This is the only defence available
here, because unlike the Tyler scrapers this loader CREATES the spine and so
cannot validate a PIN against core.parcels before requesting it.

=== PIN FORMAT ===
  URL / storage   R010431300013     R + 12 digits, unpunctuated
  Display         R01.04.31.300.013 R + 2-2-2-3-3

Segments are jurisdiction, township, section, block, parcel. '01' is Mankato
City and dominates the eCRV sample, as it should for the county's population
centre. The search form's own hint reads Rxx-xx-xx-xxx-xxx; the punctuation
is display sugar on input and the flat form is canonical.

Measured 2026-09-01 on all 3,286 non-null Blue Earth eCRV rows: length 12,
one group, no variants. So

    core.parcels.parcel_id = 'R' || outcomes.ecrv_sales.parcel_norm

is the join, and it is an equality rather than a normalisation.

=== HTML CONTRACT: editkey, NOT row labels, NOT section indices ===
Beacon tags every summary and grid value with a semantic `editkey`
attribute — sdw1_summary_Acres, sdw1_residentialbuildings_ActualYearBuilt,
sdw1_sales_SalePrice and so on. Those are parsed by editkey, which survives a
county relabelling a visible row.

Valuation and Taxation carry no editkeys, so those two parse by SECTION TITLE
then row label. Section ids (ctlBodyPane_ctlNN_mSection) are NOT usable as
positions: the verified Brick Commercial page runs ctl00-ctl19 and skips
ctl04, ctl07 and ctl17 because those sections are absent on that parcel. A
parcel with a different mix shifts every index after the gap. Titles are
resolved from ctlBodyPane_ctlNN_lblName and the matching _mSection is read.

=== WRITE TARGET ===
core.parcels, upsert on (county_code, parcel_id) — composite since
2026-08-06; MN PINs are not unique across counties.

This is a FULL-REFRESH writer and therefore calls ParcelUpsert.dump_owned(),
NOT model_dump(exclude_none=True). For a source of record a field the county
did not publish is ABSENT, not unchanged, and only dump_owned() lets a re-run
clear a value an earlier write got wrong. See the ParcelUpsert docstring and
the 2026-08-13 measurement behind it.

core.owners rides alongside on (county_code, parcel_id, source) and never
fails the run — owners are enrichment; the parcel write is the source of
truth for run status.

=== CHECKPOINTED WRITES ===
fetch() persists every _CHECKPOINT_PARCELS parcels instead of accumulating
the whole run and returning at the end.

This is the olmsted_tax_detail lesson, quoted from its own source: "fetch()
accumulates into `raw` and returns only at the end, so a cancelled run writes
NOTHING -- all 200 successfully scraped parcels were discarded. Three
consecutive Tuesdays died this way." At 1.5s per parcel a 2,991-parcel run is
roughly 75 minutes, which is a long time to hold unwritten work.

write() therefore returns counters accumulated during fetch() and writes
nothing further. parse() exists to feed source freshness.

=== SOURCE FRESHNESS ===
BaseScraper computes freshness from parsed signals' event_date. A parcel
loader has none, so this county could never be caught frozen — the failure
that hid six dead sources for up to 653 days.

Beacon publishes its own stamp in the page footer (#hlkLastUpdated, e.g.
"Last Data Upload: 9/1/2026, 6:36:17 AM"). That is the SOURCE'S content, not
one of our writes, which is exactly what the freshness mechanism asks for. It
is carried on every parsed record and surfaces as source_max_date.

=== NOT WRITTEN THIS PASS ===
lat / lng / geom  the report publishes no coordinates. Blue Earth will be the
                  first county with NULL coords in core.parcels. Honest
                  absence; the Map tab is a separate investigation and the
                  eCRV join needs no geometry.
homestead_status  the Taxation block gives a Homestead Exclusion AMOUNT. A
                  non-zero amount proves homestead; $0 proves nothing,
                  because the MN exclusion phases out entirely on higher-value
                  homesteads. So >0 sets 'homestead' and 0 stays NULL rather
                  than inventing 'non_homestead'.
Unpaid Taxes      section ctl15 carries per-parcel delinquency (verified
                  live: $25,014.00 unpaid, 2026 payable, broken out into spec
                  assessment / fees / penalty / interest). It is a real
                  distress signal available at zero extra requests, and it is
                  preserved verbatim in raw_data this pass. Promoting it to
                  signals.distress_events is a second write target with its
                  own dedup key and belongs in its own task.
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

import httpx
from bs4 import BeautifulSoup

from src.config import settings
from src.db.supabase_client import core_table, outcomes_table
from src.models.parcel import ParcelUpsert
from src.scrapers.base_scraper import BaseScraper, RunResult
from src.utils.errors import (
    ScraperAlreadyRunningError,
    ScraperDisabledError,
    SourceUnavailableError,
)
from src.utils.logger import logger

# ------------------------------------------------------------------
# Blue Earth's values are the class defaults so a bare
# BlueEarthParcelsScraper() behaves identically to a registry-built one.
# Every one is overridable per instance from core.vendor_portals.
# ------------------------------------------------------------------
_BASE_URL = "https://beacon.schneidercorp.com"
_APP_PATH = "/Application.aspx"
_APP_ID = "387"
_LAYER_ID = "5678"
_PAGE_ID = "15097"
_PAGE_TYPE_REPORT = "4"
_COUNTY_SLUG = "blue_earth"
_PIN_PREFIX = "R"
_ECRV_COUNTY_CDE = "07"

_REQUEST_TIMEOUT = 30.0
_PER_PARCEL_ATTEMPTS = 3
_POLITE_DELAY_SECONDS = 1.5

# No published quota. These are courtesy, not a measured limit: a rest every
# hundred parcels costs ~5 minutes over a full run and keeps a sustained
# 2,991-request session from looking like a scrape to a WAF that has already
# demonstrated it will block one.
_BATCH_SIZE = 100
_BATCH_REST_SECONDS = 20

# Escalating silence when the host starts refusing. Short retries during a
# block are what kept the Tyler penalty alive for 45 minutes.
_BLOCK_BACKOFFS = (120, 300, 600)
_ABORT_AFTER_BLOCKED_PARCELS = 3

# Persist this often so a killed run keeps what it collected.
_CHECKPOINT_PARCELS = 250
_DB_BATCH_SIZE = 100

_NO_RESULTS_MARKER = "no results match your search criteria"
_BLOCK_MARKERS = (
    "request unsuccessful",
    "incapsula",
    "access denied",
    "unusual traffic",
)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# --- editkeys, read verbatim off the live page 2026-09-01 ---
# The concatenated ones are literal attribute values, quotes and all.
_EK_PIN = "sdw1_summary_OriginalParcelNumber"
_EK_ADDRESS = "sdw1_summary_PropertyAddress"
_EK_CSZ = "sdw1_summary_PropertyCity+' '+PropertyState+' '+PropertyZip"
_EK_LEGAL = "sdw1_summary_Legal1"
_EK_ACRES = "sdw1_summary_Acres"
_EK_CLASS = "sdw1_summary_Class+'-'+LandUse"
_EK_TAXDIST = "sdw1_summary_TaxDistrict"
_EK_YEAR_BUILT = "sdw1_residentialbuildings_ActualYearBuilt"
_EK_BLDG_SQFT = "sdw1_residentialbuildings_BuildingSqft:n0"
_EK_SALE_DATE = "sdw1_sales_SaleDate"
_EK_SALE_PRICE = "sdw1_sales_SalePrice"
_EK_SALE_CODE = "sdw1_sales_SaleCode"
_EK_SALE_DESC = "sdw1_sales_SaleDescription"

_RE_CSZ = re.compile(r"^(?P<city>.+?)\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5})(?:-(?P<z4>\d{4}))?$")
_RE_MONEY = re.compile(r"-?[\d,]+(?:\.\d+)?")
_RE_LAST_UPLOAD = re.compile(
    r"Last Data Upload:\s*(\d{1,2})/(\d{1,2})/(\d{4})"
)
_SQFT_PER_ACRE = Decimal("43560")


class _BlockedError(Exception):
    """The host refused — bot detection, 403 or 429. Not a parcel failure:
    go quiet and retry, and abort the run if it does not clear."""


class _NotFoundError(Exception):
    """The report rendered but holds no parcel ("No results match your
    search criteria"). An honest miss, counted, never written."""


# ============================================================
# PURE PARSING HELPERS (no network — unit-testable)
# ============================================================


def flatten_pin(text: str | None) -> str | None:
    """'R01.04.31.300.013' -> 'R010431300013'. Also accepts dashes and the
    already-flat form. Returns None on anything that is not R + 12 digits,
    because a half-parsed PIN must never become a parcel_id."""
    if not text:
        return None
    cleaned = re.sub(r"[^0-9A-Za-z]", "", text).upper()
    if re.fullmatch(r"R\d{12}", cleaned):
        return cleaned
    return None


def pin_from_parcel_norm(parcel_norm: str | None) -> str | None:
    """eCRV's 12-digit parcel_norm -> Beacon's KeyValue. Measured
    2026-09-01: all 3,286 Blue Earth rows are exactly 12 digits."""
    if not parcel_norm:
        return None
    digits = parcel_norm.strip()
    if re.fullmatch(r"\d{12}", digits):
        return f"{_PIN_PREFIX}{digits}"
    return None


def parse_money(text: str | None) -> Decimal | None:
    """'$1,779,900' -> Decimal('1779900'). Blank/'-'/None -> None."""
    if not text:
        return None
    m = _RE_MONEY.search(text)
    if not m:
        return None
    try:
        return Decimal(m.group(0).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def parse_int(text: str | None) -> int | None:
    """'16,000' -> 16000. Blank -> None. Never raises."""
    if not text:
        return None
    m = re.search(r"-?\d[\d,]*", text)
    if not m:
        return None
    try:
        return int(m.group(0).replace(",", ""))
    except ValueError:
        return None


def parse_decimal(text: str | None) -> Decimal | None:
    if not text:
        return None
    m = re.search(r"-?\d[\d,]*(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return Decimal(m.group(0).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def parse_us_date(text: str | None) -> date | None:
    """'5/10/2011' -> date(2011, 5, 10). Anything else -> None."""
    if not text:
        return None
    m = re.match(r"\s*(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None


def safe_year(value: int | None) -> int | None:
    """ParcelUpsert enforces 1700 <= year_built <= 2100 and RAISES on a
    violation — that killed 500 Dakota writes once. Clamp to None here so a
    sentinel or a mis-parse costs one field, not one parcel."""
    if value is None:
        return None
    if 1700 <= value <= 2100:
        return value
    return None


def map_property_type(class_text: str | None) -> str | None:
    """Blue Earth Class Code -> ParcelUpsert.PropertyType, conservatively.

    Unmapped returns None, per COUNTY_ONBOARDING_TEMPLATE: "conservative
    property-type map (unmapped -> NULL, refine later from real Class/land-use
    distributions)". Blue Earth's vocabulary has not been surveyed across
    34,000 parcels; it has been read on two. A wrong mapping is worse than a
    null, because property_type is a live filter key.

    Order matters: '233-3A COMMERCIAL LAND AND BUILDING' contains both
    COMMERCIAL and LAND, and it is commercial.
    """
    if not class_text:
        return None
    t = class_text.upper()
    if "APARTMENT" in t:
        return "multifamily"
    if "INDUSTRIAL" in t:
        return "industrial"
    if "COMMERCIAL" in t:
        return "commercial"
    if "AGRICULTURAL" in t or "AGRI " in t:
        return "agricultural"
    if "TOWNHOUSE" in t or "TOWN HOME" in t:
        return "townhouse"
    if "CONDOMINIUM" in t or "CONDO" in t:
        return "condo"
    if "RESIDENTIAL" in t:
        # 'RES 1 UNIT', 'RESIDENTIAL SINGLE FAMILY' etc. Anything residential
        # that is not explicitly multi-unit is left to the single-family
        # branch only when it says so; otherwise unknown rather than guessed.
        if "SINGLE" in t or "1 UNIT" in t or "ONE UNIT" in t:
            return "single_family"
        return None
    if "VACANT" in t or t.strip().endswith("LAND"):
        return "land"
    return None


def acres_to_lot_sqft(acres: Decimal | None) -> int | None:
    """9.71 acres -> 422,968 sqft. Verified against the county's own Land
    grid on R010431300013: LandUnits 422968, LandUnitType 'S'. This is a
    unit conversion, not an estimate."""
    if acres is None or acres <= 0:
        return None
    try:
        return int((acres * _SQFT_PER_ACRE).quantize(Decimal("1")))
    except (InvalidOperation, ValueError):
        return None


def _ek_all(soup: BeautifulSoup, key: str) -> list[str]:
    """Every value carrying this editkey, in document order."""
    return [
        el.get_text(" ", strip=True)
        for el in soup.find_all(attrs={"editkey": key})
    ]


def _ek_first(soup: BeautifulSoup, key: str) -> str | None:
    """First NON-EMPTY value for this editkey, else None."""
    for value in _ek_all(soup, key):
        if value:
            return value
    return None


def find_section(soup: BeautifulSoup, title: str) -> Any | None:
    """Resolve a section by its visible title, never by index.

    ctlBodyPane_ctlNN_lblName holds the title; ctlBodyPane_ctlNN_mSection is
    the container. Indices are NOT positions — the verified page skips ctl04,
    ctl07 and ctl17 because those sections are absent on that parcel.
    """
    for el in soup.find_all("div", class_="title"):
        if el.get_text(strip=True) != title:
            continue
        el_id = el.get("id") or ""
        if not el_id.endswith("_lblName"):
            continue
        return soup.find(id=el_id[: -len("_lblName")] + "_mSection")
    return None


def _row_cells(tr: Any) -> list[str]:
    return [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]


def parse_year_columns(section: Any) -> tuple[list[str], dict[str, list[str]]]:
    """Read a year-column grid into (years, {row_label: [values...]}).

    Shared by Valuation ('Assessed Year 2026 2025 2024 2023') and Taxation
    ('2027 Payable 2026 Payable ...'). Taxation prefixes every row with an
    operator cell ('-', '=', ''), so the label is taken as the last cell
    before the first value-looking cell rather than as cells[0].
    """
    years: list[str] = []
    rows: dict[str, list[str]] = {}
    if section is None:
        return years, rows

    table = section.find("table")
    if table is None:
        return years, rows

    for tr in table.find_all("tr"):
        cells = _row_cells(tr)
        if not cells:
            continue
        year_cells = [c for c in cells if re.fullmatch(r"(19|20)\d{2}( Payable)?", c)]
        if year_cells and not years:
            years = [c.split()[0] for c in year_cells]
            continue
        # Label = last non-empty cell that is not an operator, scanning left
        # to right until a value cell appears.
        label = None
        values: list[str] = []
        for cell in cells:
            if label is None:
                if cell in ("", "-", "+", "="):
                    continue
                label = cell
                continue
            values.append(cell)
        if label:
            rows[label] = values
    return years, rows


def _column_index(years: list[str], values: list[str]) -> int | None:
    """Index of the newest column that actually carries a value.

    Taxation's newest column (2027 Payable) is blank until certification, so
    the newest POPULATED column is the current one — never assume index 0.
    """
    for i, v in enumerate(values):
        if v and v.strip() not in ("", "-"):
            return i
    return None


def parse_last_upload(soup: BeautifulSoup) -> date | None:
    """Footer stamp: 'Last Data Upload: 9/1/2026, 6:36:17 AM'.

    The SOURCE'S own publication date, which is the one measure of freshness
    our own writes cannot contaminate.
    """
    el = soup.find(id="hlkLastUpdated")
    text = el.get_text(" ", strip=True) if el else ""
    m = _RE_LAST_UPLOAD.search(text or "")
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None


def parse_owner_block(soup: BeautifulSoup) -> dict[str, Any]:
    """Primary owner name + mailing address from the Owners repeater.

    Markup (verified): ctlBodyPane_ctlNN_ctl01_rptOwner_ctlMM_lblOwnerType /
    _sprOwnerName1_..._lblSearch / _lblOwnerAddress, with the address
    <br/>-separated into street and 'CITY ST ZIP'. The <br/> split is what
    makes a clean absentee comparison possible.
    """
    out: dict[str, Any] = {
        "owner_name": None,
        "mailing_address": None,
        "mailing_city": None,
        "mailing_state": None,
        "mailing_zip": None,
    }
    section = find_section(soup, "Owners")
    if section is None:
        return out

    name_el = section.find(
        id=re.compile(r"rptOwner_ctl\d+_sprOwnerName1_.*lblSearch$")
    )
    if name_el is not None:
        out["owner_name"] = name_el.get_text(" ", strip=True) or None

    addr_el = section.find(id=re.compile(r"rptOwner_ctl\d+_lblOwnerAddress$"))
    if addr_el is not None:
        lines = [
            part.strip()
            for part in addr_el.get_text("\n", strip=True).split("\n")
            if part.strip()
        ]
        if lines:
            out["mailing_address"] = lines[0]
        if len(lines) > 1:
            m = _RE_CSZ.match(lines[-1])
            if m:
                out["mailing_city"] = m.group("city")
                out["mailing_state"] = m.group("state")
                out["mailing_zip"] = m.group("zip")
    return out


def parse_report(html: str, requested_pin: str) -> dict[str, Any]:
    """Parse one Beacon report page into a raw parcel dict.

    Raises _NotFoundError when the page is the honest miss, and
    _BlockedError when the host refused. Raises ValueError when the page
    renders a DIFFERENT parcel than the one requested — the anti-fabrication
    guard, which is the only defence available to a loader that creates the
    spine and so cannot pre-validate against core.parcels.
    """
    lowered = html.lower()
    if any(marker in lowered for marker in _BLOCK_MARKERS):
        raise _BlockedError("host returned a block page")
    if _NO_RESULTS_MARKER in lowered:
        raise _NotFoundError(requested_pin)

    soup = BeautifulSoup(html, "html.parser")

    displayed = _ek_first(soup, _EK_PIN)
    echoed = flatten_pin(displayed)
    if echoed is None:
        raise ValueError(
            f"no parsable Parcel ID in Summary (found {displayed!r}) "
            f"for requested {requested_pin}"
        )
    if echoed != requested_pin.upper():
        raise ValueError(
            f"page identity mismatch: requested {requested_pin}, "
            f"page reports {echoed}"
        )

    address = _ek_first(soup, _EK_ADDRESS)
    csz = _ek_first(soup, _EK_CSZ)
    city = state = zip_code = None
    if csz:
        m = _RE_CSZ.match(csz)
        if m:
            city = m.group("city")
            state = m.group("state")
            zip_code = m.group("zip")

    acres = parse_decimal(_ek_first(soup, _EK_ACRES))
    class_text = _ek_first(soup, _EK_CLASS)

    # --- Valuation: newest assessment year ---
    val_years, val_rows = parse_year_columns(
        find_section(soup, "Valuation - Assessment Year")
    )
    emv_total = emv_land = emv_building = None
    emv_year = None
    total_values = val_rows.get("EMV (Estimated Market Value) Total", [])
    idx = _column_index(val_years, total_values)
    if idx is not None:
        emv_total = parse_money(total_values[idx])
        land_values = val_rows.get("EMV Land", [])
        bldg_values = val_rows.get("EMV Improvement", [])
        if idx < len(land_values):
            emv_land = parse_money(land_values[idx])
        if idx < len(bldg_values):
            emv_building = parse_money(bldg_values[idx])
        if idx < len(val_years):
            emv_year = parse_int(val_years[idx])
            if emv_year is not None and not (1900 <= emv_year <= 2100):
                emv_year = None

    # --- Taxation: newest POPULATED payable year ---
    tax_years, tax_rows = parse_year_columns(find_section(soup, "Taxation"))
    annual_tax = special_assessments = homestead_status = None
    net_values = tax_rows.get("Net Taxes Due", [])
    t_idx = _column_index(tax_years, net_values)
    if t_idx is not None:
        annual_tax = parse_money(net_values[t_idx])
        spec_values = tax_rows.get("Special Assessments", [])
        if t_idx < len(spec_values):
            special_assessments = parse_money(spec_values[t_idx])
        hs_values = tax_rows.get("Homestead Exclusion", [])
        if t_idx < len(hs_values):
            exclusion = parse_money(hs_values[t_idx])
            # >0 proves homestead. 0 proves nothing: the MN exclusion phases
            # out entirely above roughly $413k EMV, so a homesteaded higher
            # value house also reads $0. Unknown stays unknown.
            if exclusion is not None and exclusion > 0:
                homestead_status = "homestead"

    # --- Buildings: first populated card ---
    year_built = safe_year(parse_int(_ek_first(soup, _EK_YEAR_BUILT)))
    sqft = parse_int(_ek_first(soup, _EK_BLDG_SQFT))

    # --- Sales: newest row carrying a price ---
    sale_dates = _ek_all(soup, _EK_SALE_DATE)
    sale_prices = _ek_all(soup, _EK_SALE_PRICE)
    sale_codes = _ek_all(soup, _EK_SALE_CODE)
    sale_descs = _ek_all(soup, _EK_SALE_DESC)
    sales: list[dict[str, Any]] = []
    for i, sale_date in enumerate(sale_dates):
        sales.append(
            {
                "date": sale_date,
                "price": sale_prices[i] if i < len(sale_prices) else None,
                # Beacon appends 'opens in a new tab' to the linked eCRV cell.
                "ecrv": (
                    sale_codes[i].replace("opens in a new tab", "").strip()
                    if i < len(sale_codes)
                    else None
                ),
                "condition": sale_descs[i] if i < len(sale_descs) else None,
            }
        )
    last_sale_price = last_sale_date = None
    for sale in sales:
        price = parse_money(sale.get("price"))
        sale_date = parse_us_date(sale.get("date"))
        if price is not None and price > 0 and sale_date is not None:
            last_sale_price = price
            last_sale_date = sale_date
            break

    # --- Unpaid taxes: preserved verbatim, not promoted this pass ---
    unpaid: dict[str, str] = {}
    unpaid_section = find_section(soup, "Unpaid Taxes")
    if unpaid_section is not None:
        for tr in unpaid_section.find_all("tr"):
            cells = [c for c in _row_cells(tr) if c not in ("", "-", "+", "=")]
            if len(cells) >= 2:
                unpaid[cells[0]] = cells[-1]

    owner = parse_owner_block(soup)

    raw_data: dict[str, Any] = {
        "beacon_parcel_id_display": displayed,
        "property_address": address,
        "property_city_state_zip": csz,
        "legal_description": _ek_first(soup, _EK_LEGAL),
        "acres": str(acres) if acres is not None else None,
        "class_code": class_text,
        "tax_district": _ek_first(soup, _EK_TAXDIST),
        "valuation_years": val_years,
        "valuation_rows": val_rows,
        "taxation_years": tax_years,
        "taxation_rows": tax_rows,
        "sales": sales,
        "unpaid_taxes": unpaid or None,
        "owner": owner,
        "last_data_upload": (
            parse_last_upload(soup).isoformat()
            if parse_last_upload(soup)
            else None
        ),
    }

    return {
        "parcel_id": echoed,
        "address": address,
        "city": city,
        "state_abbr": state,
        "zip": zip_code,
        "legal_description": _ek_first(soup, _EK_LEGAL),
        "use_class": class_text,
        "property_type": map_property_type(class_text),
        "school_district": _ek_first(soup, _EK_TAXDIST),
        "year_built": year_built,
        "sqft": sqft,
        "lot_sqft": acres_to_lot_sqft(acres),
        "emv_total": emv_total,
        "emv_land": emv_land,
        "emv_building": emv_building,
        "emv_year": emv_year,
        "annual_tax": annual_tax,
        "special_assessments": special_assessments,
        "homestead_status": homestead_status,
        "last_sale_price": last_sale_price,
        "last_sale_date": last_sale_date,
        "source_date": raw_data["last_data_upload"],
        "owner": owner,
        "raw_data": raw_data,
    }


def classify_owner(owner_name: str | None) -> str | None:
    """Conservative OwnerType. Unmapped -> None, never 'unknown' invented."""
    if not owner_name:
        return None
    n = owner_name.upper()
    if " LLC" in f" {n}" or n.endswith("LLC"):
        return "llc"
    if "TRUST" in n:
        return "trust"
    if "ESTATE OF" in n:
        return "estate"
    if any(k in n for k in (" INC", " CORP", " COMPANY", " CO ")):
        return "corporation"
    if any(k in n for k in ("PARTNERSHIP", " LP", " LLP")):
        return "partnership"
    if any(
        k in n
        for k in ("CITY OF", "COUNTY OF", "STATE OF", "TOWNSHIP", "SCHOOL DIST")
    ):
        return "government"
    return None


def build_owner_row(
    parcel_id: str,
    county_code: str,
    source_name: str,
    owner: dict[str, Any],
    site_address: str | None,
    now_iso: str,
) -> dict[str, Any] | None:
    """Project the Owners block into a core.owners row, or None when the
    county publishes no owner (honest absence, never a placeholder)."""
    owner_name = owner.get("owner_name")
    if not owner_name:
        return None
    mailing_address = owner.get("mailing_address")
    mailing_state = owner.get("mailing_state")

    # Absentee compares STREET LINES, which the <br/> split makes clean.
    is_absentee: bool | None = None
    if mailing_address and site_address:
        is_absentee = (
            mailing_address.strip().upper() != site_address.strip().upper()
        )
    is_out_of_state: bool | None = (
        (mailing_state != "MN") if mailing_state else None
    )
    return {
        "parcel_id": parcel_id,
        # Composite since 2026-08-06: core.owners FKs (county_code, parcel_id).
        "county_code": county_code,
        "owner_name": owner_name,
        "owner_type": classify_owner(owner_name),
        "mailing_address": mailing_address,
        "mailing_city": owner.get("mailing_city"),
        "mailing_state": mailing_state,
        "mailing_zip": owner.get("mailing_zip"),
        "is_absentee": is_absentee,
        "is_out_of_state": is_out_of_state,
        "is_current": True,
        "source": source_name,
        "observed_at": now_iso,
    }


# ============================================================
# SCRAPER
# ============================================================


class BeaconParcelsScraper(BaseScraper[dict[str, Any], dict[str, Any]]):
    """Generic Beacon/Schneider parcel-report loader.

    ONE class per VENDOR. Host and identifiers come from core.vendor_portals,
    never from a literal here and never derived — the table's
    enabled_requires_verified_id_check constraint already carries a
    WHEN 'beacon' branch requiring vendor_ids ? 'AppID' and 'appid=<value>'
    present in verified_url, so a portal row cannot be enabled without an
    identifier read off that county's own URL.
    """

    source_name: ClassVar[str] = "blue_earth_parcels"
    signal_type: ClassVar[str] = "parcel_foundation"
    county_code: ClassVar[str] = _COUNTY_SLUG

    base_url: ClassVar[str] = _BASE_URL
    app_path: ClassVar[str] = _APP_PATH
    app_id: ClassVar[str] = _APP_ID
    layer_id: ClassVar[str] = _LAYER_ID
    page_id: ClassVar[str] = _PAGE_ID
    pin_prefix: ClassVar[str] = _PIN_PREFIX
    ecrv_county_cde: ClassVar[str] = _ECRV_COUNTY_CDE

    # Config key consulted instead of source_name. Registry-driven instances
    # set 'beacon_parcels' so every Beacon county shares ONE toggle, while
    # source_name stays PER-COUNTY because audit.scraper_runs and
    # audit.source_health key on it — a collapsed source_name would mark the
    # whole vendor unhealthy when one county failed. Same shape as
    # TylerTaxDetailScraper.enable_key and MNGACParcelsScraper.enable_key.
    enable_key: ClassVar[str] = "beacon_parcels"

    def __init__(
        self,
        pins: list[str] | None = None,
        *,
        county_code: str | None = None,
        source_name: str | None = None,
        base_url: str | None = None,
        app_path: str | None = None,
        app_id: str | None = None,
        layer_id: str | None = None,
        page_id: str | None = None,
        pin_prefix: str | None = None,
        ecrv_county_cde: str | None = None,
        enable_key: str | None = None,
        max_parcels: int | None = None,
    ) -> None:
        """pins: explicit work list (the capped test path). None means the
        eCRV-derived list for this county.

        max_parcels caps a run — use it for the first live test so a parser
        fault costs 25 requests, not 2,991.

        Everything after `pins` is keyword-only. Called with no arguments this
        is a pass-through and behaves exactly as the Blue Earth subclass does.
        """
        self._pins_override = pins
        self._max_parcels = max_parcels
        self._counters = {"new": 0, "updated": 0, "failed": 0}
        self._source_date: str | None = None
        if county_code:
            self.county_code = county_code
        if source_name:
            self.source_name = source_name
        if base_url:
            self.base_url = base_url.rstrip("/")
        if app_path:
            self.app_path = app_path
        if app_id:
            self.app_id = app_id
        if layer_id:
            self.layer_id = layer_id
        if page_id:
            self.page_id = page_id
        if pin_prefix:
            self.pin_prefix = pin_prefix
        if ecrv_county_cde:
            self.ecrv_county_cde = ecrv_county_cde
        if enable_key is not None:
            self.enable_key = enable_key

    # ---- Registry ----

    @classmethod
    def from_portal_row(
        cls, row: dict[str, Any], pins: list[str] | None = None
    ) -> "BeaconParcelsScraper":
        """Build a scraper for one core.vendor_portals row.

        Raises rather than substituting a default: a portal row missing an
        AppID must not silently fall back to Blue Earth's 387 and scrape
        Blue Earth under another county's name.
        """
        county = str(row["county_code"])
        vendor = str(row["vendor"])
        if vendor != "beacon":
            raise ValueError(
                f"vendor_portals row for '{county}' is vendor={vendor!r}, "
                "not 'beacon' — wrong scraper for this portal"
            )
        ids = row.get("vendor_ids") or {}
        app_id = ids.get("AppID")
        layer_id = ids.get("LayerID")
        page_id = ids.get("PageID")
        missing = [
            name
            for name, value in (
                ("AppID", app_id),
                ("LayerID", layer_id),
                ("PageID", page_id),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                f"vendor_portals row for '{county}' has no verified "
                f"{', '.join(missing)} in vendor_ids — refusing to guess "
                "a Beacon application identifier"
            )
        ecrv_cde = ids.get("ecrv_county_cde")
        if not ecrv_cde:
            raise ValueError(
                f"vendor_portals row for '{county}' has no "
                "'ecrv_county_cde' in vendor_ids — the work list cannot be "
                "built without it"
            )
        prefix = str(row.get("app_prefix") or "").rstrip("/")
        return cls(
            pins=pins,
            county_code=county,
            source_name=f"{county}_parcels",
            base_url=str(row["base_url"]).rstrip("/"),
            app_path=f"{prefix}{_APP_PATH}",
            app_id=str(app_id),
            layer_id=str(layer_id),
            page_id=str(page_id),
            pin_prefix=str(ids.get("pin_prefix") or _PIN_PREFIX),
            ecrv_county_cde=str(ecrv_cde),
            enable_key="beacon_parcels",
        )

    @staticmethod
    def load_enabled_portals() -> list[dict[str, Any]]:
        """Enabled Beacon rows from core.vendor_portals."""
        result = (
            core_table("vendor_portals")
            .select(
                "county_code,vendor,base_url,app_prefix,vendor_ids,enabled"
            )
            .eq("vendor", "beacon")
            .eq("enabled", True)
            .execute()
        )
        return list(result.data or [])

    # ---- Work list ----

    def report_url(self, pin: str) -> str:
        return (
            f"{self.base_url}{self.app_path}"
            f"?AppID={self.app_id}"
            f"&LayerID={self.layer_id}"
            f"&PageTypeID={_PAGE_TYPE_REPORT}"
            f"&PageID={self.page_id}"
            f"&KeyValue={pin}"
        )

    def _load_pins(self) -> list[str]:
        """The parcels eCRV knows about and core.parcels does not.

        This is the eCRV-driven work list, and it is deliberately NOT the
        county's full roll. 2,991 distinct parcels against ~34,000 is a tenth
        of the requests, it produces exactly the parcels the eCRV join and
        the AVM need, and the same loader takes a full enumeration later via
        the `pins` argument when /connect/lookup coverage becomes the
        priority. One design, not two passes.

        PostgREST caps a response at 1,000 rows and there is no paging helper
        in supabase_client, so this pages explicitly with .range().
        """
        if self._pins_override:
            pins = sorted({p.strip().upper() for p in self._pins_override if p})
            logger.info(
                "PIN override in effect",
                source=self.source_name,
                pins=len(pins),
            )
            return pins[: self._max_parcels] if self._max_parcels else pins

        norms: set[str] = set()
        page_size = 1000
        start = 0
        while True:
            result = (
                outcomes_table("ecrv_sales")
                .select("parcel_norm")
                .eq("county_cde", self.ecrv_county_cde)
                .not_.is_("parcel_norm", "null")
                .range(start, start + page_size - 1)
                .execute()
            )
            rows = result.data or []
            for row in rows:
                value = row.get("parcel_norm")
                if value:
                    norms.add(str(value).strip())
            if len(rows) < page_size:
                break
            start += page_size

        pins = sorted({p for p in (pin_from_parcel_norm(n) for n in norms) if p})
        malformed = len(norms) - len(pins)
        if malformed:
            logger.warning(
                "eCRV parcel_norm values that are not 12 digits — skipped",
                source=self.source_name,
                county_cde=self.ecrv_county_cde,
                malformed=malformed,
            )
        if not pins:
            raise SourceUnavailableError(
                f"No usable parcel_norm values in outcomes.ecrv_sales for "
                f"county_cde='{self.ecrv_county_cde}' — nothing to scrape",
                source=self.source_name,
            )
        logger.info(
            "Work list built from eCRV",
            source=self.source_name,
            county_cde=self.ecrv_county_cde,
            distinct_norms=len(norms),
            pins=len(pins),
        )
        return pins[: self._max_parcels] if self._max_parcels else pins

    # ---- HTTP ----

    async def _get_report(self, client: httpx.AsyncClient, pin: str) -> str:
        response = await client.get(
            self.report_url(pin),
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        if response.status_code in (403, 429, 503):
            raise _BlockedError(f"HTTP {response.status_code}")
        response.raise_for_status()
        return response.text

    async def _scrape_parcel(
        self, client: httpx.AsyncClient, pin: str
    ) -> dict[str, Any]:
        """One parcel, one GET, with escalating silence on a block."""
        last_error: Exception | None = None
        for attempt in range(_PER_PARCEL_ATTEMPTS):
            try:
                html = await self._get_report(client, pin)
                return parse_report(html, pin)
            except _NotFoundError:
                raise
            except _BlockedError as e:
                last_error = e
                wait = _BLOCK_BACKOFFS[min(attempt, len(_BLOCK_BACKOFFS) - 1)]
                logger.warning(
                    "Blocked by host — going quiet",
                    source=self.source_name,
                    pin=pin,
                    attempt=attempt + 1,
                    sleep_seconds=wait,
                )
                await asyncio.sleep(wait)
            except (httpx.HTTPError, ValueError) as e:
                last_error = e
                if isinstance(e, ValueError):
                    # Identity mismatch is not transient — do not retry.
                    raise
                await asyncio.sleep(2 * (attempt + 1))
        raise _BlockedError(
            f"{pin}: exhausted {_PER_PARCEL_ATTEMPTS} attempts "
            f"({type(last_error).__name__}: {last_error})"
        )

    # ---- Lifecycle ----

    async def run(
        self,
        *,
        trigger: str = "scheduler",
        metadata: dict[str, Any] | None = None,
    ) -> RunResult:
        """BaseScraper.run() gates on scraper_enabled(self.source_name).

        source_name is PER-COUNTY here, so that check would demand a config
        field per county — the artefact multiplication the vendor registry
        removes. This override consults enable_key instead and then delegates
        to the UNMODIFIED _run_locked, so audit, freshness, health and the
        lock behave exactly as they do for every other scraper.
        """
        start_time = time.monotonic()
        gate = self.enable_key or self.source_name

        if not settings.scraper_enabled(gate):
            if trigger == "manual":
                raise ScraperDisabledError(
                    f"Scraper '{self.source_name}' is disabled "
                    f"(config gate '{gate}')",
                    source=self.source_name,
                )
            return RunResult(
                scraper_name=self.source_name,
                run_id=None,
                status="skipped",
                duration_seconds=0.0,
                error_message=f"Disabled in settings (gate '{gate}')",
            )

        if self._class_lock.locked():
            raise ScraperAlreadyRunningError(
                f"Scraper '{self.source_name}' is already running",
                source=self.source_name,
                context={"scraper_name": self.source_name},
            )

        with self._class_lock:
            return await self._run_locked(trigger, metadata, start_time)

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """Scrape the work list, PERSISTING every _CHECKPOINT_PARCELS.

        Checkpointing is the olmsted_tax_detail lesson: a fetch that returns
        only at the end means a cancelled run writes nothing, and three
        consecutive Tuesday runs were lost that way. Counters accumulate on
        self and write() returns them.
        """
        pins = self._load_pins()
        self._counters = {"new": 0, "updated": 0, "failed": 0}
        collected: list[dict[str, Any]] = []
        pending: list[dict[str, Any]] = []
        blocked_streak = 0
        skipped_not_found = 0
        skipped_identity = 0

        logger.info(
            "Beacon parcel run starting",
            source=self.source_name,
            county_code=self.county_code,
            parcels=len(pins),
            estimated_minutes=round(len(pins) * _POLITE_DELAY_SECONDS / 60, 1),
        )

        async with httpx.AsyncClient(
            timeout=_REQUEST_TIMEOUT, follow_redirects=True
        ) as client:
            for index, pin in enumerate(pins, start=1):
                try:
                    record = await self._scrape_parcel(client, pin)
                    blocked_streak = 0
                    if record.get("source_date") and not self._source_date:
                        self._source_date = record["source_date"]
                    pending.append(record)
                    collected.append(record)
                except _NotFoundError:
                    skipped_not_found += 1
                    self._counters["failed"] += 1
                except ValueError as e:
                    skipped_identity += 1
                    self._counters["failed"] += 1
                    logger.warning(
                        "Identity guard rejected a page — NOT written",
                        source=self.source_name,
                        pin=pin,
                        error=str(e)[:200],
                    )
                except _BlockedError as e:
                    blocked_streak += 1
                    self._counters["failed"] += 1
                    logger.warning(
                        "Parcel abandoned after block backoff",
                        source=self.source_name,
                        pin=pin,
                        blocked_streak=blocked_streak,
                        error=str(e)[:200],
                    )
                    if blocked_streak >= _ABORT_AFTER_BLOCKED_PARCELS:
                        # A block that survives the full ladder on several
                        # consecutive parcels is a session-wide refusal, not
                        # a per-parcel blip. Stop and KEEP what was collected.
                        logger.error(
                            "Aborting run — host refusal appears session-wide",
                            source=self.source_name,
                            parcels_done=index,
                            parcels_total=len(pins),
                        )
                        break

                if len(pending) >= _CHECKPOINT_PARCELS:
                    self._persist(pending)
                    pending = []

                if index % _BATCH_SIZE == 0 and index < len(pins):
                    logger.info(
                        "Proactive rest",
                        source=self.source_name,
                        parcels_done=index,
                        parcels_total=len(pins),
                        new=self._counters["new"],
                        updated=self._counters["updated"],
                        failed=self._counters["failed"],
                    )
                    await asyncio.sleep(_BATCH_REST_SECONDS)
                else:
                    await asyncio.sleep(_POLITE_DELAY_SECONDS)

        if pending:
            self._persist(pending)

        logger.info(
            "Beacon parcel run finished fetching",
            source=self.source_name,
            scraped=len(collected),
            skipped_not_found=skipped_not_found,
            skipped_identity=skipped_identity,
            source_date=self._source_date,
        )
        return collected

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Records are already typed by parse_report. This exists so source
        freshness has something to read: every signal carries event_date =
        the county's own Last Data Upload stamp."""
        for record in raw_records:
            record["event_date"] = record.get("source_date")
        return raw_records

    async def write(self, signals: list[dict[str, Any]]) -> tuple[int, int, int]:
        """Return the counters accumulated during fetch().

        The writes already happened, at checkpoints. Writing again here would
        double-count and re-touch every row.
        """
        return (
            self._counters["new"],
            self._counters["updated"],
            self._counters["failed"],
        )

    # ---- Persistence ----

    def _persist(self, records: list[dict[str, Any]]) -> None:
        """Upsert one checkpoint of parcels, plus their owners."""
        if not records:
            return
        run_started = datetime.now(timezone.utc)
        now_iso = run_started.isoformat()
        batch: list[dict[str, Any]] = []
        owner_batch: list[dict[str, Any]] = []

        for record in records:
            try:
                payload = ParcelUpsert(
                    parcel_id=record["parcel_id"],
                    county_code=self.county_code,
                    state="MN",
                    address=record.get("address"),
                    city=record.get("city"),
                    zip=record.get("zip"),
                    property_type=record.get("property_type"),  # type: ignore[arg-type]
                    year_built=record.get("year_built"),
                    sqft=record.get("sqft"),
                    lot_sqft=record.get("lot_sqft"),
                    # BOTH value columns, deliberately. Measured 2026-09-01:
                    # olmsted, ramsey, fillmore and stearns all populate
                    # estimated_market_value AND emv_total. The view and the
                    # UI read emv_total; estimated_market_value is the legacy
                    # parallel column and something still reads it.
                    estimated_market_value=record.get("emv_total"),
                    emv_total=record.get("emv_total"),
                    emv_land=record.get("emv_land"),
                    emv_building=record.get("emv_building"),
                    emv_year=record.get("emv_year"),
                    annual_tax=record.get("annual_tax"),
                    special_assessments=record.get("special_assessments"),
                    homestead_status=record.get("homestead_status"),
                    school_district=record.get("school_district"),
                    use_class=record.get("use_class"),
                    legal_description=record.get("legal_description"),
                    last_sale_price=record.get("last_sale_price"),
                    last_sale_date=record.get("last_sale_date"),
                    raw_data=record.get("raw_data"),
                    data_sources=[self.source_name],
                    last_observed_at=run_started,
                )
            except Exception as e:
                self._counters["failed"] += 1
                logger.warning(
                    "Parcel validation failed",
                    source=self.source_name,
                    parcel_id=record.get("parcel_id"),
                    error=str(e)[:200],
                )
                continue

            # dump_owned(), NOT exclude_none=True. This is a FULL-REFRESH
            # writer: a field the county did not publish is ABSENT, and only
            # emitting the null lets a re-run clear a value an earlier write
            # got wrong. See ParcelUpsert's docstring.
            row = payload.dump_owned()
            row["last_observed_at"] = now_iso
            batch.append(row)

            owner_row = build_owner_row(
                record["parcel_id"],
                self.county_code,
                self.source_name,
                record.get("owner") or {},
                record.get("address"),
                now_iso,
            )
            if owner_row is not None:
                owner_batch.append(owner_row)

            if len(batch) >= _DB_BATCH_SIZE:
                self._upsert_batch(batch, run_started)
                batch = []
            if len(owner_batch) >= _DB_BATCH_SIZE:
                self._upsert_owner_batch(owner_batch)
                owner_batch = []

        if batch:
            self._upsert_batch(batch, run_started)
        if owner_batch:
            self._upsert_owner_batch(owner_batch)

    def _upsert_batch(
        self, batch: list[dict[str, Any]], run_started: datetime
    ) -> None:
        """Upsert one batch and split inserts from updates.

        PostgREST returns EVERY row it touched, so len(result.data) counts
        nothing. core.parcels.created_at defaults to now() and is never in the
        payload, so a returned row created at or after this checkpoint's start
        is new and everything else already existed — the ramsey_parcels
        discriminator, no extra round trip.
        """
        if not batch:
            return
        try:
            result = (
                core_table("parcels")
                .upsert(batch, on_conflict="county_code,parcel_id")
                .execute()
            )
            rows = result.data or []
            if not rows:
                # The write succeeded but returned no representation. The
                # split is unknowable; call them updates so growth is
                # understated rather than invented.
                self._counters["updated"] += len(batch)
                return
            inserted = 0
            for row in rows:
                created = row.get("created_at")
                parsed = _parse_ts(created)
                if parsed is not None and parsed >= run_started:
                    inserted += 1
            self._counters["new"] += inserted
            self._counters["updated"] += len(rows) - inserted
        except Exception as e:
            self._counters["failed"] += len(batch)
            logger.warning(
                "Batch upsert to core.parcels failed",
                source=self.source_name,
                batch_size=len(batch),
                error=str(e)[:500],
            )

    def _upsert_owner_batch(self, batch: list[dict[str, Any]]) -> None:
        """Owners are enrichment: failures are logged and NEVER fail the run.
        The parcel write is the source of truth for run status."""
        if not batch:
            return
        try:
            (
                core_table("owners")
                .upsert(batch, on_conflict="county_code,parcel_id,source")
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Batch upsert to core.owners failed (non-fatal)",
                source=self.source_name,
                batch_size=len(batch),
                error=str(e)[:500],
            )


def _parse_ts(value: Any) -> datetime | None:
    """Parse a PostgREST timestamp into an aware datetime, or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class BlueEarthParcelsScraper(BeaconParcelsScraper):
    """Blue Earth County — the first Beacon county.

    Kept as a named subclass so runners, workflows and
    POST /trigger/blue_earth_parcels have a stable name, and so it gets its
    OWN _class_lock (BaseScraper.__init_subclass__) rather than sharing one
    with a registry-driven instance of another county.

    Identifiers verified live 2026-09-01 against the county's own URL.
    """

    source_name: ClassVar[str] = "blue_earth_parcels"
    county_code: ClassVar[str] = _COUNTY_SLUG
    base_url: ClassVar[str] = _BASE_URL
    app_path: ClassVar[str] = _APP_PATH
    app_id: ClassVar[str] = _APP_ID
    layer_id: ClassVar[str] = _LAYER_ID
    page_id: ClassVar[str] = _PAGE_ID
    pin_prefix: ClassVar[str] = _PIN_PREFIX
    ecrv_county_cde: ClassVar[str] = _ECRV_COUNTY_CDE
    enable_key: ClassVar[str] = "beacon_parcels"


__all__ = [
    "BeaconParcelsScraper",
    "BlueEarthParcelsScraper",
    "parse_report",
    "flatten_pin",
    "pin_from_parcel_norm",
    "map_property_type",
    "acres_to_lot_sqft",
    "parse_last_upload",
]
