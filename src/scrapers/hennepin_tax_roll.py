"""
Hennepin County Tax-Roll miner (delinquent + forfeited land).

Source: NOT an external website — this is a DATABASE MINING job. It reads the
        Hennepin parcels already loaded in core.parcels (by the
        hennepin_parcels scraper) and derives tax-distress signals from them.
        This is the "distress mining happens later via raw_data queries" step
        the hennepin_parcels docstring refers to.

=== THE TWO RULES (reverse-engineered + verified against the live data) ===
Every Hennepin parcel falls into at most one tax-distress bucket. The rules
were confirmed by reconciling against the original one-time load:

  1. TAX-FORFEIT (139 parcels):
        raw_data->>'FORFEIT_LAND_IND' == 'T'
     The redemption period expired; the parcel is forfeited to the State of
     Minnesota, now owned by "HENNEPIN FORFEITED LAND", awaiting auction.
     -> event_type = 'tax_forfeit', subtype 'state_forfeited_land'

  2. TAX-DELINQUENT (4,112 parcels):
        raw_data->>'EARLIEST_DELQ_YR' present AND non-empty
        AND NOT forfeited (the two sets are mutually exclusive — verified:
        delq_not_forfeit == has_delq_yr == 4112, forfeit == 139, sum 4251)
     The owner is behind on property taxes but the parcel has NOT yet
     forfeited. Early-stage distress.
     -> event_type = 'tax_delinquent', subtype 'property_tax_delinquent'

Verified counts: 139 forfeit + 4,112 delinquent = 4,251 — matches the
original load exactly, with zero overlap.

=== VERIFIED FIELD NAMES (from live core.parcels rows) ===
  FORFEIT_LAND_IND  'T' marks forfeited land
  EARLIEST_DELQ_YR  two-digit delinquency year, e.g. '25' = 2025
  OWNER_NM          owner name
  MKT_VAL_TOT       total market value
  MUNIC_NM          municipality

=== raw_data SHAPES (match the original rows so re-mining dedups cleanly) ===
  forfeit:    {owner_name, market_value, municipality,
               _derived_from, forfeit_land_ind:'T'}
  delinquent: {owner_name, market_value, municipality,
               _derived_from, earliest_delq_year:<int>, earliest_delq_yr_raw}

=== ARCHITECTURE ===
Unlike the web scrapers, fetch() queries the database instead of HTTP:
  fetch():  page through core.parcels for Hennepin, pulling any parcel that
            is either forfeited OR has a delinquency year.
  parse():  classify each parcel into tax_forfeit or tax_delinquent and emit
            the matching DistressEventInsert.
  write():  write_events_dedup (idempotent — re-mining is safe; unchanged
            parcels produce new=0).

=== event_value IS NULL ON BOTH PATHS — DELIBERATE (2026-08-12) ===
Both events used to set event_value=market_value. That field means "the
amount at stake in this event": the debt on a delinquency, the minimum bid
on a forfeit. The Hennepin parcel roll publishes NEITHER.

The roll flags that a parcel is delinquent (EARLIEST_DELQ_YR) and what it is
worth (MKT_VAL_TOT). It does not publish what is owed. Writing the market
value there asserted a debt figure that was never measured — 3,943 rows
where amount-owed and market value were byte-identical, including one at
$76,700,000 against an actual annual tax of $2,557,224.

The same applied to forfeits, where event_value renders as "Min. bid": these
rows are mined from the roll, not from an auction listing, so no minimum bid
exists to report.

market_value stays in raw_data, and the display reads market value from the
parcel spine (emv_total) — nothing is lost by this being NULL. An em-dash
saying "we don't hold this" is honest; a number that is not the debt is not.
GOVIRE_FOUR_TIER_SPEC.md: never conflate "you can't see this" with "this
doesn't exist".

If Hennepin ever publishes a delinquent balance, it goes here — and only
then.

Severity:
  tax_forfeit                     -> medium (already seized; awaiting auction)
  tax_delinquent, older year      -> higher distress (longer behind)
    delinquent >= 3 years         -> high
    delinquent 1-2 years          -> medium
    delinquent current year       -> low
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from src.db.supabase_client import core_table, signals_table
from src.models.signal import DistressEventInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import write_events_dedup
from src.utils.errors import SourceUnavailableError
from src.utils.logger import logger


# Verified Hennepin parcel attribute names.
_FORFEIT_FIELD = "FORFEIT_LAND_IND"
_FORFEIT_TRUE = "T"
_DELQ_YR_FIELD = "EARLIEST_DELQ_YR"
_OWNER_FIELD = "OWNER_NM"
_MKT_VAL_FIELD = "MKT_VAL_TOT"
_MUNIC_FIELD = "MUNIC_NM"
_HOUSE_NO_FIELD = "HOUSE_NO"
_STREET_FIELD = "STREET_NM"
_ZIP_FIELD = "ZIP_CD"
_TAX_TOT_FIELD = "TAX_TOT"
# Owner mailing address lines (verified): TAXPAYER_NM_1 = street,
# TAXPAYER_NM_2 = "CITY ST ZIP". TAXPAYER_NM is the taxpayer name.
_MAIL_LINE1_FIELD = "TAXPAYER_NM_1"
_MAIL_LINE2_FIELD = "TAXPAYER_NM_2"
_TAXPAYER_NAME_FIELD = "TAXPAYER_NM"

# Hennepin uses this literal when a parcel has no assigned street address
# (common for vacant forfeited land). We surface it honestly rather than
# pretending there's an address.
_ADDR_UNASSIGNED = "ADDRESS UNASSIGNED"

# HONEST NULL event_date for forfeit rows (2026-07-09). Forfeiture has no
# date in the parcel data — the county flag says THAT it forfeited, not
# WHEN. The old 2000-01-01 sentinel existed only because event_date is in
# the dedup key and NULL used to break re-mining idempotency; since the
# 2026-07-07 index fix the key is NULLS NOT DISTINCT, so NULL dedups
# exactly like a constant. Unknown is NULL, never a fabricated date.
# (Existing sentinel rows converted by
#  MIGRATION_tax_roll_honest_null_dates_2026-07-09.sql.)

# Read paging. We pull the union of (forfeited OR has delinquency year),
# which is ~4,251 of 448K — small, but page defensively.
_READ_PAGE_SIZE = 1000
_MAX_PAGES = 200

# ============================================================
# RETIREMENT (de-listing) — added 2026-08-16
# ============================================================
# THE PROBLEM THIS SOLVES
# write_events_dedup is ON CONFLICT DO NOTHING: it inserts and never
# retires. So this table was the union of every parcel that has EVER been
# tax-distressed since the first mine, not the set that is distressed NOW.
#
# Measured 2026-08-16, before this existed:
#   tax_delinquent  4,113 stored   3,112 still true   1,001 NOT true
#   tax_forfeit       142 stored     137 still true       5 NOT true
# 1,006 of 4,255 events — 23.6% — described properties the county's own
# roll says are current. A subscriber filtering Hennepin tax-delinquent
# got a thousand dead leads. Olmsted showed the same shape at 49%.
#
# WHY THE RETIREMENT LIVES IN THIS FILE AND NOT A SEPARATE JOB
# The retirement predicate MUST be the exact inverse of the mining
# predicate. In one file they cannot drift; in two files they can, and a
# drifted predicate retires live events or keeps dead ones — worse than
# doing nothing. This scraper owns the source's truth end to end:
# mine -> insert -> retire.
#
# THE ESCALATION CASE (this is why "not in the set" is not enough)
# 46 delinquent events had parcels that have since FORFEITED. A naive
# "no longer delinquent -> cured" rule stamps all 46 as cured — recording
# that the owner paid, when they lost the property to the State. Verified
# 2026-08-16: all 46 have delq_year NULL and FORFEIT_LAND_IND='T', with
# delinquency years 2018-2024 (never 2025 — consistent with Minnesota's
# ~3-year clock). They include 2629 Lake St E at $332,500 EMV.
#
# RESOLUTION MAPPING, each branch backed by a measured count:
#   tax_delinquent, parcel now forfeited      ->  'forfeited'       (46)
#   tax_delinquent, parcel qualifies for none ->  'cured'          (955)
#   tax_forfeit,    parcel qualifies for none ->  'source_removed'   (5)
#
# 'source_removed', NOT 'sold', for the forfeit case. Forfeited land
# usually leaves the roll via county auction, but it can also be conveyed
# to a city or reclassified. We observed a disappearance, not a sale.
# Same principle as event_value being NULL above: never assert a fact the
# source did not publish.
#
# NOTHING IS EVER DELETED. resolved_at + resolution are stamped; the row,
# its dates and its raw_data stay exactly as written. A cured delinquency
# is itself an outcome signal for the ML labels.
_RETIRE_PAGE_SIZE = 500

# Safety cap on a single run's retirements. The known backlog is ~1,006;
# anything beyond this means the mining predicate returned far less than
# it should (an empty fetch would otherwise retire the entire source), so
# stop and report rather than mass-retire on a bad read.
_RETIRE_MAX_PER_RUN = 2000

# Skip retirement entirely when the mine came back suspiciously small.
# A partial county load or a failed page would otherwise look exactly
# like "everybody paid their taxes".
_RETIRE_MIN_MINED = 100

_FORFEIT_TITLE = "Tax-forfeited land (state)"
_FORFEIT_DESC = (
    "Parcel forfeited to the State of Minnesota for unpaid property taxes. "
    "Subject to county liquidation under the post-Tyler v. Hennepin reforms."
)
_DELQ_TITLE = "Tax-delinquent property"
_DELQ_DESC = (
    "Property is behind on Hennepin County property taxes (delinquent since "
    "{year}). Unresolved delinquency can proceed toward tax forfeiture."
)


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        d = Decimal(str(value))
        return d if d >= 0 else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _expand_delq_year(raw_yr: str | None) -> int | None:
    """Convert Hennepin's 2-digit delinquency year ('25') to a full year.

    Century pivot (2026-07-09): map '00'-'99' to 20xx, EXCEPT that a
    delinquency cannot begin in the future — a 2-digit year that would
    land past the current year is a 19xx year ('84' = 1984, a parcel
    delinquent for decades, not one delinquent in 2084). Live data
    disproved the earlier "all observed values are recent" assumption:
    one '84' row existed, dated 2084 and mis-scored low severity."""
    s = _safe_str(raw_yr)
    if s is None:
        return None
    digits = s.zfill(2)[-2:]
    if not digits.isdigit():
        return None
    year = 2000 + int(digits)
    if year > date.today().year:
        year -= 100
    return year


def _compose_property_address(raw: dict[str, Any]) -> str | None:
    """Build the property street address from HOUSE_NO + STREET_NM.

    Returns None when the street is genuinely unassigned (vacant land) so the
    display can show an honest "Address unassigned" rather than a fake address.
    """
    street = _safe_str(raw.get(_STREET_FIELD))
    if street is None or street.upper() == _ADDR_UNASSIGNED:
        return None
    house_no = _safe_str(raw.get(_HOUSE_NO_FIELD))
    return f"{house_no} {street}".strip() if house_no else street


def _compose_owner_mailing(raw: dict[str, Any]) -> str | None:
    """Join the owner's mailing address lines (TAXPAYER_NM_1 + _2)."""
    line1 = _safe_str(raw.get(_MAIL_LINE1_FIELD))
    line2 = _safe_str(raw.get(_MAIL_LINE2_FIELD))
    parts = [p for p in (line1, line2) if p]
    return ", ".join(parts) if parts else None


def _is_absentee(property_addr: str | None, mailing_line1: str | None) -> bool:
    """Absentee owner = mailing street differs from the property street.

    A strong motivated-seller signal. We compare the property street line to
    the mailing street line (TAXPAYER_NM_1). Conservative: only True when we
    have both and they clearly differ.
    """
    if not property_addr or not mailing_line1:
        return False
    # Normalize: uppercase, collapse whitespace.
    p = " ".join(property_addr.upper().split())
    m = " ".join(mailing_line1.upper().split())
    return p != m


class HennepinTaxRollScraper(BaseScraper[dict[str, Any], DistressEventInsert]):
    """Hennepin tax-roll miner: forfeited land + delinquent properties."""

    source_name: ClassVar[str] = "hennepin_tax_roll"
    signal_type: ClassVar[str] = "tax_roll"
    county_code: ClassVar[str] = "hennepin"

    # ---- Fetch: query core.parcels for forfeited OR delinquent parcels ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """
        Read forfeited + delinquent Hennepin parcels via TWO separate queries.

        We deliberately do NOT use a single OR query. EXPLAIN showed the OR
        (plus the order-by) made the planner ignore our indexes and full-scan
        448K rows (5s, timeout-prone). Split into two single-condition queries
        and each uses its own index cleanly:

          forfeited:  raw_data->>'FORFEIT_LAND_IND' = 'T'
                      -> idx_parcels_hennepin_forfeit  (~8ms, 139 rows)
          delinquent: raw_data->>'EARLIEST_DELQ_YR' is not null
                      -> idx_parcels_hennepin_delq (partial)  (~73ms, 4112 rows)

        Both result sets are small (~4,251 total), so we fetch each whole
        (paged defensively) and combine in Python. parse() then classifies.
        """
        all_rows: list[dict[str, Any]] = []

        # --- Query 1: forfeited land (equality on indexed expression) ---
        forfeited = await self._read_filtered(
            label="forfeited",
            apply_filter=lambda q: q.eq(
                f"raw_data->>{_FORFEIT_FIELD}", _FORFEIT_TRUE
            ),
        )
        all_rows.extend(forfeited)

        # --- Query 2: delinquent (not-null, hits the partial index) ---
        delinquent = await self._read_filtered(
            label="delinquent",
            apply_filter=lambda q: q.not_.is_(
                f"raw_data->>{_DELQ_YR_FIELD}", "null"
            ),
        )
        all_rows.extend(delinquent)

        logger.info(
            "Hennepin tax-roll fetch complete",
            source=self.source_name,
            forfeited=len(forfeited),
            delinquent=len(delinquent),
            candidate_parcels=len(all_rows),
        )
        return all_rows

    async def _read_filtered(self, label, apply_filter) -> list[dict[str, Any]]:
        """Cursor-paginate one single-condition parcel query by parcel_id.

        `apply_filter` adds the specific WHERE condition to the base query so
        each call hits its own index. Cursor pagination (parcel_id > last)
        keeps every page cheap and avoids offset re-scans.
        """
        rows_out: list[dict[str, Any]] = []
        last_parcel_id = ""

        for page in range(_MAX_PAGES):
            try:
                q = (
                    core_table("parcels")
                    .select("parcel_id, county_code, address, city, raw_data")
                    .eq("county_code", self.county_code)
                )
                q = apply_filter(q)
                resp = (
                    q.gt("parcel_id", last_parcel_id)
                    .order("parcel_id")
                    .limit(_READ_PAGE_SIZE)
                    .execute()
                )
            except Exception as e:
                raise SourceUnavailableError(
                    f"Reading core.parcels ({label}) for tax-roll mining "
                    f"failed: {type(e).__name__}: {e}",
                    source=self.source_name,
                    context={"label": label, "page": page,
                             "cursor": last_parcel_id},
                ) from e

            rows = resp.data or []
            if not rows:
                break

            rows_out.extend(rows)
            last_parcel_id = rows[-1]["parcel_id"]

            logger.info(
                "Hennepin tax-roll page read",
                source=self.source_name,
                set=label,
                page=page + 1,
                rows=len(rows),
                cumulative=len(rows_out),
            )

            if len(rows) < _READ_PAGE_SIZE:
                break

        return rows_out

    # ---- Parse: classify each parcel into forfeit or delinquent ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[DistressEventInsert]:
        signals: list[DistressEventInsert] = []
        today = date.today()
        n_forfeit = 0
        n_delq = 0

        for row in raw_records:
            parcel_id = _safe_str(row.get("parcel_id"))
            if not parcel_id:
                continue

            raw = row.get("raw_data") or {}
            owner_name = _safe_str(raw.get(_OWNER_FIELD))
            municipality = (
                _safe_str(row.get("city")) or _safe_str(raw.get(_MUNIC_FIELD))
            )
            market_value = _safe_decimal(raw.get(_MKT_VAL_FIELD))
            mv_str = str(market_value) if market_value is not None else "0"

            # --- Enrichment: property identification + owner contact ---
            # Prefer the row's own address column; fall back to composing it
            # from the parcel raw_data (HOUSE_NO + STREET_NM).
            prop_addr = (
                _safe_str(row.get("address"))
                or _compose_property_address(raw)
            )
            prop_zip = _safe_str(raw.get(_ZIP_FIELD))
            if prop_zip in ("00000", "0"):
                prop_zip = None  # Hennepin uses 00000 for "no zip"
            annual_tax = _safe_decimal(raw.get(_TAX_TOT_FIELD))
            tax_tot_str = str(annual_tax) if annual_tax is not None else None
            owner_mailing = _compose_owner_mailing(raw)
            absentee = _is_absentee(prop_addr, _safe_str(raw.get(_MAIL_LINE1_FIELD)))

            forfeit_flag = _safe_str(raw.get(_FORFEIT_FIELD))
            delq_raw = _safe_str(raw.get(_DELQ_YR_FIELD))

            # --- Rule 1: forfeited (takes precedence; mutually exclusive) ---
            if forfeit_flag is not None and forfeit_flag.upper() == _FORFEIT_TRUE:
                signals.append(DistressEventInsert(
                    parcel_id=parcel_id,
                    event_type="tax_forfeit",
                    event_subtype="state_forfeited_land",
                    # Honest NULL — the county flag carries no forfeiture
                    # date. Dedup key is NULLS NOT DISTINCT (2026-07-07),
                    # so re-mining stays idempotent.
                    event_date=None,
                    # NULL, not market_value — see the module docstring. This
                    # field renders as "Min. bid" on the forfeit tab, and a
                    # roll-mined parcel has no auction listing and therefore
                    # no minimum bid. market_value is kept in raw_data below.
                    event_value=None,
                    source=self.source_name,
                    source_id=parcel_id,
                    severity="medium",  # type: ignore[arg-type]
                    title=_FORFEIT_TITLE,
                    description=_FORFEIT_DESC,
                    raw_data={
                        "owner_name": owner_name or "HENNEPIN FORFEITED LAND",
                        "market_value": mv_str,
                        "municipality": municipality,
                        # Property identification.
                        "property_address": prop_addr,
                        "property_city": municipality,
                        "property_zip": prop_zip,
                        # For forfeited land the "owner" is the State; the
                        # action path is the county auction, not contacting an
                        # owner. Mailing/absentee are not meaningful here.
                        "owner_mailing": None,
                        "is_absentee": False,
                        "annual_tax": tax_tot_str,
                        "_derived_from": "hennepin_parcels.raw_data",
                        "forfeit_land_ind": _FORFEIT_TRUE,
                    },
                    observed_at=datetime.now(timezone.utc),
                ))
                n_forfeit += 1
                continue

            # --- Rule 2: delinquent (has a delinquency year, not forfeited) ---
            if delq_raw is not None:
                delq_year = _expand_delq_year(delq_raw)
                # Severity scales with how long the parcel has been behind.
                if delq_year is not None:
                    years_behind = today.year - delq_year
                else:
                    years_behind = 0
                if years_behind >= 3:
                    severity = "high"
                elif years_behind >= 1:
                    severity = "medium"
                else:
                    severity = "low"

                desc = _DELQ_DESC.format(
                    year=delq_year if delq_year is not None else "unknown"
                )

                # Stable, meaningful event_date: Jan 1 of the delinquency year.
                # Same value every run (idempotent dedup) AND it represents
                # when the delinquency began. Honest NULL only if the year
                # somehow won't parse (verified: 0 such rows).
                delq_event_date = (
                    date(delq_year, 1, 1)
                    if delq_year is not None
                    else None
                )

                signals.append(DistressEventInsert(
                    parcel_id=parcel_id,
                    event_type="tax_delinquent",
                    event_subtype="property_tax_delinquent",
                    event_date=delq_event_date,
                    # NULL, not market_value — see the module docstring. The
                    # roll publishes no delinquent balance; annual_tax below
                    # is the only real money figure it carries.
                    event_value=None,
                    source=self.source_name,
                    source_id=parcel_id,
                    severity=severity,  # type: ignore[arg-type]
                    title=_DELQ_TITLE,
                    description=desc,
                    raw_data={
                        "owner_name": owner_name,
                        "market_value": mv_str,
                        "municipality": municipality,
                        # Property identification.
                        "property_address": prop_addr,
                        "property_city": municipality,
                        "property_zip": prop_zip,
                        # Owner contact (mailing address) + absentee signal.
                        "owner_mailing": owner_mailing,
                        "is_absentee": absentee,
                        "annual_tax": tax_tot_str,
                        "_derived_from": "hennepin_parcels.raw_data",
                        "earliest_delq_year": delq_year,
                        "earliest_delq_yr_raw": delq_raw,
                    },
                    observed_at=datetime.now(timezone.utc),
                ))
                n_delq += 1

        logger.info(
            "Hennepin tax-roll parse complete",
            source=self.source_name,
            tax_forfeit=n_forfeit,
            tax_delinquent=n_delq,
            total=len(signals),
        )
        return signals

    # ---- Write: idempotent dedup upsert ----

    async def write(
        self, signals: list[DistressEventInsert]
    ) -> tuple[int, int, int]:
        if not signals:
            # An empty mine is NOT evidence that everyone paid. Retiring on
            # a zero-row read would wipe the source. Report and stop.
            logger.warning(
                "Hennepin tax-roll mined zero parcels — skipping retirement",
                source=self.source_name,
            )
            return 0, 0, 0

        new_events, failed_events = write_events_dedup(signals)

        retired = self._retire_stale(signals)

        logger.info(
            "Hennepin tax-roll write complete",
            source=self.source_name,
            events_new=new_events,
            events_retired=retired,
            failed=failed_events,
        )
        # retired counts as records_updated — the first thing in this fleet
        # that makes that field mean something. It has been 0 everywhere.
        return new_events, retired, failed_events

    # ---- Retire events the county roll no longer supports ----

    def _retire_stale(self, signals: list[DistressEventInsert]) -> int:
        """Stamp resolved_at/resolution on events this mine did not produce.

        The mined signals ARE the current truth: every parcel that qualifies
        right now produced one. So any UNRESOLVED event from this source whose
        parcel is absent from that set has stopped being true, and the bucket
        the parcel now sits in says why.

        See the module constants for the resolution mapping and the measured
        counts behind each branch. Never raises: a retirement failure must not
        fail a run that has already written its events correctly.
        """
        if len(signals) < _RETIRE_MIN_MINED:
            logger.warning(
                "Hennepin tax-roll mine too small to retire against",
                source=self.source_name,
                mined=len(signals),
                minimum=_RETIRE_MIN_MINED,
            )
            return 0

        # Parcels that currently qualify, split by which bucket they are in.
        # A parcel in EITHER set is still distressed; which set decides the
        # resolution for a delinquent event that moved.
        forfeit_now: set[str] = set()
        delq_now: set[str] = set()
        for sig in signals:
            if sig.event_type == "tax_forfeit":
                forfeit_now.add(sig.parcel_id)
            elif sig.event_type == "tax_delinquent":
                delq_now.add(sig.parcel_id)
        qualifying = forfeit_now | delq_now

        try:
            stored = self._read_unresolved_events()
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Hennepin tax-roll could not read events for retirement",
                source=self.source_name,
                error=f"{type(e).__name__}: {e}",
            )
            return 0

        # Bucket the retirements by resolution so each is one UPDATE rather
        # than one per row.
        to_retire: dict[str, list[int]] = {}
        for row in stored:
            pid = row.get("parcel_id")
            etype = row.get("event_type")
            eid = row.get("id")
            if pid is None or eid is None:
                continue
            if pid in qualifying and not (
                etype == "tax_delinquent" and pid in forfeit_now
            ):
                continue  # still true, leave it alone

            if etype == "tax_delinquent":
                # THE ESCALATION CASE. Order matters: check forfeiture BEFORE
                # concluding the debt was paid.
                resolution = "forfeited" if pid in forfeit_now else "cured"
            elif etype == "tax_forfeit":
                # Left the roll. We saw a disappearance, not a sale.
                resolution = "source_removed"
            else:
                continue

            to_retire.setdefault(resolution, []).append(eid)

        total = sum(len(v) for v in to_retire.values())
        if total == 0:
            logger.info(
                "Hennepin tax-roll retirement: nothing to retire",
                source=self.source_name,
            )
            return 0

        if total > _RETIRE_MAX_PER_RUN:
            logger.error(
                "Hennepin tax-roll retirement ABORTED — count above cap",
                source=self.source_name,
                would_retire=total,
                cap=_RETIRE_MAX_PER_RUN,
                mined=len(signals),
                hint="A partial county load looks exactly like mass curing.",
            )
            return 0

        stamped_at = datetime.now(timezone.utc).isoformat()
        retired = 0
        for resolution, ids in to_retire.items():
            for i in range(0, len(ids), _RETIRE_PAGE_SIZE):
                chunk = ids[i:i + _RETIRE_PAGE_SIZE]
                try:
                    resp = (
                        signals_table("distress_events")
                        .update({
                            "resolved_at": stamped_at,
                            "resolution": resolution,
                        })
                        # is_ null guards idempotency: a second run cannot
                        # re-stamp, and cannot overwrite an earlier, more
                        # specific resolution with a later generic one.
                        .is_("resolved_at", "null")
                        .in_("id", chunk)
                        .execute()
                    )
                    n = len(resp.data or [])
                    retired += n
                    logger.info(
                        "Hennepin tax-roll retired events",
                        source=self.source_name,
                        resolution=resolution,
                        requested=len(chunk),
                        stamped=n,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "Hennepin tax-roll retirement chunk failed",
                        source=self.source_name,
                        resolution=resolution,
                        chunk=len(chunk),
                        error=f"{type(e).__name__}: {e}",
                    )

        return retired

    def _read_unresolved_events(self) -> list[dict[str, Any]]:
        """Page every UNRESOLVED event this source has written."""
        out: list[dict[str, Any]] = []
        last_id = 0
        for _ in range(_MAX_PAGES):
            resp = (
                signals_table("distress_events")
                .select("id, parcel_id, event_type")
                .eq("source", self.source_name)
                .eq("county_code", self.county_code)
                .is_("resolved_at", "null")
                .gt("id", last_id)
                .order("id")
                .limit(_READ_PAGE_SIZE)
                .execute()
            )
            rows = resp.data or []
            if not rows:
                break
            out.extend(rows)
            last_id = rows[-1]["id"]
            if len(rows) < _READ_PAGE_SIZE:
                break
        return out


__all__ = ["HennepinTaxRollScraper"]
