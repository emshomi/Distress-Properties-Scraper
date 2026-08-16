"""
Pydantic models for the `signals` schema.

Every scraper produces typed signal rows that land in their specific
table (code_violations, sheriff_sales, vacant_registrations, etc.) AND
a row in the unified signals.distress_events feed.

Each signal model has a `.to_event()` projection that produces the
DistressEventInsert payload for the unified feed.

================================================================
COLUMN ALIGNMENT NOTES (last reviewed 2026-05-27)
================================================================
This file is aligned to the live Supabase schema. Specifically:

  signals.distress_events columns:
    id, parcel_id, event_type, event_subtype, event_date,
    event_value, source, raw_data, observed_at, scraper_run_id,
    severity, source_id, title, description

  signals.vacant_registrations columns:
    id, parcel_id, city, registry_type, date_entered_registry,
    years_on_registry, annual_fee, monthly_pve_fine,
    cumulative_fees_paid, is_active, raw_data, observed_at

The VbrListingInsert model keeps `boarded`/`condemned` as in-memory
convenience flags so to_event() can pick the right event_type.
On DB write, the writer derives `registry_type` from these flags
and stashes the source name into raw_data (since vacant_registrations
has no `source` column).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# ============================================================
# ENUMS
# ============================================================

DistressEventType = Literal[
    "code_violation",
    "sheriff_sale",
    "pre_foreclosure_notice",
    "vbr_listing",
    "boarded_building",
    "condemned_building",
    "probate_filing",
    "usps_vacancy",
    "tax_forfeit",
    "tax_delinquent",
    "tax_assessment",
]
DistressSeverity = Literal["low", "medium", "high", "critical"]


# ============================================================
# UNIFIED EVENT FEED — signals.distress_events
# ============================================================


class DistressEventInsert(BaseModel):
    """
    Payload for inserting a distress_events row.

    Dedup key is (county_code, source, source_id, event_date), enforced by the
    unique index distress_events_source_identity_key with NULLS NOT DISTINCT.
    The event writer upserts with ignore_duplicates=True against it.

    CORRECTED 2026-08-16. This docstring named the OLD key,
    (county_code, parcel_id, event_type, event_date, source), which was
    replaced on 2026-08-15. The old key contained parcel_id -- a value WE mint
    and WE rewrite -- so re-keying an event to its real parcel let the next
    scraper run regenerate the placeholder, find no conflict, and insert a
    second copy: 373 duplicates on hennepin_sheriff and 23 on anoka_sheriff,
    each within hours of a re-key. See src/services/event_writer.py, which
    carries the full account.

    Leaving the stale text here was not harmless. On 2026-08-16 it was read as
    authoritative while diagnosing 41 duplicate anoka_sheriff rows and pointed
    at the wrong fix; the correct behaviour was already documented in
    event_writer.py and the two disagreed.

    event_date STAYS in the key on purpose: a postponed sale is a genuinely new
    published fact, and collapsing it into the original would lose the new
    date. Superseding the earlier row is handled by the trg_events_supersede
    trigger on signals.distress_events, not by the dedup key.

    All field names match Supabase column names exactly. Note that
    monetary amounts use `event_value` (not `amount`) to match the
    database.
    """

    parcel_id: str = Field(..., min_length=1, max_length=100)
    # ADDED 2026-08-07. Optional on the MODEL because callers rarely know it,
    # but the event writer DERIVES it before insert (see
    # src/utils/county.resolve_county_code) — so rows normally reach the
    # database with it set.
    #
    # Leaving it NULL is not benign. The dedup index is NULLS NOT DISTINCT,
    # so a NULL-county row never matches the correctly-labelled row for the
    # same event and every scraper run re-inserts it. Measured: 1,451
    # duplicate events accumulated in ~24 hours before this was fixed.
    #
    # A genuinely unresolvable county still stays NULL rather than being
    # given a default — the composite FK is simply not enforced then
    # (MATCH SIMPLE), which is correct for an event whose parcel does not
    # exist. 12 such rows exist as of 2026-08-07.
    county_code: str | None = Field(default=None, max_length=100)
    event_type: DistressEventType
    event_subtype: str | None = Field(default=None, max_length=100)
    # Nullable by design: when a source doesn't publish the true event date,
    # the honest value is NULL — NEVER the scrape date (fabricated dates
    # inflated vacant counts ~30x before 2026-07-07; see BUILDLOG).
    # Requires the NULLS NOT DISTINCT dedup index on distress_events.
    event_date: date | None = None
    event_value: Decimal | None = None
    source: str = Field(..., min_length=1, max_length=100)
    source_id: str | None = Field(default=None, max_length=200)
    severity: DistressSeverity = Field(default="medium")
    title: str = Field(..., min_length=1, max_length=500)
    description: str | None = Field(default=None, max_length=2000)
    raw_data: dict[str, Any] | None = None
    observed_at: datetime
    scraper_run_id: int | None = None  # populated by writer when known

    model_config = ConfigDict(extra="forbid")


class DistressEvent(BaseModel):
    """Read model for distress_events rows."""

    id: int
    parcel_id: str | None = None
    county_code: str | None = None
    event_type: DistressEventType
    event_subtype: str | None = None
    event_date: date | None = None
    event_value: Decimal | None = None
    source: str
    source_id: str | None = None
    severity: DistressSeverity = "medium"
    title: str
    description: str | None = None
    raw_data: dict[str, Any] | None = None
    observed_at: datetime | None = None
    scraper_run_id: int | None = None

    model_config = ConfigDict(extra="ignore")


# ============================================================
# CODE VIOLATIONS — signals.code_violations
# ============================================================


class CodeViolationInsert(BaseModel):
    """Code violation payload — signals.code_violations.

    === FIELD NAMES CORRECTED 2026-08-07 ===
    This model dumped FOUR fields that do not exist as columns on
    signals.code_violations, and omitted four that do. PostgREST rejects
    unknown keys with PGRST204 before on_conflict is even evaluated, so
    nothing could ever have been written. The table has been empty since it
    was created.

    Verified against pg_catalog 2026-08-07, the real columns are:
        id, parcel_id, violation_type, violation_date, fine_amount,
        status, city, source_id, raw_data, observed_at, county_code

    Renamed: case_number -> source_id, reported_date -> violation_date.
    Added:   county_code, city, fine_amount.

    Kept as IN-MEMORY ONLY (no such column; the writer must strip them
    before upsert, exactly as VbrListingInsert does with `source`):
        source, violation_description, resolved_date, severity
    They survive in raw_data and drive the distress_events projection.

    Dedup index is `code_violations_dedup (county_code, source_id)
    NULLS NOT DISTINCT` — created 2026-08-07 while the table was empty.
    `source_id` carries the source's own case number
    (e.g. Minneapolis Violation_Case_Number), which is stable per case.
    Deliberately NOT keyed on parcel_id (one property has many cases over
    time) or violation_date (a corrected date should UPDATE the row, not
    create a second one).
    """

    # ---- Direct column mappings to signals.code_violations ----
    parcel_id: str = Field(..., min_length=1, max_length=100)
    county_code: str | None = Field(default=None, max_length=100)
    city: str | None = Field(default=None, max_length=200)
    violation_type: str | None = Field(default=None, max_length=200)
    violation_date: date | None = None
    fine_amount: Decimal | None = Field(default=None, ge=0)
    status: str | None = Field(default=None, max_length=100)
    source_id: str = Field(..., min_length=1, max_length=100)
    raw_data: dict[str, Any] | None = None
    observed_at: datetime

    # ---- Not stored in code_violations; used for event projection ----
    source: str = Field(..., min_length=1, max_length=100)
    violation_description: str | None = Field(default=None, max_length=2000)
    resolved_date: date | None = None
    # Set by the scraper from the source's own result codes. Minneapolis
    # VACATE / CON / CONCIT are critical; vacancy codes are medium. The model
    # does not know those codes and must not guess at them.
    severity: DistressSeverity = "medium"

    model_config = ConfigDict(extra="forbid")

    def to_event(self) -> DistressEventInsert:
        """Project this row into the unified distress_events feed."""
        return DistressEventInsert(
            parcel_id=self.parcel_id,
            county_code=self.county_code,
            event_type="code_violation",
            event_subtype=self.violation_type,
            # THE date rule: use the true date from the source, or NULL.
            # The old `or self.observed_at.date()` fallback stamped scrape-day,
            # which makes every daily run a "new" event — the same defect that
            # inflated Saint Paul VBR counts ~30x before 2026-07-07. An event
            # with no known date is honestly NULL.
            event_date=self.violation_date,
            severity=self.severity,
            source=self.source,
            source_id=self.source_id,
            title=f"Code violation: {self.violation_type or 'unspecified'}",
            description=self.violation_description,
            event_value=self.fine_amount,
            raw_data=self.raw_data,
            observed_at=self.observed_at,
        )


# ============================================================
# SHERIFF SALES — signals.sheriff_sales
# ============================================================


class SheriffSaleInsert(BaseModel):
    """Hennepin / Ramsey sheriff sale payload."""

    parcel_id: str = Field(..., min_length=1, max_length=100)
    # ADDED 2026-08-10. signals.distress_events has a COMPOSITE foreign key
    # (county_code, parcel_id) -> core.parcels, and its dedup key is
    # (county_code, parcel_id, event_type, event_date, source). A NULL
    # county_code makes BOTH unenforced -- NULL is never equal to anything --
    # so the row points at no parcel AND cannot collide with a duplicate.
    #
    # Measured live: 1,316 events carried county_code NULL, every one written
    # on or after 2026-08-07 (the composite-key migration). mpls_311 alone was
    # 1,304 of them; 12 sheriff rows were outright duplicates the dedup key
    # should have refused. All were backfilled by hand.
    #
    # Optional so existing callers keep working; every scraper that knows its
    # county should pass it.
    county_code: str | None = Field(default=None, max_length=100)
    case_number: str | None = Field(default=None, max_length=100)
    sale_date: date
    sale_amount: Decimal | None = Field(default=None, ge=0)
    plaintiff: str | None = Field(default=None, max_length=500)
    defendant: str | None = Field(default=None, max_length=500)
    property_address: str | None = Field(default=None, max_length=500)
    redemption_period_days: int | None = Field(default=None, ge=0)
    redemption_end_date: date | None = None
    status: str | None = Field(default=None, max_length=100)
    source: str = Field(..., min_length=1, max_length=100)
    raw_data: dict[str, Any] | None = None
    observed_at: datetime

    model_config = ConfigDict(extra="forbid")

    def to_event(self) -> DistressEventInsert:
        return DistressEventInsert(
            parcel_id=self.parcel_id,
            county_code=self.county_code,
            event_type="sheriff_sale",
            event_date=self.sale_date,
            severity="high",
            source=self.source,
            source_id=self.case_number,
            title=f"Sheriff sale: {self.property_address or 'unknown address'}",
            description=(
                f"Plaintiff: {self.plaintiff}; Defendant: {self.defendant}"
                if self.plaintiff or self.defendant
                else None
            ),
            event_value=self.sale_amount,
            raw_data=self.raw_data,
            observed_at=self.observed_at,
        )


# ============================================================
# VACANT REGISTRATIONS — signals.vacant_registrations
# ============================================================
# This model serves Minneapolis VBR, Saint Paul DSI, and any other
# city vacant-building registry. It is named VbrListingInsert for
# historical reasons; the target table is signals.vacant_registrations.


class VbrListingInsert(BaseModel):
    """
    Vacant building registry payload (Minneapolis VBR / Saint Paul DSI).

    Field names match signals.vacant_registrations columns directly, with
    two exceptions:
      - `source`: Used to track which scraper produced this row. There is
        no `source` column in vacant_registrations, so the writer is
        responsible for stashing this into raw_data on insert.
      - `boarded` / `condemned`: In-memory only flags used by to_event()
        to choose the correct event_type and severity. They are NOT
        written to vacant_registrations directly; the writer encodes
        them by setting `registry_type` to "boarded" or "condemned".
    """

    # ---- Direct column mappings to signals.vacant_registrations ----
    parcel_id: str = Field(..., min_length=1, max_length=100)
    county_code: str | None = Field(default=None, max_length=100)
    city: str | None = Field(default=None, max_length=200)
    registry_type: str | None = Field(default=None, max_length=100)
    date_entered_registry: date | None = None
    years_on_registry: float | None = Field(default=None, ge=0, le=100)
    annual_fee: Decimal | None = Field(default=None, ge=0)
    monthly_pve_fine: Decimal | None = Field(default=None, ge=0)
    cumulative_fees_paid: Decimal | None = Field(default=None, ge=0)
    is_active: bool = Field(default=True)
    raw_data: dict[str, Any] | None = None
    observed_at: datetime

    # ---- Not stored in vacant_registrations table; used for event projection ----
    source: str = Field(..., min_length=1, max_length=100)
    registration_number: str | None = Field(default=None, max_length=100)
    boarded: bool = Field(default=False)
    condemned: bool = Field(default=False)
    # In-memory only: the condemnation date (Minneapolis Day_of_CON). Used by
    # to_event() so a condemned_building event carries the CONDEMNATION date,
    # not the (often blank) registry date. Stripped before typed-table write.
    condemned_date: date | None = None

    model_config = ConfigDict(extra="forbid")

    def to_event(self) -> DistressEventInsert:
        """Project this VBR row into the unified distress_events feed."""
        if self.condemned:
            event_type: DistressEventType = "condemned_building"
            severity: DistressSeverity = "critical"
            title = "Condemned building"
        elif self.boarded:
            event_type = "boarded_building"
            severity = "high"
            title = "Boarded building"
        else:
            event_type = "vbr_listing"
            severity = "medium"
            label = self.registry_type or "registered"
            title = f"Vacant building registry: {label}"

        # THE date rule: use the true date from the source — the condemnation
        # date for condemned events, else the registry-entry date. If the
        # source doesn't publish one, event_date is honestly NULL. The old
        # `or self.observed_at.date()` fallback stamped scrape-day, which
        # made every daily run a "new" event and inflated counts ~30x.
        if event_type == "condemned_building":
            true_date = self.condemned_date or self.date_entered_registry
        else:
            true_date = self.date_entered_registry

        return DistressEventInsert(
            parcel_id=self.parcel_id,
            county_code=self.county_code,
            event_type=event_type,
            event_subtype=self.registry_type,
            event_date=true_date,
            severity=severity,
            source=self.source,
            source_id=self.registration_number,
            title=title,
            event_value=self.annual_fee,
            raw_data=self.raw_data,
            observed_at=self.observed_at,
        )


# ============================================================
# PROBATE FILINGS — signals.probate_filings
# ============================================================


class ProbateFilingInsert(BaseModel):
    """Probate case payload — signals.probate_cases.

    === FIELD NAMES CORRECTED 2026-08-07 ===
    Verified against pg_catalog. The real columns are:
        id, parcel_id, case_number, filing_date, decedent_name,
        date_of_death, personal_representative_name, county_code,
        case_type, case_status, has_will, raw_data, observed_at

    Renamed: county -> county_code, filing_type -> case_type.
    Added:   date_of_death, personal_representative_name, case_status,
             has_will — all four are visible on the MCRO Case Details page
             and were simply never captured.

    Kept as IN-MEMORY ONLY (no such column; the scraper MUST strip these
    before upsert, as mpls_311 does):
        source, severity

    Dedup index is `probate_cases_dedup (county_code, case_number)
    NULLS NOT DISTINCT`, created 2026-08-07 while the table was empty.
    Keyed on the CASE, not the property — one estate can hold several
    parcels, and MCRO case numbers are not uniformly formatted
    (`03-PR-26-1092` but also `82-26-323`), so county_code guards against a
    county-local numbering scheme colliding.
    """

    # ---- Direct column mappings to signals.probate_cases ----
    parcel_id: str | None = Field(default=None, max_length=100)
    case_number: str = Field(..., min_length=1, max_length=100)
    county_code: str = Field(..., min_length=1, max_length=100)
    filing_date: date | None = None
    decedent_name: str | None = Field(default=None, max_length=500)
    date_of_death: date | None = None
    personal_representative_name: str | None = Field(default=None, max_length=500)
    case_type: str | None = Field(default=None, max_length=200)
    case_status: str | None = Field(default=None, max_length=100)
    has_will: bool | None = None
    raw_data: dict[str, Any] | None = None
    observed_at: datetime

    # ---- Not stored in probate_cases; used for event projection ----
    source: str = Field(default="mcro_probate", min_length=1, max_length=100)
    severity: DistressSeverity = "medium"

    model_config = ConfigDict(extra="forbid")

    def to_event(self) -> DistressEventInsert | None:
        """Project into distress_events — ONLY when linked to a parcel.

        Many probate filings have no property record. We still store the
        case, but a probate signal with no parcel is not actionable and
        must not reach the unified feed.
        """
        if not self.parcel_id:
            return None
        return DistressEventInsert(
            parcel_id=self.parcel_id,
            county_code=self.county_code,
            event_type="probate_filing",
            event_subtype=self.case_type,
            # THE date rule: the true date or NULL — never a scrape-day
            # fallback. `or self.observed_at.date()` made every daily run a
            # "new" event and inflated Saint Paul VBR counts ~30x before
            # 2026-07-07.
            event_date=self.filing_date,
            severity=self.severity,
            source=self.source,
            source_id=self.case_number,
            title=f"Probate: estate of {self.decedent_name or 'unknown'}",
            description=(
                f"{self.case_type or 'Probate case'}"
                + (f", died {self.date_of_death.isoformat()}"
                   if self.date_of_death else "")
            ),
            raw_data=self.raw_data,
            observed_at=self.observed_at,
        )


# ============================================================
# USPS VACANCY — signals.usps_vacancies
# ============================================================


class UspsVacancyInsert(BaseModel):
    """HUD/USPS Vacancy Indicator payload (ZIP+4 level)."""

    zip5: str = Field(..., min_length=5, max_length=5)
    zip4: str | None = Field(default=None, max_length=4)
    quarter: str = Field(..., min_length=6, max_length=10)  # e.g., "2025Q3"
    residential_total: int = Field(default=0, ge=0)
    residential_vacant: int = Field(default=0, ge=0)
    residential_vacancy_rate: float = Field(default=0.0, ge=0, le=1)
    business_total: int = Field(default=0, ge=0)
    business_vacant: int = Field(default=0, ge=0)
    business_vacancy_rate: float = Field(default=0.0, ge=0, le=1)
    source: str = Field(default="hud_usps", min_length=1, max_length=100)
    raw_data: dict[str, Any] | None = None
    observed_at: datetime

    model_config = ConfigDict(extra="forbid")


# ============================================================
# TAX FORFEIT — signals.tax_forfeit
# ============================================================


class TaxForfeitInsert(BaseModel):
    """MN tax-forfeit property payload."""

    parcel_id: str = Field(..., min_length=1, max_length=100)
    county: str = Field(..., min_length=1, max_length=50)
    # `county` above is a DISPLAY name ('St. Louis'); county_code is the
    # core.counties slug ('st_louis'). The FK needs the slug.
    county_code: str | None = Field(default=None, max_length=100)
    forfeit_date: date | None = None
    sale_date: date | None = None
    appraised_value: Decimal | None = Field(default=None, ge=0)
    minimum_bid: Decimal | None = Field(default=None, ge=0)
    status: str | None = Field(default=None, max_length=100)
    pre_2024_forfeit: bool = Field(default=False)
    source: str = Field(..., min_length=1, max_length=100)
    raw_data: dict[str, Any] | None = None
    observed_at: datetime

    model_config = ConfigDict(extra="forbid")

    def to_event(self) -> DistressEventInsert:
        severity: DistressSeverity = "critical" if self.pre_2024_forfeit else "high"
        title_suffix = " (pre-2024 forfeiture — forced liquidation window)" if self.pre_2024_forfeit else ""
        return DistressEventInsert(
            parcel_id=self.parcel_id,
            county_code=self.county_code,
            event_type="tax_forfeit",
            event_date=self.forfeit_date or self.observed_at.date(),
            severity=severity,
            source=self.source,
            title=f"Tax-forfeit property — {self.county}{title_suffix}",
            event_value=self.minimum_bid,
            raw_data=self.raw_data,
            observed_at=self.observed_at,
        )


__all__ = [
    "DistressEventType",
    "DistressSeverity",
    "DistressEventInsert",
    "DistressEvent",
    "CodeViolationInsert",
    "SheriffSaleInsert",
    "VbrListingInsert",
    "ProbateFilingInsert",
    "UspsVacancyInsert",
    "TaxForfeitInsert",
]
