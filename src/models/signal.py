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
]

DistressSeverity = Literal["low", "medium", "high", "critical"]


# ============================================================
# UNIFIED EVENT FEED — signals.distress_events
# ============================================================


class DistressEventInsert(BaseModel):
    """
    Payload for inserting a distress_events row.

    Dedup key is (parcel_id, event_type, event_date, source) — the event
    writer skips inserts that match an existing key.

    All field names match Supabase column names exactly. Note that
    monetary amounts use `event_value` (not `amount`) to match the
    database.
    """

    parcel_id: str = Field(..., min_length=1, max_length=100)
    event_type: DistressEventType
    event_subtype: str | None = Field(default=None, max_length=100)
    event_date: date
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
    """Minneapolis 311 / Saint Paul DSI code violation payload."""

    parcel_id: str = Field(..., min_length=1, max_length=100)
    case_number: str = Field(..., min_length=1, max_length=100)
    violation_type: str | None = Field(default=None, max_length=200)
    violation_description: str | None = Field(default=None, max_length=2000)
    status: str | None = Field(default=None, max_length=100)
    reported_date: date | None = None
    resolved_date: date | None = None
    source: str = Field(..., min_length=1, max_length=100)
    raw_data: dict[str, Any] | None = None
    observed_at: datetime

    model_config = ConfigDict(extra="forbid")

    def to_event(self) -> DistressEventInsert:
        """Project this row into the unified distress_events feed."""
        return DistressEventInsert(
            parcel_id=self.parcel_id,
            event_type="code_violation",
            event_subtype=self.violation_type,
            event_date=self.reported_date or self.observed_at.date(),
            severity="medium",
            source=self.source,
            source_id=self.case_number,
            title=f"Code violation: {self.violation_type or 'unspecified'}",
            description=self.violation_description,
            raw_data=self.raw_data,
            observed_at=self.observed_at,
        )


# ============================================================
# SHERIFF SALES — signals.sheriff_sales
# ============================================================


class SheriffSaleInsert(BaseModel):
    """Hennepin / Ramsey sheriff sale payload."""

    parcel_id: str = Field(..., min_length=1, max_length=100)
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

        return DistressEventInsert(
            parcel_id=self.parcel_id,
            event_type=event_type,
            event_subtype=self.registry_type,
            event_date=self.date_entered_registry or self.observed_at.date(),
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
    """MN Court Records Online probate filing payload."""

    parcel_id: str | None = Field(default=None, max_length=100)
    case_number: str = Field(..., min_length=1, max_length=100)
    decedent_name: str | None = Field(default=None, max_length=500)
    filing_date: date | None = None
    county: str = Field(..., min_length=1, max_length=50)
    filing_type: str | None = Field(default=None, max_length=200)
    source: str = Field(default="mcro_probate", min_length=1, max_length=100)
    raw_data: dict[str, Any] | None = None
    observed_at: datetime

    model_config = ConfigDict(extra="forbid")

    def to_event(self) -> DistressEventInsert | None:
        """
        Probate signals only emit events when linked to a parcel.

        Many probate filings don't have property records; we still track
        the filing but don't create a parcel-level event.
        """
        if not self.parcel_id:
            return None
        return DistressEventInsert(
            parcel_id=self.parcel_id,
            event_type="probate_filing",
            event_subtype=self.filing_type,
            event_date=self.filing_date or self.observed_at.date(),
            severity="medium",
            source=self.source,
            source_id=self.case_number,
            title=f"Probate filing: {self.decedent_name or 'unknown'}",
            description=f"Filing type: {self.filing_type or 'unspecified'}",
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
