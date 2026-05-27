"""
Pydantic models for the `signals` schema.

Every scraper produces typed signal rows that land in their specific
table (code_violations, sheriff_sales, vbr_listings, etc.) AND a row
in the unified signals.distress_events feed.

Each signal model has a `.to_event()` projection that produces the
DistressEventInsert payload for the unified feed.
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
    """

    parcel_id: str = Field(..., min_length=1, max_length=100)
    event_type: DistressEventType
    event_date: date
    severity: DistressSeverity = Field(default="medium")
    source: str = Field(..., min_length=1, max_length=100)
    source_id: str | None = Field(default=None, max_length=200)
    title: str = Field(..., min_length=1, max_length=500)
    description: str | None = Field(default=None, max_length=2000)
    amount: Decimal | None = None
    raw_data: dict[str, Any] | None = None
    observed_at: datetime

    model_config = ConfigDict(extra="forbid")


class DistressEvent(BaseModel):
    """Read model for distress_events rows."""

    id: int
    parcel_id: str
    event_type: DistressEventType
    event_date: date
    severity: DistressSeverity = "medium"
    source: str
    source_id: str | None = None
    title: str
    description: str | None = None
    amount: Decimal | None = None
    raw_data: dict[str, Any] | None = None
    observed_at: datetime

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
            amount=self.sale_amount,
            raw_data=self.raw_data,
            observed_at=self.observed_at,
        )


# ============================================================
# VACANT BUILDING REGISTRY — signals.vbr_listings
# ============================================================


class VbrListingInsert(BaseModel):
    """Minneapolis VBR / Saint Paul DSI vacant building payload."""

    parcel_id: str = Field(..., min_length=1, max_length=100)
    registration_number: str | None = Field(default=None, max_length=100)
    category: str | None = Field(default=None, max_length=100)
    status: str | None = Field(default=None, max_length=100)
    registered_date: date | None = None
    vbr_fee_assessed: Decimal | None = Field(default=None, ge=0)
    pve_monthly_fee: Decimal | None = Field(default=None, ge=0)
    boarded: bool | None = None
    condemned: bool | None = None
    source: str = Field(..., min_length=1, max_length=100)
    raw_data: dict[str, Any] | None = None
    observed_at: datetime

    model_config = ConfigDict(extra="forbid")

    def to_event(self) -> DistressEventInsert:
        if self.condemned:
            event_type: DistressEventType = "condemned_building"
            severity: DistressSeverity = "critical"
        elif self.boarded:
            event_type = "boarded_building"
            severity = "high"
        else:
            event_type = "vbr_listing"
            severity = "medium"

        return DistressEventInsert(
            parcel_id=self.parcel_id,
            event_type=event_type,
            event_date=self.registered_date or self.observed_at.date(),
            severity=severity,
            source=self.source,
            source_id=self.registration_number,
            title=f"VBR/Vacant: {self.category or self.status or 'registered'}",
            amount=self.vbr_fee_assessed,
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
# USPS VACANCY — signals.usps_vacancy
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
            amount=self.minimum_bid,
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
