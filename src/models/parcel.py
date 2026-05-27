"""
Pydantic models for the `core` schema (parcels, owners, transactions).

These define the validation contract for the most fundamental data in
the platform — the canonical record of every property we track.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# ============================================================
# ENUMS
# ============================================================

County = Literal[
    "hennepin",
    "ramsey",
    "dakota",
    "anoka",
    "washington",
    "scott",
    "carver",
    "wright",
    "st_louis",
    "olmsted",
    "stearns",
    "otter_tail",
    "cass",
    "chisago",
]

PropertyType = Literal[
    "single_family",
    "duplex",
    "triplex",
    "fourplex",
    "multifamily",
    "condo",
    "townhouse",
    "land",
    "commercial",
    "mixed_use",
    "industrial",
    "agricultural",
    "unknown",
]

OwnerType = Literal[
    "individual",
    "joint_individuals",
    "llc",
    "partnership",
    "corporation",
    "trust",
    "estate",
    "government",
    "tax_forfeit_state",
    "nonprofit",
    "unknown",
]

VacancyStatus = Literal[
    "occupied",
    "vacant",
    "unknown",
]

TransactionType = Literal[
    "sale",
    "foreclosure_sale",
    "tax_forfeit_sale",
    "transfer",
    "deed",
    "mortgage",
    "release",
    "unknown",
]


# ============================================================
# CORE.PARCELS
# ============================================================


class ParcelUpsert(BaseModel):
    """
    Payload for resolve_parcel (creates or updates a parcel row).

    The resolver merges this with the existing row according to per-field
    rules (overwrite, fill-in, or immutable). See parcel_resolver.py.
    """

    parcel_id: str = Field(..., min_length=1, max_length=100)
    county_code: str = Field(..., min_length=1, max_length=50)
    state: str = Field(default="MN", min_length=2, max_length=2)

    # Address fields (fill-in semantics)
    address: str | None = Field(default=None, max_length=500)
    city: str | None = Field(default=None, max_length=200)
    zip: str | None = Field(default=None, max_length=10)
    zip_plus_four: str | None = Field(default=None, max_length=10)

    # Geographic coordinates (fill-in semantics)
    lat: float | None = Field(default=None, ge=-90, le=90)
    lng: float | None = Field(default=None, ge=-180, le=180)

    # Property attributes (fill-in semantics)
    property_type: PropertyType | None = None
    year_built: int | None = Field(default=None, ge=1700, le=2100)
    sqft: int | None = Field(default=None, ge=0)
    lot_sqft: int | None = Field(default=None, ge=0)
    beds: int | None = Field(default=None, ge=0, le=50)
    baths: float | None = Field(default=None, ge=0, le=50)
    stories: int | None = Field(default=None, ge=0, le=20)

    # Financial estimates (overwrite semantics — newer values always win)
    estimated_market_value: Decimal | None = Field(default=None, ge=0)
    estimated_equity: Decimal | None = None
    estimated_mortgage_balance: Decimal | None = None

    # Status fields
    vacancy_status: VacancyStatus | None = None

    # Provenance (data_sources list is unioned, not overwritten)
    data_sources: list[str] = Field(default_factory=list)
    last_observed_at: datetime | None = None

    model_config = ConfigDict(extra="forbid")


class Parcel(BaseModel):
    """Read model for core.parcels rows. Tolerant of extra DB columns."""

    parcel_id: str
    county_code: str
    state: str = "MN"
    address: str | None = None
    city: str | None = None
    zip: str | None = None
    zip_plus_four: str | None = None
    lat: float | None = None
    lng: float | None = None
    property_type: str | None = None
    year_built: int | None = None
    sqft: int | None = None
    lot_sqft: int | None = None
    beds: int | None = None
    baths: float | None = None
    stories: int | None = None
    estimated_market_value: Decimal | None = None
    estimated_equity: Decimal | None = None
    estimated_mortgage_balance: Decimal | None = None
    vacancy_status: str | None = None
    data_sources: list[str] = Field(default_factory=list)
    first_observed_at: datetime | None = None
    last_observed_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = ConfigDict(extra="ignore")


# ============================================================
# CORE.OWNERS
# ============================================================


class OwnerUpsert(BaseModel):
    """
    Payload for resolve_owner.

    Owners use an append-with-history pattern: at most one row per parcel
    has is_current=True. Ownership changes close the previous row and
    open a new one.
    """

    parcel_id: str = Field(..., min_length=1, max_length=100)
    owner_name: str | None = Field(default=None, max_length=500)
    owner_type: OwnerType | None = None

    # Mailing address (for absentee detection)
    mailing_address: str | None = Field(default=None, max_length=500)
    mailing_city: str | None = Field(default=None, max_length=200)
    mailing_state: str | None = Field(default=None, max_length=2)
    mailing_zip: str | None = Field(default=None, max_length=10)

    # Derived flags
    is_absentee: bool | None = None
    is_out_of_state: bool | None = None

    # Tenure dates
    ownership_start_date: date | None = None
    ownership_end_date: date | None = None

    # Provenance
    source: str | None = None
    data_sources: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class Owner(BaseModel):
    """Read model for core.owners rows."""

    id: int | None = None
    parcel_id: str
    owner_name: str | None = None
    owner_type: str | None = None
    mailing_address: str | None = None
    mailing_city: str | None = None
    mailing_state: str | None = None
    mailing_zip: str | None = None
    is_absentee: bool | None = None
    is_out_of_state: bool | None = None
    is_current: bool = True
    ownership_start_date: date | None = None
    ownership_end_date: date | None = None
    source: str | None = None
    observed_at: datetime | None = None

    model_config = ConfigDict(extra="ignore")


# ============================================================
# CORE.TRANSACTIONS
# ============================================================


class TransactionAppend(BaseModel):
    """Append a transaction event to the parcel's history."""

    parcel_id: str = Field(..., min_length=1, max_length=100)
    transaction_type: TransactionType
    transaction_date: date
    amount: Decimal | None = None
    grantor: str | None = Field(default=None, max_length=500)
    grantee: str | None = Field(default=None, max_length=500)
    source: str = Field(..., min_length=1, max_length=100)
    source_id: str | None = Field(default=None, max_length=200)
    raw_data: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")


class Transaction(BaseModel):
    """Read model for core.transactions rows."""

    id: int
    parcel_id: str
    transaction_type: str
    transaction_date: date
    amount: Decimal | None = None
    grantor: str | None = None
    grantee: str | None = None
    source: str
    source_id: str | None = None
    raw_data: dict[str, Any] | None = None
    observed_at: datetime | None = None

    model_config = ConfigDict(extra="ignore")


__all__ = [
    "County",
    "PropertyType",
    "OwnerType",
    "VacancyStatus",
    "TransactionType",
    "ParcelUpsert",
    "Parcel",
    "OwnerUpsert",
    "Owner",
    "TransactionAppend",
    "Transaction",
]
