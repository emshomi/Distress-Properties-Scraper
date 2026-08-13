"""
Pydantic models for the `core` schema (parcels, owners, transactions).

These define the validation contract for the most fundamental data in
the platform — the canonical record of every property we track.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, ClassVar, Literal

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
    Payload for a parcel write. TWO writer classes consume this, and they
    serialise it DIFFERENTLY. Getting the wrong one silently corrupts data.

    1. ENRICHMENT writers — the signal scrapers (hennepin_sheriff,
       washington_sheriff, anoka_sheriff, dakota_sheriff, mpls_311,
       mpls_vbr, saint_paul_vacant, tax_forfeit). A sheriff notice knows an
       address; it knows nothing about emv_total or year_built. These call
       resolve_parcel(), which merges per-field (overwrite / fill-in /
       immutable / set-union) against the existing row. See
       parcel_resolver.py. They use exclude_none=True, correctly: a field
       they did not learn about must not be nulled.

    2. FULL-REFRESH writers — the county parcel loaders (mngac_parcels,
       olmsted_parcels, hennepin_parcels, ramsey_parcels, dakota_parcels,
       washington_parcels, fillmore_parcels). These are the SOURCE OF RECORD
       for the row and upsert direct, bypassing the resolver (a per-parcel
       SELECT cannot run over 2.66M rows). They MUST call dump_owned().

    Why dump_owned() exists — measured 2026-08-13:
    exclude_none=True drops any field whose value is None, and PostgREST
    builds its column list from the UNION of keys across the batch. A column
    absent from every row in a batch never enters the UPDATE, so whatever was
    already there SURVIVES. For a source of record that is wrong: "the layer
    published no value" means the value is ABSENT, not "keep the old one".

    Consequence: the 2026-08-06 composite-key incident left 18,415 rows in
    twelve MnGeo counties holding other counties' data. The 08-07 recovery
    reload corrected every column MnGeo publishes and could not touch the
    rest. 6,268 rows still carried another parcel's property_type — a live
    filter key — because _map_property_type returns None for that layer
    (control: 0 of 464,846 non-converted rows have one) and the column
    therefore never entered an UPDATE.

    Nor could anything else fix them: parcel_resolver's fill-in rule only
    writes when the existing value is null/empty, so it is structurally
    incapable of CORRECTING a populated-but-wrong field. Only the
    full-refresh loader can, and only via dump_owned().
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

    # Assessor valuation + characteristics (2026-07-14): the typed columns
    # the distress_with_parcel view and the UI actually read (emv_total etc.),
    # DISTINCT from estimated_market_value above (a parallel legacy column).
    # Added for the olmsted_parcels loader patch; optional everywhere, so a
    # loader that does not set them simply omits them.
    #
    # CORRECTED 2026-08-13. This block previously read "exclude_none dumps
    # also mean upserts never null-clobber backfilled values" as though that
    # were a safety property. It is the defect: for a full-refresh loader,
    # not null-clobbering means PRESERVING another county's value forever.
    # See the class docstring and dump_owned().
    #
    # SEPARATE OPEN DEFECT (recorded 2026-08-13, not fixed here):
    # parcel_resolver._OVERWRITE_FIELDS lists estimated_market_value /
    # _equity / _mortgage_balance but NOT emv_total / emv_land /
    # emv_building. So the columns the view and UI actually read fall through
    # to FILL-IN, and once set no scraper can ever refresh them — an assessed
    # value is frozen at whatever landed first, and equity_spread is computed
    # from it.
    emv_total: Decimal | None = Field(default=None, ge=0)
    emv_land: Decimal | None = Field(default=None, ge=0)
    emv_building: Decimal | None = Field(default=None, ge=0)
    num_units: int | None = Field(default=None, ge=0)
    use_class: str | None = Field(default=None, max_length=200)
    school_district: str | None = Field(default=None, max_length=50)

    # Status fields
    vacancy_status: VacancyStatus | None = None

    # Source-specific raw attributes preserved verbatim.
    # Schema varies by scraper — Hennepin parcels store the 80+ ArcGIS
    # attributes here for later mining of distress signals (FORFEIT_LAND_IND,
    # EARLIEST_DELQ_YR, COMP_JUDG_IND, TAXPAYER_NM, etc.).
    # Stored in core.parcels.raw_data (JSONB).
    raw_data: dict[str, Any] | None = None

    # Provenance (data_sources list is unioned, not overwritten)
    data_sources: list[str] = Field(default_factory=list)
    last_observed_at: datetime | None = None

    model_config = ConfigDict(extra="forbid")

    # Never emitted as NULL by dump_owned(), whatever the caller passed:
    #   raw_data          large, and "not re-fetched this run" is not "gone"
    #   data_sources      set-unioned by the resolver, never overwritten
    #   last_observed_at  every caller overwrites it with now_iso after dump
    _NEVER_NULL: ClassVar[frozenset[str]] = frozenset({
        "raw_data",
        "data_sources",
        "last_observed_at",
    })

    def dump_owned(self) -> dict[str, Any]:
        """Serialise for a FULL-REFRESH upsert: emit every field the caller
        explicitly passed, INCLUDING the ones that came out None.

        For a source of record, a field the layer did not publish is absent,
        not unchanged. Emitting the null is what lets a re-run CLEAR a value
        that a previous write got wrong — which exclude_none=True cannot do,
        and which parcel_resolver's fill-in rule cannot do either.

        model_fields_set is exactly the fields supplied at construction, so a
        caller that never mentions `beds` leaves it alone, while a caller that
        passes beds=sig.get("beds") and gets None writes NULL. Ownership is
        declared by the call site, not by a list here that would drift.

        ENRICHMENT writers must NOT call this — see the class docstring. Use
        model_dump(exclude_none=True) via resolve_parcel() instead.
        """
        full = self.model_dump(mode="json")
        return {
            name: value
            for name, value in full.items()
            if name in self.model_fields_set
            and not (value is None and name in self._NEVER_NULL)
        }


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
    raw_data: dict[str, Any] | None = None
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
