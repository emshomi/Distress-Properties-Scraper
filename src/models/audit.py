"""
Pydantic models for the `audit` schema tables.

Audit tracks every scraper run, every per-record error, and per-source
health state. These models are the validation layer between scraper code
and the database.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# ============================================================
# ENUMS
# ============================================================

ScraperRunStatus = Literal["running", "success", "partial", "failed", "skipped"]
ScraperErrorType = Literal[
    "fetch_error",
    "parse_error",
    "validation_error",
    "database_error",
    "unknown_error",
]


# ============================================================
# AUDIT.SCRAPER_RUNS
# ============================================================


class ScraperRunStart(BaseModel):
    """Payload for opening a new scraper_runs row."""

    scraper_name: str = Field(..., min_length=1, max_length=100)
    started_at: datetime
    status: ScraperRunStatus = Field(default="running")
    metadata: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")


class ScraperRunFinish(BaseModel):
    """Payload for closing a scraper_runs row (after the run completes)."""

    status: ScraperRunStatus
    finished_at: datetime
    duration_seconds: float | None = None
    records_fetched: int = Field(default=0, ge=0)
    records_new: int = Field(default=0, ge=0)
    records_updated: int = Field(default=0, ge=0)
    records_failed: int = Field(default=0, ge=0)
    error_message: str | None = None
    metadata: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")


class ScraperRun(BaseModel):
    """Read model for scraper_runs rows."""

    id: int
    scraper_name: str
    status: ScraperRunStatus
    started_at: datetime
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    records_fetched: int = 0
    records_new: int = 0
    records_updated: int = 0
    records_failed: int = 0
    error_message: str | None = None
    metadata: dict[str, Any] | None = None

    model_config = ConfigDict(extra="ignore")


# ============================================================
# AUDIT.SCRAPER_ERRORS
# ============================================================


class ScraperErrorEntry(BaseModel):
    """Payload for inserting a scraper_errors row."""

    scraper_run_id: int | None = None
    error_type: ScraperErrorType
    error_message: str = Field(..., max_length=5000)
    raw_record: dict[str, Any] | None = None
    occurred_at: datetime | None = None

    model_config = ConfigDict(extra="forbid")


class ScraperError(BaseModel):
    """Read model for scraper_errors rows."""

    id: int
    scraper_run_id: int | None = None
    error_type: ScraperErrorType
    error_message: str
    raw_record: dict[str, Any] | None = None
    occurred_at: datetime

    model_config = ConfigDict(extra="ignore")


# ============================================================
# AUDIT.SOURCE_HEALTH
# ============================================================


class SourceHealthUpdate(BaseModel):
    """Payload for upserting a source_health row."""

    source_name: str = Field(..., min_length=1, max_length=100)
    last_successful_run_at: datetime | None = None
    last_failed_run_at: datetime | None = None
    consecutive_failures: int = Field(default=0, ge=0)
    is_healthy: bool = Field(default=True)
    notes: str | None = Field(default=None, max_length=1000)
    updated_at: datetime

    model_config = ConfigDict(extra="forbid")


class SourceHealth(BaseModel):
    """Read model for source_health rows."""

    source_name: str
    last_successful_run_at: datetime | None = None
    last_failed_run_at: datetime | None = None
    consecutive_failures: int = 0
    is_healthy: bool = True
    notes: str | None = None
    updated_at: datetime

    model_config = ConfigDict(extra="ignore")


# ============================================================
# COMPOSITE STATUS (for /status endpoint)
# ============================================================


class ScraperStatusEntry(BaseModel):
    """Per-scraper summary returned by GET /status."""

    name: str
    enabled: bool
    is_healthy: bool | None
    consecutive_failures: int
    last_successful_run_at: datetime | None
    last_failed_run_at: datetime | None

    model_config = ConfigDict(extra="ignore")


__all__ = [
    "ScraperRunStatus",
    "ScraperErrorType",
    "ScraperRunStart",
    "ScraperRunFinish",
    "ScraperRun",
    "ScraperErrorEntry",
    "ScraperError",
    "SourceHealthUpdate",
    "SourceHealth",
    "ScraperStatusEntry",
]
