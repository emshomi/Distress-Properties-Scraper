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

# "interrupted" ADDED 2026-08-16.
#
# A run whose PROCESS is killed - Railway redeploy, OOM, container recycle -
# never reaches BaseScraper step 8, so audit_logger.finish_run is never
# called and source_health is never updated. The row stays 'running'
# forever and the daily digest keeps reporting the source HEALTHY.
#
# parcel_enrich_mngeo did exactly this 8 times between 2026-06-10 and
# 2026-08-15, including three consecutive nights (13th/14th/15th), and
# appeared in the HEALTHY list of every digest throughout.
#
# 'interrupted' is deliberately NOT 'failed'. Run 641 wrote 446 hennepin
# parcels before it died - measured by their shared last_observed_at
# fingerprint, 2026-08-15 13:34:41.851716. The counters on that row were
# simply never written, so its zeros measure nothing at all. A sweep must
# record that a run ended without claiming to know what it accomplished.
#
# audit.scraper_runs.status is plain `text` with NO check constraint
# (verified against pg_catalog 2026-08-16), so this needs no migration.
ScraperRunStatus = Literal[
    "running", "success", "partial", "failed", "skipped", "interrupted"
]
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
    """Payload for inserting a scraper_errors row.

    THE COLUMN IS created_at, NOT occurred_at. FIXED 2026-08-16.

    audit.scraper_errors has exactly six columns: id, scraper_run_id,
    error_type, error_message, raw_record, created_at. This model declared
    `occurred_at`, which does not exist on the table, so PostgREST rejected
    every insert - and audit_logger.log_error swallows that rejection into
    logger.warning by design ("error logging itself should not crash a
    scraper").

    That is why the table holds 0 rows and always has. Prior handoffs
    recorded the emptiness as a fact about the world; it was a column name.
    Both this file and audit_logger.py have sat at commit ffbc04f since the
    initial upload, so no per-record error has ever been persisted.

    THE ALIAS IS TRANSITIONAL. log_error still passes occurred_at=..., and
    it builds this payload OUTSIDE its try/except - so renaming the field
    without the alias would raise ValidationError on the first per-record
    error and take the calling scraper down with it. populate_by_name
    accepts either spelling; model_dump emits the FIELD name (created_at),
    which is what PostgREST needs. A follow-up task points log_error at
    created_at directly and removes the alias.

    created_at also carries a DEFAULT now() on the table, so an insert that
    omits it entirely is still correct.
    """

    scraper_run_id: int | None = None
    error_type: ScraperErrorType
    error_message: str = Field(..., max_length=5000)
    raw_record: dict[str, Any] | None = None
    created_at: datetime | None = Field(default=None, alias="occurred_at")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class ScraperError(BaseModel):
    """Read model for scraper_errors rows.

    created_at, not occurred_at - same fix as ScraperErrorEntry above. This
    model would have raised on every row it read, had there ever been one.
    """

    id: int
    scraper_run_id: int | None = None
    error_type: ScraperErrorType
    error_message: str
    raw_record: dict[str, Any] | None = None
    created_at: datetime

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
