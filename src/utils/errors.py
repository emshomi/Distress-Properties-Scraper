"""
Typed exception hierarchy + JSON envelope helpers.

All service errors derive from ServiceError. Each subclass represents a
specific failure mode that callers and HTTP handlers can pattern-match.

The envelope helpers produce the uniform response shape used by every
API endpoint:
    success_envelope(data) → {"success": true, "data": ...}
    error_envelope(exc)    → {"success": false, "error": {...}}
"""

from __future__ import annotations

from typing import Any


# ============================================================
# BASE EXCEPTION
# ============================================================


class ServiceError(Exception):
    """
    Base class for all internal service errors.

    Carries a human-readable message plus an optional context dict for
    structured logging. Subclasses don't add new fields — they just
    provide type discrimination for callers.
    """

    def __init__(
        self,
        message: str,
        *,
        source: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message: str = message
        self.source: str | None = source
        self.context: dict[str, Any] = context or {}

    def __str__(self) -> str:
        return self.message

    def to_dict(self) -> dict[str, Any]:
        """Render this error as a JSON-friendly dict."""
        out: dict[str, Any] = {
            "type": type(self).__name__,
            "message": self.message,
        }
        if self.source:
            out["source"] = self.source
        if self.context:
            out["context"] = self.context
        return out


# ============================================================
# REQUEST / AUTH ERRORS
# ============================================================


class UnauthorizedError(ServiceError):
    """Missing or invalid authentication credentials."""


class ForbiddenError(ServiceError):
    """Authenticated but not allowed to perform the action."""


class ValidationError(ServiceError):
    """Input data failed validation."""


class NotFoundError(ServiceError):
    """Requested resource doesn't exist."""


# ============================================================
# SCRAPER LIFECYCLE ERRORS
# ============================================================


class ScraperNotFoundError(NotFoundError):
    """No scraper registered with the given name."""


class ScraperError(ServiceError):
    """General scraper failure."""


class ScraperDisabledError(ForbiddenError):
    """Scraper is disabled in settings."""


class ScraperAlreadyRunningError(ServiceError):
    """Another invocation of this scraper is in flight."""


# ============================================================
# DATA-LAYER ERRORS
# ============================================================


class SourceUnavailableError(ServiceError):
    """
    External source returned a transient failure (HTTP 5xx, timeout, etc.)
    that should be retried by the caller.
    """


class ParseError(ServiceError):
    """
    Data was retrieved but couldn't be parsed (malformed JSON, broken HTML,
    PDF format change, etc.). Permanent — retrying won't help.
    """


class SupabaseUnreachableError(ServiceError):
    """Could not connect to or query Supabase."""


class DatabaseError(ServiceError):
    """A database operation failed (write conflict, constraint violation, etc.)."""


# ============================================================
# ENVELOPE HELPERS
# ============================================================


def success_envelope(data: Any) -> dict[str, Any]:
    """
    Wrap a successful response payload.

    Usage:
        return success_envelope({"foo": "bar"})
        → {"success": true, "data": {"foo": "bar"}}
    """
    return {
        "success": True,
        "data": data,
    }


def error_envelope(exc: ServiceError) -> dict[str, Any]:
    """
    Wrap a ServiceError as an error response payload.

    Usage:
        return error_envelope(ScraperNotFoundError("..."))
        → {"success": false, "error": {"type": "ScraperNotFoundError", ...}}
    """
    return {
        "success": False,
        "error": exc.to_dict(),
    }


def exception_to_envelope(exc: BaseException) -> dict[str, Any]:
    """
    Wrap any exception (typed or untyped) as an error envelope.

    For untyped exceptions, returns a generic InternalServerError shape
    that doesn't leak the exception message (which might contain secrets).
    """
    if isinstance(exc, ServiceError):
        return error_envelope(exc)

    return {
        "success": False,
        "error": {
            "type": "InternalServerError",
            "message": "An unexpected error occurred.",
        },
    }


__all__ = [
    "ServiceError",
    "UnauthorizedError",
    "ForbiddenError",
    "ValidationError",
    "NotFoundError",
    "ScraperNotFoundError",
    "ScraperError",
    "ScraperDisabledError",
    "ScraperAlreadyRunningError",
    "SourceUnavailableError",
    "ParseError",
    "SupabaseUnreachableError",
    "DatabaseError",
    "success_envelope",
    "error_envelope",
    "exception_to_envelope",
]
