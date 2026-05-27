"""
Retry decorators for transient HTTP failures.

Wraps tenacity with sensible defaults for our scraper workload:
  - Retry on SourceUnavailableError, httpx network errors, asyncio timeouts
  - Don't retry on ParseError, ValidationError (permanent failures)
  - Exponential backoff: 2s, 4s, 8s (capped)
  - Max 3 attempts by default (configurable via settings)

Both async (@retry_on_transient) and sync (@retry_on_transient_sync)
wrappers are provided.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, TypeVar

import httpx
from tenacity import (
    AsyncRetrying,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import settings
from src.utils.errors import SourceUnavailableError
from src.utils.logger import logger


# Exceptions that mean "try again later"
TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    SourceUnavailableError,
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.PoolTimeout,
    asyncio.TimeoutError,
    ConnectionError,
)


# HTTP status codes that indicate transient failures
TRANSIENT_STATUS_CODES: frozenset[int] = frozenset({
    408,  # Request Timeout
    425,  # Too Early
    429,  # Too Many Requests
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
})


def status_code_is_transient(status: int) -> bool:
    """True if the HTTP status code suggests retrying might succeed."""
    return status in TRANSIENT_STATUS_CODES


F = TypeVar("F", bound=Callable[..., Any])


def retry_on_transient(source: str | None = None, max_attempts: int | None = None):
    """
    Decorator for ASYNC functions. Retries on transient failures with
    exponential backoff.

    Args:
        source: Identifier for log messages (e.g., 'hennepin_sheriff').
        max_attempts: Override default retry count from settings.

    Usage:
        @retry_on_transient(source="my_scraper")
        async def fetch_page(url): ...
    """
    attempts = max_attempts or (settings.scraper_max_retries + 1)

    def decorator(fn):
        async def wrapper(*args, **kwargs):
            attempt_number = 0
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(attempts),
                wait=wait_exponential(multiplier=2, min=2, max=30),
                retry=retry_if_exception_type(TRANSIENT_EXCEPTIONS),
                reraise=True,
            ):
                with attempt:
                    attempt_number += 1
                    if attempt_number > 1:
                        logger.info(
                            "Retrying after transient failure",
                            source=source,
                            function=fn.__name__,
                            attempt=attempt_number,
                            max_attempts=attempts,
                        )
                    return await fn(*args, **kwargs)

        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper

    return decorator


def retry_on_transient_sync(source: str | None = None, max_attempts: int | None = None):
    """
    Decorator for SYNC functions. Same behavior as retry_on_transient
    but for synchronous code.
    """
    attempts = max_attempts or (settings.scraper_max_retries + 1)

    def decorator(fn):
        def wrapper(*args, **kwargs):
            attempt_number = 0
            for attempt in Retrying(
                stop=stop_after_attempt(attempts),
                wait=wait_exponential(multiplier=2, min=2, max=30),
                retry=retry_if_exception_type(TRANSIENT_EXCEPTIONS),
                reraise=True,
            ):
                with attempt:
                    attempt_number += 1
                    if attempt_number > 1:
                        logger.info(
                            "Retrying after transient failure",
                            source=source,
                            function=fn.__name__,
                            attempt=attempt_number,
                            max_attempts=attempts,
                        )
                    return fn(*args, **kwargs)

        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper

    return decorator


__all__ = [
    "TRANSIENT_EXCEPTIONS",
    "TRANSIENT_STATUS_CODES",
    "status_code_is_transient",
    "retry_on_transient",
    "retry_on_transient_sync",
]
