"""
Structured logging configuration using loguru.

Production mode: JSON-formatted logs (Railway log aggregation friendly).
Development mode: colored human-readable output.

The `logger` instance exported here is used throughout the codebase.
A stdlib logging InterceptHandler routes any logging.* calls (from
third-party libraries) through loguru so output stays uniform.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

from loguru import logger as _loguru_logger

from src.config import settings


# ============================================================
# STDLIB INTERCEPT
# ============================================================
# Third-party libraries (httpx, supabase, apscheduler, etc.) use the
# stdlib `logging` module. We intercept those calls and route them
# through loguru so all output flows through the same formatter and
# destination.


class InterceptHandler(logging.Handler):
    """Route stdlib logging records to loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        # Find the loguru level matching the stdlib level
        try:
            level: str | int = _loguru_logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find the calling frame so loguru reports the right source location
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        _loguru_logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


# ============================================================
# CONFIGURATION
# ============================================================


def _configure_logger() -> None:
    """
    Set up loguru's sinks based on environment.

    Removes the default loguru handler and adds one suited to the
    deployment. Production uses JSON for Railway log aggregation;
    development uses colored human-readable output.
    """
    # Remove any default handlers
    _loguru_logger.remove()

    is_prod = settings.environment == "production"

    if is_prod:
        # JSON output for log aggregation
        _loguru_logger.add(
            sys.stdout,
            level=settings.log_level,
            serialize=True,
            backtrace=False,
            diagnose=False,
            enqueue=True,  # Thread-safe queue
        )
    else:
        # Colored human-readable for dev
        _loguru_logger.add(
            sys.stdout,
            level=settings.log_level,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>"
            ),
            colorize=True,
            backtrace=True,
            diagnose=True,
        )

    # Route stdlib logging to loguru
    logging.basicConfig(
        handlers=[InterceptHandler()],
        level=0,
        force=True,
    )

    # Silence overly chatty libraries
    for noisy in (
        "httpx",
        "httpcore",
        "hpack",
        "apscheduler.executors.default",
        "apscheduler.scheduler",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# Configure on import
_configure_logger()


# ============================================================
# EXPORT
# ============================================================

logger = _loguru_logger


__all__ = ["logger"]
