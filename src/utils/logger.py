"""
Structured logging configuration using loguru.

Production mode: newline-delimited JSON with top-level `message`, `level`
and `traceback` keys, which is the shape Railway's log ingester renders.
loguru's own `serialize=True` envelope nests everything under `text` and
`record`, so Railway found no message field and printed empty entries --
that is why production tracebacks were invisible.

Development mode: colored human-readable output.

The `logger` instance exported here is used throughout the codebase.
A stdlib logging InterceptHandler routes any logging.* calls (from
third-party libraries) through loguru so output stays uniform.
"""

from __future__ import annotations

import json
import logging
import sys
import traceback as _traceback
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

        # Find the calling frame so loguru reports the right source location.
        #
        # The previous implementation called sys._getframe(6) unguarded. When
        # the stack is shallower than six frames that raises ValueError inside
        # emit(), logging routes it to handleError(), and the record is lost.
        # Third-party library errors -- supabase/postgrest/httpx -- are exactly
        # the records that arrive on short stacks, so this silently dropped the
        # errors most worth seeing.
        depth = 0
        frame: Any = sys._getframe(1)
        while frame is not None and (
            frame.f_code.co_filename == logging.__file__
            or frame.f_code.co_filename == __file__
        ):
            frame = frame.f_back
            depth += 1

        _loguru_logger.opt(depth=depth + 1, exception=record.exc_info).log(
            level, record.getMessage()
        )


# ============================================================
# PRODUCTION JSON SINK
# ============================================================


def _json_sink(message: Any) -> None:
    """
    Write one JSON object per line to stdout.

    Keys are flat and top-level on purpose: Railway looks for `message` and
    `level` and renders nothing when they are nested. The traceback is a
    single JSON string value, so embedded newlines are escaped and the whole
    stack trace stays on one physical line -- one log entry, not forty.
    """
    record = message.record

    payload: dict[str, Any] = {
        "timestamp": record["time"].isoformat(),
        "level": record["level"].name,
        "message": record["message"],
        "logger": record["name"],
        "function": record["function"],
        "line": record["line"],
    }

    extra = record.get("extra") or {}
    if extra:
        payload["extra"] = extra

    exception = record.get("exception")
    if exception is not None:
        exc_type = getattr(exception.type, "__name__", None) or str(exception.type)
        payload["exception_type"] = exc_type
        payload["exception_message"] = str(exception.value)
        payload["traceback"] = "".join(
            _traceback.format_exception(
                exception.type, exception.value, exception.traceback
            )
        )

    try:
        line = json.dumps(payload, default=str)
    except (TypeError, ValueError):
        # Never let a log call bring down the request that made it.
        line = json.dumps(
            {
                "timestamp": record["time"].isoformat(),
                "level": record["level"].name,
                "message": str(record["message"]),
                "log_serialisation_error": True,
            }
        )

    sys.stdout.write(line + "\n")
    sys.stdout.flush()


# ============================================================
# CONFIGURATION
# ============================================================


def _configure_logger() -> None:
    """
    Set up loguru's sinks based on environment.

    Removes the default loguru handler and adds one suited to the
    deployment. Production uses flat JSON for Railway log aggregation;
    development uses colored human-readable output.
    """
    # Remove any default handlers
    _loguru_logger.remove()

    is_prod = settings.environment == "production"

    if is_prod:
        # Flat JSON output for log aggregation.
        #
        # enqueue=False: loguru sinks are already lock-protected and therefore
        # thread-safe. enqueue is for multi-PROCESS use; here it only added a
        # background queue that can drop records on a hard exit.
        #
        # backtrace=True adds frames only. diagnose stays False -- it dumps
        # local variable VALUES, which would put connection strings and row
        # payloads into the log stream.
        _loguru_logger.add(
            _json_sink,
            level=settings.log_level,
            backtrace=True,
            diagnose=False,
            enqueue=False,
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
