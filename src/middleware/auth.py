"""
Admin authentication middleware for the FastAPI service.

Validates the X-Admin-Key header on protected endpoints. Uses
constant-time comparison (hmac.compare_digest) to defend against
timing attacks on the comparison loop.
"""

from __future__ import annotations

import hmac
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

from src.config import settings
from src.utils.logger import logger


_ADMIN_HEADER_NAME: str = "X-Admin-Key"


async def require_admin_key(
    x_admin_key: Annotated[
        str | None,
        Header(
            alias=_ADMIN_HEADER_NAME,
            description=(
                "Admin secret for accessing protected endpoints. "
                "Must match ADMIN_API_KEY configured for the service."
            ),
        ),
    ] = None,
) -> None:
    """
    FastAPI dependency that validates the X-Admin-Key header.

    Raises:
        HTTPException 503: If no admin key is configured on the service.
        HTTPException 401: If the header is missing or doesn't match.
    """
    if settings.admin_api_key is None:
        logger.error(
            "ADMIN_API_KEY not configured — rejecting all admin requests",
            hint="Set ADMIN_API_KEY in environment variables and redeploy",
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin authentication not configured on this service",
        )

    if x_admin_key is None:
        logger.warning(
            "Admin endpoint accessed without X-Admin-Key header",
            header_expected=_ADMIN_HEADER_NAME,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Admin-Key header",
            headers={"WWW-Authenticate": 'AdminKey realm="admin"'},
        )

    expected = settings.admin_api_key.get_secret_value()
    if not hmac.compare_digest(x_admin_key, expected):
        logger.warning(
            "Admin endpoint received invalid X-Admin-Key",
            received_length=len(x_admin_key),
            expected_length=len(expected),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid X-Admin-Key",
            headers={"WWW-Authenticate": 'AdminKey realm="admin"'},
        )


# Pre-built Depends for cleaner endpoint signatures
AdminKeyRequired = Depends(require_admin_key)


__all__ = ["require_admin_key", "AdminKeyRequired"]
