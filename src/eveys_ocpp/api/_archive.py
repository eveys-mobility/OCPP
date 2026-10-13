"""Sanitized failure boundary for archive-only REST reads."""

from __future__ import annotations

from typing import Any

from fastapi import Request

from eveys_ocpp.api._errors import ERR_ARCHIVE_UNAVAILABLE, ApiError
from eveys_ocpp.observability import get_logger

log = get_logger(__name__)


async def archive_read(request: Request, method: str, **kwargs: Any) -> Any:
    """An unreachable/unconfigured archive cannot prove a history is empty.

    Preserve task cancellation. Driver messages and query data stay out
    of the HTTP envelope and log; only the exception class is observed.
    """
    client = getattr(request.app.state, "ch_client", None)
    if client is not None:
        try:
            return await getattr(client, method)(**kwargs)
        except Exception as exc:
            log.warning(
                "rest.archive_unavailable", operation=method, error_class=type(exc).__name__
            )
    raise ApiError(
        status_code=503,
        error_code=ERR_ARCHIVE_UNAVAILABLE,
        message="archive read unavailable",
    )
