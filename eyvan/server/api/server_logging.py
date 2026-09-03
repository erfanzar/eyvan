# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Uvicorn logging configuration for the Eyvan control-plane process."""

from __future__ import annotations

from eyvan._internal.access_logging import (
    CREDENTIAL_QUERY_PARAMETERS,
    CURSOR_QUERY_PARAMETERS,
    REDACTED_QUERY_PARAMETERS,
    REDACTED_QUERY_VALUE,
    REDACTED_URL_LOGGERS,
    QueryRedactionFilter,
    RedactingAccessFormatter,
    redact_request_target,
    uvicorn_log_config,
)
from eyvan._internal.compat import StrEnum
from eyvan._internal.json_types import JsonObject


class ServerLogLevel(StrEnum):
    """Log levels accepted by both Uvicorn and ``eyvan-server``."""

    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    DEBUG = "debug"
    TRACE = "trace"


def server_log_config(level: ServerLogLevel) -> JsonObject:
    """Return one handler graph for Uvicorn and all Eyvan package loggers.

    The access formatter is
    :class:`~eyvan._internal.access_logging.RedactingAccessFormatter`, not
    uvicorn's, and every logger in
    :data:`~eyvan._internal.access_logging.REDACTED_URL_LOGGERS` carries the
    matching filter, so neither the inbound log-page request line nor httpx's
    outbound one writes a ``?after=`` cursor. That is unconditional rather than
    opt-in: DR-0002 states
    that cursors are not logged at ordinary verbosity, an access line is not a
    wire payload, and a switch that turns the leak back on would be a switch
    someone eventually flips.

    Args:
        level: The level applied to the uvicorn and ``eyvan`` loggers.

    Returns:
        A ``logging.config.dictConfig`` document.
    """

    configured_level = level.value.upper()
    return uvicorn_log_config(
        configured_level,
        {"eyvan": {"handlers": ["default"], "level": configured_level, "propagate": False}},
    )


__all__ = (
    "CREDENTIAL_QUERY_PARAMETERS",
    "CURSOR_QUERY_PARAMETERS",
    "REDACTED_QUERY_PARAMETERS",
    "REDACTED_QUERY_VALUE",
    "REDACTED_URL_LOGGERS",
    "QueryRedactionFilter",
    "RedactingAccessFormatter",
    "ServerLogLevel",
    "redact_request_target",
    "server_log_config",
    "uvicorn_log_config",
)
