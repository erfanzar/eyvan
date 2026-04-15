"""Uvicorn logging configuration for the Eyvan control-plane process."""

from __future__ import annotations

from eyvan._compat import StrEnum
from eyvan._json import JsonObject


class ServerLogLevel(StrEnum):
    """Log levels accepted by both Uvicorn and ``eyvan-server``."""

    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    DEBUG = "debug"
    TRACE = "trace"


def server_log_config(level: ServerLogLevel) -> JsonObject:
    """Return one handler graph for Uvicorn and all Eyvan package loggers."""

    configured_level = level.value.upper()
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "%(levelprefix)s %(message)s",
                "use_colors": None,
            },
            "access": {
                "()": "uvicorn.logging.AccessFormatter",
                "fmt": '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
            "access": {
                "formatter": "access",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": configured_level, "propagate": False},
            "uvicorn.error": {"level": configured_level},
            "uvicorn.access": {"handlers": ["access"], "level": configured_level, "propagate": False},
            "eyvan": {"handlers": ["default"], "level": configured_level, "propagate": False},
        },
    }


__all__ = ("ServerLogLevel", "server_log_config")
