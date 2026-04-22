# Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
