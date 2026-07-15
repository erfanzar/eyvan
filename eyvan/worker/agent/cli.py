# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Cell-agent process entry point."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from eyvan.capacity.providers.gcp.runtime.profile import GcpNativeRuntimeProfileStore
from eyvan.config import EyvanSettings
from eyvan.worker.agent.app import create_agent_app


def _settings_for_runtime_profile(profile_path: Path | None) -> EyvanSettings:
    """Resolve explicit owner-only native settings or the ordinary environment-backed settings."""

    if profile_path is None:
        return EyvanSettings.from_env()
    profile = GcpNativeRuntimeProfileStore(profile_path).load()
    return EyvanSettings.from_gcp_native_runtime_profile(profile)


def _allow_unauthenticated_from_env() -> bool:
    """Resolve the explicit EYVAN_AGENT_ALLOW_UNAUTHENTICATED opt-out."""

    raw = os.getenv("EYVAN_AGENT_ALLOW_UNAUTHENTICATED")
    return raw is not None and raw.lower() in {"1", "true", "yes", "on"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an Eyvan cell agent")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", default=8090, type=int)
    parser.add_argument("--log-level", default="info")
    parser.add_argument("--runtime-profile", type=Path, help=argparse.SUPPRESS)
    parser.add_argument(
        "--allow-unauthenticated",
        action="store_true",
        default=_allow_unauthenticated_from_env(),
        help="run without bearer authentication (local development only); "
        "also enabled via EYVAN_AGENT_ALLOW_UNAUTHENTICATED=1",
    )
    args = parser.parse_args()

    import uvicorn

    # Imported here, beside uvicorn itself, so importing this module stays free
    # of the server dependency it only needs once it is actually serving.
    from eyvan._internal.access_logging import uvicorn_log_config

    uvicorn.run(
        create_agent_app(
            settings=_settings_for_runtime_profile(args.runtime_profile),
            allow_unauthenticated=args.allow_unauthenticated,
        ),
        host=args.host,
        port=args.port,
        # Uvicorn's own graph, with the access formatter that keeps a log-page
        # request's ``?after=`` cursor out of this host's access log.
        log_config=uvicorn_log_config(),
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
