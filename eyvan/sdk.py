# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Compatibility exports for Eyvan's typed SDK clients."""

from __future__ import annotations

from eyvan.client.async_client import AsyncEyvanClient
from eyvan.client.callables import EyvanCallableSubmitter
from eyvan.client.identity import ClientIdentity
from eyvan.client.sync import EyvanClient

__all__ = ("AsyncEyvanClient", "ClientIdentity", "EyvanCallableSubmitter", "EyvanClient")
