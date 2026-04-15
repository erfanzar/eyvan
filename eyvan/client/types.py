"""HTTP query and header aliases used by Eyvan transports."""

from __future__ import annotations

import typing as tp

QueryValue: tp.TypeAlias = str | int | float | bool | None
QueryParams: tp.TypeAlias = tp.Mapping[str, QueryValue]
HeaderMap: tp.TypeAlias = tp.Mapping[str, str]
