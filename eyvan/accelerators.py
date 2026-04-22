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
"""Shared accelerator-type normalization and presentation helpers."""

from __future__ import annotations

import typing as tp


def _accelerator_type_order(value: str) -> tuple[str, int, str]:
    """Sort a conventional ``family-size`` type by numeric size."""

    family, separator, raw_size = value.rpartition("-")
    if separator and family and raw_size.isdigit():
        return family, int(raw_size), value
    return value, -1, value


def sort_accelerator_types(values: tp.Iterable[str]) -> tuple[str, ...]:
    """Return unique accelerator types in stable family and numeric-size order."""

    return tuple(sorted(set(values), key=_accelerator_type_order))


__all__ = ("sort_accelerator_types",)
