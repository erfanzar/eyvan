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
"""Versioned pricing samples used for admission and budget reservation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation

from eyvan._compat import UTC


def _money(value: Decimal | int | float | str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("price must be a finite decimal") from exc
    if not result.is_finite() or result < 0:
        raise ValueError("price must be finite and non-negative")
    return result


@dataclass(frozen=True, slots=True)
class PriceKey:
    """Lookup dimensions for one accelerator price."""

    provider: str
    accelerator_type: str
    capacity_class: str
    project: str | None = None
    region: str | None = None
    zone: str | None = None


@dataclass(frozen=True, slots=True)
class PriceSample:
    """Observed or configured hourly price for one allocation unit."""

    key: PriceKey
    hourly_unit_usd: Decimal
    source: str
    observed_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "hourly_unit_usd", _money(self.hourly_unit_usd))
        if self.observed_at.tzinfo is None:
            raise ValueError("price samples require a timezone-aware observed_at")


@dataclass(frozen=True, slots=True)
class CostProjection:
    """Admission-time cost and accelerator-hour reservation."""

    hourly_cost_usd: Decimal
    reserved_cost_usd: Decimal
    accelerator_hours: Decimal
    price_observed_at: datetime
    price_source: str


class PricingCatalog:
    """In-memory view of persisted samples with deterministic specificity."""

    def __init__(self, samples: tuple[PriceSample, ...] = ()):
        self._samples: dict[PriceKey, PriceSample] = {}
        for sample in samples:
            self.put(sample)

    def put(self, sample: PriceSample) -> None:
        current = self._samples.get(sample.key)
        if current is None or sample.observed_at >= current.observed_at:
            self._samples[sample.key] = sample

    def quote(self, key: PriceKey) -> PriceSample:
        candidates = [
            sample
            for candidate_key, sample in self._samples.items()
            if candidate_key.provider == key.provider
            and candidate_key.accelerator_type == key.accelerator_type
            and candidate_key.capacity_class == key.capacity_class
            and (candidate_key.project is None or candidate_key.project == key.project)
            and (candidate_key.region is None or candidate_key.region == key.region)
            and (candidate_key.zone is None or candidate_key.zone == key.zone)
        ]
        if not candidates:
            raise LookupError(
                f"no price for {key.provider}/{key.accelerator_type}/{key.capacity_class} in {key.project or '*'}"
            )

        def rank(sample: PriceSample) -> tuple[int, datetime, str, str, str]:
            dimensions = (sample.key.project, sample.key.region, sample.key.zone)
            return (
                sum(value is not None for value in dimensions),
                sample.observed_at,
                # Equal-rank quotes resolve by scope tuple, never insertion order.
                sample.key.project or "",
                sample.key.region or "",
                sample.key.zone or "",
            )

        return max(candidates, key=rank)

    def project(
        self,
        key: PriceKey,
        *,
        accelerator_units: int,
        duration_seconds: int,
    ) -> CostProjection:
        if accelerator_units <= 0 or duration_seconds <= 0:
            raise ValueError("cost projection requires positive accelerator units and duration")
        sample = self.quote(key)
        hours = Decimal(accelerator_units) * Decimal(duration_seconds) / Decimal(3600)
        return CostProjection(
            hourly_cost_usd=sample.hourly_unit_usd * accelerator_units,
            reserved_cost_usd=sample.hourly_unit_usd * hours,
            accelerator_hours=hours,
            price_observed_at=sample.observed_at,
            price_source=sample.source,
        )


def configured_price(
    provider: str,
    accelerator_type: str,
    capacity_class: str,
    hourly_unit_usd: Decimal | int | float | str,
    *,
    source: str = "configured",
) -> PriceSample:
    """Construct a global fallback price sample."""

    return PriceSample(
        key=PriceKey(provider, accelerator_type, capacity_class),
        hourly_unit_usd=_money(hourly_unit_usd),
        source=source,
        observed_at=datetime.now(UTC),
    )
