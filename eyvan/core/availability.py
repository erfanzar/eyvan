"""Empirical capacity beliefs and bounded probing policy."""

from __future__ import annotations

import math
import typing as tp
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from eyvan._compat import UTC, StrEnum


def p95_latency(latencies_s: tp.Iterable[float]) -> float | None:
    """Return the nearest-rank p95 of observed latencies, or ``None`` when empty."""

    ordered = sorted(latencies_s)
    if not ordered:
        return None
    index = min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)
    return ordered[index]


class ProvisionOutcome(StrEnum):
    """Provider observations that update one capacity belief."""

    SUCCESS = "success"
    STOCKOUT = "stockout"
    QUOTA_ERROR = "quota_error"
    PROVIDER_ERROR = "provider_error"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True, slots=True)
class CapacityPoolKey:
    """Provider-native capacity pool whose availability is correlated."""

    provider: str
    project: str
    zone: str
    accelerator_type: str
    capacity_class: str


@dataclass(frozen=True, slots=True)
class CapacityBelief:
    """Decayed evidence used for placement scoring and probe throttling."""

    key: CapacityPoolKey
    success_evidence: float = 1.0
    failure_evidence: float = 1.0
    provision_latencies_s: tuple[float, ...] = ()
    interruptions: float = 0.0
    successful_hours: float = 0.0
    consecutive_failures: int = 0
    cooldown_until: datetime | None = None
    last_observed_at: datetime | None = None

    @property
    def success_probability(self) -> float:
        return self.success_evidence / (self.success_evidence + self.failure_evidence)

    @property
    def uncertainty(self) -> float:
        alpha = self.success_evidence
        beta = self.failure_evidence
        return math.sqrt((alpha * beta) / (((alpha + beta) ** 2) * (alpha + beta + 1)))

    @property
    def interruption_rate_per_hour(self) -> float:
        return self.interruptions / max(self.successful_hours, 1.0)

    @property
    def provisioning_latency_p95_s(self) -> float | None:
        return p95_latency(self.provision_latencies_s)

    def observe(
        self,
        outcome: ProvisionOutcome,
        *,
        now: datetime | None = None,
        latency_s: float | None = None,
        successful_hours: float = 0.0,
        evidence_decay: float = 0.98,
        max_backoff_s: int = 3600,
    ) -> CapacityBelief:
        """Return a new belief after one provider observation."""

        now = now or datetime.now(UTC)
        if now.tzinfo is None:
            raise ValueError("capacity observations require a timezone-aware timestamp")
        if not 0 < evidence_decay <= 1:
            raise ValueError("evidence_decay must be in (0, 1]")
        if latency_s is not None and latency_s < 0:
            raise ValueError("latency_s must be non-negative")
        success = self.success_evidence * evidence_decay
        failure = self.failure_evidence * evidence_decay
        interruptions = self.interruptions * evidence_decay
        hours = self.successful_hours * evidence_decay + successful_hours
        failures = self.consecutive_failures
        cooldown = self.cooldown_until
        latencies = self.provision_latencies_s

        if outcome is ProvisionOutcome.SUCCESS:
            success += 1
            failures = 0
            cooldown = None
            if latency_s is not None:
                latencies = (*latencies, latency_s)[-128:]
        elif outcome is ProvisionOutcome.INTERRUPTED:
            interruptions += 1
        else:
            failure += 1
            failures += 1
            base = 60 if outcome is ProvisionOutcome.STOCKOUT else 15
            cooldown = now + timedelta(seconds=min(max_backoff_s, base * (2 ** (failures - 1))))

        return replace(
            self,
            success_evidence=success,
            failure_evidence=failure,
            provision_latencies_s=latencies,
            interruptions=interruptions,
            successful_hours=hours,
            consecutive_failures=failures,
            cooldown_until=cooldown,
            last_observed_at=now,
        )

    def should_probe(
        self,
        *,
        blocked_value_usd: float,
        probe_cost_usd: float,
        probe_budget_remaining_usd: float,
        now: datetime | None = None,
        uncertainty_threshold: float = 0.08,
    ) -> bool:
        """Decide whether bounded exploration is economically justified."""

        now = now or datetime.now(UTC)
        return (
            blocked_value_usd > probe_cost_usd
            and probe_cost_usd <= probe_budget_remaining_usd
            and self.uncertainty >= uncertainty_threshold
            and (self.cooldown_until is None or now >= self.cooldown_until)
        )
