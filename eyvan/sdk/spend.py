# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""What one run cost, and what the same run would have cost on demand.

The sentence a spot-first scheduler exists to say, and none of Eyvan's
competitors prints it. Everything here is arithmetic over records the control
plane already keeps: settled lease costs, accelerator-hours, and the unit
prices an operator declared on their pools.

Nothing is inferred. There is no price catalogue in Eyvan and no spot-discount
constant, so a comparison is offered only when somebody actually declared an
on-demand price for the same shape -- and reported as unknown otherwise. A
plausible number here would be worse than silence: it is the figure a team puts
in a budget.
"""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass

#: Resource dimensions billed per accelerator-hour, in the order they are read.
_ACCELERATOR_DIMENSIONS: tuple[str, ...] = ("tpu", "gpu")


@dataclass(frozen=True, slots=True)
class RunSpend:
    """What one workload's capacity cost, and what it used.

    Attributes:
        accelerator_hours: Settled accelerator-hours per dimension. Always
            known, because it is measured rather than priced.
        settled: What the fleet charged, summed over the workload's leases.
        priced: Whether every contributing lease came from a pool that declares
            a price. ``settled`` is meaningless when this is false -- an
            unpriced pool settles to zero, which is not the same as free.
        reserved: What is currently reserved for leases that have not settled.
        leases: How many leases contributed.
    """

    accelerator_hours: dict[str, float]
    settled: float
    priced: bool
    reserved: float
    leases: int

    @property
    def dimension(self) -> str | None:
        """Return the accelerator dimension this run used, if exactly one."""

        used = [name for name in _ACCELERATOR_DIMENSIONS if self.accelerator_hours.get(name)]
        return used[0] if len(used) == 1 else None

    @property
    def hours(self) -> float:
        """Return the accelerator-hours of this run's single dimension."""

        dimension = self.dimension
        return float(self.accelerator_hours.get(dimension, 0.0)) if dimension else 0.0


def spend_of(leases: tp.Sequence[tp.Mapping[str, object]], *, priced_cells: tp.Container[str] | None = None) -> RunSpend:
    """Total what a workload's leases used and were charged.

    Args:
        leases: Lease documents belonging to one workload.
        priced_cells: Cell ids whose pool declares a unit price. ``None`` means
            "unknown", which is reported as unpriced rather than assumed.

    Returns:
        The totals, with prices flagged as trustworthy only when every lease
        came from a cell known to be priced.
    """

    hours: dict[str, float] = {}
    settled = 0.0
    reserved = 0.0
    priced = bool(leases) and priced_cells is not None
    for lease in leases:
        measured = lease.get("accelerator_hours_settled")
        if isinstance(measured, dict):
            for dimension, value in measured.items():
                if isinstance(value, int | float) and not isinstance(value, bool):
                    hours[str(dimension)] = hours.get(str(dimension), 0.0) + float(value)
        for key, target in (("settled_cost", "settled"), ("reserved_cost", "reserved")):
            value = lease.get(key)
            if isinstance(value, int | float) and not isinstance(value, bool):
                if target == "settled":
                    settled += float(value)
                else:
                    reserved += float(value)
        cell_id = lease.get("cell_id")
        if priced_cells is not None and (not isinstance(cell_id, str) or cell_id not in priced_cells):
            priced = False
    return RunSpend(
        accelerator_hours=hours,
        settled=settled,
        priced=priced,
        reserved=reserved,
        leases=len(leases),
    )


def render(
    workload_id: str,
    state: str,
    spend: RunSpend,
    *,
    shape: str | None,
    capacity_class: str | None,
    on_demand_hourly: float | None,
    on_demand_source: str,
) -> tuple[str, ...]:
    """Render one run's spend, saying *unknown* wherever it is unknown."""

    lines = [f"  workload    {workload_id} ({state})"]
    if shape:
        capacity = f"{shape}" if not capacity_class else f"{shape}, {capacity_class}"
        lines.append(f"  capacity    {capacity}")
    if not spend.leases:
        lines.append("  used        nothing; this workload never held a lease")
        return tuple(lines)

    dimension = spend.dimension
    if dimension:
        lines.append(f"  used        {spend.hours:.1f} {dimension.upper()}-hours across {spend.leases} lease(s)")
    elif spend.accelerator_hours:
        measured = ", ".join(
            f"{value:.1f} {name.upper()}-hours" for name, value in sorted(spend.accelerator_hours.items())
        )
        lines.append(f"  used        {measured} across {spend.leases} lease(s)")
    else:
        lines.append(f"  used        {spend.leases} lease(s); no accelerator hours were settled")

    if spend.priced:
        lines.append(f"  cost        ${spend.settled:.2f}")
    else:
        # An unpriced pool settles to zero. Printing "$0.00" would present the
        # absence of a price as the absence of a bill.
        lines.append("  cost        unknown; no pool this run used declares a unit price")
    if spend.reserved:
        lines.append(f"  reserved    ${spend.reserved:.2f} still held by unsettled leases")

    if on_demand_hourly is None:
        lines.append(f"  on demand   unknown; {on_demand_source}")
        return tuple(lines)
    alternative = on_demand_hourly * spend.hours
    lines.append(f"  on demand   ${alternative:.2f} ({on_demand_source})")
    if spend.priced:
        saved = alternative - spend.settled
        share = f" ({saved / alternative:.0%} less)" if alternative > 0 else ""
        lines.append(f"  saved       ${saved:.2f}{share}")
    return tuple(lines)


__all__ = ("RunSpend", "render", "spend_of")
