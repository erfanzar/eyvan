# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""What a host's accelerators actually are, as opposed to what it was sold as.

Everything else in this package describes a cell from its declaration: the
hardware catalog says what a shape carries, ``CellAccelerator`` reports what the
catalog said, and ``GpuRankLayout`` hands a rank a ``CUDA_VISIBLE_DEVICES`` built
from that description. None of it looks at the machine.

That gap has a shape, observed on two real cells on 2026-08-13. An
``nvidia-tesla-p100`` cell came up with the GPU attached, booted the accelerator
image, ran an Eyvan callable to completion and reported success -- while
``nvidia-smi`` could not reach the driver, because the image pins NVIDIA 580 and
that branch dropped Pascal. The host had ``/dev/nvidiactl`` and no ``/dev/nvidia0``.
A ``nvidia-tesla-t4`` cell on the same image had six loaded modules and a real
``/dev/nvidia0``. The declaration was identical for both; only the machine
differed, and only the machine could say so.

Stdlib only, and no subprocess: this runs on the worker path, and shelling out to
``nvidia-smi`` would make the answer depend on a binary the failing case is
precisely the one that cannot run.

**Nothing calls this yet, deliberately.** It is a correct probe with no consumer,
which is the shape of defect this package has shipped before -- a working
feature no client can ask for -- so it declares itself here rather than reading
as protection that is in force. Wiring it belongs in
``_with_execution_readiness`` in ``eyvan.worker.agent.service``, which already
downgrades a cell to ``EXECUTION_CONNECTING`` when a readiness probe refuses.

Two things gate that. It cannot be qualified without a GPU cell, and the
readiness path it would join is the one every TPU cell depends on, so a
mis-scoped branch would break capacity that works today in order to guard
capacity nobody is currently running. When a GPU is in scope again, gate
strictly on a GPU accelerator kind and qualify against both cases above -- the
P100 is the one that reported success while its driver could not open the GPU.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

#: ``/dev/nvidia0``, ``/dev/nvidia1``... The numbered nodes are the ones a
#: process opens. ``nvidiactl``, ``nvidia-uvm`` and friends are deliberately not
#: matched: they exist on a host whose driver loaded but bound no GPU, which is
#: the exact case this module exists to tell apart.
_DEVICE_NODE = re.compile(r"^nvidia(\d+)$")


@dataclass(frozen=True, slots=True)
class ObservedNvidiaDevices:
    """Numbered NVIDIA device nodes present on this host, and the control node."""

    devices: int
    control_node_present: bool

    @property
    def driver_present_but_no_device(self) -> bool:
        """Whether a driver is installed that cannot drive this host's GPU.

        The P100 signature. Worth separating from "no GPU here at all" because
        the two call for opposite responses: this one is a cell that should never
        have accepted GPU work, while a CPU host with no NVIDIA nodes is simply a
        CPU host and nothing is wrong with it.
        """

        return self.control_node_present and self.devices == 0


def observe_nvidia_devices(dev_root: Path | str = Path("/dev")) -> ObservedNvidiaDevices:
    """Return the NVIDIA device nodes this host exposes.

    A missing or unreadable ``/dev`` is reported as nothing observed rather than
    raising: this is a probe, and a probe that takes down the caller is worse
    than one that says it saw nothing.
    """

    root = Path(dev_root)
    try:
        names = [entry.name for entry in root.iterdir()]
    except OSError:
        return ObservedNvidiaDevices(devices=0, control_node_present=False)
    return ObservedNvidiaDevices(
        devices=sum(1 for name in names if _DEVICE_NODE.match(name)),
        control_node_present="nvidiactl" in names,
    )


__all__ = ("ObservedNvidiaDevices", "observe_nvidia_devices")
