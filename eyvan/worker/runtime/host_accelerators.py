# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""What a host's accelerators actually are, as opposed to what it was sold as.

Everything else in this package describes a cell from its declaration: the
hardware catalog says what a shape carries, ``CellAccelerator`` reports what the
catalog said, and ``GpuRankLayout`` hands a rank a ``CUDA_VISIBLE_DEVICES`` built
from that description. None of it looks at the machine.

An attached GPU is not necessarily usable. An incompatible driver can create
``/dev/nvidiactl`` without binding a GPU or creating ``/dev/nvidia0``, while a
callable still reports success. Only host-side probing distinguishes that case
from a usable accelerator.

Stdlib only, and no subprocess: this runs on the worker path, and shelling out to
``nvidia-smi`` would make the answer depend on a binary the failing case is
precisely the one that cannot run.

**Who calls it.** The native worker's ``GET /api/v1/accelerators`` route runs
:func:`check_gpu_readiness` against the count the agent passes, and the agent's
execution resolver asks for it only for a cell whose published
``cell_accelerator`` is a catalog GPU shape (``ExecutionDriverResolver.readiness``
in ``eyvan.worker.runtime.registry``). A refusal there reaches
``_with_execution_readiness`` in ``eyvan.worker.agent.service``, which
downgrades the cell to ``EXECUTION_CONNECTING`` with the reason as its detail --
so the incompatible-driver host described above becomes a cell that never turns
schedulable, rather than one that runs a workload on the CPU and reports success.

It is gated strictly on a GPU accelerator kind: the readiness path is the one
every TPU cell depends on, and a TPU or uncatalogued cell publishes no GPU device
count, so it never reaches this module. Unknown stays unknown; it is never read
as "expects zero GPUs".

The count comes from the catalog row, which is the whole host. A worker that is
given a subset of the host's devices is held to that subset through
``required_indices``: each named device must have its node, whatever the host
total is.

**What this does not prove.** A device node is the kernel's claim that a GPU is
bound, not that CUDA can open it: a fallen-off-the-bus GPU, an Xid storm or an
ECC-retired device all keep their node. Those need NVML or DCGM, which is a
dependency this probe deliberately does not take. And it says nothing about the
container path's NVIDIA toolkit. A one-shot ``docker run --gpus ... nvidia-smi
-L`` would, but it needs an image the worker does not own (images are per
workload), it can pull gigabytes on first use, and readiness is polled on every
observation -- so a container cell's toolkit is proven by its first workload,
and this probe answers only the question it can answer in microseconds.
"""

from __future__ import annotations

import os
import pwd
import re
import stat
import struct
import typing as tp
from dataclasses import dataclass
from pathlib import Path

#: ``/dev/nvidia0``, ``/dev/nvidia1``... The numbered nodes are the ones a
#: process opens. ``nvidiactl``, ``nvidia-uvm`` and friends are deliberately not
#: matched: they exist on a host whose driver loaded but bound no GPU, which is
#: the exact case this module exists to tell apart.
_DEVICE_NODE = re.compile(r"^nvidia(\d+)$")


#: The non-numbered nodes a CUDA process opens besides its GPUs. ``nvidia-uvm``
#: appears only after its module loads, so it is checked when present and never
#: required: requiring it would refuse a host whose first CUDA process has not
#: yet triggered the load.
_CONTROL_NODES: tp.Final = ("nvidiactl", "nvidia-uvm")


@dataclass(frozen=True, slots=True)
class ObservedNvidiaDevices:
    """Numbered NVIDIA device nodes present on this host, and the control node."""

    devices: int
    control_node_present: bool
    #: The minor numbers seen, sorted. Kept so a worker given a subset of the
    #: host's devices can be held to the ones it was given rather than a total.
    indices: tuple[int, ...] = ()

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
    indices = sorted(int(match.group(1)) for name in names if (match := _DEVICE_NODE.match(name)))
    return ObservedNvidiaDevices(
        devices=len(indices),
        control_node_present="nvidiactl" in names,
        indices=tuple(indices),
    )


@dataclass(frozen=True, slots=True)
class GpuReadiness:
    """Whether this host can run the GPU work its cell was declared for, and why not."""

    ready: bool
    detail: str
    observed: ObservedNvidiaDevices


def evaluate_gpu_readiness(
    observed: ObservedNvidiaDevices,
    *,
    expected_devices: int | None,
    required_indices: tp.Sequence[int] = (),
) -> GpuReadiness:
    """Hold an observation to what the cell was declared to carry.

    More devices than expected is ready: a worker that owns a slice of a larger
    host sees every node on it, and the catalog count is a floor for the host,
    not an exact claim about which process uses what. Fewer is refused, and so is
    any named device whose node is missing.

    Args:
        observed: What :func:`observe_nvidia_devices` saw.
        expected_devices: GPUs the cell's catalog row says one host carries, or
            ``None`` when nothing declared a count.
        required_indices: Specific devices this worker was configured to use.

    Returns:
        The verdict, with a detail that names the host's failure in the terms an
        operator would check it in.
    """

    if expected_devices is not None and (
        isinstance(expected_devices, bool) or not isinstance(expected_devices, int) or expected_devices < 0
    ):
        raise ValueError("expected GPU device count must be a non-negative integer or absent")
    wanted = expected_devices or 0
    missing = sorted(set(required_indices) - set(observed.indices))
    declared = wanted
    if required_indices:
        # A slice is held to its own devices: the rest of the host belongs to
        # other cells, so neither their count nor their health is this cell's.
        wanted = 0
    if observed.driver_present_but_no_device and (wanted or required_indices):
        return GpuReadiness(
            False,
            "the NVIDIA driver is loaded (/dev/nvidiactl exists) but exposes no GPU device node "
            f"(/dev/nvidia0...); expected {wanted or len(required_indices)}. The driver cannot drive this "
            "host's GPU: check `nvidia-smi` and `dmesg` for an unsupported GPU or a driver/module mismatch",
            observed,
        )
    if observed.devices == 0 and (wanted or required_indices):
        return GpuReadiness(
            False,
            f"no NVIDIA device nodes under /dev; expected {wanted or len(required_indices)} GPUs. "
            "The NVIDIA driver is not loaded on this host",
            observed,
        )
    if observed.devices < wanted:
        seen = ",".join(str(index) for index in observed.indices)
        return GpuReadiness(
            False,
            f"host exposes {observed.devices} of the {wanted} GPU device nodes its accelerator type declares "
            f"(present: /dev/nvidia{{{seen}}}). A GPU is missing, fell off the bus, or is not bound by the driver",
            observed,
        )
    if missing:
        return GpuReadiness(
            False,
            "configured GPU devices have no device node: " + ", ".join(f"/dev/nvidia{index}" for index in missing),
            observed,
        )
    if required_indices and len(required_indices) < declared:
        return GpuReadiness(
            False,
            f"this worker's GPU slice has {len(required_indices)} devices but its cell declares {declared}; "
            "set the pool's gpu_devices_per_host to the slice size",
            observed,
        )
    if not wanted and not required_indices:
        return GpuReadiness(True, "no GPU devices were declared for this cell", observed)
    return GpuReadiness(True, f"{observed.devices} GPU device nodes present", observed)


# POSIX ACL extended-attribute layout (``system.posix_acl_access``): a
# little-endian version word, then 8-byte entries of tag, permission and id.
_ACL_XATTR: tp.Final = "system.posix_acl_access"
_ACL_VERSION: tp.Final = 2
_ACL_USER_OBJ, _ACL_USER, _ACL_GROUP_OBJ, _ACL_GROUP, _ACL_MASK, _ACL_OTHER = 0x01, 0x02, 0x04, 0x08, 0x10, 0x20
_READ_WRITE: tp.Final = 0o6


def _read_acl(path: str) -> bytes | None:
    getxattr = getattr(os, "getxattr", None)
    if getxattr is None:
        return None
    try:
        return tp.cast(bytes, getxattr(path, _ACL_XATTR))
    except OSError:
        return None


def _granted_read_write(mode: int, owner: int, group: int, *, uid: int, gids: frozenset[int], acl: bytes | None) -> bool:
    """Apply the POSIX ACL access-check algorithm for read and write.

    The order is the kernel's: owner, then named user (masked), then any
    matching group entry (masked), then other. Without an ACL the mode bits are
    the three-entry ACL they encode.
    """

    if uid == 0:
        return True
    entries: list[tuple[int, int, int]] = []
    if acl is not None and len(acl) >= 4 and (len(acl) - 4) % 8 == 0:
        (version,) = struct.unpack_from("<I", acl, 0)
        if version == _ACL_VERSION:
            entries = [struct.unpack_from("<HHI", acl, offset) for offset in range(4, len(acl), 8)]
    if not entries:
        entries = [
            (_ACL_USER_OBJ, (mode >> 6) & 0o7, 0),
            (_ACL_GROUP_OBJ, (mode >> 3) & 0o7, 0),
            (_ACL_OTHER, mode & 0o7, 0),
        ]
    mask = next((perm for tag, perm, _ in entries if tag == _ACL_MASK), 0o7)
    if uid == owner:
        return all(perm & _READ_WRITE == _READ_WRITE for tag, perm, _ in entries if tag == _ACL_USER_OBJ)
    named = [perm for tag, perm, ident in entries if tag == _ACL_USER and ident == uid]
    if named:
        return named[0] & mask & _READ_WRITE == _READ_WRITE
    groups = [
        perm
        for tag, perm, ident in entries
        if (tag == _ACL_GROUP_OBJ and group in gids) or (tag == _ACL_GROUP and ident in gids)
    ]
    if groups:
        return any(perm & mask & _READ_WRITE == _READ_WRITE for perm in groups)
    return any(perm & _READ_WRITE == _READ_WRITE for tag, perm, _ in entries if tag == _ACL_OTHER)


def inaccessible_nvidia_devices(
    user: str,
    dev_root: Path | str = Path("/dev"),
    *,
    acl_reader: tp.Callable[[str], bytes | None] = _read_acl,
    indices: tp.Collection[int] | None = None,
) -> tuple[str, ...]:
    """Return the NVIDIA nodes ``user`` cannot open read-write, by mode and ACL.

    The native path runs a workload as an unprivileged user, so a GPU it cannot
    open is as absent to it as a GPU with no node -- and the failure shows up
    only as CUDA reporting no device, inside a job that was admitted. The GCP
    bootstrap grants these with ``setfacl``, which does not survive a reboot or
    a driver reload; an operator-owned host gets them from a udev rule (see
    ``docs/runtimes.md``) or not at all. This check is how readiness tells.

    Computed from ``stat`` and the ACL xattr rather than by switching user,
    because the worker must not fork as another identity to answer a poll.

    Returns:
        Paths that exist and are not readable and writable by ``user``. An
        unknown user is reported as every node being inaccessible, since a
        workload launched as that user would fail the same way.
    """

    root = Path(dev_root)
    try:
        account = pwd.getpwnam(user)
    except KeyError:
        uid, gids = -1, frozenset()
    else:
        uid = account.pw_uid
        gids = frozenset(os.getgrouplist(user, account.pw_gid))
    try:
        names = sorted(entry.name for entry in root.iterdir())
    except OSError:
        return ()
    blocked: list[str] = []
    for name in names:
        device = _DEVICE_NODE.match(name)
        if not (device or name in _CONTROL_NODES):
            continue
        if device and indices is not None and int(device.group(1)) not in indices:
            continue  # another cell's GPU
        path = str(root / name)
        try:
            status = os.stat(path)
        except OSError:
            continue
        if not stat.S_ISCHR(status.st_mode) and not stat.S_ISREG(status.st_mode):
            continue
        if uid < 0 or not _granted_read_write(
            stat.S_IMODE(status.st_mode), status.st_uid, status.st_gid, uid=uid, gids=gids, acl=acl_reader(path)
        ):
            blocked.append(path)
    return tuple(blocked)


_DEVICE_MINOR = re.compile(r"^Device Minor:\s*(\d+)", re.MULTILINE)


def gpu_index_minor_mismatches(
    indices: tp.Sequence[int], proc_root: Path | str = Path("/proc/driver/nvidia/gpus")
) -> tuple[str, ...]:
    """Return slice devices whose NVML index and ``/dev/nvidiaN`` minor differ.

    A slice names its GPUs by device minor (``--device /dev/nvidiaN``, the
    cgroup rule), while ``--gpus device=N`` and ``nvidia-smi`` count by NVML
    index, which follows PCI bus order. The two agree wherever the driver
    probed in bus order; where they do not, one slice would be handed two
    different GPUs. The driver lists every GPU under ``proc_root`` by bus id
    with its minor, so sorting those ids gives the NVML order. Anything
    unreadable is reported as no mismatch: this narrows a check, it never
    invents a failure.
    """

    root = Path(proc_root)
    try:
        buses = sorted(entry.name.lower() for entry in root.iterdir())
    except OSError:
        return ()
    minors: list[int] = []
    for bus in buses:
        try:
            match = _DEVICE_MINOR.search((root / bus / "information").read_text())
        except OSError:
            return ()
        if match is None:
            return ()
        minors.append(int(match.group(1)))
    return tuple(
        f"NVML index {index} is /dev/nvidia{minors[index]}"
        for index in indices
        if index < len(minors) and minors[index] != index
    )


def check_gpu_readiness(
    expected_devices: int | None,
    *,
    required_indices: tp.Sequence[int] = (),
    workload_user: str | None = None,
    dev_root: Path | str = Path("/dev"),
    acl_reader: tp.Callable[[str], bytes | None] = _read_acl,
    proc_root: Path | str = Path("/proc/driver/nvidia/gpus"),
) -> GpuReadiness:
    """Observe this host and hold it to a declared GPU count and device access.

    Args:
        expected_devices: GPUs one host of this cell's accelerator type carries.
        required_indices: Specific devices this worker was configured to use.
        workload_user: The unprivileged user native workloads run as, or
            ``None`` on a path that opens devices as root (the container path).
        dev_root: Device directory, for tests.
        acl_reader: ACL source, for tests.
        proc_root: The driver's per-GPU directory, for tests.

    Returns:
        The verdict. Cheap enough to run on every readiness poll: one directory
        listing and, for the native path, one ``stat`` per NVIDIA node.
    """

    verdict = evaluate_gpu_readiness(
        observe_nvidia_devices(dev_root),
        expected_devices=expected_devices,
        required_indices=required_indices,
    )
    if verdict.ready and required_indices:
        mismatched = gpu_index_minor_mismatches(required_indices, proc_root)
        if mismatched:
            return GpuReadiness(
                False,
                "this host's GPU numbering differs between NVML and /dev ("
                + "; ".join(mismatched)
                + "), so a slice would not get the GPUs it names; slice by device minor only where they agree",
                verdict.observed,
            )
    if not verdict.ready or workload_user is None or not (expected_devices or required_indices):
        return verdict
    blocked = inaccessible_nvidia_devices(
        workload_user, dev_root, acl_reader=acl_reader, indices=set(required_indices) or None
    )
    if blocked:
        return GpuReadiness(
            False,
            f"workload user {workload_user!r} cannot open {', '.join(blocked)} read-write; "
            'grant access with the udev rule in docs/runtimes.md ("NVIDIA device access")',
            verdict.observed,
        )
    return verdict


__all__ = (
    "GpuReadiness",
    "ObservedNvidiaDevices",
    "check_gpu_readiness",
    "evaluate_gpu_readiness",
    "gpu_index_minor_mismatches",
    "inaccessible_nvidia_devices",
    "observe_nvidia_devices",
)
