"""Detached worker used by the Eyvan native-local execution backend."""

from __future__ import annotations

import argparse
import fcntl
import os
import signal
import subprocess
import time
import typing as tp
from pathlib import Path
from types import FrameType

from eyvan._json import JsonObject
from eyvan.drivers.base import ExecutionWorkloadStatus
from eyvan.runtime.local_process import _mutate_record, _read_record, _utcnow

# Grace period between a SIGTERM stop request and the SIGKILL escalation. The
# executor scrubs EYVAN_* from the worker environment, so this stays a plain
# constant rather than environment configuration.
_STOP_GRACE_S = 30.0


def _run(record_path: Path, log_path: Path, owner_path: Path, worker_token: str) -> int:
    owner_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    owner_descriptor = os.open(owner_path, os.O_CREAT | os.O_RDWR, 0o600)
    os.fchmod(owner_descriptor, 0o600)
    fcntl.flock(owner_descriptor, fcntl.LOCK_EX)
    child: subprocess.Popen[bytes] | None = None
    stop_requested = False

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        nonlocal stop_requested
        stop_requested = True
        if child is not None and child.poll() is None:
            try:
                child.terminate()
            except ProcessLookupError:
                pass

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        record = None
        # The parent persists this worker PID immediately after ``Popen``.
        # Under a busy controller host the child can start before that atomic
        # write is scheduled, so retain the normal startup grace rather than
        # treating a harmless publication delay as a failed workload.
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            candidate = _read_record(record_path)
            if (
                candidate is not None
                and candidate.get("worker_pid") == os.getpid()
                and candidate.get("worker_token") == worker_token
            ):
                record = candidate
                break
            time.sleep(0.01)
        if record is None:
            return 2
        if bool(record.get("stop_requested")):
            stop_requested = True

            def stopped_before_start(current: JsonObject) -> None:
                current["status"] = ExecutionWorkloadStatus.STOPPED.value
                current["message"] = "native-local worker stopped before command launch"
                current["exit_code"] = None
                current["finished_at"] = _utcnow()

            _mutate_record(record_path, stopped_before_start)
            return 0

        entrypoint = record.get("entrypoint")
        environment = record.get("environment") or {}
        if not isinstance(entrypoint, str) or not isinstance(environment, dict):
            raise RuntimeError("native-local execution record has an invalid command or environment")

        def running(current: JsonObject) -> None:
            current["status"] = ExecutionWorkloadStatus.RUNNING.value
            current["message"] = "native-local command is running"
            current["started_at"] = _utcnow()

        _mutate_record(record_path, running)
        log_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Create the log with owner-only permissions from birth: a plain open
        # followed by chmod leaves a window where workload output is readable
        # under the process umask. fchmod also tightens a pre-existing file,
        # whose mode ``O_CREAT`` leaves untouched.
        log_descriptor = os.open(log_path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
        os.fchmod(log_descriptor, 0o600)
        with os.fdopen(log_descriptor, "ab", buffering=0) as log:
            # Service configuration, including native-worker credentials, must
            # never leak into the untrusted workload process. Workload-scoped
            # EYVAN_* values are persisted explicitly in ``environment``.
            inherited_environment = {key: value for key, value in os.environ.items() if not key.startswith("EYVAN_")}
            child = subprocess.Popen(
                ("/bin/sh", "-lc", entrypoint),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                close_fds=True,
                env={**inherited_environment, **environment},
            )
            if stop_requested:
                child.terminate()
            # A workload that ignores SIGTERM must not hold the owner lock
            # forever: escalate to SIGKILL after the stop grace period, then
            # reap the child so the durable record reaches a terminal state.
            # The workload deliberately shares the worker's process group —
            # the owning executor signals and cleans up that whole group — so
            # only the direct child is killed here and the worker survives to
            # persist the terminal record.
            stop_deadline = time.monotonic() + _STOP_GRACE_S if stop_requested else None
            while True:
                exit_code = child.poll()
                if exit_code is not None:
                    break
                if stop_requested:
                    if stop_deadline is None:
                        stop_deadline = time.monotonic() + _STOP_GRACE_S
                    elif time.monotonic() >= stop_deadline:
                        try:
                            child.kill()
                        except ProcessLookupError:
                            pass
                        exit_code = child.wait()
                        break
                time.sleep(0.05)

        def finished(current: JsonObject) -> None:
            stopped = stop_requested or bool(current.get("stop_requested"))
            current["status"] = (
                ExecutionWorkloadStatus.STOPPED.value
                if stopped
                else ExecutionWorkloadStatus.SUCCEEDED.value
                if exit_code == 0
                else ExecutionWorkloadStatus.FAILED.value
            )
            current["message"] = (
                "native-local command was stopped"
                if stopped
                else "native-local command completed"
                if exit_code == 0
                else f"native-local command exited with code {exit_code}"
            )
            current["exit_code"] = exit_code
            current["finished_at"] = _utcnow()

        _mutate_record(record_path, finished)
        return 0
    except BaseException as exc:
        failure_message = str(exc)

        def failed(current: JsonObject) -> None:
            current["status"] = ExecutionWorkloadStatus.FAILED.value
            current["message"] = f"native-local worker failed: {failure_message}"
            current["finished_at"] = _utcnow()

        _mutate_record(record_path, failed)
        return 1
    finally:
        fcntl.flock(owner_descriptor, fcntl.LOCK_UN)
        os.close(owner_descriptor)


def main(argv: tp.Sequence[str] | None = None) -> int:
    """Run one persisted native-local command."""

    parser = argparse.ArgumentParser(description="Run one Eyvan native-local workload")
    parser.add_argument("--record", type=Path, required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--owner", type=Path, required=True)
    parser.add_argument("--worker-token", required=True)
    args = parser.parse_args(argv)
    return _run(args.record, args.log, args.owner, args.worker_token)


if __name__ == "__main__":
    raise SystemExit(main())
