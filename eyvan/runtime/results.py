"""Public callable-result API backed by the shared worker protocol."""

from __future__ import annotations

from eyvan.runtime.callable_protocol import FORMAT_VERSION, dump_result, load_result

__all__ = ("FORMAT_VERSION", "dump_result", "load_result")
