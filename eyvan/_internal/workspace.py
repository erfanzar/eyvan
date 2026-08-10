# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Deterministic workspace-bundle packaging and hardened extraction.

A workspace bundle is a content-addressed ``tar.gz`` of the caller's working
directory, shipped alongside a callable so application modules travel with the
job instead of having to be pre-baked into an image or staged as fleet
artifacts.

Two properties are load-bearing.

**Determinism.** The archive is the content address, so the same tree must
always produce the same bytes. Member order is sorted, mtimes are pinned to the
epoch, ownership is stripped, modes are normalized to two values, and gzip is
written with ``mtime=0``. Two callers packaging identical trees therefore
deduplicate to one stored object.

**Extraction is hostile-input handling.** The archive is unpacked as
root-staged content on a shared accelerator host, so :func:`extract_workspace`
never trusts a member. Absolute paths, ``..`` traversal, symlinks, hardlinks,
device nodes, FIFOs, and post-resolution escapes are all rejected rather than
sanitized, and both the member count and the total uncompressed size are
capped so a small archive cannot exhaust the host. ``tarfile``'s own ``data``
filter is not used: it requires Python 3.12 while this package supports 3.10,
and explicit rejection gives actionable errors.

Two of those defences are easy to lose to a refactor and worth naming. Files are
created with ``O_EXCL``, not ``open(path, "wb")``: a hardlink already sitting in
the destination is indistinguishable from a normal directory entry, so no amount
of path resolution can see it, and a plain write would truncate whatever it
points at. And the destination must be empty, because extraction never removes
anything -- a module left behind by a previous bundle would stay importable from
``sys.path[0]`` under a name the current bundle never shipped.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import os
import subprocess
import tarfile
import typing as tp
from dataclasses import dataclass
from pathlib import Path

#: Directories and files never worth shipping. Callers may add to this set but
#: cannot remove from it -- these are caches, build output, and secrets.
#:
#: The secret entries are not a scanner and do not pretend to be one: a bundle
#: is uploaded to shared artifact storage and unpacked on a leased multi-tenant
#: host, so the cost of shipping ``.env`` or ``~/.ssh`` by accident is a
#: credential leak. These cover the names that land in a working directory by
#: default. Anything else is the caller's to exclude.
DEFAULT_WORKSPACE_EXCLUDES: frozenset[str] = frozenset(
    {
        ".DS_Store",
        ".aws",
        ".env",
        ".env.local",
        ".git",
        ".gnupg",
        ".hg",
        ".ipynb_checkpoints",
        ".mypy_cache",
        ".netrc",
        ".npmrc",
        ".pypirc",
        ".pytest_cache",
        ".ruff_cache",
        ".ssh",
        ".svn",
        ".tox",
        ".venv",
        "__pycache__",
        "credentials.json",
        "id_dsa",
        "id_ecdsa",
        "id_ed25519",
        "id_rsa",
        "node_modules",
        "service-account.json",
        "venv",
    }
)

DEFAULT_EXCLUDED_SUFFIXES: frozenset[str] = frozenset({".pyc", ".pyo", ".so", ".o", ".pem", ".key", ".p12"})

#: Path *components* excluded by suffix. ``.egg-info`` is a directory, so it can
#: only be matched here: as a file suffix it matched nothing, because the files
#: inside it are named ``PKG-INFO``, ``SOURCES.txt``, and so on.
DEFAULT_EXCLUDED_DIRECTORY_SUFFIXES: frozenset[str] = frozenset({".egg-info", ".dist-info", ".egg"})

#: Ceiling on the packaged bytes. Mirrors the callable ceiling's intent: a
#: workspace is source, not data or checkpoints.
DEFAULT_MAX_WORKSPACE_BYTES = 64 * 1024 * 1024

#: Ceiling on files in one bundle, so a pathological tree fails fast at package
#: time rather than during extraction on a leased accelerator host.
DEFAULT_MAX_WORKSPACE_FILES = 20_000

#: Bounded so a pathological repository cannot hang a submission.
_GIT_TIMEOUT_S = 30.0

_EPOCH = 0
_FILE_MODE = 0o644
_EXEC_MODE = 0o755
_DIR_MODE = 0o755


class WorkspaceError(ValueError):
    """Raised when a workspace cannot be packaged or safely extracted."""


@dataclass(frozen=True, slots=True)
class WorkspaceArchive:
    """One packaged workspace and its content address."""

    payload: bytes
    sha256: str
    file_count: int

    @property
    def size_bytes(self) -> int:
        """Return the compressed size actually transferred."""

        return len(self.payload)


def _is_excluded(relative: Path, *, excludes: frozenset[str], excluded_suffixes: frozenset[str]) -> bool:
    if any(part in excludes for part in relative.parts):
        return True
    # Checked against every component, not just the filename: an excluded
    # directory must exclude what is inside it.
    if any(part.endswith(suffix) for part in relative.parts for suffix in DEFAULT_EXCLUDED_DIRECTORY_SUFFIXES):
        return True
    return relative.suffix in excluded_suffixes


def _git_tracked_paths(root: Path) -> list[Path] | None:
    """Return the paths git would keep, or ``None`` when ``root`` is not a repo.

    Delegating to git is the point. ``.gitignore`` matching has negations,
    ``**`` semantics, directory-only patterns, per-directory files, precedence
    between them, ``.git/info/exclude`` and the user's global excludesfile.
    A reimplementation that got any of that subtly wrong would either ship a
    checkpoint directory the user believed was ignored, or silently drop a
    source file the job needs -- and the second failure appears an hour later on
    a leased accelerator.

    ``--cached --others --exclude-standard`` is exactly "tracked, plus untracked
    that is not ignored", which is what a user means by "upload my workspace".
    ``-z`` because filenames may contain newlines.
    """

    try:
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            check=False,
            capture_output=True,
            timeout=_GIT_TIMEOUT_S,
        )
    except (OSError, subprocess.SubprocessError):
        # git missing, or a repo so large it timed out. Fall back to the walk
        # rather than fail: shipping too much is recoverable via the size
        # ceiling, refusing to ship at all is not.
        return None
    if result.returncode != 0:
        return None
    names = [name for name in result.stdout.split(b"\0") if name]
    return [root / os.fsdecode(name) for name in names]


def _collect(
    root: Path,
    *,
    excludes: frozenset[str],
    excluded_suffixes: frozenset[str],
    max_files: int,
    respect_gitignore: bool = True,
) -> list[tuple[str, Path]]:
    """Return sorted ``(arcname, path)`` pairs for every regular file to ship.

    Symlinks are skipped rather than followed: following them would silently
    copy content from outside the workspace, and storing them would reintroduce
    the link handling that extraction deliberately refuses.

    ``.gitignore`` decides *candidates*; the exclude sets still decide what
    ships. That order is deliberate and not redundant: a repository can track a
    ``.env`` or an ``id_rsa``, and a bundle is unpacked on a leased multi-tenant
    host, so git's opinion must never be able to widen what leaves the machine.
    """

    candidates: tp.Iterable[Path]
    tracked = _git_tracked_paths(root) if respect_gitignore else None
    candidates = sorted(tracked) if tracked is not None else sorted(root.rglob("*"))

    members: list[tuple[str, Path]] = []
    for path in candidates:
        try:
            relative = path.relative_to(root)
        except ValueError:
            # git can name a path outside the root only via a submodule quirk;
            # never ship something the caller did not point at.
            continue
        if _is_excluded(relative, excludes=excludes, excluded_suffixes=excluded_suffixes):
            continue
        if path.is_symlink() or not path.is_file():
            continue
        members.append((relative.as_posix(), path))
        if len(members) > max_files:
            raise WorkspaceError(f"workspace contains more than {max_files} files; narrow it or add excludes")
    return members


def build_workspace(
    root: str | Path,
    *,
    extra_excludes: tp.Iterable[str] = (),
    max_bytes: int = DEFAULT_MAX_WORKSPACE_BYTES,
    max_files: int = DEFAULT_MAX_WORKSPACE_FILES,
    respect_gitignore: bool = True,
) -> WorkspaceArchive:
    """Package one directory into a deterministic, content-addressed archive.

    When ``root`` is a git repository, ``.gitignore`` selects what is shipped.
    Without that, a normal project sends its virtualenv, checkpoints, datasets
    and build output, and a 64 MB ceiling that exists to keep bundles to *source*
    turns into an error the user cannot act on.
    """

    directory = Path(root).expanduser().resolve()
    if not directory.is_dir():
        raise WorkspaceError(f"workspace root {directory} is not a directory")

    excludes = DEFAULT_WORKSPACE_EXCLUDES | {str(item) for item in extra_excludes}
    members = _collect(
        directory,
        excludes=excludes,
        excluded_suffixes=DEFAULT_EXCLUDED_SUFFIXES,
        max_files=max_files,
        respect_gitignore=respect_gitignore,
    )
    if not members:
        raise WorkspaceError(f"workspace root {directory} contains no packageable files")

    raw = io.BytesIO()
    # mtime=0 keeps the gzip header itself deterministic; without it the digest
    # changes on every call and nothing ever deduplicates.
    with gzip.GzipFile(fileobj=raw, mode="wb", mtime=_EPOCH) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as archive:
            for arcname, path in members:
                info = tarfile.TarInfo(name=arcname)
                data = path.read_bytes()
                info.size = len(data)
                info.mtime = _EPOCH
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                info.type = tarfile.REGTYPE
                info.mode = _EXEC_MODE if path.stat().st_mode & 0o111 else _FILE_MODE
                archive.addfile(info, io.BytesIO(data))

    payload = raw.getvalue()
    if len(payload) > max_bytes:
        raise WorkspaceError(
            f"packaged workspace is {len(payload)} bytes, above the {max_bytes}-byte limit; "
            "exclude data, checkpoints, or build output"
        )
    return WorkspaceArchive(payload=payload, sha256=hashlib.sha256(payload).hexdigest(), file_count=len(members))


def _safe_member_path(name: str, destination: Path) -> Path:
    """Resolve one archive member under ``destination`` or reject it."""

    if not name or name.startswith("/") or "\x00" in name:
        raise WorkspaceError(f"workspace archive member {name!r} is not a relative path")
    candidate = Path(name)
    if candidate.is_absolute() or candidate.drive or candidate.root:
        raise WorkspaceError(f"workspace archive member {name!r} is not a relative path")
    if any(part == ".." for part in candidate.parts):
        raise WorkspaceError(f"workspace archive member {name!r} traverses outside the workspace")
    resolved = (destination / candidate).resolve()
    # Belt and braces: even with no ".." segment, a resolved path must still land
    # inside the destination -- an already-present symlink could otherwise
    # redirect the write.
    if resolved != destination and destination not in resolved.parents:
        raise WorkspaceError(f"workspace archive member {name!r} escapes the workspace directory")
    return resolved


def _create_exclusive(target: Path, mode: int) -> int:
    """Open ``target`` for writing, refusing to reuse anything already there.

    ``O_EXCL`` is the load-bearing flag. Plain ``open(target, "wb")`` follows a
    hardlink that is already present in the destination and truncates whatever
    it points at -- and ``Path.resolve``, which guards the rest of this module,
    cannot see hardlinks at all because they are indistinguishable from the
    original directory entry. ``O_EXCL`` refuses any pre-existing entry, so the
    hardlink is never opened. ``O_NOFOLLOW`` covers the symlink case on the
    final component for platforms where ``O_EXCL`` semantics are looser.
    """

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        return os.open(target, flags, mode)
    except FileExistsError as exc:
        raise WorkspaceError(
            f"workspace destination already contains {target.name!r}; extract into a clean directory"
        ) from exc
    except OSError as exc:
        raise WorkspaceError(f"cannot write workspace member {target.name!r}: {exc}") from exc


def extract_workspace(
    payload: bytes,
    destination: str | Path,
    *,
    max_total_bytes: int = DEFAULT_MAX_WORKSPACE_BYTES * 8,
    max_files: int = DEFAULT_MAX_WORKSPACE_FILES,
) -> int:
    """Extract a workspace archive under ``destination`` and return the file count.

    Every member is validated before anything is written. Only regular files and
    directories are accepted; links and special files are rejected outright.

    ``destination`` must be empty or absent. Extraction does not clean it, and
    reusing a populated directory would leave a file from a previous bundle
    importable from ``sys.path[0]`` under a name the current bundle never
    shipped -- silently changing what the callable executes.
    """

    root = Path(destination).expanduser().resolve()
    if root.exists() and any(root.iterdir()):
        raise WorkspaceError(
            f"workspace destination {root} is not empty; extraction never removes files, and a leftover "
            "module from a previous bundle would stay importable"
        )
    create_private_directory(root)

    written = 0
    members = 0
    total = 0
    with gzip.GzipFile(fileobj=io.BytesIO(payload), mode="rb") as compressed:
        with tarfile.open(fileobj=compressed, mode="r|") as archive:
            for info in archive:
                if info.islnk() or info.issym():
                    raise WorkspaceError(f"workspace archive member {info.name!r} is a link, which is not allowed")
                if info.ischr() or info.isblk() or info.isfifo() or info.isdev():
                    raise WorkspaceError(f"workspace archive member {info.name!r} is a special file")
                if not (info.isfile() or info.isdir()):
                    raise WorkspaceError(f"workspace archive member {info.name!r} has an unsupported type")

                # Every member counts, directories included. Directory entries
                # carry no payload, so counting only files let a 282 KB archive
                # create 60 000 inodes without tripping either cap.
                members += 1
                if members > max_files:
                    raise WorkspaceError(f"workspace archive contains more than {max_files} entries")

                target = _safe_member_path(info.name, root)
                if info.isdir():
                    target.mkdir(mode=_DIR_MODE, parents=True, exist_ok=True)
                    continue

                total += info.size
                if total > max_total_bytes:
                    raise WorkspaceError(
                        f"workspace archive expands beyond the {max_total_bytes}-byte limit; refusing to continue"
                    )
                written += 1

                extracted = archive.extractfile(info)
                if extracted is None:
                    raise WorkspaceError(f"workspace archive member {info.name!r} has no readable content")
                target.parent.mkdir(mode=_DIR_MODE, parents=True, exist_ok=True)
                mode = _EXEC_MODE if info.mode & 0o111 else _FILE_MODE
                with os.fdopen(_create_exclusive(target, mode), "wb") as handle:
                    handle.write(extracted.read())
                target.chmod(mode)
    return written


__all__ = (
    "DEFAULT_EXCLUDED_DIRECTORY_SUFFIXES",
    "DEFAULT_EXCLUDED_SUFFIXES",
    "DEFAULT_MAX_WORKSPACE_BYTES",
    "DEFAULT_MAX_WORKSPACE_FILES",
    "DEFAULT_WORKSPACE_EXCLUDES",
    "WorkspaceArchive",
    "WorkspaceError",
    "build_workspace",
    "create_private_directory",
    "extract_workspace",
)


def create_private_directory(path: Path, *, mode: int = 0o700) -> Path:
    """Create ``path`` and every missing parent at ``mode``, not just the leaf.

    ``Path.mkdir(mode=..., parents=True)`` applies the mode to the final
    directory only; every parent it creates gets ``0o777 & ~umask``. Creating
    ``~/.eyvan/autoscale`` therefore left ``~/.eyvan`` at 0755 under the ordinary
    umask, and the credential store -- correctly -- then refused to use a profile
    directory the group could read. A first run could lock itself out by running
    an autoscale command before a credentialed one.

    Existing directories are left exactly as they are. Widening or narrowing a
    directory this process did not create is not this function's business, and
    ``$HOME`` is one of the parents it walks.

    Args:
        path: Directory to create.
        mode: Permission bits for directories this call creates.

    Returns:
        Path: ``path``.
    """

    missing: list[Path] = []
    current = path
    while not current.exists():
        missing.append(current)
        if current.parent == current:
            break
        current = current.parent
    for directory in reversed(missing):
        directory.mkdir(mode=mode, exist_ok=True)
    return path
