# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Apply the Eyvan AGPL-3.0 SPDX header to every Eyvan source file.

Matches the header spectrax uses, so every file in the stack carries the same
notice. Runs as a pre-commit hook so a new file cannot ship without one, and is
idempotent -- including over the Apache-2.0 block this replaced, which it
rewrites rather than stacks on top of.
"""

from __future__ import annotations

import pathlib
import sys

HEADER_LINES = (
    "Copyright (C) 2026 Erfan Zare Chavoshi",
    "This file is part of Eyvan.",
    "",
    "SPDX-License-Identifier: AGPL-3.0-or-later",
)
SPDX_MARKER = "SPDX-License-Identifier: AGPL-3.0-or-later"
_LINE_COMMENT_SUFFIXES = frozenset({".ts", ".tsx", ".css"})
# Header text this one supersedes. A file carrying it is rewritten, not appended
# to, so the tree never accumulates two conflicting notices.
_SUPERSEDED_MARKERS = (
    "Licensed under the Apache License",
    "Copyright 2026 The EasyDeL/Eyvan Author",
)


def _comment_prefix(path: pathlib.Path) -> str:
    return "//" if path.suffix in _LINE_COMMENT_SUFFIXES else "#"


def header_for(path: pathlib.Path) -> str:
    """Return the header block for one file's comment syntax."""

    prefix = _comment_prefix(path)
    return "\n".join(f"{prefix} {line}".rstrip() for line in HEADER_LINES) + "\n"


def _leading_comment_block(text: str, prefix: str) -> tuple[str, str]:
    """Split ``text`` into its leading comment block and the remainder."""

    lines = text.split("\n")
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.startswith((prefix, "/*", " *", "*/")) or not line.strip():
            index += 1
            continue
        break
    # Trailing blank lines belong to the separator, not the block.
    while index > 0 and not lines[index - 1].strip():
        index -= 1
    return "\n".join(lines[:index]), "\n".join(lines[index:]).lstrip("\n")


def apply(path: pathlib.Path) -> bool:
    """Ensure ``path`` starts with the header. Returns True when changed."""

    original = path.read_text(encoding="utf-8")
    shebang, body = "", original
    if original.startswith("#!"):
        line, _, rest = original.partition("\n")
        shebang, body = f"{line}\n", rest

    prefix = _comment_prefix(path)
    block, remainder = _leading_comment_block(body, prefix)
    if any(marker in block for marker in _SUPERSEDED_MARKERS) or SPDX_MARKER in block:
        body = remainder

    updated = f"{shebang}{header_for(path)}\n{body.lstrip(chr(10))}"
    if updated == original:
        return False
    path.write_text(updated, encoding="utf-8")
    return True


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    if argv:
        targets = [pathlib.Path(argument) for argument in argv]
    else:
        targets = [
            path
            for suffix in ("*.py", "*.ts", "*.tsx")
            for path in root.rglob(suffix)
            if not {"node_modules", "dist", ".venv"} & set(path.parts)
        ]
    changed = [path for path in targets if path.is_file() and apply(path)]
    for path in changed:
        print(f"licensed: {path}")
    return 1 if changed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
