"""Apply the Apache-2.0 header to every Eyvan source file.

Runs as a pre-commit hook so new files cannot ship without a header, and is
idempotent so it can be re-run over the tree at any time. Exits non-zero when
it changed something, matching the convention of the formatting hooks.
"""

from __future__ import annotations

import pathlib
import sys

OWNER = "The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi)"
_BODY = (
    "Licensed under the Apache License, Version 2.0 (the \"License\");\n"
    "you may not use this file except in compliance with the License.\n"
    "You may obtain a copy of the License at\n"
    "\n"
    "    https://www.apache.org/licenses/LICENSE-2.0\n"
    "\n"
    "Unless required by applicable law or agreed to in writing, software\n"
    "distributed under the License is distributed on an \"AS IS\" BASIS,\n"
    "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
    "See the License for the specific language governing permissions and\n"
    "limitations under the License."
)


def _hash_header() -> str:
    lines = [f"Copyright 2026 {OWNER}.", ""] + _BODY.split("\n")
    return "\n".join(f"# {line}".rstrip() for line in lines) + "\n"


def _block_header() -> str:
    lines = [f"Copyright 2026 {OWNER}.", ""] + _BODY.split("\n")
    return "/*\n" + "\n".join(f" * {line}".rstrip() for line in lines) + "\n */\n"


def apply(path: pathlib.Path) -> bool:
    """Prepend the header to ``path`` when absent. Returns True when changed."""

    text = path.read_text(encoding="utf-8")
    if "Licensed under the Apache License" in text.split("\n\n", 1)[0] or "Apache License" in text[:900]:
        return False
    header = _block_header() if path.suffix in {".ts", ".tsx", ".css"} else _hash_header()
    if text.startswith("#!"):
        shebang, _, rest = text.partition("\n")
        path.write_text(f"{shebang}\n{header}{rest.lstrip(chr(10))}", encoding="utf-8")
    else:
        path.write_text(header + text, encoding="utf-8")
    return True


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    if argv:
        targets = [pathlib.Path(a) for a in argv]
    else:
        targets = [
            p
            for suffix in ("*.py", "*.ts", "*.tsx")
            for p in root.rglob(suffix)
            if "node_modules" not in p.parts and "dist" not in p.parts and ".venv" not in p.parts
        ]
    changed = [p for p in targets if p.is_file() and apply(p)]
    for path in changed:
        print(f"added license header: {path}")
    return 1 if changed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
