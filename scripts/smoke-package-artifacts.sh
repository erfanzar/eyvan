#!/usr/bin/env bash
# Copyright (C) 2026 Erfan Zare Chavoshi
# SPDX-License-Identifier: AGPL-3.0-or-later
set -euo pipefail

readonly ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly UV_BIN="${UV_BIN:-uv}"
command -v "${UV_BIN}" >/dev/null 2>&1 || { echo "missing required uv binary" >&2; exit 1; }

tmp="$(mktemp -d "${TMPDIR:-/tmp}/eyvan-package-smoke.XXXXXX")"
trap 'rm -rf "${tmp}"' EXIT
mkdir -p "${tmp}/dist" "${tmp}/outside-source"
"${UV_BIN}" build --wheel --sdist --out-dir "${tmp}/dist" "${ROOT}"

shopt -s nullglob
wheels=("${tmp}"/dist/*.whl)
sdists=("${tmp}"/dist/*.tar.gz)
[[ ${#wheels[@]} -eq 1 ]] || { echo "expected exactly one built wheel" >&2; exit 1; }
[[ ${#sdists[@]} -eq 1 ]] || { echo "expected exactly one built sdist" >&2; exit 1; }

smoke_artifact() {
  local kind="$1"
  local artifact="$2"
  local venv="${tmp}/venv-${kind}"
  "${UV_BIN}" venv "${venv}"
  "${UV_BIN}" pip install --python "${venv}/bin/python" "${artifact}"
  (
    cd "${tmp}/outside-source"
    unset PYTHONPATH
    "${venv}/bin/python" -I -c \
      'import pathlib, eyvan; path = pathlib.Path(eyvan.__file__).resolve(); assert "site-packages" in path.parts, path'
    for entrypoint in eyvan eyvan-server eyvan-agent eyvan-native-worker eyvan-migrate; do
      "${venv}/bin/${entrypoint}" --help >/dev/null
    done
  )
}

smoke_artifact wheel "${wheels[0]}"
smoke_artifact sdist "${sdists[0]}"
echo "Built wheel and sdist passed isolated installed-entrypoint smoke tests."
