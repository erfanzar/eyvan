# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""What answers when somebody opens the URL every command prints.

Eyvan states its control-plane URL constantly -- on every local command, in
`eyvan start`, in `doctor`, in the notice above a development session -- so
sooner or later somebody puts it in a browser. What they got was FastAPI's
`{"detail":"Not Found"}`, which reads as a plane that is broken rather than one
that serves an API and no UI.

Two things are pinned here. A plane that *has* a bundle serves it at "/", which
is what makes a development plane behave like the head it stands in for. A plane
that has none says so, in a page that needs nothing it cannot serve.
"""

from __future__ import annotations

from pathlib import Path

from eyvan.server.api import app as app_module


def test_a_checkout_serves_the_dashboard_beside_the_package(monkeypatch) -> None:
    """The bundle sits next to the package in a source tree; a head sets a
    variable to find its own. Without this fallback the development plane is the
    only one of the three that answers its own printed URL with a 404."""

    monkeypatch.delenv("EYVAN_DASHBOARD_DIR", raising=False)

    bundle = app_module.dashboard_bundle()

    assert bundle is not None, "this checkout has dashboard/dist built"
    assert bundle.name == "dist"
    assert (bundle / "index.html").is_file()


def test_the_declared_directory_wins_over_the_checkout(monkeypatch, tmp_path) -> None:
    """A head and the container image both point at their own built bundle, and
    a checkout that happens to be on the same disk must not displace it."""

    declared = tmp_path / "bundle"
    declared.mkdir()
    monkeypatch.setenv("EYVAN_DASHBOARD_DIR", str(declared))

    assert app_module.dashboard_bundle() == declared


def test_a_declared_directory_that_is_not_there_is_not_substituted(monkeypatch, tmp_path) -> None:
    """Silently serving a different bundle than the one an operator named is a
    worse answer than serving none: the UI would be from another build."""

    monkeypatch.setenv("EYVAN_DASHBOARD_DIR", str(tmp_path / "absent"))

    assert app_module.dashboard_bundle() is None


def test_a_wheel_install_has_no_bundle_to_find(monkeypatch, tmp_path) -> None:
    """The dashboard is a Node build tree and cannot travel inside a Python
    distribution, so the fallback has to come up empty rather than guess."""

    monkeypatch.delenv("EYVAN_DASHBOARD_DIR", raising=False)
    monkeypatch.setattr(app_module, "__file__", str(tmp_path / "eyvan/server/api/app.py"))

    assert app_module.dashboard_bundle() is None


def test_a_plane_without_a_dashboard_explains_itself_at_the_root() -> None:
    """The page replaces `{"detail":"Not Found"}`, which sends people to debug
    the plane instead of reading that it simply hosts no UI."""

    page = app_module.NO_DASHBOARD_PAGE

    assert "Eyvan control plane" in page
    assert "eyvan dashboard-dev" in page
    assert "/healthz" in page


def test_the_root_page_needs_nothing_it_cannot_serve() -> None:
    """A plane hosting no bundle hosts no stylesheet, font or script either, so
    a page with one link renders unstyled -- the failure it exists to avoid."""

    page = app_module.NO_DASHBOARD_PAGE

    assert "<link" not in page
    assert "<script" not in page
    assert "http://" not in page and "https://" not in page


def test_the_root_page_is_registered_only_when_no_bundle_is_mounted() -> None:
    """A route at "/" is matched before a mount at "/", so registering both
    would shadow the dashboard's index with the page that says there is none."""

    source = Path(app_module.__file__).read_text(encoding="utf-8")
    mount = source.index('app.mount("/"')
    fallback = source.index("async def dashboard_absent")

    assert mount < fallback, "the fallback belongs in the else branch of the mount"
    assert source.count("async def dashboard_absent") == 1
