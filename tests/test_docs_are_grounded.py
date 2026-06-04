# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Documentation that names a command, flag or setting that exists.

Docs rot in one specific way: a command is renamed, the code and its tests move
together, and the prose keeps confidently describing the old name. Nothing
fails, because prose has no compiler. The reader finds out.

That is worse than no documentation. Absent docs make someone read the code;
wrong docs make them trust a sentence and lose an afternoon -- and they lose it
believing the tool is broken rather than the page.

So every ``eyvan …`` invocation in ``docs/`` is resolved against the real click
command tree, and every ``EYVAN_*`` variable against the real settings loader.
This does not check that the prose is *true*, which no test can. It checks the
part that can be checked, which is exactly the part that silently drifts.
"""

from __future__ import annotations

import pathlib
import re

import click
import pytest

from eyvan.sdk import cli

DOCS = pathlib.Path(__file__).resolve().parent.parent / "docs"

#: Invocations that are illustrative rather than literal.
_PLACEHOLDER = re.compile(r"[<{]")


def _pages() -> list[pathlib.Path]:
    return sorted(DOCS.glob("*.md"))


def _command_paths(text: str) -> set[tuple[str, ...]]:
    """Every ``eyvan a b c`` invocation, as the command path it names."""

    found: set[tuple[str, ...]] = set()
    for line in text.splitlines():
        for match in re.finditer(r"`?\beyvan((?: [a-z][a-z0-9-]*)+)", line):
            words = tuple(match.group(1).split())
            if words:
                found.add(words)
    return found


def _names_a_real_command(path: tuple[str, ...]) -> bool:
    """Whether a documented invocation names a command that exists.

    Word by word, and the distinction is exact: a word that follows a *group* has
    to be one of its subcommands, while words that follow a *leaf* are its
    arguments and are not this test's business.

    An earlier version accepted any resolvable prefix, to let arguments through.
    It let a renamed subcommand through as well -- ``eyvan capacity warmness``
    passed because ``capacity`` resolved -- which is precisely the rot this file
    exists to catch, and a mutation found it.
    """

    node: click.Command = cli.main
    for word in path:
        if not isinstance(node, click.Group):
            return True  # a leaf was reached; the rest are arguments
        child = node.commands.get(word)
        if child is None:
            return False
        node = child
    return True


def test_the_docs_directory_exists_and_is_not_empty() -> None:
    """The README was 49 KB of reference prose and there was no ``docs/``.

    A single page that long is one nobody reads to the end, so the tasks that
    matter -- install, operate, secure, debug -- were each buried in it.
    """

    assert _pages(), "docs/ is empty"
    assert {page.name for page in _pages()} >= {
        "install.md",
        "operate.md",
        "security.md",
        "troubleshoot.md",
    }


@pytest.mark.parametrize("page", _pages(), ids=lambda page: page.name)
def test_every_documented_command_exists(page: pathlib.Path) -> None:
    """A renamed command leaves the prose describing the old name.

    Nothing fails, because prose has no compiler -- so this is the compiler.
    """

    unknown: list[str] = []
    for path in sorted(_command_paths(page.read_text(encoding="utf-8"))):
        if not _names_a_real_command(path):
            unknown.append("eyvan " + " ".join(path))

    assert not unknown, f"{page.name} names commands that do not exist: {', '.join(unknown)}"


def _known_variables() -> set[str]:
    """Every ``EYVAN_*`` name the package actually spells.

    Not just the settings loader. Two distinct families exist and both are
    documented: variables an *operator* sets to configure a control plane, and
    variables the control plane *sets for a job* -- ``EYVAN_SOURCE_JOB_ID`` and
    its siblings, which a diagnostic reads inside an exec'd process. Checking
    only the loader would have rejected the second family as unknown, which is
    how a guard that is too narrow trains people to weaken it.
    """

    package = pathlib.Path(__file__).resolve().parent.parent / "eyvan"
    found: set[str] = set()
    for path in package.rglob("*.py"):
        found |= set(re.findall(r"\bEYVAN_[A-Z0-9_]+\b", path.read_text(encoding="utf-8")))
    return found


@pytest.mark.parametrize("page", _pages(), ids=lambda page: page.name)
def test_every_documented_variable_exists(page: pathlib.Path) -> None:
    """An environment variable that was renamed is worse than one never documented.

    The reader sets it, nothing complains, and the behaviour they were promised
    silently does not happen.
    """

    documented = set(re.findall(r"\bEYVAN_[A-Z0-9_]+\b", page.read_text(encoding="utf-8")))
    unknown = sorted(documented - _known_variables())

    assert not unknown, f"{page.name} names variables the package does not spell: {', '.join(unknown)}"


def test_the_settings_family_is_still_checked_against_the_loader() -> None:
    """Widening the guard must not stop it catching a renamed *setting*.

    A variable the package merely mentions is not necessarily one the loader
    reads, and the operator-facing family is the one whose drift is expensive.
    """

    import inspect

    from eyvan.config import EyvanSettings

    loader = set(re.findall(r'"(EYVAN_[A-Z0-9_]+)"', inspect.getsource(EyvanSettings)))
    install = set(re.findall(r"\bEYVAN_[A-Z0-9_]+\b", (DOCS / "install.md").read_text(encoding="utf-8")))

    assert install
    assert install <= loader, (
        f"install.md names settings the loader does not read: {', '.join(sorted(install - loader))}"
    )


@pytest.mark.parametrize("page", _pages(), ids=lambda page: page.name)
def test_no_page_links_to_a_file_that_is_not_there(page: pathlib.Path) -> None:
    """A broken relative link is the cheapest possible rot and the most common."""

    broken = [
        target
        for target in re.findall(r"\]\((?!https?:)([^)#]+)", page.read_text(encoding="utf-8"))
        if not (page.parent / target).exists()
    ]

    assert not broken, f"{page.name} links to missing files: {', '.join(broken)}"


def test_the_readme_points_at_the_split() -> None:
    """Otherwise the pages exist and nobody arriving at the repository finds them."""

    readme = (DOCS.parent / "README.md").read_text(encoding="utf-8")

    assert "docs/install.md" in readme
    assert "docs/troubleshoot.md" in readme


def test_the_install_page_still_states_the_real_number_of_prerequisites() -> None:
    """Thirteen separately-configured things, said out loud.

    A quickstart that works locally and strands a reader at step nine is the
    documentation failure this page exists to avoid, and the honest count is
    what prevents it.
    """

    install = (DOCS / "install.md").read_text(encoding="utf-8")
    rows = [line for line in install.splitlines() if re.match(r"^\| \d+ \|", line)]

    assert len(rows) == 13
    assert "thirteen" in install.lower()


def test_the_proofs_page_states_a_criterion_and_a_shape_for_each_run() -> None:
    """A proof without a pass criterion is a plan to look at something.

    The page exists so approving a spend is a decision about a known quantity,
    and a row that says what it costs without saying what would satisfy it is
    the opposite of that.
    """

    proofs = (DOCS / "proofs.md").read_text(encoding="utf-8")
    sections = [block for block in proofs.split("\n## ") if block.startswith("P")]

    assert len(sections) >= 8
    for section in sections:
        title = section.splitlines()[0]
        assert "**Passes when**" in section, f"{title} states no pass criterion"
        assert "**Shape**" in section, f"{title} states no shape"


def test_the_proofs_page_does_not_claim_any_of_them_has_run() -> None:
    """The whole value of the page is that it is honest about what is unproven.

    A future edit that quietly moves a row from "not run" to "verified" without
    a run behind it is the exact failure this repository has avoided all along.
    """

    # Whitespace-normalized: the prose wraps, and a substring check against
    # wrapped text asserts the line width rather than the sentence.
    proofs = " ".join((DOCS / "proofs.md").read_text(encoding="utf-8").split()).lower()

    assert "nothing here has been run." in proofs
    assert "none of that is evidence about a tpu" in proofs
    # And no row may quietly acquire a result without a run behind it.
    for claimed in ("verified on hardware", "confirmed on hardware", "passed on v5p", "proven on v5p"):
        assert claimed not in proofs
