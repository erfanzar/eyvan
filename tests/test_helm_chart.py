# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""What can be checked about a Helm chart without a Helm binary.

This chart was written in an environment with no ``helm``, so ``helm lint`` and
``helm template`` have not been run against it. Saying that plainly is the first
requirement; a chart nobody has rendered is a chart that probably does not
render, and presenting one as verified would be worse than shipping none.

What *is* checkable here is the class of mistake that a Go template engine would
also let through, and that an operator discovers at install time:

* a ``.Values`` reference to a key that does not exist in ``values.yaml``,
  which renders as an empty string and produces a deployment missing an
  environment variable rather than an error;
* a default that is a credential or a permissive setting;
* a migration hook that is not actually a hook;
* the hardening decisions the raw manifests carry, silently absent from the
  templated copies.

Deliberately not a re-implementation of Go templating. Testing a hand-rolled
evaluator would test the evaluator.
"""

from __future__ import annotations

import pathlib
import re

import pytest
import yaml

CHART = pathlib.Path(__file__).resolve().parent.parent / "deploy" / "helm" / "eyvan"


def _values() -> dict:
    return yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))


def _templates() -> list[pathlib.Path]:
    """Every template, including ``_helpers.tpl``.

    The helper file is where ``nameOverride`` and the namespace are read, so a
    scan restricted to ``*.yaml`` reports them as configurable-but-unused --
    which is the kind of false positive that gets a guard deleted.
    """

    return sorted(path for path in (CHART / "templates").iterdir() if path.suffix in {".yaml", ".tpl"})


def _resolve(values: dict, path: str) -> bool:
    """Whether ``a.b.c`` names something in ``values.yaml``."""

    node: object = values
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return False
        node = node[part]
    return True


def _references(text: str) -> set[str]:
    """Every ``.Values.a.b`` path a template reads."""

    return {match.group(1) for match in re.finditer(r"\.Values\.([A-Za-z0-9_.]+)", text)}


# --------------------------------------------------------------------------- shape


def test_the_chart_metadata_parses_and_names_a_version() -> None:
    """A malformed Chart.yaml fails at install, after the operator has committed."""

    chart = yaml.safe_load((CHART / "Chart.yaml").read_text(encoding="utf-8"))

    assert chart["apiVersion"] == "v2"
    assert chart["name"] == "eyvan"
    assert chart["version"]


def test_the_values_file_parses() -> None:
    """Everything else here depends on it, so it is asserted first."""

    assert isinstance(_values(), dict)


@pytest.mark.parametrize("template", _templates(), ids=lambda path: path.name)
def test_every_values_reference_resolves(template: pathlib.Path) -> None:
    """The mistake Go templating does not catch.

    A reference to a key that is not in ``values.yaml`` renders as the empty
    string. The release succeeds, the Deployment applies, and the container
    starts without the environment variable it needed -- which surfaces as a
    runtime failure with no connection to the typo that caused it.
    """

    values = _values()
    dangling = sorted(path for path in _references(template.read_text(encoding="utf-8")) if not _resolve(values, path))

    assert not dangling, f"{template.name} reads values that do not exist: {', '.join(dangling)}"


def test_no_value_is_referenced_only_by_its_own_default() -> None:
    """A values key nothing reads is configuration an operator sets with no effect.

    Worse than a missing key, because it looks like it worked.
    """

    referenced = {path for template in _templates() for path in _references(template.read_text(encoding="utf-8"))}

    def leaves(node: object, prefix: str = "") -> list[str]:
        if not isinstance(node, dict):
            return [prefix]
        return [leaf for key, value in node.items() for leaf in leaves(value, f"{prefix}.{key}".lstrip("."))]

    def is_read(leaf: str) -> bool:
        # A whole subtree can be consumed at once -- `toYaml .Values.x.resources`
        # renders every key beneath it -- so an ancestor reference counts.
        parts = leaf.split(".")
        return any(".".join(parts[: index + 1]) in referenced for index in range(len(parts)))

    unread = sorted(leaf for leaf in leaves(_values()) if not is_read(leaf))

    assert not unread, f"these values are configurable and read by nothing: {', '.join(unread)}"


# --------------------------------------------------------------------------- defaults


def test_no_default_is_a_credential() -> None:
    """A chart that ships a working secret ships it to everyone who installs it."""

    values = (CHART / "values.yaml").read_text(encoding="utf-8")

    for suspicious in ("password", "secretKey", "token:", "-----BEGIN"):
        assert suspicious not in values, f"values.yaml appears to carry a credential: {suspicious}"


def test_the_insecure_options_default_off() -> None:
    """An operator who installs with no values gets a plane that refuses to serve.

    Which is the correct direction: an empty trusted-callable list refuses
    pickled callables, no issuer means every request is 401, and the schema is
    touched only by the migration hook.
    """

    values = _values()

    assert values["controlPlane"]["autoCreateSchema"] is False
    assert values["controlPlane"]["trustedCallableProjects"] == ""
    assert values["controlPlane"]["oidc"]["issuer"] == ""
    assert values["controlPlane"]["repositoryBackend"] == "postgres"


def test_the_protective_options_default_on() -> None:
    """The reverse of the rule above, and just as load-bearing.

    A NetworkPolicy or disruption budget an operator has to remember to enable
    is one most deployments will not have.
    """

    values = _values()

    assert values["networkPolicy"]["enabled"] is True
    assert values["podDisruptionBudget"]["enabled"] is True
    assert values["migration"]["enabled"] is True


def test_the_service_monitor_defaults_off_because_its_crd_may_not_exist() -> None:
    """A chart that installs an object of an unknown kind fails the whole release."""

    assert _values()["serviceMonitor"]["enabled"] is False


# --------------------------------------------------------------------------- ordering


def test_the_migration_is_a_pre_install_hook() -> None:
    """Otherwise an operator orders the Job against the rollout by hand.

    Getting that wrong starts a control plane against a schema it refuses, which
    is correct behaviour and a confusing first impression.
    """

    migration = (CHART / "templates" / "migration-job.yaml").read_text(encoding="utf-8")

    assert '"helm.sh/hook": pre-install,pre-upgrade' in migration
    assert "helm.sh/hook-weight" in migration


# --------------------------------------------------------------------------- hardening


def test_the_templated_control_plane_keeps_the_manifest_hardening() -> None:
    """The templates are a second copy, and a second copy is where a setting is lost.

    Each of these was a deliberate decision in ``deploy/control-plane.yaml``, and
    a chart that quietly dropped one would hand an operator a *less* safe
    deployment than the raw manifest they were replacing.
    """

    template = (CHART / "templates" / "control-plane.yaml").read_text(encoding="utf-8")

    for required in (
        "runAsNonRoot: true",
        "allowPrivilegeEscalation: false",
        "readOnlyRootFilesystem: true",
        'drop: ["ALL"]',
        "path: /readyz",
        "path: /healthz",
    ):
        assert required in template, f"the chart drops {required!r} from the control plane"


def test_the_templated_probes_are_not_the_same_endpoint() -> None:
    """Pointing liveness at the database check restarts every replica on a blip."""

    template = (CHART / "templates" / "control-plane.yaml").read_text(encoding="utf-8")
    readiness = template.index("readinessProbe")
    liveness = template.index("livenessProbe")

    assert "/readyz" in template[readiness:liveness]
    assert "/healthz" in template[liveness:]


def test_the_templated_agent_has_a_liveness_probe() -> None:
    """A wedged agent leaves capacity nothing can observe, release, or stop paying for."""

    assert "livenessProbe" in (CHART / "templates" / "cell-agent.yaml").read_text(encoding="utf-8")


def test_the_chart_states_that_it_has_not_been_rendered() -> None:
    """A chart nobody has rendered is one that probably does not render.

    Presenting it as verified would be worse than shipping none, so the README
    says so and this keeps the admission from being quietly deleted.
    """

    readme = (CHART / "README.md").read_text(encoding="utf-8")

    assert "helm lint" in readme
    assert "has not been rendered" in readme.lower() or "have not been run" in readme.lower()
