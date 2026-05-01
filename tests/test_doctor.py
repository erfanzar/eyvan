# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""`eyvan doctor` preflight: pass, warn, fail, and skip must stay distinct."""

from __future__ import annotations

import typing as tp

import httpx
import pytest

from eyvan.sdk.client.identity import ClientIdentity
from eyvan.sdk.commands.doctor import CheckStatus, DoctorCommandService, DoctorReport
from eyvan.sdk.commands.state import CliSession, ContextOverrides
from eyvan.sdk.contexts import EyvanContext, EyvanContextConfig


class _Store:
    """Context store stub; doctor never writes through it."""

    def load(self) -> EyvanContextConfig:  # pragma: no cover - not exercised
        return EyvanContextConfig()

    def save(self, config: EyvanContextConfig) -> None:  # pragma: no cover
        raise AssertionError("doctor must not mutate saved contexts")


class _FakeClient:
    """Minimal stand-in for the CLI HTTP client.

    ``DoctorCommandService`` only calls ``request`` and consumes parsed JSON,
    so routing through a real ``EyvanClient`` would add a transport with no
    injection point and test nothing extra.
    """

    def __init__(self, handler: tp.Callable[[str], tp.Any]) -> None:
        self._handler = handler

    def request(self, method: str, path: str, **_kwargs) -> tp.Any:
        del method
        return self._handler(path)


def _session(
    handler: tp.Callable[[str], tp.Any],
    *,
    active: EyvanContext | None = None,
    org: str | None = "acme",
) -> CliSession:
    config = EyvanContextConfig(contexts=(active,), active_name=active.name) if active else EyvanContextConfig()
    return CliSession(
        client=_FakeClient(handler),
        identity=ClientIdentity(),
        context_store=_Store(),
        context_config=config,
        base_url="http://test",
        org_id=org,
        project_id="research" if org else None,
        overrides=ContextOverrides(),
    )


def _healthy(controller: str = "active", fleets: tp.Any = None) -> tp.Callable[[str], tp.Any]:
    payload = [{"name": "research"}] if fleets is None else fleets

    def handler(path: str) -> tp.Any:
        if path.endswith("/health"):
            return {"status": "ok", "controller": controller}
        # An empty fleet is the healthy default here: these tests assert check
        # wiring, and price drift needs real cells to have anything to compare.
        if path.endswith("/cells") or path.endswith("/capacity-pools"):
            return []
        return payload

    return handler


def _unreachable(path: str) -> tp.Any:
    raise httpx.ConnectError("connection refused")


def _by_name(report: DoctorReport) -> dict[str, CheckStatus]:
    return {check.name: check.status for check in report.checks}


@pytest.fixture(autouse=True)
def _isolated_runtime_profile(tmp_path, monkeypatch) -> None:
    """Point the staged-wheel check at a profile that does not exist.

    Otherwise every test in this module reports differently depending on
    whether the machine running it has staged a native GCP worker.
    """

    monkeypatch.setattr(
        "eyvan.sdk.commands.doctor.DEFAULT_GCP_NATIVE_RUNTIME_PROFILE_PATH",
        str(tmp_path / "no-runtime-profile.json"),
    )


def test_healthy_control_plane_with_one_fleet_passes_everything() -> None:
    report = DoctorCommandService(_session(_healthy())).report()

    assert report.ok is True
    assert _by_name(report) == {
        "control-plane-url": CheckStatus.PASS,
        "control-plane-reachable": CheckStatus.PASS,
        "controller-mode": CheckStatus.PASS,
        "identity-scope": CheckStatus.PASS,
        "submittable-fleet": CheckStatus.PASS,
        "budget-pricing": CheckStatus.PASS,
        "price-drift": CheckStatus.PASS,
        "kueue-preconditions": CheckStatus.PASS,
        "native-worker-route": CheckStatus.PASS,
        "staged-worker-wheel": CheckStatus.PASS,
    }


def test_unreachable_control_plane_fails_once_and_skips_the_rest() -> None:
    """Derived failures would bury the single real cause."""

    report = DoctorCommandService(_session(_unreachable)).report()

    statuses = _by_name(report)
    assert report.ok is False
    assert statuses["control-plane-reachable"] is CheckStatus.FAIL
    assert statuses["identity-scope"] is CheckStatus.SKIP
    assert statuses["submittable-fleet"] is CheckStatus.SKIP


def test_api_only_replica_warns_rather_than_fails() -> None:
    """An API-only replica serves reads; it just will not reconcile."""

    report = DoctorCommandService(_session(_healthy(controller="api_only"))).report()

    assert report.ok is True
    assert _by_name(report)["controller-mode"] is CheckStatus.WARN
    remedy = next(check.remedy for check in report.checks if check.name == "controller-mode")
    assert "EYVAN_CONTROLLER_ENABLED" in remedy


def test_no_applied_fleet_is_a_failure_with_an_actionable_remedy() -> None:
    report = DoctorCommandService(_session(_healthy(fleets=[]))).report()

    assert report.ok is False
    check = next(check for check in report.checks if check.name == "submittable-fleet")
    assert check.status is CheckStatus.FAIL
    assert "eyvan up" in check.remedy


def test_multiple_fleets_without_a_selection_warns() -> None:
    handler = _healthy(fleets=[{"name": "a"}, {"name": "b"}])

    report = DoctorCommandService(_session(handler)).report()

    assert report.ok is True
    assert _by_name(report)["submittable-fleet"] is CheckStatus.WARN


def test_missing_scope_warns_without_failing() -> None:
    report = DoctorCommandService(_session(_healthy(), org=None)).report()

    assert report.ok is True
    assert _by_name(report)["identity-scope"] is CheckStatus.WARN


def test_unrecognized_controller_mode_warns_rather_than_assuming_health() -> None:
    """WARN, not SKIP: a SKIP now fails the run, and this may just be a newer server.

    The point is unchanged -- an unrecognized mode is never reported as PASS --
    but it must not be fatal, because a client older than its control plane is
    an ordinary situation rather than a broken environment.
    """

    report = DoctorCommandService(_session(_healthy(controller="something-new"))).report()

    assert _by_name(report)["controller-mode"] is CheckStatus.WARN
    assert report.ok is True


def test_a_control_plane_that_goes_away_mid_preflight_does_not_exit_zero() -> None:
    """The exact shape that made ``doctor`` report success on a dead fleet.

    ``EyvanUnavailableError`` surfaces as a ``ClickException``, not an
    ``EyvanApiError``, so it fell into the fleet check's generic handler and was
    reported as SKIP -- which ``ok`` counted as a pass. Health answering first
    makes this reachable: the run got past the reachability check and then
    declared everything fine.
    """

    from eyvan.sdk.client.endpoint import ControlPlaneEndpoint
    from eyvan.sdk.client.transport import EyvanUnavailableError

    def handler(path: str) -> tp.Any:
        if path.endswith("/health"):
            return {"status": "ok", "controller": "active"}
        raise EyvanUnavailableError(ControlPlaneEndpoint.parse("http://test"), "GET")

    with pytest.raises(SystemExit) as raised:
        DoctorCommandService(_session(handler)).run()

    assert raised.value.code == 1
    report = DoctorCommandService(_session(handler)).report()
    check = next(check for check in report.checks if check.name == "submittable-fleet")
    assert check.status is CheckStatus.FAIL
    assert "unreachable" in check.detail
    assert check.remedy


def test_a_check_that_could_not_run_is_not_a_pass() -> None:
    """The module contract says SKIP is distinct from passing; ``ok`` must agree."""

    report = DoctorReport()
    report.checks.append(
        next(
            check
            for check in DoctorCommandService(_session(_unreachable)).report().checks
            if check.status is CheckStatus.SKIP
        )
    )

    assert report.ok is False


def test_an_endpoint_that_is_not_a_control_plane_fails(tmp_path) -> None:
    """Something answered /health, but not with an Eyvan health document."""

    def handler(path: str) -> tp.Any:
        return "<html>hello from nginx</html>"

    report = DoctorCommandService(_session(handler)).report()

    assert report.ok is False
    check = next(check for check in report.checks if check.name == "controller-mode")
    assert check.status is CheckStatus.FAIL


def test_run_exits_non_zero_when_a_check_failed() -> None:
    with pytest.raises(SystemExit) as raised:
        DoctorCommandService(_session(_unreachable)).run()

    assert raised.value.code == 1


def test_report_json_is_machine_readable() -> None:
    payload = DoctorCommandService(_session(_healthy())).report().as_json()

    assert payload["ok"] is True
    assert payload["client_version"]
    assert {check["name"] for check in payload["checks"]} >= {"control-plane-reachable", "submittable-fleet"}


def test_a_hard_money_budget_over_unpriced_capacity_is_a_failure() -> None:
    """An unpriced pool costs exactly zero, so a hard budget silently does not bind.

    ``ResourceVector.dot`` returns zero for a resource with no unit price, and
    ``Scheduler._budget_allows`` is a hard admission gate — so this combination admits
    unlimited work while reporting compliance and never moving ``spent``. A budget an
    operator believes protects them, and does not, is worse than no budget.
    """

    def handler(path: str) -> tp.Any:
        if path.endswith("/health"):
            return {"status": "ok", "controller": "active"}
        if path.endswith("/budgets"):
            return [{"org_id": "acme", "project_id": "research", "limit": 500.0, "hard_limit": True}]
        if path.endswith("/capacity-pools"):
            return [
                {"id": "research/v5p-16", "enabled": True, "capacity": {"tpu": 8}, "unit_prices": {}},
                {"id": "research/cpu", "enabled": True, "capacity": {"cpu": 8}, "unit_prices": {"cpu": 0.05}},
            ]
        if path.endswith("/cells"):
            return []
        return [{"name": "research"}]

    report = DoctorCommandService(_session(handler)).report()

    assert report.ok is False
    check = next(check for check in report.checks if check.name == "budget-pricing")
    assert check.status is CheckStatus.FAIL
    assert "research/v5p-16" in check.detail
    # The priced pool is not reported: only unenforceable capacity is a finding.
    assert "research/cpu" not in check.detail
    assert "unit_prices" in check.remedy


def test_unpriced_capacity_without_a_hard_budget_is_not_a_finding() -> None:
    """Local and manual development fleets legitimately price nothing."""

    def handler(path: str) -> tp.Any:
        if path.endswith("/health"):
            return {"status": "ok", "controller": "active"}
        if path.endswith("/budgets"):
            return [{"org_id": "acme", "project_id": "research", "limit": 500.0, "hard_limit": False}]
        if path.endswith("/capacity-pools"):
            return [{"id": "research/dev", "enabled": True, "capacity": {"cpu": 8}, "unit_prices": {}}]
        if path.endswith("/cells"):
            return []
        return [{"name": "research"}]

    report = DoctorCommandService(_session(handler)).report()

    assert report.ok is True
    assert _by_name(report)["budget-pricing"] is CheckStatus.PASS


def test_kueue_preconditions_pass_when_no_pool_uses_kueue() -> None:
    """A fleet with no Kueue queues has no ClusterQueue that could evict anything.

    PASS rather than SKIP is load-bearing: ``DoctorReport.ok`` treats a SKIP as a
    failure, because a skipped check is one that could not run. This one ran and
    found the hazard cannot exist.
    """

    report = DoctorCommandService(_session(_healthy())).report()

    assert _by_name(report)["kueue-preconditions"] is CheckStatus.PASS
    assert report.ok is True


def test_a_pool_routing_through_kueue_is_reported_as_unverified() -> None:
    """Cluster-side preemption cannot be read from the CLI, and saying otherwise would lie."""

    def handler(path: str) -> tp.Any:
        if path.endswith("/health"):
            return {"status": "ok", "controller": "active"}
        if path.endswith("/capacity-pools"):
            return [
                {
                    "id": "research/k8s",
                    "enabled": True,
                    "capacity": {"cpu": 8},
                    "unit_prices": {"cpu": 0.05},
                    "provider_options": {"queue_name": "research-queue"},
                }
            ]
        if path.endswith("/cells"):
            return []
        return [{"name": "research"}]

    report = DoctorCommandService(_session(handler)).report()

    check = next(item for item in report.checks if item.name == "kueue-preconditions")
    assert check.status is CheckStatus.WARN
    assert "research-queue" in check.detail
    assert "Never" in check.remedy
    assert report.ok is True, "an unverifiable precondition warns; it does not block the CLI"


def _pools_handler(pools: list[dict[str, tp.Any]]) -> tp.Callable[[str], tp.Any]:
    """Serve one capacity-pool list; the route check reads nothing else."""

    def handler(path: str) -> tp.Any:
        if path.endswith("/capacity-pools"):
            return pools
        raise AssertionError(f"the native-worker route check must not call {path}")

    return handler


def test_a_fleet_without_native_pools_needs_no_worker_route(monkeypatch) -> None:
    """PASS, not SKIP: the check ran and found the hazard cannot exist.

    ``DoctorReport.ok`` treats SKIP as a failure because a skipped check is one
    that could not run. A fleet whose pools use no eyvan-native executor has no
    worker for this machine to reach, so there is nothing to verify -- and no
    network call is made to decide that.
    """

    from eyvan.sdk.commands.doctor import DoctorCommandService

    def _never_called() -> str | None:
        raise AssertionError("a fleet with no native pools must not probe the network")

    monkeypatch.setattr(DoctorCommandService, "_observed_public_ipv4", staticmethod(_never_called))
    session = _session(_pools_handler([{"id": "p", "executor": "native-local", "enabled": True}]))

    result = DoctorCommandService(session)._native_worker_route_check()

    assert result.status is CheckStatus.PASS
    assert "no enabled pool" in result.detail


def test_an_unobservable_address_warns_rather_than_claiming_reachability(monkeypatch) -> None:
    """Not knowing the address is not the same as knowing it is fine.

    The ingress rule is pinned to this machine's public IPv4. If that cannot be
    observed, the honest answer is that it was not verified -- reporting PASS
    would assert a route nobody checked.
    """

    from eyvan.sdk.commands.doctor import DoctorCommandService

    monkeypatch.setattr(DoctorCommandService, "_observed_public_ipv4", staticmethod(lambda: None))
    session = _session(_pools_handler([{"id": "p", "executor": "eyvan-native", "enabled": True}]))

    result = DoctorCommandService(session)._native_worker_route_check()

    assert result.status is CheckStatus.WARN
    assert "could not be determined" in result.detail


def test_the_remedy_names_the_address_the_rule_must_allow(monkeypatch) -> None:
    """The whole point: an operator should not have to walk the chain by hand."""

    from eyvan.sdk.commands.doctor import DoctorCommandService

    monkeypatch.setattr(DoctorCommandService, "_observed_public_ipv4", staticmethod(lambda: "203.0.113.7"))
    session = _session(_pools_handler([{"id": "p", "executor": "eyvan-native", "enabled": True}]))

    result = DoctorCommandService(session)._native_worker_route_check()

    assert result.status is CheckStatus.PASS
    assert "203.0.113.7" in result.detail
    assert result.remedy is not None
    assert "203.0.113.7/32" in result.remedy
    assert "EXECUTION_CONNECTING" in result.remedy
