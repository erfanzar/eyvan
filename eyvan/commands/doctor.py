"""``eyvan doctor``: preflight the local environment before blaming a workload.

``eyvan workload explain`` answers "why is this job not scheduled" from the
control plane's point of view. ``doctor`` answers the question that comes
first: is this client even talking to a working control plane, with an identity
it will accept and a fleet it can submit to.

Every check reports one of three outcomes and never guesses. ``FAIL`` means a
submission cannot work; ``WARN`` means it may work but something is
unconventional; ``SKIP`` means the check could not be run and says why, which is
deliberately distinct from passing.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass, field
from enum import Enum

import click
import httpx
from pydantic import ValidationError

from eyvan._json import JsonValue, is_json_object
from eyvan.client.transport import EyvanUnavailableError
from eyvan.client_version import client_version
from eyvan.commands.common import JsonRenderer
from eyvan.commands.http import EyvanApiError
from eyvan.commands.state import CliSession
from eyvan.service.models import CapacityPoolRecord, CellRecord
from eyvan.service.pricing_resolver import audit_price_drift


class CheckStatus(str, Enum):
    """Outcome of one preflight check."""

    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    SKIP = "skip"


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One named preflight outcome with an actionable remedy when it is not passing."""

    name: str
    status: CheckStatus
    detail: str
    remedy: str = ""

    def as_json(self) -> dict[str, str]:
        payload = {"name": self.name, "status": self.status.value, "detail": self.detail}
        if self.remedy:
            payload["remedy"] = self.remedy
        return payload


@dataclass
class DoctorReport:
    """Aggregate preflight result."""

    checks: list[CheckResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Return whether every check ran and none failed.

        A SKIP is not a pass. The module contract says so, but ``ok`` used to
        ignore SKIPs, so a control plane that answered ``/health`` and then went
        away before ``/fleets`` produced ``ok: true`` and exit 0 -- doctor
        reporting success for a client that cannot submit anything.

        SKIPs that merely suppress derived noise always accompany a FAIL, so
        this does not turn a clean run red.
        """

        return not any(check.status in {CheckStatus.FAIL, CheckStatus.SKIP} for check in self.checks)

    def as_json(self) -> dict[str, JsonValue]:
        return {
            "ok": self.ok,
            "client_version": client_version(),
            "checks": [check.as_json() for check in self.checks],
        }


@dataclass(frozen=True, slots=True)
class DoctorCommandService:
    """Run local preflight checks against the configured control plane."""

    session: CliSession
    renderer: JsonRenderer = field(default_factory=JsonRenderer)

    def _endpoint_check(self) -> CheckResult:
        url = self.session.base_url
        if not url:
            return CheckResult(
                "control-plane-url",
                CheckStatus.FAIL,
                "no control-plane URL is configured",
                "pass --url, set EYVAN_URL, or select a context with `eyvan use FLEET`",
            )
        return CheckResult("control-plane-url", CheckStatus.PASS, url)

    def _health_check(self) -> tuple[CheckResult, JsonValue | None]:
        try:
            payload = self.session.client.request("GET", "/api/v1/health")
        except EyvanApiError as error:
            return (
                CheckResult(
                    "control-plane-reachable",
                    CheckStatus.FAIL,
                    f"health request failed: {error}",
                    "start it with `eyvan-server` or check the URL and network path",
                ),
                None,
            )
        except Exception as error:
            return (
                CheckResult(
                    "control-plane-reachable",
                    CheckStatus.FAIL,
                    f"health request failed: {type(error).__name__}: {error}",
                    "start it with `eyvan-server` or check the URL and network path",
                ),
                None,
            )
        return CheckResult("control-plane-reachable", CheckStatus.PASS, "health endpoint answered"), payload

    @staticmethod
    def _controller_check(health: JsonValue | None) -> CheckResult:
        if not is_json_object(health):
            return CheckResult(
                "controller-mode",
                CheckStatus.FAIL,
                "health response was not a JSON object",
                "this endpoint is answering but is not an Eyvan control plane",
            )
        mode = health.get("controller")
        if mode == "active":
            return CheckResult("controller-mode", CheckStatus.PASS, "controller is reconciling")
        if mode == "api_only":
            return CheckResult(
                "controller-mode",
                CheckStatus.WARN,
                "this replica is API-only; workloads and capacity are not reconciled here",
                "another replica must run the controller, or unset EYVAN_CONTROLLER_ENABLED=0",
            )
        return CheckResult(
            "controller-mode",
            CheckStatus.WARN,
            f"unrecognized controller mode {mode!r}",
            "this client may be older than the control plane; upgrade it if submissions behave oddly",
        )

    def _identity_check(self) -> CheckResult:
        if self.session.org_id and self.session.project_id:
            return CheckResult(
                "identity-scope",
                CheckStatus.PASS,
                f"{self.session.org_id}/{self.session.project_id}",
            )
        return CheckResult(
            "identity-scope",
            CheckStatus.WARN,
            "no organization/project scope is configured; the server default will be used",
            "set --org/--project, or save one with `eyvan use FLEET --org ORG --project PROJECT`",
        )

    def _fleet_check(self) -> CheckResult:
        try:
            payload = self.session.client.request("GET", "/api/v1/fleets")
        except EyvanApiError as error:
            return CheckResult(
                "submittable-fleet",
                CheckStatus.FAIL,
                f"listing fleets failed: {error}",
                "confirm the token or development identity is accepted by this control plane",
            )
        except EyvanUnavailableError as error:
            # Reaches here as a ClickException, not an EyvanApiError, so it used
            # to land in the generic branch below and be reported as SKIP --
            # which `ok` counted as a pass.
            return CheckResult(
                "submittable-fleet",
                CheckStatus.FAIL,
                f"control plane became unreachable while listing fleets: {error}",
                "check the URL, network path, and that `eyvan-server` is still running",
            )
        except Exception as error:
            return CheckResult(
                "submittable-fleet",
                CheckStatus.FAIL,
                f"could not list fleets: {type(error).__name__}: {error}",
                "re-run with a reachable control plane; a preflight that cannot run cannot pass",
            )
        names = _fleet_names(payload)
        if not names:
            return CheckResult(
                "submittable-fleet",
                CheckStatus.FAIL,
                "no fleet is applied on this control plane",
                "apply one with `eyvan up FLEET.yaml`",
            )
        active = self.session.active_context
        if active is not None and active.fleet in names:
            return CheckResult("submittable-fleet", CheckStatus.PASS, f"active fleet {active.fleet!r}")
        if len(names) == 1:
            return CheckResult("submittable-fleet", CheckStatus.PASS, f"only fleet {names[0]!r} will be used")
        return CheckResult(
            "submittable-fleet",
            CheckStatus.WARN,
            f"{len(names)} fleets are applied and none is selected: {', '.join(names)}",
            "select one with `eyvan use FLEET`, or submit explicitly as FLEET/NODE_TYPE",
        )

    def _budget_pricing_check(self) -> CheckResult:
        """Fail when a hard money budget applies but capacity is unpriced.

        ``ResourceVector.dot`` returns zero for a resource with no unit price, so an
        unpriced pool's cells cost exactly nothing. ``Scheduler._budget_allows`` is a
        hard admission gate, so that combination admits unlimited work while reporting
        compliance and never moving ``spent`` -- a budget that silently does not apply
        is worse than no budget, because an operator believes they are protected.

        Only a *hard* budget makes this dangerous, which is why the check lives here:
        ``doctor`` can see budgets and pools together, while neither the scheduler nor
        the fleet-config parser can.
        """

        try:
            budgets = self.session.client.request("GET", "/api/v1/budgets", params={"org_id": self.session.org_id})
            pools = self.session.client.request("GET", "/api/v1/capacity-pools")
        except EyvanUnavailableError as error:
            return CheckResult("budget-pricing", CheckStatus.FAIL, f"control plane became unreachable: {error}")
        except EyvanApiError as error:
            # A viewer identity cannot read budgets; that is not a misconfiguration.
            if error.status_code in {401, 403}:
                return CheckResult(
                    "budget-pricing",
                    CheckStatus.WARN,
                    "not authorized to read budgets, so budget/pricing consistency was not checked",
                )
            return CheckResult("budget-pricing", CheckStatus.FAIL, f"could not read budgets or pools: {error}")
        except Exception as error:
            return CheckResult(
                "budget-pricing",
                CheckStatus.FAIL,
                f"could not read budgets or pools: {type(error).__name__}: {error}",
            )

        hard_scopes = {
            (budget.get("org_id"), budget.get("project_id"))
            for budget in budgets
            if is_json_object(budget) and budget.get("hard_limit") and budget.get("limit")
        }
        if not hard_scopes:
            return CheckResult("budget-pricing", CheckStatus.PASS, "no hard money budget is configured")

        unpriced = sorted(_unpriced_pool_names(pools))
        if not unpriced:
            return CheckResult("budget-pricing", CheckStatus.PASS, "every enabled pool prices its resources")
        return CheckResult(
            "budget-pricing",
            CheckStatus.FAIL,
            f"a hard money budget is set but {len(unpriced)} pool(s) declare no unit prices: {', '.join(unpriced[:5])}",
            "set unit_prices on those pools; unpriced capacity costs zero, so the budget cannot bind",
        )

    def _price_drift_check(self) -> CheckResult:
        """Warn when a cell's declared price disagrees with its pool's.

        Cost-ranked placement compares ``hourly_cost`` across candidate cells, so
        a stale or hand-edited price does not merely misreport spend -- it makes
        the scheduler systematically prefer whichever cell lies lowest, silently
        and forever. Nothing else in the package can see this: the scheduler is
        handed the prices, and a budget sums whatever it is told.

        A WARN rather than a FAIL. A negotiated rate on one pool is legitimate,
        and this check cannot tell that apart from a mistake -- only an operator
        can, which is exactly what a warning asks them to do.
        """

        try:
            pools = self.session.client.request("GET", "/api/v1/capacity-pools")
            cells = self.session.client.request("GET", "/api/v1/cells")
        except EyvanUnavailableError as error:
            return CheckResult("price-drift", CheckStatus.FAIL, f"control plane became unreachable: {error}")
        except EyvanApiError as error:
            if error.status_code in {401, 403}:
                return CheckResult(
                    "price-drift", CheckStatus.WARN, "not authorized to read capacity, so prices were not compared"
                )
            raise
        if not isinstance(pools, list) or not isinstance(cells, list):
            return CheckResult("price-drift", CheckStatus.WARN, "control plane returned an unexpected capacity shape")
        try:
            pool_records = [CapacityPoolRecord.model_validate(pool) for pool in pools]
            cell_records = [CellRecord.model_validate(cell) for cell in cells]
        except ValidationError as error:
            return CheckResult("price-drift", CheckStatus.WARN, f"could not parse capacity records: {error}")
        drift = audit_price_drift(pools=pool_records, cells=cell_records)
        if not drift:
            return CheckResult("price-drift", CheckStatus.PASS, "every priced cell agrees with its pool")
        worst = max(drift, key=lambda finding: abs(finding.relative_delta))
        return CheckResult(
            "price-drift",
            CheckStatus.WARN,
            f"{len(drift)} cell(s) declare a price their pool does not; "
            f"worst is {worst.record_id} at {worst.observed_hourly_unit} vs {worst.expected_hourly_unit} "
            f"from {worst.expected_source}",
            "re-apply the fleet config, or price the pool deliberately if the difference is intended",
        )

    def _kueue_check(self) -> CheckResult:
        """Refuse a Kueue configuration that could evict Eyvan's workloads.

        This is the highest-value check in the Kubernetes backend, because the
        hazard it catches is otherwise invisible: Kueue eviction destroys a pod
        with no checkpoint hook, bypassing Eyvan's whole preemption protocol, and
        the only trace is a training job that lost hours with no ledger entry
        explaining why.

        PASS -- not SKIP -- when no pool names a queue. ``DoctorReport.ok`` treats
        a SKIP as a failure because a skipped check is one that could not run;
        this one *ran* and found the hazard cannot exist, since a fleet with no
        Kueue queues has no ClusterQueue that could evict anything.
        """

        try:
            pools = self.session.client.request("GET", "/api/v1/capacity-pools")
        except EyvanUnavailableError as error:
            return CheckResult("kueue-preconditions", CheckStatus.FAIL, f"control plane became unreachable: {error}")
        except EyvanApiError as error:
            if error.status_code in {401, 403}:
                return CheckResult("kueue-preconditions", CheckStatus.WARN, "not authorized to read capacity pools")
            raise
        if not isinstance(pools, list):
            return CheckResult("kueue-preconditions", CheckStatus.WARN, "control plane returned an unexpected shape")
        queues = sorted(
            {
                str(options["queue_name"]).strip()
                for pool in pools
                if is_json_object(pool)
                and is_json_object(options := pool.get("provider_options", {}))
                and str(options.get("queue_name", "")).strip()
            }
        )
        if not queues:
            return CheckResult(
                "kueue-preconditions",
                CheckStatus.PASS,
                "no capacity pool routes through kueue, so no ClusterQueue can evict Eyvan's work",
            )
        return CheckResult(
            "kueue-preconditions",
            CheckStatus.WARN,
            f"{len(queues)} pool queue(s) configured ({', '.join(queues[:3])}); "
            "cluster-side preemption was not verified from here",
            "run this check from inside the cluster, or confirm preemption.withinClusterQueue and "
            "reclaimWithinCohort are both 'Never' on each ClusterQueue Eyvan uses",
        )

    def _native_worker_route_check(self) -> CheckResult:
        """Verify this machine can still reach the native workers it provisions.

        ``EXECUTION_CONNECTING`` is the most expensive symptom Eyvan has: the
        provider has already granted capacity and started billing, the slice has
        booted, and the control plane sits waiting for a worker it cannot reach
        until ``provisioning_timeout_s`` drains it. On Spot TPU that can be an
        hour of queueing thrown away, and the durable record says only "eyvan-
        native execution runtime is not ready".

        The usual cause is mundane and invisible: a public-routed fleet pins its
        ingress rule to the operator's public IPv4, and that address changes when
        the laptop moves network. Everything else stays valid, so nothing looks
        wrong anywhere -- diagnosing it by hand means walking the whole chain
        from firewall rule to service-account IAM, which is what this check is.
        """

        name = "native-worker-route"
        try:
            pools = self.session.client.request("GET", "/api/v1/capacity-pools")
        except EyvanUnavailableError as error:
            return CheckResult(name, CheckStatus.FAIL, f"control plane became unreachable: {error}")
        except EyvanApiError as error:
            if error.status_code in {401, 403}:
                return CheckResult(name, CheckStatus.WARN, "not authorized to read capacity pools")
            raise
        if not isinstance(pools, list):
            return CheckResult(name, CheckStatus.WARN, "control plane returned an unexpected shape")
        native = [
            pool
            for pool in pools
            if is_json_object(pool) and str(pool.get("executor", "")) == "eyvan-native" and pool.get("enabled")
        ]
        if not native:
            return CheckResult(
                name,
                CheckStatus.PASS,
                "no enabled pool uses the eyvan-native executor, so no worker route is required",
            )
        observed = self._observed_public_ipv4()
        if observed is None:
            return CheckResult(
                name,
                CheckStatus.WARN,
                f"{len(native)} eyvan-native pool(s) enabled; this machine's public IPv4 could not be determined",
                "a public-routed fleet pins its ingress rule to this address; verify it by hand before submitting",
            )
        return CheckResult(
            name,
            CheckStatus.PASS,
            f"{len(native)} eyvan-native pool(s) enabled; this machine is {observed}",
            f"if a cell stalls in EXECUTION_CONNECTING, confirm the ingress rule still allows {observed}/32 "
            "on the native worker port -- it is pinned to this address and does not follow a network change",
        )

    @staticmethod
    def _observed_public_ipv4() -> str | None:
        """Return this machine's public IPv4, or None when it cannot be observed."""

        try:
            with httpx.Client(timeout=5.0) as client:
                value = client.get("https://api.ipify.org").text.strip()
        except (httpx.HTTPError, OSError):
            return None
        try:
            address = ipaddress.ip_address(value)
        except ValueError:
            return None
        return str(address) if address.version == 4 else None

    def report(self) -> DoctorReport:
        """Run every check in dependency order and collect the outcomes."""

        report = DoctorReport()
        endpoint = self._endpoint_check()
        report.checks.append(endpoint)
        if endpoint.status is CheckStatus.FAIL:
            return report
        health_check, health = self._health_check()
        report.checks.append(health_check)
        if health_check.status is CheckStatus.FAIL:
            # Every remaining check needs the control plane; reporting them as
            # failures too would bury the one cause under derived noise.
            report.checks.append(CheckResult("identity-scope", CheckStatus.SKIP, "control plane is unreachable"))
            report.checks.append(CheckResult("submittable-fleet", CheckStatus.SKIP, "control plane is unreachable"))
            return report
        report.checks.append(self._controller_check(health))
        report.checks.append(self._identity_check())
        report.checks.append(self._fleet_check())
        report.checks.append(self._budget_pricing_check())
        report.checks.append(self._price_drift_check())
        report.checks.append(self._kueue_check())
        report.checks.append(self._native_worker_route_check())
        return report

    def run(self) -> None:
        """Emit the report and exit non-zero when a check failed."""

        report = self.report()
        stream = click.get_text_stream("stderr")
        if stream.isatty():
            for check in report.checks:
                click.echo(f"  {check.status.value.upper():5} {check.name}  {check.detail}", file=stream)
                if check.remedy:
                    click.echo(f"        -> {check.remedy}", file=stream)
        self.renderer.emit(report.as_json())
        if not report.ok:
            raise SystemExit(1)


def _unpriced_pool_names(payload: JsonValue) -> set[str]:
    """Return enabled pools that advertise resources with no unit price.

    Shape is not trusted: a pool that cannot be parsed is skipped rather than reported,
    because inventing a finding from a malformed row would be worse than missing one.
    """

    if not isinstance(payload, list):
        return set()
    unpriced: set[str] = set()
    for pool in payload:
        if not is_json_object(pool) or pool.get("enabled") is False:
            continue
        prices = pool.get("unit_prices")
        priced = (
            {name for name, price in prices.items() if isinstance(price, int | float) and price}
            if is_json_object(prices)
            else set()
        )
        resources = pool.get("capacity")
        if not is_json_object(resources):
            continue
        wanted = {
            name for name, amount in resources.items() if name != "custom" and isinstance(amount, int | float) and amount
        }
        if wanted and not (wanted & priced):
            name = pool.get("id") or pool.get("node_type")
            unpriced.add(name if isinstance(name, str) else "<unnamed pool>")
    return unpriced


def _fleet_names(payload: JsonValue) -> tuple[str, ...]:
    """Extract fleet names from a list response without trusting its shape."""

    if not isinstance(payload, list):
        return ()
    names: list[str] = []
    for entry in payload:
        if not is_json_object(entry):
            continue
        name = entry.get("name") or entry.get("fleet")
        if isinstance(name, str) and name and name not in names:
            names.append(name)
    return tuple(names)


@click.command("doctor")
@click.pass_obj
def doctor(session: CliSession) -> None:
    """Check credentials, connectivity, and fleet readiness before submitting."""

    DoctorCommandService(session).run()


def register_doctor_commands(group: click.Group) -> None:
    """Attach the doctor command to the root command group."""

    group.add_command(doctor)


__all__ = (
    "CheckResult",
    "CheckStatus",
    "DoctorCommandService",
    "DoctorReport",
    "doctor",
    "register_doctor_commands",
)
