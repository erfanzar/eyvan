# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Machine-decided verdicts for the hardware proofs.

``docs/proofs.md`` states each proof's pass criterion in prose, which means the
verdict is whatever a person concludes from a screen of JSON at the end of a
spend they are relieved to have finished. That is the worst possible moment to
ask someone to be sceptical, and it has already gone wrong once: a proof was
started against a stale agent and its output looked fine.

So the criteria live here as pure functions over observations. The plane is
polled by a thin command; every judgement is made by code that can be tested
without hardware, which is the same split that makes the soak runner
changeable.

**Three verdicts, not two.** A proof whose preconditions never occurred is
INCONCLUSIVE, and inconclusive exits non-zero. This is the defect shape this
package keeps producing -- a check that passes because there was nothing to
check -- and on a proof it is the most expensive version of it, because the
answer arrives after the money is spent and reads as evidence. "No cell was ever
created, so nothing contradicted the claim" is not a pass.
"""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass

import click

from eyvan._internal.compat import StrEnum
from eyvan.sdk.commands.common import JsonRenderer
from eyvan.sdk.commands.state import CliSession
from eyvan.server.service.subjobs import MAX_LIVE_CHILDREN


class Verdict(StrEnum):
    """The outcome of one check or one proof."""

    PASS = "pass"
    FAIL = "fail"
    #: The observation could not settle the question. Distinct from FAIL because
    #: the responses differ: a failure means the behaviour is wrong, and an
    #: inconclusive means the run did not exercise it and has to be repeated.
    INCONCLUSIVE = "inconclusive"


#: Exit codes, so a scheduled run can tell the two non-passes apart.
EXIT_CODES = {Verdict.PASS: 0, Verdict.FAIL: 1, Verdict.INCONCLUSIVE: 2}


@dataclass(frozen=True, slots=True)
class Check:
    """One stated criterion and what the observation said about it."""

    name: str
    verdict: Verdict
    detail: str

    def as_dict(self) -> dict[str, object]:
        return {"name": self.name, "verdict": self.verdict.value, "detail": self.detail}


@dataclass(frozen=True, slots=True)
class ProofResult:
    """Every check for one proof, and the verdict they combine to."""

    proof: str
    checks: tuple[Check, ...]

    @property
    def verdict(self) -> Verdict:
        """Return the worst check's verdict.

        A failure outranks an inconclusive: if one criterion is definitively
        wrong, "we could not tell" about another does not soften it.
        """

        if any(check.verdict is Verdict.FAIL for check in self.checks):
            return Verdict.FAIL
        if not self.checks or any(check.verdict is Verdict.INCONCLUSIVE for check in self.checks):
            return Verdict.INCONCLUSIVE
        return Verdict.PASS

    def as_dict(self) -> dict[str, object]:
        return {
            "proof": self.proof,
            "verdict": self.verdict.value,
            "checks": [check.as_dict() for check in self.checks],
        }


def _passed(name: str, detail: str) -> Check:
    return Check(name, Verdict.PASS, detail)


def _failed(name: str, detail: str) -> Check:
    return Check(name, Verdict.FAIL, detail)


def _unknown(name: str, detail: str) -> Check:
    return Check(name, Verdict.INCONCLUSIVE, detail)


# --------------------------------------------------------------------------- P1


def evaluate_catalog_authority(
    pools: tp.Sequence[dict],
    cells: tp.Sequence[dict],
    *,
    accelerator_type: str,
    catalog_hosts: int | None,
) -> ProofResult:
    """P1 -- the catalog, not the provider response, decides hosts per slice.

    A provider returning one host of two yields a silently half-sized JAX
    cluster: every rank starts, the mesh is wrong, and the job runs at half
    throughput while reporting success. Only real hardware produces that case,
    which is why it is a proof rather than a test.
    """

    checks: list[Check] = []
    matching = [pool for pool in pools if pool.get("accelerator_type") == accelerator_type]
    if not matching:
        return ProofResult(
            "P1",
            (
                _unknown(
                    "pool-exists",
                    f"no pool declares accelerator_type {accelerator_type!r}; the fleet never applied",
                ),
            ),
        )
    checks.append(_passed("pool-exists", f"{len(matching)} pool(s) for {accelerator_type}"))

    if catalog_hosts is None:
        checks.append(
            _unknown(
                "catalog-records-hosts",
                f"the catalog has no host count for {accelerator_type}, so it cannot be the authority for one",
            )
        )
    else:
        checks.append(_passed("catalog-records-hosts", f"catalog says {catalog_hosts} host(s)"))

    pool_ids = {pool["id"] for pool in matching}
    members = [cell for cell in cells if cell.get("pool_id") in pool_ids and cell.get("state") != "deleted"]
    if not members:
        checks.append(_unknown("cells-exist", "the pool holds no live cell, so no observation was made"))
        return ProofResult("P1", tuple(checks))
    checks.append(_passed("cells-exist", f"{len(members)} live cell(s)"))

    unobserved = [cell["id"] for cell in members if cell.get("observed_tpu_vms") is None]
    if unobserved:
        checks.append(
            _unknown(
                "hosts-observed",
                f"{len(unobserved)} cell(s) never reported observed_tpu_vms: {', '.join(sorted(unobserved)[:3])}",
            )
        )
    elif catalog_hosts is not None:
        wrong = {cell["id"]: cell["observed_tpu_vms"] for cell in members if cell["observed_tpu_vms"] != catalog_hosts}
        if wrong:
            checks.append(
                _failed(
                    "hosts-match-catalog",
                    f"expected {catalog_hosts} host(s) per slice; observed {wrong}",
                )
            )
        else:
            checks.append(_passed("hosts-match-catalog", f"every cell reports {catalog_hosts} host(s)"))
    return ProofResult("P1", tuple(checks))


# --------------------------------------------------------------------------- P5


def evaluate_spot_preemption(workload: dict, cells: tp.Sequence[dict], pools: tp.Sequence[dict]) -> ProofResult:
    """P5 -- a reclaimed Spot slice is a preemption, not a provider error.

    The distinguishing case, and the reason the run sets ``--max-retries 0``:
    under the old shared budget the workload would have failed outright. So a
    surviving workload whose *preemption* budget moved and whose *failure*
    budget did not is the entire claim.
    """

    checks: list[Check] = []
    preemptions = int(workload.get("preemption_count", 0))
    losses = int(workload.get("capacity_loss_count", 0))

    if preemptions == 0 and losses == 0:
        # The common outcome of a short run: GCP simply never reclaimed the
        # slice. Reporting that as a pass would turn "we were not preempted"
        # into "preemption is classified correctly".
        return ProofResult(
            "P5",
            (
                _unknown(
                    "preemption-observed",
                    "the slice was never reclaimed; nothing about classification was exercised",
                ),
            ),
        )
    checks.append(_passed("preemption-observed", f"preemption_count={preemptions}, capacity_loss_count={losses}"))

    if preemptions == 0:
        checks.append(
            _failed(
                "charged-to-the-preemption-budget",
                f"capacity_loss_count moved to {losses} and preemption_count stayed at 0, "
                "so the reclamation was charged to the failure budget",
            )
        )
    elif losses:
        checks.append(
            _failed(
                "charged-to-the-preemption-budget",
                f"both budgets moved: preemption_count={preemptions}, capacity_loss_count={losses}",
            )
        )
    else:
        checks.append(_passed("charged-to-the-preemption-budget", "only preemption_count moved"))

    codes = {cell.get("provider_failure_code") for cell in cells if cell.get("provider_failure_code")}
    if not codes:
        checks.append(_unknown("classified-as-preempted", "no cell carries a provider_failure_code"))
    elif "preempted" in codes:
        checks.append(_passed("classified-as-preempted", f"cells report {sorted(codes)}"))
    else:
        checks.append(_failed("classified-as-preempted", f"expected 'preempted'; cells report {sorted(codes)}"))

    # ``INTERRUPTED`` is the one provision outcome that records no failure and
    # sets no cooldown (eyvan/domain/availability.py:114). A stockout would have
    # started a 60s exponential backoff and a provider error a 15s one, so a cell
    # in cooldown after a reclamation is the misclassification this proves
    # against -- and it is worse than a wrong label, because a cooled-down pool
    # stops being asked and the fleet quietly shrinks.
    pool_ids = {pool["id"] for pool in pools}
    cooling = [
        cell["id"]
        for cell in cells
        if cell.get("pool_id") in pool_ids and (cell.get("availability") or {}).get("cooldown_until")
    ]
    if cooling:
        checks.append(
            _failed(
                "no-stockout-cooldown",
                f"a reclamation put {len(cooling)} cell(s) into cooldown: {', '.join(sorted(cooling)[:3])}; "
                "an interruption records no failure and sets none",
            )
        )
    else:
        checks.append(_passed("no-stockout-cooldown", "no cell entered a cooldown"))

    state = workload.get("state")
    if state in {"running", "succeeded"}:
        checks.append(_passed("workload-survived", f"workload is {state}"))
    elif state == "failed":
        checks.append(_failed("workload-survived", "the workload failed rather than retrying on its preemption budget"))
    else:
        checks.append(_unknown("workload-survived", f"workload is {state}, which settles nothing yet"))
    return ProofResult("P5", tuple(checks))


# --------------------------------------------------------------------------- P6


def evaluate_warm_window(inside: dict, outside: dict, billing: dict, *, pool_id: str) -> ProofResult:
    """P6 -- a window holds a floor through its hours and releases after.

    Two observations, either side of the window's own boundary. One observation
    cannot settle it: a floor that is held and a floor that is *always* held look
    identical from inside the window, and the second is a pool that never
    scales to zero.
    """

    checks: list[Check] = []

    def _pool(report: dict) -> dict | None:
        return next((entry for entry in report.get("pools", []) if entry.get("pool_id") == pool_id), None)

    during, after = _pool(inside), _pool(outside)
    if during is None or after is None:
        return ProofResult(
            "P6",
            (_unknown("pool-observed", f"pool {pool_id!r} is absent from one of the two warmth reports"),),
        )

    if not during.get("window_active"):
        checks.append(
            _unknown(
                "window-was-active",
                "the first observation was taken outside the window, so it says nothing about holding one",
            )
        )
    else:
        checks.append(_passed("window-was-active", f"floor {during.get('floor')} in force"))

    held = int(during.get("warm_cells", 0))
    floor = int(during.get("floor", 0))
    if floor <= 0:
        checks.append(_unknown("floor-declared", "the window raised no floor, so nothing was promised"))
    elif held < floor:
        checks.append(_failed("floor-held", f"{held} of {floor} cell(s) held while the window was active"))
    else:
        checks.append(_passed("floor-held", f"{held} of {floor} cell(s) held"))

    if after.get("window_active"):
        checks.append(_unknown("window-closed", "the second observation was still inside the window"))
    elif int(after.get("floor", 0)) >= floor and floor > 0:
        checks.append(
            _failed(
                "floor-released",
                f"the floor stayed at {after.get('floor')} after the window closed",
            )
        )
    else:
        checks.append(_passed("floor-released", f"the floor fell to {after.get('floor')} after the window closed"))

    # The cost half. A window that holds capacity and bills nothing is a fleet
    # whose pool carries no price, and reporting that as a passing cost check is
    # how an unpriced fleet reads as a free one.
    idle_seconds = float(billing.get("idle_seconds", 0))
    idle_cost = float(billing.get("idle_cost", 0))
    unpriced = int(billing.get("unpriced_cells", 0))
    if idle_seconds <= 0:
        checks.append(_unknown("idle-cost-accrued", "no idle time was billed, so the warm period was not measured"))
    elif idle_cost > 0:
        # Cost was computed, so the failure this check exists for -- a fleet that
        # reads as free because nothing carries a price -- did not occur. Any
        # unpriced cells are named anyway: on a pool that has been rebuilt they
        # are the *released* incarnations, and they drag the total down without
        # explaining themselves.
        detail = f"{idle_seconds:.0f}s idle, {idle_cost:.4f} accrued"
        checks.append(
            _passed("idle-cost-accrued", detail if not unpriced else f"{detail} ({unpriced} released cell(s) unpriced)")
        )
    elif unpriced:
        checks.append(
            _unknown(
                "idle-cost-accrued",
                f"{unpriced} cell(s) carry no price and nothing accrued, "
                "so cost cannot distinguish a free pool from an unpriced one",
            )
        )
    else:
        checks.append(_failed("idle-cost-accrued", f"{idle_seconds:.0f}s idle on a priced pool accrued no cost"))
    return ProofResult("P6", tuple(checks))


# --------------------------------------------------------------------------- P4


def evaluate_secret_delivery(
    events: tp.Sequence[dict],
    workload: dict,
    logs: str,
    *,
    secret_value: str,
) -> ProofResult:
    """P4 -- the value is delivered, used, and recorded nowhere.

    The leak check is the reason this is code rather than a person grepping. A
    human checks the log, remembers to check the record, and does not think to
    check the audit payloads -- which is the one place a well-meaning "context"
    field would put it.
    """

    checks: list[Check] = []
    if not secret_value:
        return ProofResult(
            "P4",
            (_unknown("value-known", "no secret value was supplied, so absence cannot be checked"),),
        )

    issued = [event for event in events if event.get("action") == "secret.issued"]
    read = [event for event in events if event.get("action") == "secret.read"]
    if not issued and not read:
        return ProofResult(
            "P4",
            (_unknown("secret-was-used", "the ledger shows no secret event; the mount was never exercised"),),
        )

    for name, matched in (("one-issuance", issued), ("one-read", read)):
        if len(matched) == 1:
            checks.append(_passed(name, f"exactly one {matched[0]['action']}"))
        else:
            checks.append(_failed(name, f"expected exactly one; found {len(matched)}"))

    # A read with no issuance in the same attempt is the signal a leaked token
    # would produce, which is why both events exist rather than just the read.
    attempts = {event.get("payload", {}).get("attempt") for event in issued}
    orphans = [event for event in read if event.get("payload", {}).get("attempt") not in attempts]
    if orphans:
        checks.append(_failed("read-matches-an-issuance", f"{len(orphans)} read(s) with no issuance in that attempt"))
    else:
        checks.append(_passed("read-matches-an-issuance", "every read has an issuance in its attempt"))

    leaks = []
    if secret_value in logs:
        leaks.append("the workload log")
    if secret_value in repr(workload):
        leaks.append("the workload record")
    if any(secret_value in repr(event) for event in events):
        leaks.append("an audit payload")
    if leaks:
        checks.append(_failed("value-never-recorded", f"the secret value appears in {', '.join(leaks)}"))
    else:
        checks.append(_passed("value-never-recorded", "the value appears in no log, record or audit payload"))
    return ProofResult("P4", tuple(checks))


# --------------------------------------------------------------------------- P7


def evaluate_kubernetes_preconditions(observations: tp.Mapping[str, str | None]) -> ProofResult:
    """P7 -- the Kueue precondition, decided against a real API server.

    ``observations`` maps a case name to the failure code the driver produced,
    or ``None`` when it allowed the configuration.

    Both halves matter and they fail differently. Refusing everything is a
    driver nobody can use with Kueue at all; refusing nothing is the hazard the
    check exists for -- Kueue eviction destroys a pod with no checkpoint hook,
    bypassing Eyvan's whole preemption protocol, and the only trace is a
    training job that lost hours with no ledger entry explaining why.
    """

    expected: tuple[tuple[str, str | None, str], ...] = (
        ("no-queue", None, "a pool naming no queue has nothing to verify"),
        ("safe-queue", None, "preemption Never/Never cannot evict Eyvan's work"),
        ("unsafe-queue", "configuration", "preemption that could evict must be refused"),
        ("missing-queue", "configuration", "a queue that does not exist suspends every Job forever"),
    )
    checks: list[Check] = []
    for case, want, why in expected:
        if case not in observations:
            checks.append(_unknown(case, f"the run did not exercise this case ({why})"))
            continue
        got = observations[case]
        if got == want:
            checks.append(_passed(case, "allowed" if want is None else f"refused with {want!r}"))
        elif want is None:
            checks.append(_failed(case, f"refused with {got!r}; {why}"))
        else:
            checks.append(_failed(case, f"allowed; {why}"))
    return ProofResult("P7", tuple(checks))


# --------------------------------------------------------------------------- P3


def evaluate_in_job_submission(
    parent: dict,
    children: tp.Sequence[dict],
    *,
    expected_children: int,
    late_submit_status: int | None,
    ceiling_status: int | None,
) -> ProofResult:
    """P3 -- a running job submits children under its owner's identity.

    ``created_by`` is the criterion that matters most and the one most easily
    got wrong in a way nothing notices. A child attributed to
    ``workload:<id>`` competes as a fresh per-principal fair-share leaf and
    against a fresh quota scope, so a self-submitting launcher would escape the
    limits its owner is subject to simply by fanning out. The job tree would look
    correct the whole time.
    """

    checks: list[Check] = []
    if not children:
        return ProofResult(
            "P3",
            (_unknown("children-created", "the parent submitted no children; nothing about the capability was used"),),
        )
    if len(children) == expected_children:
        checks.append(_passed("children-created", f"{len(children)} child(ren) accepted"))
    else:
        checks.append(_failed("children-created", f"expected {expected_children}, found {len(children)}"))

    owner = parent.get("created_by")
    synthetic = [child["id"] for child in children if child.get("created_by") != owner]
    if synthetic:
        checks.append(
            _failed(
                "children-carry-the-owner",
                f"{len(synthetic)} child(ren) not attributed to {owner!r}: {', '.join(sorted(synthetic)[:3])}",
            )
        )
    else:
        checks.append(_passed("children-carry-the-owner", f"every child is attributed to {owner!r}"))

    orphans = [child["id"] for child in children if child.get("parent_workload_id") != parent.get("id")]
    scope = {(child.get("org_id"), child.get("project_id")) for child in children}
    if orphans:
        checks.append(_failed("children-are-parented", f"{len(orphans)} child(ren) name a different parent"))
    elif scope != {(parent.get("org_id"), parent.get("project_id"))}:
        checks.append(_failed("children-are-parented", f"children landed in {sorted(scope)}, not the parent's scope"))
    else:
        checks.append(_passed("children-are-parented", "every child is parented and in the parent's scope"))

    if late_submit_status is None:
        checks.append(_unknown("capability-expires-with-the-parent", "reuse after termination was not attempted"))
    elif late_submit_status == 409:
        checks.append(_passed("capability-expires-with-the-parent", "refused with 409 once the parent was terminal"))
    else:
        checks.append(
            _failed(
                "capability-expires-with-the-parent",
                f"a terminated parent still accepted a child (HTTP {late_submit_status})",
            )
        )

    if ceiling_status is None:
        checks.append(
            _unknown(
                "width-ceiling",
                f"the live-children ceiling was not reached; it takes {MAX_LIVE_CHILDREN} concurrent children",
            )
        )
    elif ceiling_status == 409:
        checks.append(_passed("width-ceiling", "the ceiling refused with 409"))
    else:
        checks.append(_failed("width-ceiling", f"the ceiling did not refuse (HTTP {ceiling_status})"))
    return ProofResult("P3", tuple(checks))


# --------------------------------------------------------------------------- the command


@click.group("proof")
def proof() -> None:
    """Decide a hardware proof's stated criterion against the live plane.

    Every criterion in ``docs/proofs.md`` is evaluated here rather than read off
    a screen at the end of a spend. Exits 0 on pass, 1 on failure, and 2 when the
    run did not exercise the behaviour -- because "nothing contradicted it" is
    not evidence, and it is the answer most likely to be mistaken for one.
    """


def _emit(result: ProofResult) -> None:
    JsonRenderer().emit(result.as_dict())
    raise SystemExit(EXIT_CODES[result.verdict])


def _gather(proof_name: str, fetch: tp.Callable[[], ProofResult]) -> ProofResult:
    """Run ``fetch``, turning an unreachable plane into INCONCLUSIVE.

    Found by pointing the finished command at a real plane: a mistyped workload
    id exited 1, which is the code for "this proof failed". A scheduled run would
    have reported the preemption behaviour as broken when nothing had been asked
    about it.

    That is defect shape (a) from the plan -- a handler turning "I could not find
    out" into a fact -- and it is worse here than anywhere else, because the fact
    it invents is a verdict about the thing the spend existed to establish.
    """

    try:
        return fetch()
    except click.ClickException as error:
        return ProofResult(proof_name, (_unknown("plane-reachable", f"the plane could not answer: {error.message}"),))
    except OSError as error:
        return ProofResult(proof_name, (_unknown("plane-reachable", f"the plane could not be reached: {error}"),))


@proof.command("catalog")
@click.option("--accelerator-type", required=True, help="The shape the fleet declares, e.g. v5p-16.")
@click.pass_obj
def catalog_proof(session: CliSession, accelerator_type: str) -> None:
    """P1 -- pool resources and host count come from the catalog."""

    from eyvan.capacity.hardware_catalog import shape_for

    shape = shape_for(accelerator_type)
    _emit(
        _gather(
            "P1",
            lambda: evaluate_catalog_authority(
                session.client.request("GET", "/api/v1/capacity-pools"),
                session.client.request("GET", "/api/v1/cells"),
                accelerator_type=accelerator_type,
                catalog_hosts=None if shape is None else shape.hosts,
            ),
        )
    )


@proof.command("preemption")
@click.argument("workload_id")
@click.pass_obj
def preemption_proof(session: CliSession, workload_id: str) -> None:
    """P5 -- a reclaimed Spot slice is charged to the preemption budget."""

    _emit(
        _gather(
            "P5",
            lambda: evaluate_spot_preemption(
                session.client.request("GET", f"/api/v1/workloads/{workload_id}"),
                session.client.request("GET", "/api/v1/cells"),
                session.client.request("GET", "/api/v1/capacity-pools"),
            ),
        )
    )


@proof.command("warm-window")
@click.option("--pool", "pool_id", required=True)
@click.option(
    "--inside", type=click.Path(exists=True, dir_okay=False), required=True, help="Warmth JSON from inside the window."
)
@click.pass_obj
def warm_window_proof(session: CliSession, pool_id: str, inside: str) -> None:
    """P6 -- a warm window held its floor and released it.

    ``--inside`` is a warmth report captured earlier, because one observation
    cannot distinguish a floor that is held from one that is never released.
    Capture it with ``eyvan capacity warmth --json`` during the window.
    """

    import json

    with open(inside, encoding="utf-8") as handle:
        captured = json.load(handle)
    _emit(
        _gather(
            "P6",
            lambda: evaluate_warm_window(
                captured,
                session.client.request("GET", "/api/v1/capacity/warmth"),
                # Scoped to this pool. The fleet-wide report counts every cell
                # the plane has ever held, and a long-lived fleet's historical
                # unpriced cells would make the cost half inconclusive on every
                # run regardless of how the pool under test is priced -- found
                # by running P6 against a plane with 33 of them.
                session.client.request("GET", "/api/v1/capacity/billing", params={"pool_id": pool_id}),
                pool_id=pool_id,
            ),
        )
    )


@proof.command("secret")
@click.argument("workload_id")
@click.option(
    "--value-file",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    help="File holding the secret's plaintext, for the absence check. A file rather than an argument: "
    "a value on the command line lands in shell history and in every process listing on the host.",
)
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.pass_obj
def secret_proof(
    session: CliSession, workload_id: str, value_file: str, org_id: str | None, project_id: str | None
) -> None:
    """P4 -- the secret is delivered, used, and recorded nowhere.

    The absence half is why this is code. A person checks the log, remembers the
    workload record, and does not think to check the audit payloads -- which is
    the one place a well-meaning "context" field would put it.
    """

    with open(value_file, encoding="utf-8") as handle:
        value = handle.read().strip()
    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    _emit(
        _gather(
            "P4",
            lambda: evaluate_secret_delivery(
                session.client.request(
                    "GET",
                    "/api/v1/events",
                    params={"org_id": scope.org_id, "project_id": scope.project_id, "limit": "1000"},
                ),
                session.client.request("GET", f"/api/v1/workloads/{workload_id}"),
                str(session.client.request("GET", f"/api/v1/workloads/{workload_id}/logs")),
                secret_value=value,
            ),
        )
    )


@proof.command("kubernetes")
@click.option("--api-server", required=True, help="https://<master-ip> of the cluster under test.")
@click.option("--token-file", type=click.Path(exists=True, dir_okay=False), required=True)
@click.option("--ca-file", type=click.Path(exists=True, dir_okay=False), required=True)
@click.option("--safe-queue", default="eyvan-safe", show_default=True)
@click.option("--unsafe-queue", default="eyvan-unsafe", show_default=True)
def kubernetes_proof(api_server: str, token_file: str, ca_file: str, safe_queue: str, unsafe_queue: str) -> None:
    """P7 -- drive the real Kubernetes transport against a real API server.

    Credentials are files rather than arguments for the same reason the secret
    proof takes a file: a bearer token on a command line lands in shell history
    and in every process listing on the host.

    Needs a cluster with Kueue installed and two ClusterQueues -- one with
    preemption Never/Never, one with preemption enabled. Everything this decides
    is driver-to-API-server, so no workload has to run and no worker has to
    reach the control plane.
    """

    from eyvan.capacity.drivers.base import CapacityProviderError
    from eyvan.capacity.drivers.kubernetes import KubernetesCapacityDriver
    from eyvan.capacity.providers.kubernetes.api import KubernetesApi

    driver = KubernetesCapacityDriver(api=KubernetesApi(base_url=api_server, token_path=token_file, ca_path=ca_file))

    def probe(queue: str | None) -> str | None:
        try:
            driver._require_safe_queue(queue)
        except CapacityProviderError as error:
            return error.code.value
        return None

    _emit(
        _gather(
            "P7",
            lambda: evaluate_kubernetes_preconditions(
                {
                    "no-queue": probe(None),
                    "safe-queue": probe(safe_queue),
                    "unsafe-queue": probe(unsafe_queue),
                    "missing-queue": probe("eyvan-proof-absent-queue"),
                }
            ),
        )
    )


@proof.command("children")
@click.argument("parent_id")
@click.option("--expected", type=int, default=3, show_default=True, help="How many children the job submitted.")
@click.option(
    "--late-submit-status",
    type=int,
    help="HTTP status from reusing the capability after the parent terminated. Omit if not attempted.",
)
@click.option("--ceiling-status", type=int, help="HTTP status from exceeding the live-children ceiling.")
@click.pass_obj
def children_proof(
    session: CliSession,
    parent_id: str,
    expected: int,
    late_submit_status: int | None,
    ceiling_status: int | None,
) -> None:
    """P3 -- a running job submits children under its owner's identity.

    The two status codes are passed in rather than provoked here: reusing a
    capability needs the token the job was given, which only the job ever holds,
    and reaching the width ceiling needs hundreds of concurrent children. Both
    are omitted rather than faked when a run did not exercise them, and both
    report inconclusive when omitted.
    """

    _emit(
        _gather(
            "P3",
            lambda: evaluate_in_job_submission(
                session.client.request("GET", f"/api/v1/workloads/{parent_id}"),
                session.client.request("GET", f"/api/v1/workloads/{parent_id}/children"),
                expected_children=expected,
                late_submit_status=late_submit_status,
                ceiling_status=ceiling_status,
            ),
        )
    )


def register_proof_commands(main: click.Group) -> None:
    """Attach the proof command group to the CLI root."""

    main.add_command(proof)


__all__ = (
    "EXIT_CODES",
    "Check",
    "ProofResult",
    "Verdict",
    "evaluate_catalog_authority",
    "evaluate_in_job_submission",
    "evaluate_kubernetes_preconditions",
    "evaluate_secret_delivery",
    "evaluate_spot_preemption",
    "evaluate_warm_window",
    "proof",
    "register_proof_commands",
)
