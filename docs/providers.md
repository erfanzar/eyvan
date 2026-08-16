# Writing an Eyvan capacity provider

Eyvan ships six capacity backends (`gcp`, `aws-ec2`, `azure-vm`, `kubernetes`,
`manual`, `onprem-ssh`) and a development one (`local`). This document is for the next one,
which somebody else writes: CoreWeave, Slurm, a lab scheduler, a hardware
vendor's API. You publish a distribution, Eyvan loads it through an entry point,
and your provider serves a `capacity_backend` name the same way the in-tree ones
do.

`aws-ec2` is worth reading as the worked example, because it was written after
this contract existed rather than before: it is one driver, one capability entry
and a transport, with no edit to the scheduler, the planner or the durable model.
It also shows what the conformance suite is for -- two real ownership bugs in it
were found by the suite and not by its own tests.

Nothing here is a private API. If you need to reach into `eyvan.server` or copy a
driver to make your provider work, that is a bug in this contract -- open an
issue rather than forking.

Nothing that *decides* -- the scheduler, the capacity planner, the durable model
-- may name a provider, and `tests/test_provider_boundary.py` enforces it. If
your provider needs a decision made differently, that is a capability to
declare rather than a branch to add: `BackendCapabilities` in
`capacity/drivers/base.py` is how a backend says it can allocate one cell
spanning several machines. Declaring it is what makes multislice-shaped work
reachable on your cloud; without it every cell you build is one machine, which
is the safe reading of a driver that has not said otherwise.

---

## 1. The shape

An entry point in the group `eyvan.capacity_providers` resolves to a **plugin
object** with exactly three members. A module is an object, so the smallest legal
plugin is a module with two constants and a function, importing nothing from
Eyvan:

```python
# eyvan_acme_provider/__init__.py
abi_version = "1.0"
backend_names = ("acme",)


def create(name, *, state, settings):
    """Build the driver for one of ``backend_names``."""

    from eyvan_acme_provider.driver import AcmeCapacityDriver

    return AcmeCapacityDriver(state, region=settings.region)
```

```toml
# pyproject.toml
[project.entry-points."eyvan.capacity_providers"]
acme = "eyvan_acme_provider"
```

An instance works too (`acme = "eyvan_acme_provider:plugin"`), which is what you
want if one distribution ships several providers.

`abi_version` is a **string**, deliberately, so you can declare which contract
you were built against without importing Eyvan. That matters precisely in the
case the version check exists for: the build you are loaded into may not be the
one you were written against.

`create` returns an object satisfying `eyvan.capacity.drivers.base.CapacityDriver`
-- six methods: `capabilities`, `observe`, `apply_capacity`, `reconcile`,
`drain`, `release`. It is a `typing.Protocol`; do not subclass anything.

## 2. Turning it on

Discovery is **opt-in**, and off by default:

```bash
export EYVAN_CAPACITY_PROVIDER_PLUGINS=acme          # trust one entry point
export EYVAN_CAPACITY_PROVIDER_PLUGINS=acme,zeta     # trust several
export EYVAN_CAPACITY_PROVIDER_PLUGINS='*'           # trust everything installed
```

With the variable unset, Eyvan does not scan, does not import, and behaves
exactly as it did before this ABI existed. A control plane whose set of capacity
backends can be extended by a transitive dependency is a supply-chain problem,
not a feature; `*` trades that guarantee away and is fine for an image you build
yourself.

A plugin can never take a name a built-in already serves, and two plugins
claiming one name are **both** refused -- resolving that by installation order
would mean the same set of packages produces different fleets on different hosts.

## 3. The version contract

Eyvan implements one ABI version at a time. Today it is **1.0**
(`CAPACITY_PROVIDER_ABI_VERSION`).

| Your `abi_version` | Host is 1.0 | Why |
|---|---|---|
| `1.0` | loads | same contract |
| `1.0` on a 1.4 host | loads | within a major version Eyvan only *adds* guarantees |
| `1.1` on a 1.0 host | **refused** | you require a guarantee this build does not make |
| `2.0`, `0.9` | **refused** | a major version withdrew or changed a guarantee |
| `"1"`, `"one.zero"`, `1.0` (float) | **refused** | an unparseable version is not a guessable one |

*Refused* means refused: the backend does not exist, the reason is logged at
`ERROR`, it is in `ProviderPluginDiscovery.failures`, and it is in the text of
the `ValueError` an operator gets when they select that backend. Eyvan never
adapts to a version it does not implement, and never degrades a broken plugin
into "no such backend" -- those two situations send an operator to different
files.

**MINOR** goes up when Eyvan adds a guarantee (a new optional protocol, a new
metadata key it promises to preserve). **MAJOR** goes up when a guarantee below
is withdrawn or a signature changes.

## 4. What Eyvan guarantees to you

1. **One driver, one process.** `create` is called once per agent process, with
   the fleet-state store and settings that agent owns. Your driver is used from
   that process only.
2. **A durable, fenced record store.** The `state` argument is a
   `FleetStateStore`: a compare-and-swap JSON document with a mutation lease, a
   per-cell record (`generation`, `fencing_token`, `provider_generation`,
   `idempotency_keys`, `metadata`), and tombstones that outlive a released cell.
   You do not need a database of your own, and you must not add one to Eyvan's.
3. **Every mutation is fenced.** `apply_capacity`, `reconcile`, `drain` and
   `release` receive a `MutationContext` with an `idempotency_key`, a
   `generation`, and a `fencing_token`. A retry of one command reuses all three.
4. **Retry semantics you control.** Raise `CapacityProviderError` with
   `retryable=True` and Eyvan retries with backoff; with `retryable=False` it
   stops and surfaces your diagnostic. Nothing else is inferred from the text.
5. **An exception is never read as "nothing happened".** A failed call leaves the
   cell in a state Eyvan will reconcile again, not a state it assumes.
6. **A closed observation vocabulary.** `CapacityStatus`, `CapacityProviderFailureCode`
   and `ProvisionOutcome` are stable within a major version.
7. **Your diagnostics reach operators.** `CellObservation.detail` and
   `CapacityProviderError.diagnostic` appear in `eyvan workload explain` and the
   audit log. Do not put secrets in them.

## 5. What you guarantee back

These are not style preferences. Each one is a property `capacity/conformance.py`
tests, and each has a failure mode that has happened in a real fleet.

1. **A stale fence cannot mutate.** A `MutationContext` whose generation is older
   than the cell's, or whose token does not own the current generation, must
   raise `StaleMutationError` and change **nothing** -- not the durable record,
   not your substrate. Raising after mutating is the same bug with a better
   error message.
2. **Create is idempotent.** The same `MutationContext` replayed must not create
   a second incarnation and must not bump `provider_generation`. The outbox
   retries; on backends that derive a worker token from the incarnation, a bump
   also locks out every worker that was already running.
3. **Leaks are detected, never adopted.** Stamp your provider-side resource with
   something you can compare against the durable fence -- GCP uses a
   `management_uid`, Kubernetes a generation label plus a fencing-token
   fingerprint, `onprem-ssh` a marker file. An observed resource that disagrees
   is a *previous incarnation*: report `NEEDS_RECREATE`, publish no endpoints,
   and let the controller replace it.
4. **Inconclusive is not a conclusion.** If your control API could not be
   reached, you learned nothing. Report `CapacityStatus.UNOBSERVED` with
   `failure_code=PROVIDER_UNAVAILABLE`, or raise
   `CapacityProviderError(code=PROVIDER_UNAVAILABLE, retryable=True)`. Never
   `ABSENT` (it deletes live capacity), never `HEALTHY` (it schedules onto
   nothing), never a non-retryable error (it discards a command that should be
   retried).
5. **Conclusive failures say so.** A missing credential, a nonexistent project, a
   rejected manifest: `retryable=False` with `CONFIGURATION` or `QUOTA_EXCEEDED`.
   A permanent failure reported as retryable holds its lease and keeps the cell
   undeletable forever.
6. **Release frees the substrate, and can be replayed.** After `release` nothing
   of yours may still be running or billing. A replayed `release` -- durable
   record already gone -- must complete, not raise: use the tombstone metadata to
   carry whatever identity the cleanup needs.
7. **Routable capabilities.** `capacity_classes` must come from
   `{spot, on-demand, reserved, guaranteed}` and `executor` must be a canonical
   execution backend name. An invented value does not fail loudly; it just never
   matches a pool.
8. **No external I/O inside someone else's transaction.** Your driver runs in the
   cell agent, so this mostly means: do not hold the fleet mutation lease longer
   than the provider calls that need it, and revalidate ownership around each
   one (`FleetStateStore.require_lease`).

## 6. The conformance suite

```python
# tests/test_conformance.py in your distribution
from eyvan.capacity.conformance import CapacityProviderConformanceTests, ProviderConformanceHarness


class AcmeHarness(ProviderConformanceHarness):
    def new_driver(self): ...
    def desired_capacity(self, cell_id): ...
    def provider_incarnations(self, driver, cell_id): ...
    def leak_previous_incarnation(self, driver, cell_id): ...
    def provider_unreachable(self, driver): ...
    def provider_rejects_permanently(self, driver): ...


class TestAcmeConformance(CapacityProviderConformanceTests):
    def harness(self):
        return AcmeHarness()
```

Twelve checks run, one per property, so a failure names the property rather than
a line number. `run_conformance(harness)` gives the same checks as a
`ConformanceReport` if you would rather assert on the report.

The six hooks are the facts only you can produce. `provider_incarnations` is the
leak counter -- count VMs, Jobs, claims, whatever would keep costing money if
Eyvan forgot about it, counted from the substrate and never from the record.
`leak_previous_incarnation` must make the substrate hold something the durable
fence disowns. `provider_unreachable` and `provider_rejects_permanently` are the
two failure kinds you must be able to tell apart.

Run them against a fake of your provider's API, not the real one: the properties
are about your driver's logic, and a suite that needs an account is a suite
nobody runs. `libs/eyvan/tests/test_provider_abi_onprem.py` is the worked
example -- an in-memory rack, ~90 lines, that the shipped `onprem-ssh` driver
passes.

## 7. The in-tree example: `onprem-ssh`

`capacity/drivers/onprem_ssh.py` is the reference implementation, and it is
deliberately the least cloud-like backend possible: hosts an operator already
owns, reached over SSH, with no account, no API, and no billing.

It differs from `manual` in one way that matters here: it *claims* each host by
writing a fenced incarnation marker, so a host that was reused, or a worker left
from a previous incarnation, is detectable rather than indistinguishable. That
claim is what makes leak detection meaningful on a substrate with no control
plane of its own.

```yaml
node_types:
  rack-a:
    capacity_backend: onprem-ssh
    provider_options:
      hosts: ["eyvan@10.0.0.11", "eyvan@10.0.0.12"]
      state_dir: /var/lib/eyvan/native-worker
      worker_port: 8100
      start_command: ["systemctl", "--user", "start", "eyvan-native-worker"]
      stop_command: ["systemctl", "--user", "stop", "eyvan-native-worker"]
```

Eyvan owns the claim, the fence, the marker, and the endpoint list. You own how
the worker process starts: leave the commands out and it adopts a worker your
own systemd unit already runs. Spot is not offered -- hardware you own is not
preemptible by a provider, and saying otherwise would let the scheduler make an
interruption promise with no meaning.

## 8. `provider_options` on the in-tree GCP backends

`provider_options` is a per-node-type mapping, validated when the fleet config
is loaded rather than when a VM is created, so a typo fails when
the configuration is read and not forty seconds into a provision. An unknown
key is refused with the accepted keys listed in the message.

### `bootstrap_egress` (required for `executor: eyvan-native`)

How a native worker reaches Eyvan's artifact bucket, its control plane, and
PyPI during bootstrap. There is no default, because every value needs
infrastructure the operator has to have built already, and guessing one
produces a VM that provisions successfully and then never registers -- the
most expensive failure shape this backend has.

| value | what the VM uses | what must already exist |
|---|---|---|
| `private-google-access` | Google APIs over internal addresses only | Private Google Access enabled on the subnet. Eyvan additionally stages a wheelhouse, because such a VM cannot reach PyPI. |
| `cloud-nat` | outbound internet through a NAT gateway, no external address | a Cloud NAT gateway on the subnet's region |
| `public-internet` | an external IP on the instance | nothing; refused when the node type is also internal-only (`internal_ips` for TPU, `internal_ip_only` for Compute), since that combination cannot egress at all |

`_` is accepted in place of `-` and the value is lowercased, so
`PRIVATE_GOOGLE_ACCESS` is read as `private-google-access`. Anything else is
refused naming all three.

```yaml
node_types:
  cpu:
    capacity_backend: gcp-compute
    executor: eyvan-native
    provider_options:
      machine_type: n2-standard-8
      bootstrap_egress: private-google-access
```

`eyvan autoscale init` asks the same question interactively and writes the
answer into the generated fleet.

## 9. Known limits

* **A plugin's backend name is `other` in P8 soak receipts.** `SAFE_CAPACITY_BACKENDS`
  in `soak_contract.py` is a closed vocabulary on purpose -- receipts must not
  carry unbounded strings -- so an out-of-tree backend is recorded as `other`
  rather than by name. Everything else about the cell is recorded normally.
* **The hardware catalog is in-tree.** Shapes and prices for your accelerators
  live in `capacity/data/`; a provider cannot add catalog entries yet. Until it
  can, declare shapes that already exist or contribute the catalog entry.
* **Kueue-style capacity admission is Kubernetes-specific.** There is no generic
  "ask the substrate what is free" hook (that is W3, and it is not yet an ABI
  surface).
* **`onprem-ssh`'s remote script is tested through `/bin/sh`, not through `ssh`.**
  The driver logic and the marker protocol are covered; a real SSH transport
  against a real host is not, and cannot honestly be claimed to be.
