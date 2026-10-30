# Part 4 — The CLI

`eyvan` is a Click app composed in `eyvan/sdk/cli.py`. Global options: `--url`
(`EYVAN_URL`), `--token` (`EYVAN_TOKEN`), `--user` (`EYVAN_DEV_USER`), `--org`,
`--project`, `--role` (default `operator`). Against a loopback plane with no
explicit identity it upgrades to `fleet_admin` and prints one dim line saying so —
it is your own development plane, and it tells you who you are acting as.

The endpoint resolution is: flag → saved context (`eyvan use`) → this node's own
plane. Run `eyvan <command> --help` for authoritative options; this part maps the
territory.

## Connection and orientation

| command | what it does |
|---|---|
| `eyvan start` | discover what exists: a local plane, or heads in your project |
| `eyvan connect` | connect to / inspect a control plane |
| `eyvan use` / `eyvan context` | save / show the active context (URL, org, project, fleet) |
| `eyvan doctor` | what is missing, named together; `--server-host 0.0.0.0` previews network-bind refusals |

## Fleets and capacity

| command | what it does |
|---|---|
| `eyvan up [CONFIG]` | apply a fleet config (`examples/laptop.yaml`, a generated zone fleet, `--region/--zone --preemptible`); `--select <fleet>` re-enables one; on a loopback URL it also starts and supervises the plane |
| `eyvan down <config-path> -y` | drain every cell the fleet manages — takes the **config path**, not a fleet name |
| `eyvan status` | fleet status |
| `eyvan config validate <file>` | validate a config without applying it |
| `eyvan capacity fleets` | what exists, what is drained |
| `eyvan capacity list` | pools this project can submit into |
| `eyvan capacity quota --fits v5p-16:2` | zone-by-zone fit for a shape (quota **and** admission **and** supply are three different gates) |
| `eyvan capacity launch-latency` | per-pool handover seconds the plane itself recorded |
| `eyvan capacity warmth` / `queue-trend` / `billing` / `usage` / `carbon` / `builds` / `orphans` / `invariants` | the capacity reports |
| `eyvan capacity pool list/get`, `eyvan capacity cell get`, `eyvan capacity ingress list/release` | pool/cell/ingress detail |
| `eyvan gcp browse [--project P] [--json]` | list existing TPU VMs and unreachable locations; a terminal can pick and confirm attachment, while JSON/non-TTY use only lists |
| `eyvan gcp connect --tpu NAME [--project P] [--zone Z] --dry-run` | preview attaching an existing TPU without creating, stopping or deleting it |
| `eyvan gcp connect --ssh user@host --cpus N --memory-gib N --dry-run` | preview a reserved CPU fleet backed by an already-prepared native-worker host |

### Attach existing hardware

```bash
eyvan gcp browse --project my-project --json
eyvan gcp connect --tpu my-tpu --project my-project --zone us-east5-a --dry-run
eyvan gcp connect --tpu my-tpu --project my-project --zone us-east5-a --yes
# Once ready, use the printed fleet name in init(fleet="...").
```

TPUs must be READY and have a shipped catalog shape and an address on every
host. Their actual Spot/on-demand/reserved class is retained. A missing zone is
resolved from project inventory; ambiguous names or unreachable locations require
an explicit `--zone`. `--endpoint-mode public` is the default: the CLI reuses the
profile-owned laptop IPv4 `/32` ingress rule and adds its tag. `internal` skips
that ingress and uses internal addresses for workers and SSH/SCP.

Execution builds the local worker wheel, copies it to every host, delivers a
per-incarnation token over SSH stdin into a root-owned `0600` file, and reuses the
native-worker Python/venv/systemd bootstrap. It requires the local native GCP
runtime profile (`eyvan runtime init`) and passwordless sudo on the TPU hosts.
The wheel uses `scp --worker=all`; stdin-bearing SSH calls select each worker
separately so parallel SSH children cannot consume one another's private input.
No IAM or Secret Manager changes are made.

After every worker passes health checks, the command writes
`~/.eyvan/attached/<fleet>.yaml`, applies it through the same service as `eyvan up`,
activates its context and waits for readiness. `--fleet NAME` overrides the stable
name; `--timeout SECONDS` bounds worker-health and fleet-readiness waits.
`--dry-run` prints every step and the YAML without building, writing, SSH or cloud
mutations. Mutating non-TTY use requires `--yes`; `--json` keeps stdout to one
plan/result document. A failure leaves installed workers/ingress intact, names its
step, and never attempts to destroy hardware as rollback.

The `--ssh` route is deliberately narrower: **one already-prepared CPU native
worker**, an onprem-ssh agent with SSH access and matching worker authentication,
and an absolute worker state directory writable by that SSH user are prerequisites.
It does not install software, distribute GCP credentials, or create firewall rules.
The CLI checks worker liveness; agent authentication and the fenced claim are
established during fleet readiness; with `EYVAN_NATIVE_WORKER_ROOT_SECRET` set the agent
pushes the per-cell worker token over SSH before each start (unit and pool setup in
`docs/reference/capacity-and-providers.md`). Declare real resources using `--cpus` and
`--memory-gib`; capacity defaults to `reserved` (`--capacity-class on_demand` is
also supported, never Spot), and `--region` defaults to `onprem`.

These CLI paths have offline validation only; successful attachment, multi-host
SSH behavior and real TPU execution have not been hardware-verified.

## Autoscale (generated zone fleets)

```bash
eyvan autoscale init                      # once: edit ~/.eyvan/autoscale/defaults.yaml interactively
eyvan autoscale generate --zone us-central1-a   # cache configs/<prefix>-<zone>.yaml from live quota
eyvan up --select research-us-central1-a --preemptible
```

Generation discovers GCP TPU quota, resolves typed profiles, filters documented
exclusions and cached queue-admission denials, and compiles one validated fleet per
eligible zone. Generated files are derived — never hand-edit them; regenerate after
changing defaults.

## Heads (a GCP control-plane VM)

```bash
eyvan head up fleet.yaml --dry-run   # print every action and resource name; issues nothing
eyvan head up fleet.yaml --yes       # create/adopt the VM, generate credentials, run migrations, systemd
eyvan head status fleet.yaml
eyvan head rollback fleet.yaml
eyvan head down fleet.yaml --yes     # requires --yes; never infers confirmation
```

`head down` demands `--yes` because the head keeps the control plane's durable
state on its own disk.

## Workloads

Top-level convenience: `eyvan run` (run a callable/command workload),
`eyvan workload exec` (exec beside a running workload).

The `eyvan workload …` family:

- submission: `submit`, `submit-batch`, `submit-dag`, `estimate`
- inspection: `list`, `get`, `status`, `timeline`, `cost`, `explain`,
  `process-status`, `logs`, `log-page`, `children`, `endpoints`
- lifecycle: `cancel`, `checkpoint`, `retry`, `kick`

`explain` is the diagnostic workhorse: it prints whether the workload is placeable,
candidate cells, provisioning cell ids, and delivery state/error.

## Operations

| command | what it does |
|---|---|
| `eyvan cells` | every cell and its stage |
| `eyvan cell-drain` / `cell-delete` | drain/delete a cell by id |
| `eyvan leases` | active leases |
| `eyvan events` | the audit event feed — input for a watchdog |
| `eyvan preemption-force-finalize` / `workload-force-finalize` | the stuck-case escape hatches (a cancel that raced a dispatch pass) |

## Governance, tenancy, federation

- `eyvan quota list/get/set/delete`, `eyvan budget list/get/status/set/roll`,
  `eyvan policy list/get/set`, `eyvan webhook list/add/remove`,
  `eyvan system-reconcile` — policy surfaces
- `eyvan org list/get/create`, `eyvan project list/create`,
  `eyvan grant list/add/revoke` — tenancy and role grants
- `eyvan federation peer list/add`, `eyvan federation grant issue/revoke` (federation budget grants),
  `eyvan federation quota-grant list/issue/revoke` — plane-to-plane federation

## Proof, soak, services, query, resources

- `eyvan proof catalog | children | logs | preemption | warm-window | secret |
  kubernetes | verify` — executable pass criteria; exit 0 pass, 1 fail,
  2 did-not-exercise, 3 refusal (see Part 14)
- `eyvan soak` / `eyvan soak-monitor` — longitudinal fleet checks
- `eyvan service health` / `service decisions` — managed-service replica health
  and explained decisions
- `eyvan query` — read-only query console (SQL-backed repositories only);
  `eyvan process stacks/allocations` — process diagnostics
- `eyvan list <kind>` / `eyvan get <kind> <id>` — a generic surface over `workloads`, `cells`,
  `pools`, `leases`, `events`, `fleets`, `quotas`, `budgets`

## Artifacts

`eyvan artifacts list | usage | inventory | collectable | references | collect |
hold | release-hold` — the managed artifact ledger (Part 11). A listing failure is
never rendered as an empty scope; `collectable` output is report-only and is not a
deletion claim.

## Runtime and dashboard

- `eyvan runtime init` — stage the local native runtime profile
  (`~/.eyvan/gcp-native-runtime.json`: agent shared secret, native worker root
  secret, artifact root, wheel URIs)
- `eyvan runtime restart` / `runtime deploy` — rebuild and redeploy the local
  plane and agent from current source
- `eyvan dashboard` — open/serve the console; `eyvan dashboard-dev` — run the Vite
  dev server against the plane (Part 13)
