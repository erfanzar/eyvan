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
