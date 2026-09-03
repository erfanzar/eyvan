# Part 14 — Operations

The runbook: `docs/operate.md`, `docs/troubleshoot.md`, `docs/proofs.md`. This
part compresses them into the working loop.

## Every day

```bash
eyvan capacity fleets            # what exists, what is drained
eyvan capacity list              # pools this project can submit into
eyvan cells                      # every cell and its stage
eyvan capacity quota --fits v5p-16:2   # zone-by-zone fit for a shape
eyvan capacity launch-latency    # per-pool handover seconds, plane-recorded
eyvan events                     # the audit feed — input for a watchdog
```

`quota --fits` flags zones whose usage is *unknown* — treat those as unproven, not
free: the europe-west4-b receipt shows a zone whose quota row exists but whose
queue admission is refused.

## Production design contracts

- **Durable state wins.** If the plane dies, restart it — everything reconciles
  from the store (demonstrated: 113 workloads recovered from
  `~/.eyvan/control-plane.json` on 2026-08-29).
- **Restart with credentials, or don't restart.** A bare `eyvan-server` comes up
  without the runtime profile's secrets and 401-loops against a still-running
  agent (cells flip `unavailable` in a recreate loop). Start it with the profile:
  ```bash
  export EYVAN_AGENT_SHARED_SECRET=$(python3 -c \
    'import json;print(json.load(open("$HOME/.eyvan/gcp-native-runtime.json"))["agent_shared_secret"])')
  eyvan-server --log-level info
  ```
- **Cells cost money; the plane reaps them.** Idle cells drain after the fleet's
  idle timeout. Tearing down for the night: drain fleets first
  (`eyvan down <config> -y`).

## Running a job on accelerators

- `blocked … the plane is retrying (attempt N)` = the provider refused an attempt
  and the plane is retrying within its budget — **watch, don't intervene**, until
  the attempt budget is spent.
- A queue-admission denial ("no permission … in location X") is **terminal** —
  pick another zone.
- Follow detached: `eyvan workload status|logs|cancel <id>`, and
  `eyvan workload explain <id>` for placement truth.

## Choosing a zone: race it, don't guess

The 2026-08-29 one-probe-per-zone race (v5p-16 × 1, spot, `jax.device_count()`):

| zone | verdict |
|---|---|
| `us-central1-a` | granted + ready in **409.7 s** (idle zone) |
| `us-east5-a` | no grant in ~14 min — `WAITING_FOR_RESOURCES`, congested |
| `europe-west4-b` | refused at queue admission — no v5p permission there |

Quota is permission to be *billed*; queue admission is permission to *ask* — a
zone needs both. Spot supply is a market: re-race before big spends, and prefer
shapes with receipts (spot v6e-8 boots "in about two minutes, repeatedly"; larger
spot slices have been accepted and reclaimed before a host finished booting).

## When something is queued and not running

1. `eyvan workload explain <id>` — placeable? provisioning? delivery state?
2. `hard_constraint: no cell satisfies all hard workload constraints` **with** a
   provisioning cell id → capacity is being built; wait.
3. Same, with **no** provisioning cell → no pool in the fleet matches the shape;
   check `capacity fleets` / `capacity list`.
4. Provider queue `WAITING_FOR_RESOURCES` for tens of minutes → the zone is
   congested. Cancel and race another zone rather than waiting blind.

## Troubleshooting quick table

| symptom | cause | action |
|---|---|---|
| `EyvanUnavailableError: control plane is unavailable` | plane process died | restart `eyvan-server`; state reconciles |
| 401 loops on `/api/v1/cells/<id>/…` after a plane restart | bare restart without the agent secret | restart with `EYVAN_AGENT_SHARED_SECRET` from the runtime profile |
| agent exits: shared secret required | agent started bare | pass the secret / use the profile; `--allow-unauthenticated` is dev-only |
| cells `unavailable` + recreate loop | plane↔agent auth mismatch or provider `PROVIDER_ERROR` | read both logs |
| `leased · attempt N` stuck; `explain` shows quarantined lease | plane crashed mid-dispatch | cancel and resubmit (known rough edge) |
| workload stuck in CANCELLING | cancel raced a dispatch/reap pass | give the controller a pass; `eyvan workload-force-finalize` for the stuck case |
| `GcsCommandError … HTTPError 412` on submit | concurrent identical submissions, pre-`e7abd59` behavior | update past that commit |
| `eyvan down/up` says config missing | given a fleet name, not a path | use `~/.eyvan/autoscale/configs/<fleet>.yaml` |

## Proof runs

Hardware claims go through `eyvan proof` — executable pass criteria with four exit
codes: **0** pass, **1** fail, **2** did not exercise the behaviour, **3**
refusal (the run never happened and must not wear a verdict). Verdict 2 is the one
that matters: a spot slice never reclaimed, a pool that never got a cell, an
observation outside the warm window — each produces a clean-looking run that
establishes nothing. Subcommands: `catalog`, `children`, `logs`, `preemption`,
`warm-window`, `secret`, `kubernetes`, `verify`.

The proof ledger (P0–P7) in `docs/proofs.md` records what is established and at
what cost — e.g. P0 (multislice callable end-to-end, passed 2026-08), P2
(multi-host logs with a rank lost mid-stream, passed on the seventh attempt), P5
(spot reclamation charged to the preemption budget, not the failure budget), P7
(Kubernetes against a real API server). **No historical run qualifies the current
candidate without an immutable exact-candidate receipt** — re-run against the
current candidate when a spend decision depends on it.

## Watchdogging

Alert on: plane-down (health surface `/healthz` + controller status), 401 loops
between plane and agent, and cells sitting in `unavailable` or `draining` past a
threshold. Each of these is distinguishable in `eyvan events` before it costs a
night.
