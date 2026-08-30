# Troubleshooting

Symptom → cause → action. The plane's own diagnostics name most of these;
this page is the cross-reference. For background read
[eyvan-explained](eyvan-explained.md).

## A workload sits PENDING

| diagnostic | meaning | action |
|---|---|---|
| `blocked … the plane is retrying (attempt N)` | provider refused a capacity attempt; plane retries within its budget | wait; each refusal renders with the provider's own words |
| `hard_constraint: no cell satisfies all hard workload constraints` **with** a provisioning cell id in `eyvan workload explain` | capacity is being built for this workload | wait — the cell id in `message` is the live attempt |
| same, with **no** provisioning cell | no pool in the fleet matches the shape (wrong fleet, drained pools, or a type the fleet does not serve) | `eyvan capacity fleets` / `capacity list`; submit into a fleet whose pools cover your type |
| `WAITING_FOR_RESOURCES` for tens of minutes | zone congested — the queued resource exists but nothing grants it | cancel and race another zone (`capacity launch-latency` picks the fast one) |
| queue admission denied ("no permission … in location X") | the project may not submit that accelerator in that zone | terminal; pick another zone. A quota row with *usage unknown* often means exactly this |

## Capacity and cells

| symptom | cause | action |
|---|---|---|
| `cells` shows `unavailable` and recreate loops | usually the plane cannot reach its agent (401 loop) or the provider resource is in `PROVIDER_ERROR` | read both logs; restart the plane **with** `EYVAN_AGENT_SHARED_SECRET` from the runtime profile |
| cell `draining` for a long time | provider deletion pending or wedged | `eyvan cell-drain` / check the provider console; `eyvan cells` shows the stage |
| cells exist that nothing claims | leftovers after a crash | the controller reaps on reconcile passes; a restarted plane reconciles from durable state first |
| capacity billing and no tenant is charged | a leased cell whose workload is gone | cancel the workload, drain the fleet config (`eyvan down <config> -y`) |

## The plane and the agent

| symptom | cause | action |
|---|---|---|
| `EyvanUnavailableError: control plane is unavailable at http://…` | the plane process died (the SDK's wait gives up after a bounded grace) | restart `eyvan-server`; durable state reconciles. Keep it running while jobs are active |
| 401 loops on `/api/v1/cells/<id>/reconcile` right after a plane restart | the bare plane started without the agent shared secret | restart the plane with `EYVAN_AGENT_SHARED_SECRET` (and matching worker secrets) from `~/.eyvan/gcp-native-runtime.json` — receipt: 2026-08-29, fixed in one restart |
| agent exits: `EYVAN_AGENT_SHARED_SECRET is required` | agent started bare | pass the secret, or use the profile; `--allow-unauthenticated` is local development only |
| agent exits: `eyvan-native execution requires EYVAN_NATIVE_WORKER_ROOT_SECRET` | native runtime enabled without the root secret | source `native_worker_root_secret` from the runtime profile |

## Workloads wedged

| symptom | cause | action |
|---|---|---|
| `leased · attempt N` for a long time; `explain` shows `delivery_error: cell … has not recovered the quarantined lease` | the plane crashed mid-dispatch and the lease quarantined; dispatch retries against a draining cell | cancel and resubmit (`eyvan workload cancel`); known rough edge, recorded in [proofs](proofs.md) |
| workload will not leave CANCELLING | cancel raced a dispatch/reap pass | give the controller a pass (it retries on an interval); `eyvan workload-force-finalize` exists for the stuck case |
| `EyvanJobError … finished in state failed` with a provider sentence | the provider's refusal traveled with the completion | the sentence is the answer — act on it (zone, shape, quota, permission) |

## Submission itself fails

| symptom | cause | action |
|---|---|---|
| `GcsCommandError: 'gcloud storage cp --no-clobber …' failed … HTTPError 412` | two submitters raced the same digest and the loser treated "object exists" as failure | fixed in `e7abd59` (2026-08-29): the loser verifies the winner's digest and continues — update past that commit |
| `GcsCommandError` with credentials/quota words | the CLI's own diagnostic traveled with the error | read it; it names the fault (see [artifact-safety](artifact-safety.md)) |
| `invalid fleet configuration <name>: No such file or directory` | `eyvan down/up` given a fleet name where a config path is required | use `~/.eyvan/autoscale/configs/<fleet>.yaml` |

## Alerting before an operator has to notice

The health surface (`/healthz`, controller status in the response) and the
events feed (`eyvan events`) are the inputs for a watchdog: alert on
plane-down, on 401 loops between plane and agent, and on cells that sit in
`unavailable` or `draining` past a threshold. Each symptom above is
distinguishable in events before it costs a night.
