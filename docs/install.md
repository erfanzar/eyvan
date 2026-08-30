# Installing Eyvan

A network-bound deployment needs fifteen separately configured things. Local
`eyvan up` needs none of them: it is a loopback development path, not a remote
installation recipe. This page covers both.

## Ask before you start

Three requirements are refused at server startup rather than defaulted, and
they are reported together — also on demand, before anything runs:

```bash
eyvan doctor --server-host 0.0.0.0     # the address eyvan-server will bind
```

The check reads this machine's `EYVAN_*` environment and reports every
requirement a server bound to that address would refuse without —
`EYVAN_REPOSITORY_BACKEND=postgres`, `EYVAN_METRICS_TOKEN`, and
`EYVAN_ARTIFACT_TOKEN_KEY` — each with the reason it exists. It exits
non-zero when any is unsatisfied. Without `--server-host` it answers for
`eyvan-server`'s default of `127.0.0.1`, which requires none of them.

## The laptop path

```bash
uv sync
eyvan start        # discovers: local plane reachable? heads in the project?
eyvan up           # loopback fleet (laptop-cpu), no cloud, no secrets
```

State lands under `~/.eyvan/`: `control-plane.json` (the loopback plane's
durable world), `autoscale/configs/` (generated zone fleets), and
`cell-agent-*` records. Run the plane and agent as separate processes when
you use accelerators:

```bash
eyvan-server --log-level info              # the plane
eyvan-agent --port 8090 --log-level info   # a cell agent
```

The agent requires `EYVAN_AGENT_SHARED_SECRET` (and, for native workers,
`EYVAN_NATIVE_WORKER_ROOT_SECRET`) — see [security](security.md). For a
profiled GCP setup both already sit in `~/.eyvan/gcp-native-runtime.json`.

## One command, for a GCP head

For a real deployment the plane runs on a head VM with PostgreSQL and an
object store; `eyvan head` / `eyvan runtime deploy` provision and print the
exact manifest. The non-negotiables:

- **PostgreSQL** for the durable model (`EYVAN_REPOSITORY_BACKEND=postgres`,
  `EYVAN_DATABASE_URL`).
- **An artifact store** — GCS or S3 — with `EYVAN_ARTIFACT_TOKEN_KEY` set.
- **Metrics auth** (`EYVAN_METRICS_TOKEN`) when bound off-loopback.
- **The runtime profile** (`~/.eyvan/gcp-native-runtime.json`): agent shared
  secret, native worker root secret, artifact root, worker package URIs.
  Treat it as secret material; it is written `0600`.

## Secret and non-secret configuration

Secrets (`EYVAN_AGENT_SHARED_SECRET`, `EYVAN_NATIVE_WORKER_ROOT_SECRET`,
`EYVAN_ARTIFACT_TOKEN_KEY`, `EYVAN_METRICS_TOKEN`, provider credentials)
come from your secret manager into the environment of the process that needs
them — plane, agent, or SDK — and never into fleet configs or the repo.
Everything else (timeouts, batch sizes, backend selection) is
[environment surface](env-surface.md) with safe defaults.

## After install

- `eyvan doctor` again, on the head, bound address included.
- `eyvan capacity fleets` — what exists; generate zone fleets with
  `eyvan up --region <r> --zone <z> --preemptible`.
- Smoke-test with a trivial callable before spending (the zone-race probe in
  [operate](operate.md) is a good template).
- Read [operate](operate.md) for the runbook and [proofs](proofs.md) for
  what is actually established about providers.

## Safe schema upgrades

The plane owns its schema (`EYVAN_AUTO_CREATE_SCHEMA` for development).
Upgrade the plane binary, let its migrations run against a backed-up
database, and keep the previous release's artifacts until the new one has
served real work — the [artifact-safety](artifact-safety.md) page defines
what may be cleaned and when.
