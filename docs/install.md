# Installing Eyvan

A remote deployment needs thirteen separately-configured things. That number is
the honest one, and stating it up front is more useful than a quickstart that
works locally and strands you at step nine.

Local development needs none of them: `eyvan up` on a laptop runs a loopback
control plane with a local repository and development auth. Everything below is
about the other case.

## The thirteen

Each is required, and each fails differently when missing. The failure mode is
listed because the error alone rarely names the cause.

| # | What | Why it cannot be defaulted | Missing looks like |
|---|------|---------------------------|--------------------|
| 1 | A PostgreSQL database, and `EYVAN_DATABASE_URL` | The local repository is refused on a non-loopback bind: it has no leader election, so two replicas would both schedule. | Startup refuses with a bind/backend mismatch. |
| 2 | The schema, applied by `eyvan-migrate` | `EYVAN_AUTO_CREATE_SCHEMA` bootstraps a database that can still be migrated afterwards, but the production path is the migration job. | `SchemaRevisionMismatch` at startup, naming the revision it expected. |
| 3 | An OIDC issuer, `EYVAN_OIDC_ISSUER` | Development auth is refused on a non-loopback bind. There is no built-in user store, deliberately. | Every request is 401. |
| 4 | An OIDC audience, `EYVAN_OIDC_AUDIENCE` | A token minted for another service must not authenticate here. | Tokens verify at the IdP and are rejected here. |
| 5 | An IdP that mints the `eyvan_grants` claim, **or** durable grants created with `eyvan grant add` | Roles have to come from somewhere. The claim was once the only source; durable grants are now the ordinary path. | Authenticated requests are 403 with no grants. |
| 6 | An artifact signing key of at least 32 bytes, `EYVAN_ARTIFACT_TOKEN_KEY`, **identical on every replica** | Capability tokens are HMACs. A replica with a different key rejects tokens its peers issued. | Intermittent 403 on artifact and secret transfers, proportional to replica count. |
| 7 | An artifact store, `EYVAN_ARTIFACT_STORE_URI` (`gs://…`) | A per-pod filesystem is not shared, so a result written by one replica is invisible to the next. | Results upload and cannot be read back. |
| 8 | A public URL, `EYVAN_PUBLIC_URL`, over HTTPS | Every capability URL handed to a job is built from it. A wrong value produces addresses jobs cannot reach. | Jobs fail fetching their own callable or secrets. |
| 9 | A trusted-callable project allowlist, `EYVAN_TRUSTED_CALLABLE_PROJECTS` | A pickled callable executes arbitrary code on your accelerators. Empty means nobody may submit one. | Callable submissions are refused; command workloads still work. |
| 10 | An agent shared secret | The control plane and every cell agent authenticate to each other with it. | Agents register and every mutation is refused. |
| 11 | A `platform.yaml` | Names the default cell-agent endpoint the fleet applies against. | Pool upserts fail with "no default cell agent is configured". |
| 12 | One agent per capacity boundary, each with its own `EYVAN_FLEET_STATE_URI` | Two agents sharing a fleet-state document fence each other out. | Cells flap between agents; fence conflicts in the audit trail. |
| 13 | A pinned wheel and a dependency wheelhouse for native workers | A worker VM with private egress cannot reach PyPI. | Workers bootstrap and never register. |

Plus: TLS termination, secrets that are not in this repository, and mTLS in
front of every agent.

## Order

The migration must complete before the control plane starts, and nothing
enforces that ordering for you today -- there is no init container and no Helm
hook. Run `deploy/migration-job.yaml` to completion, then apply
`deploy/control-plane.yaml`, then `deploy/cell-agent.yaml`.

## Verifying

```
eyvan doctor            # ten checks, exits non-zero on a real problem
eyvan capacity invariants   # the durability properties, checked live
eyvan status            # what the fleet believes about itself
```

`eyvan doctor` runs on your laptop and can only see what an operator can see.
Anything requiring cluster access -- the Kueue safety precondition, for one --
is checked by the driver at apply time instead, and surfaces as a refused pool.
