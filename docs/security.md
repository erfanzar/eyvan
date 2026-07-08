# Security model

## Pending production boundary decisions

These accepted design records are implementation-pending and must not be read as
current security claims:

- [artifact scope and generation-fenced deletion](design/0001-scoped-artifact-lifecycle.md)
  aligns the metadata key with the tenant-scoped byte store;
- [log rank and cursor compatibility](design/0002-global-log-ranks-and-cursors.md)
  treats an opaque cursor as untrusted state, not as a credential;
- [service health and metrics ownership](design/0003-service-health-and-metrics.md)
  keeps requests, responses, and raw application telemetry out of Eyvan; and
- [federation trust and witnessed ownership](design/0004-federation-consistency.md)
  requires external PKI and an external quorum-backed lease/CAS service rather
  than an Eyvan-built CA or consensus protocol.

## The authority boundary

The control plane holds the credentials that matter: a database URL, an artifact
signing key, an agent shared secret, and whatever cloud identity provisions
capacity. Everything else in the system derives its authority from that, and the
design question throughout is how little authority each derived thing can be
given.

## Who a request is

An identity comes from an OIDC token, verified against a configured issuer and
audience. Roles come from durable `RoleGrant` records (`eyvan grant add`) or
from the `eyvan_grants` claim, and are scoped to an organization or a project.
`Authorizer` maps (role, permission, scope) to a decision at roughly
seventy-five call sites, and `test_two_tenants_on_one_plane` asserts that every
route in the application either enforces that scope or states why it does not.

Development auth exists and is **refused on a non-loopback bind**. A control
plane reachable from anywhere cannot be one that trusts a header.

## Capability tokens

Three capabilities exist, each an HMAC over claims with a distinct version
prefix *inside the signed material*. A token minted for one cannot verify as
another even if a claim check were wrong.

| capability | prefix | expires | revoked by |
|---|---|---|---|
| artifact read/write | `v1` | yes, `artifact_token_ttl_s` | the result URI moving on the spec |
| child submission | `eyvw1` | no | the workload's generation advancing |
| secret read | `eyvs1` | yes, `secret_token_ttl_s` | its exact dispatch reservation, attempt, or generation changing |

The submit capability does not expire deliberately: revocation is a state check
that is strictly stronger than a TTL, and an expiring one would inherit seven
days and break any job that runs longer than a week -- precisely the kind that
fans out.

The secret capability does expire, for the opposite reason. It hands back a
*value*, and once read no later revocation can un-read it.

## Secrets

A spec carries a *reference*, never a value. `PolicyRecord.allowed_secret_references`
is a per-project scheme/authority/path-segment root allow-list checked at
submission and redemption, and it is the security-critical piece: without it
any submitter can name any secret the control plane's own identity can read,
which turns "may submit jobs" into "may read every secret in the project". The
comparison is not a raw string prefix: `gcp-secret://p/training` does not admit
`gcp-secret://p/training-prod`, and `file:///run/secrets` admits only canonical
children of that directory. File references have one accepted spelling,
`file:///absolute/path`; equivalent `file:/` and extra-slash spellings are
refused at both policy and value validation. File reads repeat the matched root
in a dirfd component walk with `O_NOFOLLOW`; dot segments, encoded paths, and
symlinks are refused by the same operation that opens the file.

A file allow-list root is therefore a **trusted, operator-owned directory**. It
must not be writable by a workload or any other untrusted principal. POSIX
`O_NOFOLLOW` prevents symlink traversal but cannot distinguish a hard link from
the file's original directory entry; an attacker allowed to create hard links
inside a root could expose another file on the same filesystem. Eyvan relies on
the root ownership/write-permission boundary instead of an inode owner or link
count rule, because projected secret volumes and ordinary operator-managed
secret files do not share one portable ownership/link-count contract.

Its empty value means **none**, inverting the convention its neighbours in the
same command follow. Every other allow-list there means "no restriction" when
empty.

Every fetch is audited twice over -- one issuance event per fenced dispatch
reservation naming the mounts granted, and one event per redemption, granted or
refused, written in the same transaction as the authorization read. The
reservation and issuance audit commit before the cell-agent RPC; that transaction
is released before the agent synchronously redeems the capability. The response
then activates the lease only after a fresh transaction revalidates workload,
lease, generation, and outbox fences. A retry replaces the reservation even when
attempt and generation stay unchanged, immediately killing capabilities from the
abandoned RPC. A `secret.read` with no matching `secret.issued` fence is the
signal that a capability leaked.

A durable execution that declares a secret mount intentionally exposes no
workload logs, structured log pages, archived logs, exec output, or process
error detail. Exact streaming redaction cannot be reconstructed after a worker
restart or env-file cleanup without retaining the plaintext value solely for
redaction, which Eyvan refuses to do. The durable record stores only a boolean
`secret_bearing` marker and output is suppressed fail-closed for the whole
attempt, including after restart and terminal collection.

## What jobs can do

A job holds exactly the capabilities its spec asked for. `can_submit_children`
is off by default; `secret_mounts` is empty by default and gated by the
allow-list above. A child's `created_by` is its *parent's* owner, not a
synthetic `workload:` subject -- a synthetic one would create a fresh fair-share
leaf and silently escape per-principal quota.

## Exposure

`/metrics` exports `eyvan_project_spend_usd` labeled by (org, project) -- every
tenant's spend. It is authenticated by a scrape token, and the shipped
NetworkPolicy is the layer that holds when an operator has not configured one.
Reaching the API requires an explicit `eyvan.dev/api-client` label; scraping
requires a separate one, so neither confers the other.

There is no CORS policy, no TrustedHost middleware and no rate limiting. That is
acceptable in the bundled same-origin topology the image ships and nowhere else;
put a reverse proxy in front of any other arrangement.

## What is not covered

Per-attempt *cloud* identity. Outside Kubernetes the pod or VM service account
is the identity boundary, and narrowing it further needs provider workload
identity federation. Nothing here invents a substitute, because a plausible
imitation of one would be worse than its absence.
