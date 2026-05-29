# Security model

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
| secret read | `eyvs1` | yes, `secret_token_ttl_s` | the workload's *attempt* advancing |

The submit capability does not expire deliberately: revocation is a state check
that is strictly stronger than a TTL, and an expiring one would inherit seven
days and break any job that runs longer than a week -- precisely the kind that
fans out.

The secret capability does expire, for the opposite reason. It hands back a
*value*, and once read no later revocation can un-read it.

## Secrets

A spec carries a *reference*, never a value. `PolicyRecord.allowed_secret_references`
is a per-project prefix allow-list checked at submission, and it is the
security-critical piece: without it any submitter can name any secret the
control plane's own identity can read, which turns "may submit jobs" into "may
read every secret in the project".

Its empty value means **none**, inverting the convention its neighbours in the
same command follow. Every other allow-list there means "no restriction" when
empty.

Every fetch is audited twice over -- one issuance event per attempt naming the
mounts granted, and one event per redemption, granted or refused, written in the
same transaction as the authorization read. A `secret.read` for an attempt with
no matching `secret.issued` is the signal that a capability leaked.

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
