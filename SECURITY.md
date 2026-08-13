# Security policy

Eyvan is a control plane. It holds cloud credentials, provisions machines, and
runs other people's code on them, so a bug here can cost money or expose a
tenant's work to another tenant.

## Reporting a vulnerability

Report privately through GitHub's **Report a vulnerability** button on the
Security tab of https://github.com/erfanzar/eyvan. Please do not open a public
issue for anything exploitable.

Include what you did, what happened, and what you expected. A failing command or
a short script is worth more than a description. You will get an acknowledgement
within a week.

## What is in scope

- Authentication or authorization bypass: reaching another tenant's workloads,
  leases, logs, artifacts, or spend.
- Credential exposure: secrets in argv, logs, events, metrics, or API responses.
- Fencing failures: a stale generation that still writes, or a lease that two
  attempts hold at once.
- Provisioning a resource an operator did not ask for, or failing to release one
  they asked to delete.

## What is not in scope, and already known

These are stated in `docs/proofs.md` and in the README. They are gaps, not
vulnerabilities, and reports of them will be closed as duplicates:

- **No hardware or provider qualification.** No current release has a receipt
  proving a real GCP, Kubernetes, GPU, or TPU path.
- **A head created by `eyvan head up` has no identity provider by default**, so
  it refuses every authenticated request with 401 until `--oidc-issuer` is set.
  The command says so when it finishes.
- **No TLS termination is provided.** The control plane speaks HTTP and expects
  to sit behind something that terminates TLS. Its firewall rule is restricted
  to the VPC's own subnets.
- Anything requiring an attacker who already has root on the control-plane host
  or the operator's cloud credentials.

## Supported versions

Only the latest release. Eyvan is pre-1.0 and there are no backports.
