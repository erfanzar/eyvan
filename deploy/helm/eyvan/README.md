# The Eyvan Helm chart

```
helm install eyvan ./deploy/helm/eyvan \
  --namespace eyvan-system --create-namespace \
  --set controlPlane.publicUrl=https://eyvan.example.com \
  --set controlPlane.artifactStoreUri=gs://your-bucket/eyvan \
  --set controlPlane.oidc.issuer=https://idp.example.com \
  --set controlPlane.oidc.dashboardClientId=eyvan-dashboard
```

## What it does not do

**It does not create your Secret.** `existingSecret` names one you make
yourself, holding `database-url`, `artifact-token-key` and
`agent-shared-secret`. A chart that generated an artifact signing key would
generate a *different* one on every upgrade, and every capability token already
issued to a running job would stop verifying at the next `helm upgrade`.

**It does not provision a database.** Eyvan refuses the local repository on a
non-loopback bind, so PostgreSQL is a prerequisite rather than a subchart --
and a database whose lifecycle is tied to a release is one a `helm uninstall`
can delete.

**It has not been rendered here.** This chart was written without a `helm`
binary available, so `helm lint` and `helm template` have not been run against
it. `tests/test_helm_chart.py` checks what can be checked without a Go template
engine -- that the metadata and values parse, that every `.Values` reference
resolves against `values.yaml`, that the migration hook annotations are present,
and that the hardening decisions the raw manifests carry survive into the
templates. Run `helm lint` before trusting it in an environment you care about.

## Defaults

Nothing defaults to a credential and nothing defaults to a permissive setting.
An operator who installs with no values gets a control plane that refuses to
serve rather than one that serves insecurely: no OIDC issuer means every request
is 401, an empty trusted-callable list means pickled callables are refused, and
`autoCreateSchema` is off so the migration hook is the only path that touches
the schema.

`serviceMonitor.enabled` is off because it needs the Prometheus Operator's CRDs,
and a chart that installs an object whose kind does not exist fails the whole
release.
