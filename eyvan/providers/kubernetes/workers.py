# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Manifest builders for Kubernetes-backed Eyvan capacity cells.

**One cell is one ``batch/v1`` Job** with ``completionMode: Indexed``,
``parallelism == completions == worker_count``, and ``backoffLimit: 0``. Four
other mappings were considered and each fails on something structural:

- a *namespace* is not sized, so it cannot report an honest ``ResourceVector``;
- a Kueue ``LocalQueue`` is an admission channel that holds no capacity, and a
  cell must *hold* capacity;
- a pre-provisioned *node pool* is the right physical notion but is not portably
  creatable through the Kubernetes API, and is many failure units rather than one;
- a bare *Pod* needs per-pod group labels and gives no multi-host completion
  semantics.

The Job wins on four counts. A deterministic name from ``cell_id`` makes create
idempotent -- a 409 ``AlreadyExists`` is a replay, the exact analogue of
``FleetCellRecord.qr_id`` on GCP. ``worker_count -> parallelism`` reproduces the
shipped multislice model, so ``native_worker_endpoints`` has N entries and
``runtime/registry.py:resolve`` selects ``NativeSliceExecutor`` **unchanged**. An
Indexed Job plus a headless Service and ``pod.spec.subdomain`` gives stable DNS,
which is load-bearing: ``ExecutionDriverResolver`` caches drivers on the endpoint
fingerprint and closes stale entries, so pod-IP churn would thrash that cache and
could silently repoint an executor at a *different* pod. And release is a Job
delete with ``propagationPolicy: Foreground``, which genuinely frees capacity.

**The token never appears in a manifest.** Workers read it from a Secret, and the
Job carries only a *fingerprint* annotation for leak detection -- annotations are
readable by anyone with ``get`` in the namespace, so the token itself would be
readable too.
"""

from __future__ import annotations

import hashlib
import typing as tp

from eyvan._json import JsonObject

#: Kubernetes object names are DNS-1123 labels, at most 63 characters. Eyvan cell
#: ids can reach 88, so long names are truncated and disambiguated by a digest --
#: the same shape ``autoscale_fleet_name`` already uses, rather than a second
#: truncation rule that could collide differently.
MAX_OBJECT_NAME = 63

#: Label carrying the Eyvan cell a Job belongs to. The Kueue precondition check
#: reads it to prove every Workload in a ClusterQueue is Eyvan's.
CELL_LABEL = "eyvan.dev/cell-id"
GENERATION_LABEL = "eyvan.dev/provider-generation"
#: Annotation carrying a *fingerprint* of the fencing token, never the token. An
#: observed Job whose fingerprint disagrees with the durable record is a leaked
#: previous incarnation -- the role ``management_uid`` plays on GCP.
FENCE_ANNOTATION = "eyvan.dev/fence-fingerprint"


def object_name(prefix: str, cell_id: str) -> str:
    """Return a DNS-1123 name for one cell's object.

    Deterministic: the same cell always produces the same name, which is what
    makes ``create`` idempotent and a 409 a replay rather than a conflict.
    """

    base = f"{prefix}-{cell_id}".lower().replace("_", "-").replace(".", "-")
    base = "".join(character if character.isalnum() or character == "-" else "-" for character in base)
    base = base.strip("-") or prefix
    if len(base) <= MAX_OBJECT_NAME:
        return base
    suffix = f"-{hashlib.sha256(cell_id.encode()).hexdigest()[:8]}"
    return f"{base[: MAX_OBJECT_NAME - len(suffix)].rstrip('-')}{suffix}"


def fence_fingerprint(fencing_token: str) -> str:
    """Return a non-reversible fingerprint of a fencing token.

    Annotations are readable by anyone with ``get`` in the namespace, so the
    token cannot go there. A fingerprint is enough for the only question asked of
    it: does this observed Job belong to the incarnation the record describes?
    """

    return hashlib.sha256(fencing_token.encode()).hexdigest()[:32]


def headless_service_manifest(cell_id: str, namespace: str) -> JsonObject:
    """Return the headless Service that gives Indexed Job pods stable DNS.

    ``clusterIP: None`` plus the pods' ``subdomain`` yields
    ``<job>-<index>.<service>.<namespace>.svc`` -- an address that survives a pod
    restart, which pod IPs do not.
    """

    name = object_name("eyvan", cell_id)
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": namespace, "labels": {CELL_LABEL: cell_id}},
        "spec": {
            "clusterIP": "None",
            # Published before readiness so DNS resolves while the workers are
            # still starting; the control plane decides readiness from Job status,
            # not from whether a name resolves.
            "publishNotReadyAddresses": True,
            "selector": {CELL_LABEL: cell_id},
            "ports": [{"name": "native-worker", "port": 8100, "targetPort": 8100}],
        },
    }


def worker_endpoints(cell_id: str, namespace: str, worker_count: int, *, port: int = 8100) -> tuple[str, ...]:
    """Return the stable DNS endpoint of each worker, in index order.

    Index order is the rank order: ``NativeSliceExecutor`` derives per-rank
    checkpoint URIs from position, so a set that reordered between observations
    would silently repoint rank 1's checkpoint at rank 2's.
    """

    if worker_count < 1:
        raise ValueError("a Kubernetes capacity cell needs at least one worker")
    name = object_name("eyvan", cell_id)
    return tuple(f"http://{name}-{index}.{name}.{namespace}.svc:{port}" for index in range(worker_count))


def job_manifest(
    *,
    cell_id: str,
    namespace: str,
    image: str,
    worker_count: int,
    provider_generation: int,
    fencing_token: str,
    resources: tp.Mapping[str, str],
    service_account: str = "eyvan-native-worker",
    secret_name: str | None = None,
    queue_name: str | None = None,
    priority_class: str | None = None,
    node_selector: tp.Mapping[str, str] | None = None,
    tolerations: tp.Sequence[JsonObject] = (),
) -> JsonObject:
    """Return the Job manifest for one capacity cell.

    ``backoffLimit: 0`` because a cell is a *capacity* unit, not a retry unit:
    Eyvan's own retry budgets decide whether failed work runs again, and a Job
    that restarted itself would resurrect capacity the control plane believes it
    released.

    ``suspend`` is set only when a Kueue queue is named. Kueue is opt-in because
    it is not installed in most clusters, and requiring it would make this backend
    untestable for most users; absent it, kube-scheduler and the cluster
    autoscaler place the pods and ``provisioning_timeout_s`` is the only bound.
    """

    if worker_count < 1:
        raise ValueError("a Kubernetes capacity cell needs at least one worker")
    name = object_name("eyvan", cell_id)
    labels = {CELL_LABEL: cell_id, GENERATION_LABEL: str(provider_generation)}
    if queue_name:
        labels["kueue.x-k8s.io/queue-name"] = queue_name
    if priority_class:
        labels["kueue.x-k8s.io/priority-class"] = priority_class
    pod_spec: JsonObject = {
        "restartPolicy": "Never",
        "serviceAccountName": service_account,
        "subdomain": name,
        "containers": [
            {
                "name": "native-worker",
                "image": image,
                "ports": [{"name": "native-worker", "containerPort": 8100}],
                "env": [
                    {"name": "EYVAN_CELL_ID", "value": cell_id},
                    {"name": "EYVAN_PROVIDER_GENERATION", "value": str(provider_generation)},
                    # Indexed Jobs expose the completion index here; it is the
                    # worker's rank and must match the endpoint ordering above.
                    {
                        "name": "EYVAN_WORKER_INDEX",
                        "valueFrom": {
                            "fieldRef": {"fieldPath": "metadata.annotations['batch.kubernetes.io/job-completion-index']"}
                        },
                    },
                ],
                "resources": {"limits": dict(resources), "requests": dict(resources)},
            }
        ],
    }
    if secret_name:
        # The token reaches the worker as a mounted Secret. Never as an env value
        # in this manifest and never as an annotation: both are readable by
        # anyone who can read the Job.
        pod_spec["volumes"] = [{"name": "eyvan-worker-token", "secret": {"secretName": secret_name}}]
        container = pod_spec["containers"][0]
        assert isinstance(container, dict)
        container["volumeMounts"] = [{"name": "eyvan-worker-token", "mountPath": "/var/run/eyvan", "readOnly": True}]
    if node_selector:
        pod_spec["nodeSelector"] = dict(node_selector)
    if tolerations:
        pod_spec["tolerations"] = [dict(toleration) for toleration in tolerations]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels,
            "annotations": {FENCE_ANNOTATION: fence_fingerprint(fencing_token)},
        },
        "spec": {
            "completionMode": "Indexed",
            "completions": worker_count,
            "parallelism": worker_count,
            "backoffLimit": 0,
            **({"suspend": True} if queue_name else {}),
            "template": {"metadata": {"labels": labels}, "spec": pod_spec},
        },
    }


def secret_manifest(cell_id: str, namespace: str, token: str) -> JsonObject:
    """Return the Secret carrying one cell's derived native-worker token.

    Rewritten on every recreate, because ``derive_native_worker_token`` hashes
    ``(management_uid, provider_generation)`` -- a stale Secret would authenticate
    workers of a previous incarnation, which is exactly what the generation
    exists to prevent.
    """

    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": object_name("eyvan-token", cell_id),
            "namespace": namespace,
            "labels": {CELL_LABEL: cell_id},
        },
        "type": "Opaque",
        "stringData": {"token": token},
    }


__all__ = (
    "CELL_LABEL",
    "FENCE_ANNOTATION",
    "GENERATION_LABEL",
    "MAX_OBJECT_NAME",
    "fence_fingerprint",
    "headless_service_manifest",
    "job_manifest",
    "object_name",
    "secret_manifest",
    "worker_endpoints",
)
