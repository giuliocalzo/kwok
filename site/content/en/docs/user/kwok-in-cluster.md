---
title: "`kwok` in Cluster"
aliases:
- /docs/user/kwok-in-cluster-old
---

# Deploy `kwok` in a Cluster

{{< hint "info" >}}

This document walks you through how to deploy `kwok` in a Kubernetes cluster.

{{< /hint >}}


## Helm Chart Installation

The kwok helm chart is listed on the [artifact hub](https://artifacthub.io/packages/helm/kwok/kwok).

## YAML Installation

You can also install `kwok` using the provided YAML manifests.

### Variables preparation

``` bash
# KWOK repository
KWOK_REPO=kubernetes-sigs/kwok
# Get latest
KWOK_LATEST_RELEASE=$(curl "https://api.github.com/repos/${KWOK_REPO}/releases/latest" | jq -r '.tag_name')
```

### Deploy kwok and set up custom resource definitions (CRDs)

``` bash
kubectl apply -f "https://github.com/${KWOK_REPO}/releases/download/${KWOK_LATEST_RELEASE}/kwok.yaml"
```

### Set up default custom resources (CRs) of stages (required)

{{< hint "warning" >}}

NOTE: This step is required to configure the pod/node simulation behavior.

{{< /hint >}}

The default simulation behavior is defined in [fast stage for pod][fast-stage-pod] and [fast stage for node][fast-stage-node].
They are suitable for scenarios such as scheduling and autoscaling.
You can customize the simulation behavior with your own configuration file.

``` bash 
kubectl apply -f "https://github.com/${KWOK_REPO}/releases/download/${KWOK_LATEST_RELEASE}/stage-fast.yaml"
```

### Set up default custom resources (CRs) of resource usage (optional)

This allows to simulate the resource usage of nodes, pods and containers.

``` bash 
kubectl apply -f "https://github.com/${KWOK_REPO}/releases/download/${KWOK_LATEST_RELEASE}/metrics-usage.yaml"
```

The above configuration sets the CPU and memory usage of all the containers managed by `kwok` to `1m` and to `1Mi` respectively.
To override the defaults, you can add annotation `"kwok.x-k8s.io/usage-cpu"` (for cpu usage) and
`"kwok.x-k8s.io/usage-memory"` (for memory usage) with any quantity value you want to the fake pods.

The resource usage simulation used above is annotation-based and the configuration is available at [here]([metrics-usage]).
For the explanation of how it works and more complex resource usage simulation methods, please refer to [ResourceUsage configuration].

## High availability

By default a single `kwok` replica runs the controllers.
To run more replicas, enable leader election: only the elected replica runs the controllers and the server, and the others serve only the health checks (`/healthz`, `/readyz`, `/livez`) until they take over.

``` yaml
apiVersion: config.kwok.x-k8s.io/v1alpha1
kind: KwokConfiguration
options:
  leaderElection:
    leaderElect: true
```

Or pass `--leader-elect`.
The lease is `kwok-controller` in the namespace `kwok` runs in; change it with `--leader-elect-resource-name` and `--leader-elect-resource-namespace`.

A replica that loses leadership exits and restarts as a standby.
The new leader takes over at once the node leases written by the other replicas (they carry the `kwok.x-k8s.io/leader-election` annotation naming the election lease), so nodes stay Ready across a failover, and repoints each node's `InternalIP` from the old leader to itself, so `kubectl logs`, `exec`, `attach`, `port-forward` and metrics keep working.

Limitations:

- Repointing and the immediate node lease takeover need node leases (`nodeLeaseDurationSeconds` greater than `0`).
- Pods that were already running keep the old leader's IP in `status.hostIP`.
- Nodes initialized by a `kwok` version without this feature are not repointed; recreate them.
- With `hostNetwork`, schedule the replicas on different nodes, since every replica binds the same port.
- Standbys are Ready, so a Service in front of the `kwok` port also routes to them, and they answer only the health checks; scrape metrics from the leader directly.
- Node leases held by anything else (a kubelet, another `kwok` deployment) still wait for their lease to expire, as without leader election; node leases written before leader election was enabled carry no annotation, so the first failover after enabling it waits up to `nodeLeaseDurationSeconds` for them.

## Next steps

Now, you can use `kwok` to [manage nodes and pods] in the Kubernetes cluster.

[manage nodes and pods]: {{< relref "/docs/user/kwok-manage-nodes-and-pods" >}}
[fast-stage-pod]: https://github.com/kubernetes-sigs/kwok/tree/main/kustomize/stage/pod/fast
[fast-stage-node]: https://github.com/kubernetes-sigs/kwok/tree/main/kustomize/stage/node/fast
[metrics-usage]: https://github.com/kubernetes-sigs/kwok/tree/main/kustomize/metrics/usage
[ResourceUsage configuration]: {{< relref "/docs/user/resource-usage-configuration" >}}
