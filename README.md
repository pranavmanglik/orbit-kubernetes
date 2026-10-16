# Orbit Kubernetes

`orbit-kubernetes` is the Kubernetes provider adapter for Orbit capability packages. The first
adapter implements `orbit_discovery.ServiceDiscovery`; Core and `orbit-discovery` remain free of
Kubernetes SDK dependencies.

This package is in alpha and supports Python 3.11–3.14. The Kubernetes client is an optional
extra:

```bash
python -m pip install 'orbit-kubernetes[kubernetes]'
```

Orbit also provides a separately installable Go process-plugin variant, `orbit-kubernetes-go`, for
deployments that prefer a standalone Go provider process. Both variants implement the same
`orbit.discovery.v1` contract. Select one explicitly in application wiring: the Python variant
registers `KubernetesServiceDiscovery`; the Go variant is supervised by Core and exposed through
`orbit-discovery[process]`'s `ProcessServiceDiscovery`. Core remains Python in both cases.

## Resolve a Service

Create one resolver for the namespace and Service port used by your application, register it under
`orbit_discovery.DISCOVERY_DEPENDENCY_KEY` with Core's container, and close it during application
shutdown:

```python
from orbit_kubernetes import KubernetesServiceDiscovery

async with await KubernetesServiceDiscovery.in_cluster(
    namespace="shop", port_name="http", scheme="https", cache_ttl=5
) as discovery:
    instances = await discovery.resolve("catalog")
    for instance in instances:
        print(instance.base_url)
```

For local development, use `from_kubeconfig(namespace="shop", port_name="http")`. Both factories
use an isolated client configuration instead of changing Kubernetes SDK global configuration. The
adapter owns and closes its API client. It scopes all reads to one namespace and requests only the
EndpointSlices labeled for the requested Service.

## Behavior and limits

- Reads Kubernetes `discovery.k8s.io/v1` EndpointSlices, not the legacy Endpoints API. EndpointSlice
  is Kubernetes' scalable backend representation and supports IPv4 and IPv6.
- Accepts only EndpointSlices declaring IPv4 or IPv6, and validates each address against that
  family. Deprecated FQDN address slices are rejected rather than converted into arbitrary DNS
  destinations.
- A configured named Service port is required. Only TCP endpoints with that port name are returned.
- Explicitly unready endpoints are filtered. Kubernetes defines an unset `ready` condition as
  ready, so unset values remain eligible.
- Results are deterministic immutable `ServiceInstance` snapshots, deduplicated by host and port.
  The adapter does not choose an instance, connect to it, or perform health checks.
- Refreshes are coalesced per service using bounded lock striping. Pages cannot exceed the
  requested page size; total endpoint records are capped at 4,096. Cache capacity, page count, page
  size, TTL, and API request timeout have finite configuration bounds.
- API failures become `DiscoveryError` without copying provider error details or credentials into
  messages. Task cancellation propagates. A missing Service resolves to an empty tuple.
- This adapter does not watch EndpointSlices. Lookups reflect the Kubernetes API at the time of
  the last cache refresh; choose a short cache TTL to limit staleness.
- This package currently provides service discovery only. It does not provision Deployments,
  autoscaling policies, scale-to-zero controllers, service-mesh mTLS, or certificate rotation;
  those belong to separately operated Kubernetes controllers and platform policy.

The [scale-to-zero deployment example](docs/deployment/scale-to-zero.md) documents an optional
KEDA HTTP Add-on integration for stateless HTTP workloads. It is a cluster deployment example, not
an autoscaler implementation bundled by this Python adapter.

The Kubernetes identity should have read-only `list` permission for EndpointSlices in the selected
namespace. Avoid cluster-wide roles for this adapter. Network egress policy still needs to validate
discovered addresses before an application connects to them; discovery results are not an SSRF
defense.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e '.[dev,kubernetes]'
pytest
ruff check .
ruff format --check .
mypy
python -m build
```

Tests use a fake async API client and do not require a cluster. Live cluster RBAC, API compatibility,
rollout, and network behavior need validation in a representative Kubernetes environment before a
production release.

Licensed under Apache-2.0.

