# Orbit Kubernetes: operations

For an optional HTTP workload scale-to-zero deployment example, see the [KEDA integration
guide](../deployment/scale-to-zero.md). The Python adapter itself manages Kubernetes discovery
reads and does not own workload scaling.

For an opt-in service-mesh mTLS policy, see the [Istio deployment
guide](../deployment/service-to-service-mtls.md). Do not enable strict mode until callers and the
ingress path have been migrated to the mesh.

`orbit-kubernetes` resolves service backends from Kubernetes `discovery.k8s.io/v1` EndpointSlices.
Install the optional async SDK only when using this Python adapter:

```bash
python -m pip install 'orbit-kubernetes[kubernetes]'
```

The default factory for a pod uses its mounted service-account configuration:

```python
async with await KubernetesServiceDiscovery.in_cluster(
    namespace="shop",
    port_name="http",
    scheme="https",
    cache_ttl=5.0,
) as discovery:
    instances = await discovery.resolve("catalog")
```

For local development, use `from_kubeconfig(namespace=..., port_name=...)`. The `scheme` option
controls the URL scheme returned for discovered application endpoints; it does not configure TLS
for the Kubernetes API client. Kubeconfig or in-cluster SDK settings configure that separate
control-plane connection. Factories create isolated client configurations rather than mutating
global SDK configuration.

## Core resource ownership

Register the adapter as a managed container resource so Core closes it with the application:

```python
from contextlib import asynccontextmanager

from orbit_discovery import DISCOVERY_DEPENDENCY_KEY
from orbit_kubernetes import KubernetesServiceDiscovery


@asynccontextmanager
async def discovery_resource(_container):
    async with await KubernetesServiceDiscovery.in_cluster(
        namespace="shop", port_name="http"
    ) as discovery:
        yield discovery


application.container.register_resource(DISCOVERY_DEPENDENCY_KEY, discovery_resource)
```

Without a managed container registration, the code that creates the resolver owns `aclose()`.
Shutdown stops new lookups, drains active resolutions, clears cached snapshots, and closes the owned
API client once. A cancellation during a lookup propagates to the caller.

## Configuration and bounds

| Setting | Default | Accepted range |
| --- | ---: | ---: |
| `cache_ttl` | 5 seconds | 0–300 seconds |
| `max_cache_services` | 128 | 1–1,024 services |
| `page_size` | 100 | 1–500 EndpointSlices per request |
| `max_pages` | 64 | 1–64 pages per lookup |
| `request_timeout` | 5 seconds | 0.05–60 seconds per API request |

At most 4,096 endpoint records are processed per resolution. Cache refreshes for the same service
are coalesced; up to 64 lock stripes bound the in-flight coordination state. Set `cache_ttl=0` to
disable snapshot reuse. A nonzero TTL is a freshness window, not a watch or a guarantee that
endpoints are healthy.

## Kubernetes permissions

The in-cluster identity needs only `list` on `endpointslices` in the selected namespace. A namespaced
Role and RoleBinding are sufficient:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: orbit-service-discovery
  namespace: shop
rules:
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["list"]
```

Bind this Role to the workload's dedicated ServiceAccount. Do not grant cluster-wide list access
unless the application intentionally resolves across namespaces. The adapter does not watch
EndpointSlices; each refresh performs a bounded list and follows continuation tokens.

## Failure behavior

- A Kubernetes 404 lookup returns an empty tuple; other API failures become sanitized
  `DiscoveryError` values.
- Page payloads larger than the requested page size, unsupported address types, invalid IP-family
  data, or more than 4,096 endpoint records fail the lookup.
- A `ready` value of `False` is filtered. `True` or unset readiness is eligible.
- Only a matching named TCP port is returned. Other protocols and invalid port values are ignored.
- Endpoints are deduplicated and sorted by host and port. The adapter does not load-balance, probe,
  connect to, or retry endpoint requests.

Validate the client version, namespace RoleBinding, service-account token rotation, real API
timeouts, deployment proxy/egress controls, and shutdown during active requests in a representative
cluster. Local fake-client tests do not validate a live cluster or production topology.
