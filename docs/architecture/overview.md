# Orbit Kubernetes: architecture

## Package boundaries

`orbit-kubernetes` is the Python Kubernetes provider adapter for the provider-neutral
`orbit-discovery` capability. Its runtime dependency is `orbit-discovery`; the Kubernetes SDK is an
optional extra. The package does not add Kubernetes dependencies to Core or to the discovery
contract.

```text
application / Orbit Core container
              │ ServiceDiscovery
              ▼
       orbit-discovery
              ▲
              │ implements resolve() / aclose()
       orbit-kubernetes ───── kubernetes-asyncio ───── Kubernetes API
```

The alternative `orbit-kubernetes-go` process adapter uses the versioned
`orbit.discovery.v1` contract. Select and wire one provider implementation explicitly; this Python
package does not launch or supervise the Go process.

## Resolution flow

For each uncached Service name, the adapter:

1. Validates the Service name as a lowercase Kubernetes DNS label.
2. Lists EndpointSlices in its configured namespace, filtered by
   `kubernetes.io/service-name=<service>` and bounded by the configured page size and timeout.
3. Follows Kubernetes continuation tokens up to `max_pages`.
4. Requires the slice address type to be `IPv4` or `IPv6`, filters explicitly unready endpoints,
   and selects the configured TCP port name.
5. Validates address family, builds immutable `ServiceInstance` values, deduplicates host/port
   pairs, sorts the result, and caches the snapshot when caching is enabled.

This implementation rejects FQDN EndpointSlices. Kubernetes documents IPv4/IPv6 address literals
for those slice types and marks FQDN as deprecated with no defined semantics; controller-generated
slices use IPv4 or IPv6 [EndpointSlice API reference](https://kubernetes.io/docs/reference/kubernetes-api/discovery/endpoint-slice-v1/).

## Cache and concurrency

The cache is an in-process bounded LRU keyed by Service name. A striped lock coalesces concurrent
refreshes for the same key without allocating one persistent lock per possible Service. Independent
Service lookups can proceed concurrently. The cache uses monotonic time and can be disabled with a
zero TTL. Each resolver instance and worker has independent cache contents.

`max_pages`, `page_size`, the 4,096 endpoint ceiling, and cache-capacity bounds keep processing
finite even when a caller misconfigures the adapter or an API response exceeds its requested page
size. Lookup results are snapshots; the adapter does not watch or maintain a live stream of
EndpointSlice updates.

## Ownership and errors

Factories create isolated Kubernetes client configuration and return a resolver that owns its API
client. `aclose()` prevents new lookups, drains active requests, clears the cache, and closes the
client. A caller that supplies a custom API object may supply a close callback; the default
constructor does not assume ownership of arbitrary injected clients.

Kubernetes-specific failures are normalized to `DiscoveryError` without exposing SDK response
bodies or credentials. Task cancellation propagates. A 404 maps to an empty endpoint tuple. The
caller owns endpoint selection, health checks, outbound connection policy, and application retries.

## Implemented module

- `src/orbit_kubernetes/discovery.py`: factories, endpoint pagination and validation, cache,
  concurrency coalescing, and client lifecycle.
- `deploy/kubernetes/`: optional KEDA HTTP scale-to-zero and Istio mTLS reference manifests. These
  deployment examples do not add runtime dependencies or imply that Orbit operates those controllers.
