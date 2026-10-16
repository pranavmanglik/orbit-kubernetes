# Orbit Kubernetes: security

This package reads Kubernetes EndpointSlices and returns application endpoint snapshots. It does
not authenticate application requests, authorize callers, connect to discovered hosts, or establish
that an endpoint is safe to reach.

## Kubernetes permissions

Use a dedicated workload ServiceAccount and bind a namespaced Role with only `list` permission for
`discovery.k8s.io/endpointslices`. The adapter scopes each API request to its configured namespace
and filters by the requested Service label. It does not need permission to read Secrets, Services,
Pods, or cluster-wide resources. Keep RBAC declarations alongside deployment configuration and
verify the effective RoleBinding in the target cluster.

For local use, `from_kubeconfig` uses the user's kubeconfig context. Avoid developer credentials in
shared automation. In-cluster credentials should be delivered and rotated by Kubernetes; do not
copy service-account tokens into application configuration or logs.

## Treat results as untrusted destinations

Endpoint addresses originate from cluster state and may be stale or changed by another controller
or an authorized cluster user. Validate destinations against application-specific network policy
before making outbound requests. Apply egress controls, DNS/IP allow-lists where appropriate, and
normal TLS certificate and hostname verification in the client that consumes a `ServiceInstance`.
Discovery is not an SSRF defense and the adapter does not perform health checks.

This adapter accepts IPv4 and IPv6 address literals and rejects unsupported EndpointSlice address
types. It validates that each literal matches the slice's declared family, but does not decide which
network ranges the application may contact. The `scheme` option controls only the returned
application URL; Kubernetes API transport security is configured separately by kubeconfig or the
in-cluster SDK configuration.

## Errors, logs, and lifecycle

Provider failures are converted to `DiscoveryError` without copying response bodies, credentials,
or other provider details into the exception message. Applications should still avoid logging
raw request context or credential-bearing configuration. Task cancellation propagates so shutdown
and request deadlines remain effective. Close the resolver during application shutdown so its API
client and cached endpoint data are released.

## Reporting a vulnerability

Follow the repository [security policy](../../SECURITY.md) for private reporting. This document
describes package behavior and operational guidance; it is not a third-party security audit or a
claim that a hosting-platform reporting setting is enabled.
