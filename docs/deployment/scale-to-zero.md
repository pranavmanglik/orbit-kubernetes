# HTTP scale-to-zero with KEDA

`orbit-kubernetes` does not operate autoscaling controllers. This guide shows how to connect an
Orbit HTTP workload to the optional [KEDA HTTP Add-on](https://keda.sh/http-add-on/) so the cluster
can scale it to zero while an interceptor accepts and forwards incoming requests during cold
starts. The add-on, KEDA operator, Gateway API controller, and their CRDs are separately operated
cluster dependencies.

## Example resources

Review and adapt
[`keda-http-scale-to-zero.example.yaml`](../../deploy/kubernetes/keda-http-scale-to-zero.example.yaml):

- `InterceptorRoute` reports concurrent requests and routes them to the existing `orbit-core`
  Service on port `8000`.
- `ScaledObject` targets the existing `orbit-core` Deployment, permits zero replicas, caps the
  maximum at 20, polls every 15 seconds while scaled to zero, and waits five minutes after the last
  active trigger before scale-down.
- `ReferenceGrant` allows only the `orbit` namespace's HTTPRoutes to reference the named interceptor
  Service in `keda`.
- `HTTPRoute` sends public traffic through the KEDA interceptor. Do not route public traffic
  directly to the application Service when relying on HTTP-driven activation; bypass traffic will
  not activate the HTTP scaler.

Install KEDA and its HTTP Add-on using their official versioned installation instructions. Confirm
the installed release serves `http.keda.sh/v1beta1`, `keda.sh/v1alpha1`, and the Gateway API versions
used by the example. Install the Gateway API CRDs and configure `orbit-public` as in the
[`orbit-gateway` deployment guide](../../../orbit-gateway/docs/deployment/nginx-gateway-fabric.md).
Apply the `InterceptorRoute` before the `ScaledObject`, then apply the `ReferenceGrant` and
`HTTPRoute`. Check readiness conditions for the route and scaler before shifting production
traffic.

The example uses the HTTP Add-on external-push scaler service at
`keda-add-ons-http-external-scaler.keda:9090` and interceptor Service at
`keda-add-ons-http-interceptor-proxy.keda:8080`. Change these names and ports if the add-on is
installed with different settings. The KEDA HTTP Add-on documentation describes
[workload routing](https://keda.sh/http-add-on/0.16/user-guide/autoscale-an-app/) and
[scaling metrics and cooldown](https://keda.sh/http-add-on/0.16/user-guide/configure-scaling/).

## Production checks

Scale-to-zero introduces a cold-start delay and a new request path. Before enabling it:

- Measure application startup and readiness time, interceptor buffering limits, client and Gateway
  deadlines, and the observed first-request latency under concurrent cold starts.
- Keep a nonzero minimum for latency-sensitive or stateful services. Do not scale singleton,
  leader-elected, or in-memory-session workloads to zero without an explicit recovery design.
- Keep the workload stateless or persist state in external services. Verify graceful termination and
  request draining during scale-down.
- Restrict access to KEDA's scaler and interceptor Services using namespace policy and network
  policy. Apply only the narrow cross-namespace permission in the sample ReferenceGrant.
- Configure TLS termination, upstream encryption, authentication, request-size limits, and trusted
  forwarding headers at the ingress according to the cluster threat model. This example does not
  provision mTLS, certificates, or network policies.
- Test scaler unavailability, failed readiness, maximum replica saturation, cancellation, and
  ingress behavior in a representative cluster. The checked-in manifest is a reference example,
  not a verified live deployment or an availability guarantee.

For field semantics, use the official [KEDA HTTP Add-on guide](https://keda.sh/http-add-on/) and
[KEDA `ScaledObject` reference](https://keda.sh/docs/latest/reference/scaledobject-spec/).
