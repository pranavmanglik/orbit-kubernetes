# Service-to-service mTLS with Istio

Orbit leaves service identity, certificate issuance and rotation, and workload-to-workload mTLS to
the selected service mesh. This guide shows an opt-in Istio policy for one labeled workload; it
does not add Istio SDK dependencies to Orbit packages.

Review [`istio-peer-authentication.example.yaml`](../../deploy/kubernetes/istio-peer-authentication.example.yaml)
and change the namespace and selector to match the receiving workload's Pod labels. The policy uses
the current `security.istio.io/v1` API and `STRICT` mode. It is scoped to matching workloads in the
`orbit` namespace; it does not change the whole mesh.

## Apply only after mesh migration

First install and configure Istio, inject the receiving workload and every caller into the mesh,
and confirm Istio workload identity and certificate rotation are healthy. Then apply the policy and
verify both an authorized mesh caller and a plaintext connection rejected by the receiving proxy.
Istio automatically uses mutual TLS for eligible in-mesh connections, while `PeerAuthentication`
`STRICT` rejects plaintext at the selected workload. See the official [authentication policy
guide](https://istio.io/latest/docs/tasks/security/authentication/authn-policy/) and
[PeerAuthentication reference](https://istio.io/latest/docs/reference/config/security/peer_authentication/).

Do not apply this policy blindly to an application behind a non-mesh ingress. A Gateway that is not
in the mesh cannot meet the workload's strict mTLS requirement. Integrate the ingress with the mesh
and configure its backend TLS, or keep this policy scoped to internal workloads. Confirm the actual
data path, sidecar/ambient mode, health probes, startup probes, monitoring scrapes, and administrative
traffic before enforcement. Keep a rollback path available during migration.

This reference does not provision Istio, its CA, `AuthorizationPolicy`, external certificate
authorities, namespace injection, or network policies. Cluster operators own those resources and
must validate certificate trust, identity authorization, and rotation in the target environment.
