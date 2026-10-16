from __future__ import annotations

from pathlib import Path

import yaml

MANIFEST = (
    Path(__file__).parents[1] / "deploy" / "kubernetes" / "keda-http-scale-to-zero.example.yaml"
)


def test_keda_scale_to_zero_route_uses_interceptor_and_narrow_reference_grant() -> None:
    """Keep the sample wired through KEDA and restrict its cross-namespace Service grant."""
    documents = list(yaml.safe_load_all(MANIFEST.read_text(encoding="utf-8")))
    by_kind = {document["kind"]: document for document in documents}

    assert set(by_kind) == {"InterceptorRoute", "ScaledObject", "ReferenceGrant", "HTTPRoute"}
    scaled_object = by_kind["ScaledObject"]
    assert scaled_object["spec"]["scaleTargetRef"]["name"] == "orbit-core"
    assert scaled_object["spec"]["minReplicaCount"] == 0
    route = by_kind["HTTPRoute"]
    backend = route["spec"]["rules"][0]["backendRefs"][0]
    assert backend["name"] == "keda-add-ons-http-interceptor-proxy"
    assert backend["namespace"] == "keda"
    grant = by_kind["ReferenceGrant"]["spec"]
    assert grant["from"] == [
        {"group": "gateway.networking.k8s.io", "kind": "HTTPRoute", "namespace": "orbit"}
    ]
    assert grant["to"] == [
        {
            "group": "",
            "kind": "Service",
            "name": "keda-add-ons-http-interceptor-proxy",
        }
    ]
