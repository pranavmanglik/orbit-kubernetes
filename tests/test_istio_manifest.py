from __future__ import annotations

from pathlib import Path

import yaml

MANIFEST = (
    Path(__file__).parents[1] / "deploy" / "kubernetes" / "istio-peer-authentication.example.yaml"
)


def test_istio_mtls_example_is_scoped_to_a_workload_and_requires_strict_mtls() -> None:
    """Prevent the reference policy from silently enforcing mesh-wide mTLS."""
    document = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    assert document["apiVersion"] == "security.istio.io/v1"
    assert document["kind"] == "PeerAuthentication"
    assert document["spec"]["selector"]["matchLabels"] == {"app": "orbit-core"}
    assert document["spec"]["mtls"]["mode"] == "STRICT"
