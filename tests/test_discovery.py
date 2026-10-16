from __future__ import annotations

import asyncio
from types import SimpleNamespace as NS
from typing import Any

import pytest
from orbit_discovery import DiscoveryError, ServiceDiscovery

import orbit_kubernetes.discovery as discovery_module
from orbit_kubernetes import KubernetesServiceDiscovery


def port(name: str, number: int, protocol: str | None = "TCP") -> NS:
    return NS(name=name, port=number, protocol=protocol)


def endpoint(address: str, ready: bool | None = True) -> NS:
    return NS(addresses=[address], conditions=NS(ready=ready))


def slice_item(
    name: str,
    endpoints: list[NS],
    ports: list[NS] | None = None,
    address_type: str = "IPv4",
) -> NS:
    return NS(
        metadata=NS(name=name),
        address_type=address_type,
        endpoints=endpoints,
        ports=ports or [port("http", 8080)],
    )


def page(items: list[NS], continuation: str | None = None) -> NS:
    return NS(items=items, metadata=NS(_continue=continuation))


class FakeApi:
    def __init__(self, pages: list[NS] | None = None, error: Exception | None = None) -> None:
        self.pages = list(pages or [])
        self.error = error
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def list_namespaced_endpoint_slice(self, namespace: str, **kwargs: Any) -> NS:
        self.calls.append((namespace, kwargs))
        if self.error is not None:
            raise self.error
        return self.pages.pop(0)


def resolver(api: FakeApi, **options: Any) -> KubernetesServiceDiscovery:
    return KubernetesServiceDiscovery(api, namespace="apps", port_name="http", **options)


@pytest.mark.asyncio
async def test_resolves_ready_addresses_by_named_tcp_port_and_uses_endpoint_slice_selector() -> (
    None
):
    api = FakeApi(
        [
            page(
                [
                    slice_item(
                        "catalog-abc",
                        [endpoint("10.0.0.2"), endpoint("10.0.0.3", False)],
                        [port("metrics", 9090), port("http", 8080)],
                    )
                ]
            )
        ]
    )
    discovery = resolver(api, cache_ttl=0)

    assert isinstance(discovery, ServiceDiscovery)
    instances = await discovery.resolve("catalog")
    assert [(item.host, item.port) for item in instances] == [("10.0.0.2", 8080)]
    assert api.calls[0][0] == "apps"
    assert api.calls[0][1]["label_selector"] == "kubernetes.io/service-name=catalog"
    assert api.calls[0][1]["limit"] == 100
    assert api.calls[0][1]["_request_timeout"] == 5.0
    assert "slice" not in repr(instances[0])
    await discovery.aclose()


@pytest.mark.asyncio
async def test_follows_pages_deduplicates_and_caches_immutable_snapshots() -> None:
    api = FakeApi(
        [
            page(
                [slice_item("catalog-a", [endpoint("2001:db8::1", None)], address_type="IPv6")],
                "next",
            ),
            page([slice_item("catalog-b", [endpoint("2001:db8::1")], address_type="IPv6")]),
        ]
    )
    discovery = resolver(api, cache_ttl=30)

    first = await discovery.resolve("catalog")
    second = await discovery.resolve("catalog")
    assert first == second
    assert len(first) == 1
    assert first[0].host == "2001:db8::1"
    assert first[0].authority == "[2001:db8::1]:8080"
    assert len(api.calls) == 2
    assert api.calls[0][1]["_continue"] is None
    assert api.calls[1][1]["_continue"] == "next"
    await discovery.aclose()


@pytest.mark.asyncio
async def test_coalesces_concurrent_refreshes() -> None:
    class SlowApi(FakeApi):
        async def list_namespaced_endpoint_slice(self, namespace: str, **kwargs: Any) -> NS:
            await asyncio.sleep(0)
            return await super().list_namespaced_endpoint_slice(namespace, **kwargs)

    api = SlowApi([page([slice_item("catalog-a", [endpoint("10.0.0.4")])])])
    discovery = resolver(api, cache_ttl=30)
    results = await asyncio.gather(*(discovery.resolve("catalog") for _ in range(20)))
    assert all(result == results[0] for result in results)
    assert len(api.calls) == 1
    await discovery.aclose()


@pytest.mark.asyncio
async def test_propagates_cancellation_and_normalizes_provider_failures() -> None:
    class CancelApi(FakeApi):
        async def list_namespaced_endpoint_slice(self, namespace: str, **kwargs: Any) -> NS:
            raise asyncio.CancelledError

    discovery = resolver(CancelApi())
    with pytest.raises(asyncio.CancelledError):
        await discovery.resolve("catalog")
    await discovery.aclose()

    provider_error = RuntimeError("bearer-secret")
    provider_error.status = 403  # type: ignore[attr-defined]
    discovery = resolver(FakeApi(error=provider_error))
    with pytest.raises(DiscoveryError, match="lookup failed") as caught:
        await discovery.resolve("catalog")
    assert "bearer-secret" not in str(caught.value)
    await discovery.aclose()


@pytest.mark.asyncio
async def test_not_found_is_empty_and_other_limits_fail_closed() -> None:
    not_found = RuntimeError("not found")
    not_found.status = 404  # type: ignore[attr-defined]
    discovery = resolver(FakeApi(error=not_found))
    assert await discovery.resolve("missing") == ()
    await discovery.aclose()

    discovery = resolver(
        FakeApi([page([], "page-2"), page([], "page-3")]), max_pages=1, cache_ttl=0
    )
    with pytest.raises(DiscoveryError, match="page limit"):
        await discovery.resolve("catalog")
    await discovery.aclose()


@pytest.mark.asyncio
async def test_rejects_unsupported_address_types_and_oversized_api_pages() -> None:
    discovery = resolver(
        FakeApi(
            [page([slice_item("catalog-a", [endpoint("attacker.example")], address_type="FQDN")])]
        )
    )
    with pytest.raises(DiscoveryError, match="unsupported endpoint type"):
        await discovery.resolve("catalog")
    await discovery.aclose()

    discovery = resolver(
        FakeApi(
            [
                page(
                    [
                        slice_item("catalog-a", [endpoint("10.0.0.1")]),
                        slice_item("catalog-b", [endpoint("10.0.0.2")]),
                    ]
                )
            ]
        ),
        page_size=1,
    )
    with pytest.raises(DiscoveryError, match="invalid EndpointSlice page"):
        await discovery.resolve("catalog")
    await discovery.aclose()


@pytest.mark.asyncio
async def test_close_waits_for_active_resolution_and_rejects_new_work() -> None:
    started = asyncio.Event()
    finish = asyncio.Event()

    class SlowApi(FakeApi):
        async def list_namespaced_endpoint_slice(self, namespace: str, **kwargs: Any) -> NS:
            started.set()
            await finish.wait()
            return page([])

    closed: list[bool] = []

    async def close() -> None:
        closed.append(True)

    discovery = KubernetesServiceDiscovery(
        SlowApi(), namespace="apps", port_name="http", close=close
    )
    resolve_task = asyncio.create_task(discovery.resolve("catalog"))
    await started.wait()
    close_task = asyncio.create_task(discovery.aclose())
    await asyncio.sleep(0)
    assert not close_task.done()
    finish.set()
    await resolve_task
    await close_task
    await discovery.aclose()
    assert closed == [True]
    with pytest.raises(DiscoveryError, match="closed"):
        await discovery.resolve("catalog")


@pytest.mark.asyncio
async def test_in_cluster_factory_accepts_sync_loader_and_owns_isolated_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state: dict[str, Any] = {}

    class Configuration:
        pass

    class ApiClient:
        def __init__(self, *, configuration: Configuration) -> None:
            state["configuration"] = configuration
            state["closed"] = False

        async def close(self) -> None:
            state["closed"] = True

    class Client:
        DiscoveryV1Api = staticmethod(lambda api_client: FakeApi([page([])]))

    Client.Configuration = Configuration
    Client.ApiClient = ApiClient

    class Config:
        @staticmethod
        def load_incluster_config(*, client_configuration: Configuration) -> None:
            state["loaded_configuration"] = client_configuration

    monkeypatch.setattr(discovery_module, "_load_sdk", lambda: (Client, Config))
    discovery = await KubernetesServiceDiscovery.in_cluster(namespace="apps", port_name="http")
    assert state["loaded_configuration"] is state["configuration"]
    assert await discovery.resolve("catalog") == ()
    await discovery.aclose()
    assert state["closed"] is True

    with pytest.raises(ValueError, match="namespace"):
        await KubernetesServiceDiscovery.in_cluster(namespace="Bad/namespace", port_name="http")
    assert state["closed"] is True


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"namespace": "bad/name", "port_name": "http"}, "namespace"),
        ({"namespace": "apps", "port_name": "HTTP"}, "port_name"),
        ({"namespace": "apps", "port_name": "http", "page_size": 501}, "page_size"),
        ({"namespace": "apps", "port_name": "http", "cache_ttl": float("nan")}, "cache_ttl"),
    ],
)
def test_rejects_invalid_configuration(kwargs: dict[str, Any], message: str) -> None:
    values: dict[str, Any] = {"namespace": "apps", "port_name": "http"}
    values.update(kwargs)
    with pytest.raises(ValueError, match=message):
        KubernetesServiceDiscovery(FakeApi(), **values)
