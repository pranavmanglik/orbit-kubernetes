# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Async Kubernetes EndpointSlice adapter for ``orbit-discovery``."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import ipaddress
import re
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import Any, Protocol

from orbit_discovery import DiscoveryError, ServiceInstance

_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_PORT_NAME = re.compile(r"[a-z](?:[a-z0-9-]{0,13}[a-z0-9])?\Z")
_MAX_CACHE_SERVICES = 1_024
_MAX_ENDPOINTS = 4_096
_LOCK_STRIPES = 64


class _EndpointSliceApi(Protocol):
    """Small async surface required from Kubernetes DiscoveryV1Api."""

    async def list_namespaced_endpoint_slice(self, namespace: str, **kwargs: Any) -> Any:
        """List EndpointSlices in a namespace."""


class KubernetesServiceDiscovery:
    """Resolve Kubernetes Services from bounded, cached EndpointSlice snapshots.

    The adapter owns its API client, cache, and in-flight request lifecycle. Construct it with one
    Kubernetes namespace and a Service port name so that each lookup uses a least-privilege,
    unambiguous EndpointSlice query. API credentials and provider errors are never included in
    diagnostics.
    """

    def __init__(
        self,
        api: _EndpointSliceApi,
        *,
        namespace: str,
        port_name: str,
        scheme: str = "http",
        cache_ttl: float = 5.0,
        max_cache_services: int = 128,
        page_size: int = 100,
        max_pages: int = 64,
        request_timeout: float = 5.0,
        close: Callable[[], Any] | None = None,
    ) -> None:
        """Create an adapter around an async DiscoveryV1Api-like client.

        ``close`` is used by tests and custom transports. Factory-created clients are closed by
        the factory-owned callback. All size and duration settings are capped to avoid unbounded
        work or resource retention from caller configuration.
        """
        if not isinstance(namespace, str) or _DNS_LABEL.fullmatch(namespace) is None:
            raise ValueError("namespace must be a lowercase Kubernetes DNS label.")
        if not isinstance(port_name, str) or _PORT_NAME.fullmatch(port_name) is None:
            raise ValueError("port_name must be a valid lowercase Kubernetes port name.")
        if not isinstance(scheme, str) or scheme not in {"http", "https"}:
            raise ValueError("scheme must be http or https.")
        _bounded_number(cache_ttl, "cache_ttl", 0.0, 300.0)
        _bounded_int(max_cache_services, "max_cache_services", 1, _MAX_CACHE_SERVICES)
        _bounded_int(page_size, "page_size", 1, 500)
        _bounded_int(max_pages, "max_pages", 1, 64)
        _bounded_number(request_timeout, "request_timeout", 0.05, 60.0)
        if not callable(getattr(api, "list_namespaced_endpoint_slice", None)):
            raise TypeError("api must provide an async EndpointSlice listing operation.")
        if close is not None and not callable(close):
            raise TypeError("close must be callable or None.")
        self._api = api
        self._namespace = namespace
        self._port_name = port_name
        self._scheme = scheme
        self._cache_ttl = cache_ttl
        self._max_cache_services = max_cache_services
        self._page_size = page_size
        self._max_pages = max_pages
        self._request_timeout = request_timeout
        self._close = close
        self._cache: OrderedDict[str, tuple[float, tuple[ServiceInstance, ...]]] = OrderedDict()
        self._locks = tuple(asyncio.Lock() for _ in range(_LOCK_STRIPES))
        self._condition = asyncio.Condition()
        self._close_lock = asyncio.Lock()
        self._close_done = False
        self._active = 0
        self._closed = False

    @classmethod
    async def from_kubeconfig(
        cls,
        *,
        namespace: str,
        port_name: str,
        config_file: str | None = None,
        context: str | None = None,
        **options: Any,
    ) -> KubernetesServiceDiscovery:
        """Load user kubeconfig into an isolated async client configuration."""
        client, config = _load_sdk()
        configuration = client.Configuration()
        api_client: Any | None = None
        try:
            await config.load_kube_config(
                config_file=config_file,
                context=context,
                client_configuration=configuration,
            )
            api_client = client.ApiClient(configuration=configuration)
            api = client.DiscoveryV1Api(api_client)
        except asyncio.CancelledError:
            if api_client is not None:
                await _close_factory_client(api_client)
            raise
        except Exception:
            if api_client is not None:
                await _close_factory_client(api_client)
            raise DiscoveryError("Could not configure the Kubernetes API client.") from None
        try:
            return cls(
                api, namespace=namespace, port_name=port_name, close=api_client.close, **options
            )
        except BaseException:
            await _close_factory_client(api_client)
            raise

    @classmethod
    async def in_cluster(
        cls,
        *,
        namespace: str,
        port_name: str,
        **options: Any,
    ) -> KubernetesServiceDiscovery:
        """Load service-account credentials into an isolated in-cluster client."""
        client, config = _load_sdk()
        configuration = client.Configuration()
        api_client: Any | None = None
        try:
            config.load_incluster_config(client_configuration=configuration)
            api_client = client.ApiClient(configuration=configuration)
            api = client.DiscoveryV1Api(api_client)
        except asyncio.CancelledError:
            if api_client is not None:
                await _close_factory_client(api_client)
            raise
        except Exception:
            if api_client is not None:
                await _close_factory_client(api_client)
            raise DiscoveryError(
                "Could not configure the in-cluster Kubernetes API client."
            ) from None
        try:
            return cls(
                api, namespace=namespace, port_name=port_name, close=api_client.close, **options
            )
        except BaseException:
            await _close_factory_client(api_client)
            raise

    async def resolve(self, service: str) -> tuple[ServiceInstance, ...]:
        """Return a cached or freshly listed endpoint snapshot for one Service."""
        normalized = _validate_service(service)
        async with self._condition:
            self._ensure_open()
            self._active += 1
        try:
            return await self._resolve_active(normalized)
        finally:
            async with self._condition:
                self._active -= 1
                if self._active == 0:
                    self._condition.notify_all()

    async def aclose(self) -> None:
        """Stop accepting work, wait for active requests, then close the owned client once."""
        async with self._close_lock:
            if self._close_done:
                return
            async with self._condition:
                self._closed = True
                await self._condition.wait_for(lambda: self._active == 0)
            self._cache.clear()
            if self._close is not None:
                try:
                    result = self._close()
                    if hasattr(result, "__await__"):
                        await result
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise DiscoveryError("Could not close the Kubernetes API client.") from None
            self._close_done = True

    async def __aenter__(self) -> KubernetesServiceDiscovery:
        """Return this open resolver as an async context manager."""
        async with self._condition:
            self._ensure_open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Close the owned client when leaving an async context."""
        await self.aclose()

    async def _resolve_active(self, service: str) -> tuple[ServiceInstance, ...]:
        cached = self._cache.get(service)
        now = time.monotonic()
        if cached is not None and now - cached[0] < self._cache_ttl:
            self._cache.move_to_end(service)
            return cached[1]
        lock = self._locks[hash(service) % len(self._locks)]
        async with lock:
            cached = self._cache.get(service)
            now = time.monotonic()
            if cached is not None and now - cached[0] < self._cache_ttl:
                self._cache.move_to_end(service)
                return cached[1]
            snapshot = await self._list_endpoints(service)
            if self._cache_ttl > 0:
                self._cache[service] = (time.monotonic(), snapshot)
                self._cache.move_to_end(service)
                while len(self._cache) > self._max_cache_services:
                    self._cache.popitem(last=False)
            return snapshot

    async def _list_endpoints(self, service: str) -> tuple[ServiceInstance, ...]:
        selector = f"kubernetes.io/service-name={service}"
        continuation: str | None = None
        pages = 0
        count = 0
        result: dict[tuple[str, int], ServiceInstance] = {}
        try:
            while True:
                pages += 1
                if pages > self._max_pages:
                    raise DiscoveryError("Kubernetes EndpointSlice page limit exceeded.")
                page = await self._api.list_namespaced_endpoint_slice(
                    self._namespace,
                    label_selector=selector,
                    limit=self._page_size,
                    _continue=continuation,
                    _request_timeout=self._request_timeout,
                )
                items = getattr(page, "items", None)
                if not isinstance(items, list) or len(items) > self._page_size:
                    raise DiscoveryError("Kubernetes returned an invalid EndpointSlice page.")
                for slice_item in items:
                    address_type = getattr(slice_item, "address_type", None)
                    if address_type not in {"IPv4", "IPv6"}:
                        raise DiscoveryError("Kubernetes returned an unsupported endpoint type.")
                    ports = _slice_ports(slice_item, self._port_name)
                    for endpoint in getattr(slice_item, "endpoints", ()) or ():
                        count += 1
                        if count > _MAX_ENDPOINTS:
                            raise DiscoveryError("Kubernetes endpoint limit exceeded.")
                        conditions = getattr(endpoint, "conditions", None)
                        if getattr(conditions, "ready", None) is False:
                            continue
                        for address in getattr(endpoint, "addresses", ()) or ():
                            for port in ports:
                                instance = _make_instance(
                                    service,
                                    self._namespace,
                                    getattr(getattr(slice_item, "metadata", None), "name", "slice"),
                                    address,
                                    port,
                                    self._scheme,
                                    address_type,
                                )
                                result[(instance.host, instance.port)] = instance
                                if len(result) > _MAX_ENDPOINTS:
                                    raise DiscoveryError("Kubernetes endpoint limit exceeded.")
                continuation = getattr(getattr(page, "metadata", None), "_continue", None) or None
                if continuation is None:
                    break
            return tuple(sorted(result.values(), key=lambda item: (item.host, item.port)))
        except asyncio.CancelledError:
            raise
        except DiscoveryError:
            raise
        except Exception as error:
            if getattr(error, "status", None) == 404:
                return ()
            raise DiscoveryError("Kubernetes EndpointSlice lookup failed.") from None

    def _ensure_open(self) -> None:
        """Reject operations once shutdown has started."""
        if self._closed:
            raise DiscoveryError("Kubernetes service discovery is closed.")


def _slice_ports(slice_item: Any, expected_name: str) -> tuple[int, ...]:
    result: set[int] = set()
    for port in getattr(slice_item, "ports", ()) or ():
        if getattr(port, "name", None) != expected_name:
            continue
        protocol = getattr(port, "protocol", None)
        number = getattr(port, "port", None)
        if protocol not in {None, "TCP"} or isinstance(number, bool) or not isinstance(number, int):
            continue
        if 1 <= number <= 65_535:
            result.add(number)
    return tuple(sorted(result))


def _make_instance(
    service: str,
    namespace: str,
    slice_name: str,
    address: str,
    port: int,
    scheme: str,
    address_type: str,
) -> ServiceInstance:
    """Create a stable endpoint from a canonical IP of the slice's declared address family."""
    try:
        parsed_address = ipaddress.ip_address(address)
    except (TypeError, ValueError):
        raise DiscoveryError("Kubernetes returned an invalid endpoint IP address.") from None
    if (address_type == "IPv4" and parsed_address.version != 4) or (
        address_type == "IPv6" and parsed_address.version != 6
    ):
        raise DiscoveryError("Kubernetes endpoint IP does not match its address type.")
    address = parsed_address.compressed
    digest = hashlib.sha256(f"{namespace}\0{service}\0{address}\0{port}".encode()).hexdigest()[:32]
    return ServiceInstance(
        service=service,
        instance_id=f"k8s-{digest}",
        host=address,
        port=port,
        scheme=scheme,
        metadata={"namespace": namespace, "slice": slice_name},
    )


def _validate_service(service: str) -> str:
    """Restrict names to safe DNS labels before composing a Kubernetes label selector."""
    if not isinstance(service, str) or _DNS_LABEL.fullmatch(service) is None:
        raise ValueError("service must be a lowercase Kubernetes DNS label.")
    return service


def _bounded_int(value: object, name: str, minimum: int, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}.")


def _bounded_number(
    value: object,
    name: str,
    minimum: float,
    maximum: float,
) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number between {minimum} and {maximum}.")
    number = float(value)
    if (
        number < minimum
        or number > maximum
        or not (number == number and abs(number) != float("inf"))
    ):
        raise ValueError(f"{name} must be a finite number between {minimum} and {maximum}.")


def _load_sdk() -> tuple[Any, Any]:
    """Import the optional async provider SDK only when a real client is requested."""
    try:
        client = importlib.import_module("kubernetes_asyncio.client")
        config = importlib.import_module("kubernetes_asyncio.config")
    except ImportError:
        raise RuntimeError(
            "Install orbit-kubernetes[kubernetes] to use Kubernetes client factories."
        ) from None
    return client, config


async def _close_factory_client(api_client: Any) -> None:
    """Best-effort close a partially configured SDK client without masking setup failures."""
    try:
        result = api_client.close()
        if hasattr(result, "__await__"):
            await result
    except BaseException:
        return


__all__ = ["KubernetesServiceDiscovery"]
