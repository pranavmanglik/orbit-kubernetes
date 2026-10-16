# Orbit Kubernetes: development

The Python adapter is an async provider for `orbit-discovery`. Most development and regression
work can be done without a Kubernetes cluster: tests inject a small fake EndpointSlice API and
exercise pagination, validation, caching, concurrency, cancellation, and client ownership.

## Set up and validate

From the repository root, install the development tools and optional Kubernetes SDK, then run the
same checks used by the package workflow:

```bash
python -m pip install -e '.[dev,kubernetes]' build
pytest
ruff check src tests
ruff format --check src tests
mypy
python -m build
```

The `kubernetes` extra is optional at runtime and is required only to use the kubeconfig and
in-cluster factories. Tests of the adapter contract should prefer injected fake clients so they
remain deterministic and do not depend on credentials, network access, or a live cluster.

## Making a change

- Keep the `orbit-discovery` contract provider-neutral. Kubernetes API objects and SDK types belong
  in this adapter.
- Add focused tests for API call bounds, malformed provider responses, lifecycle, and failure
  behavior when those areas change.
- Document new options with defaults and limits in the README or operations guide. Describe which
  client owns each resource and how cancellation and failures behave.
- Keep errors useful without embedding provider response bodies, credentials, or raw configuration.
- If behavior differs between the Python and Go variants, document the difference and preserve the
  shared public discovery contract where possible.

## Live-cluster checks

Fake-client tests do not verify a Kubernetes SDK against a particular server, a ServiceAccount's
effective RBAC, token rotation, reverse-proxy behavior, pod egress policy, or rolling-shutdown
behavior. Exercise those separately in a disposable representative cluster before release. Record
the Kubernetes server and SDK versions and the exact deployment setup; do not present local checks
as evidence of hosted CI or production validation.

## Documentation and project policies

Update package-specific guides and docstrings whenever public behavior, configuration, ownership,
or security assumptions change. Follow the repository's contribution, code of conduct, license,
and security policy linked from the [documentation index](../README.md).
