# rolling-ops
[![Tests](https://github.com/canonical/charm-rolling-ops/actions/workflows/ci.yaml/badge.svg)](https://github.com/canonical/charm-rolling-ops/actions/workflows/ci.yaml)

## Overview

This repository hosts the `rollingops` charm library, which provides a reusable
mechanism for coordinating rolling operations (such as restarts or upgrades)
across units of a Juju application.

The repository also includes reference charms and tests that demonstrate and
validate how the library is intended to be used. These charms are **not intended
for production deployment**.

## The library

The `rollingops` charm library implements a locking mechanism to coordinate operations
within a single app. The logic is based in peer-relation databags. 

When the lock is granted, the library automatically runs a given callback.

### Versions

The Rolling Ops library follows Charmhub library versioning conventions.

| Version | Location |
|-------|----------|
| v1 | [./lib/charms/rolling_ops/v1/rollingops.py](./lib/charms/rolling_ops/v1/rollingops.py). |
| v0 | [./lib/charms/rolling_ops/v0/rollingops.py](./lib/charms/rolling_ops/v0/rollingops.py). |

While v1 builds on the same conceptual model as v0, it introduces
several important behavioral and structural improvements:

- Support for multiple queued lock requests per unit
- Support for deferring events inside callback functions
- Support for passing arguments to callback functions
- Explicit retry support with per-operation retry policies
- Deterministic scheduling:
  - Operations are scheduled in order of request
  - First-time requests are prioritized over retries

`v1` does not provide backward compatibility with `v0`.

Charm authors are strongly encouraged to adopt `v1` for new integrations.

## Usage

Follow standard Charmhub library inclusion practices:

1. Fetch the library using `charmcraft fetch-lib`

2. Import the library in your charm:

Explicitly import the version you depend on:

```python
from charms.rolling_ops.v1.rollingops import RollingOpsManagerV1
```

```python
from charms.rolling_ops.v0.rollingops import RollingOpsManager
```

3. Add a peer relation

```
peers:
    rolling_ops:
        interface: rolling_ops
```

4. Register callbacks and request rolling operations as needed

Operators may use this charm as a reference when including the `rollingops`
lib in their own charms.

## Repository layout
``` 
.
├── lib/
│   └── charms/
│       └── rolling_ops/
│           ├── v0/
│           └── v1/
├── src/
│   └── charm.py               # Reference charm for v1 (example usage)
├── tests/
│   ├── unit/                  # Unit & scenario tests for the library
│   ├── integration/           # Integration tests using Juju
│   └── charms/
│       └── v0/                # Test-only charm for integration tests
└── README.md
```

## Testing

This repository includes tests for the library:

- Unit tests
```
tox -e unit
```

- Integration tests
```
tox -e integration
```
- Manual testing instructions are included in [./tests/QA.md](./tests/QA.md)

## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines
on enhancements to this charm following best practice guidelines, and [CONTRIBUTING.md](./CONTRIBUTING.md) for developer guidance.

## License

Apache-2.0. See [LICENSE](./LICENSE)` for details.