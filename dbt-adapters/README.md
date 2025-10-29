<p align="center">
    <img
        src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg"
        alt="dbt logo"
        width="500"
    />
</p>

<p align="center">
    <a href="https://pypi.org/project/dbt-adapters/">
        <img src="https://badge.fury.io/py/dbt-adapters.svg" />
    </a>
    <a target="_blank" href="https://pypi.org/project/dbt-adapters/" style="background:none">
        <img src="https://img.shields.io/pypi/pyversions/dbt-adapters">
    </a>
    <a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg" />
    </a>
    <a href="https://github.com/python/mypy">
        <img src="https://www.mypy-lang.org/static/mypy_badge.svg" />
    </a>
    <a href="https://pepy.tech/project/dbt-athena">
        <img src="https://static.pepy.tech/badge/dbt-adapters/month" />
    </a>
</p>

# dbt-adapters

## Summary

The `dbt-adapters` package provides the foundational framework for building database adapters in dbt (data build tool). This package defines the common interfaces and base implementations that enable dbt to connect to and interact with various data platforms.

**What is dbt-adapters?**
- A Python package that provides base classes and protocols for creating dbt database adapters
- The core abstraction layer that allows dbt to work with different databases (Postgres, BigQuery, Snowflake, etc.)
- A standardized interface for SQL and non-SQL data platforms

**Who should use this?**
- Developers building custom dbt adapters for new data platforms
- Maintainers of existing dbt adapters
- Contributors to the dbt ecosystem

**Key Features:**
- Base adapter implementations for SQL and non-SQL platforms
- Connection management and credential handling
- Relation caching and metadata management
- Query execution and result handling
- Integration with dbt-core's compilation and execution engine

## Adapters

There are two major adapter types: [base](/dbt-adapters/src/dbt/adapters/base/impl.py) and [sql](/dbt-adapters/src/dbt/adapters/sql/impl.py).

### `base`

`BaseAdapter` defines the base functionality an adapter is required to implement in order to function with `dbt-core`.
There are several methods which have default implementations as well as methods that require the concrete adapter to implement them.

### `sql`

`SQLAdapter` inherits from `BaseAdapter`, updates default implementations to work with SQL-based platforms,
and defines additional required methods to support those defaults.

## Components

An adapter is composed of several components.

- connections
- dialect
- relation caching
- integration with `dbt-core`

The first two are platform-specific and require significant implementation in a concrete adapter.
The last two are largely implemented in `dbt-adapters` with minor adjustments in a concrete adapter.

### Connections

This component is responsible for creating and managing connections to storage and compute.

#### Files
- `dbt/adapters/{base|sql}/connections.py`

### Dialect

This component is responsible for translating a request from `dbt-core` into a specific set of actions on the platform.

#### Files
- `dbt/adapters/base/column.py`
- `dbt/adapters/base/query_headers.py`
- `dbt/adapters/base/relation.py`
- `dbt/adapters/relation_configs/*`
- `dbt/adapters/clients/jinja.py`
- `dbt/include/global_project/*`

### Relation caching

This component is responsible for managing a local cache of relations, relation metadata, and dependencies between relations.

#### Files
- `dbt/adapters/cache.py`

### Integration with `dbt-core`

This component is responsible for managing the interface between `dbt-core` and a concrete adapter.

#### Files
- `dbt/adapters/{base|sql}/impl.py`
- `dbt/adapters/base/meta.py`
- `dbt/adapters/base/plugin.py`
- `dbt/adapters/capability.py`
- `dbt/adapters/factory.py`
- `dbt/adapters/protocol.py`
- `dbt/adapters/contracts/*`
- `dbt/adapters/events/*`
- `dbt/adapters/exceptions/*`
