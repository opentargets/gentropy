---
title: Installation
hide:
  - navigation
  - toc
---

## Requirements

To install gentropy one needs to have pre installed:

- Python >=3.10, <3.13
- PySpark >=3.5.0, <3.6
- Java 11, 17 (for hail support Java 11 is recommended, see [troubleshooting](development/troubleshooting.md) for more details)

## Installation

To install Gentropy we recoomend using [uv](https://docs.astral.sh/uv/), which is a tool for managing Python environments and dependencies.

```bash
uv add gentropy
```

## Pypi

We recommend installing Open Targets Gentropy using Pypi:

```bash
pip install gentropy
```

## Source

Alternatively, you can install Open Targets Gentropy from source. Check the [contributing](development/contributing.md) section for more information.

For any issues with the installation, check the [troubleshooting section](development/troubleshooting.md).
