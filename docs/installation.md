---
title: Installation
hide:
  - navigation
  - toc
---

## Requirements

To install gentropy one needs to have pre installed:

- Python >=3.11, <3.14
- PySpark >=3.5.0, <3.6
- Java 11, 17 (for hail support Java 11 is recommended, see [troubleshooting](development/troubleshooting.md) for more details)

## Installation

To install Gentropy we recommend using [uv](https://docs.astral.sh/uv/), which is a tool for managing Python environments and dependencies.

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

## xgboost

To use gentropy `LocusToGene` model the `xgboost` package is required. To reduce the size of the dependencies, gentropy uses the full `xgboost` package
only when `xgboost-cpu` is not available:

- `amd64` and `x86_64` will utilize `xgboost-cpu`.
- `arm64` and `aarch64` will utilize `xgboost`.
