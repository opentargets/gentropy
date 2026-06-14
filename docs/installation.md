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

### Install matrix

Gentropy ships optional extras that unlock additional functional surfaces.
Choose the install command for your use case:

| Use case                                                                      | Command                      |
| ----------------------------------------------------------------------------- | ---------------------------- |
| Spark-only steps                                                              | `pip install gentropy`       |
| + hail-backed datasources (gnomAD LD, FinnGen finemapping, PanUKBB LD, susie) | `pip install gentropy[hail]` |
| + L2G training, prediction, and HuggingFace Hub publishing                    | `pip install gentropy[l2g]`  |
| Full pipeline (hail + L2G)                                                    | `pip install gentropy[all]`  |

If a hail-backed or L2G step is invoked from an environment without the matching extra, gentropy raises an `ImportError` naming the missing extra.

## Source

Alternatively, you can install Open Targets Gentropy from source. Check the [contributing](development/contributing.md) section for more information.

For any issues with the installation, check the [troubleshooting section](development/troubleshooting.md).

## xgboost

To use the gentropy `LocusToGene` model the `xgboost` package is required.
It ships behind the `[l2g]` extra (`pip install gentropy[l2g]`). To reduce
the size of the dependencies, gentropy uses the full `xgboost` package only
when `xgboost-cpu` is not available:

- `amd64` and `x86_64` will utilize `xgboost-cpu`.
- `arm64` and `aarch64` will utilize `xgboost`.
