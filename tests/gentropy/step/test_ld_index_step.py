"""Tests of the LD index step."""

from __future__ import annotations

from pathlib import Path

import pytest
from omegaconf import ListConfig
from pyspark.sql import SparkSession

from gentropy.dataset.ld_index import LDIndex
from gentropy.ld_index import LDIndexStep


def _index(spark: SparkSession, rows: list[tuple[str, str, float]]) -> LDIndex:
    """LD index from (population, chromosome, r) rows of one self pair each.

    Args:
        spark (SparkSession): Spark session.
        rows (list[tuple[str, str, float]]): One tuple per row.

    Returns:
        LDIndex: The index.
    """
    return LDIndex(
        _df=spark.createDataFrame(
            [(pop, chr_, f"{chr_}_1_A_T", f"{chr_}_1_A_T", r) for pop, chr_, r in rows],
            LDIndex.get_schema(),
        ),
        _schema=LDIndex.get_schema(),
    )


def test_write_replaces_only_the_populations_written(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Rebuilding a population replaces all of it and leaves the others as they were."""
    path = (tmp_path / "ld_index").as_posix()
    LDIndexStep._write(_index(spark, [("nfe", "1", 0.1), ("nfe", "2", 0.1)]), path)
    LDIndexStep._write(_index(spark, [("fin", "1", 0.2)]), path)
    LDIndexStep._write(_index(spark, [("nfe", "1", 0.3)]), path)

    rows = spark.read.schema(LDIndex.get_schema()).parquet(path).collect()

    assert sorted((row.population, row.chromosome, row.r) for row in rows) == [
        ("fin", "1", 0.2),
        ("nfe", "1", 0.3),
    ]


def test_write_refuses_several_populations(spark: SparkSession, tmp_path: Path) -> None:
    """One population per write, so a rebuild never replaces another population."""
    with pytest.raises(ValueError, match="one population"):
        LDIndexStep._write(
            _index(spark, [("nfe", "1", 0.1), ("fin", "1", 0.1)]),
            (tmp_path / "ld_index").as_posix(),
        )


def test_chromosomes_from_hydra_filter_rows(spark: SparkSession) -> None:
    """A Hydra list option arrives as a ListConfig; it filters like a list."""
    df = spark.createDataFrame([("1",), ("22",), ("X",)], "chromosome string")

    kept = LDIndexStep._on_chromosomes(df, ListConfig(["22", "X"]))

    assert sorted(row.chromosome for row in kept.collect()) == ["22", "X"]
