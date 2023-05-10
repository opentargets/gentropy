"""Test PICS finemapping."""

from __future__ import annotations

import pyspark.sql.functions as f
import pytest
from pyspark.sql.types import ArrayType

from otg.dataset.study_locus import StudyLocus
from otg.method.pics import PICS


def test_pics(mock_study_locus: StudyLocus) -> None:
    """Test PICS."""
    assert isinstance(PICS.finemap(mock_study_locus), StudyLocus)


class TestFinemap:
    """Test PICS finemap function under different scenarios."""

    def test_finemap_empty_array(
        self: TestFinemap, mock_study_locus: StudyLocus
    ) -> None:
        """Test finemap works when `credibleSet` is an empty array by returning an empty array."""
        df = mock_study_locus.df.select(
            "variantId",
            f.rand().alias("neglog_pvalue"),
            f.array().alias("credibleSet"),
        )
        observed = (
            df.select(self._finemap_udf_expr.alias("credibleSet"))
            .limit(1)
            .collect()[0]["credibleSet"]
        )

        assert observed == []

    def test_finemap_null_r2(self: TestFinemap, mock_study_locus: StudyLocus) -> None:
        """Test finemap works when `r2Overall` is null by returning the same `credibleSet` content."""
        df = mock_study_locus.df.withColumn(
            "credibleSet",
            f.transform(
                "credibleSet",
                lambda x: x.withField("r2Overall", f.lit(None)),
            ),
        ).select("variantId", f.rand().alias("neglog_pvalue"), "credibleSet")

        observed = (
            df.select(self._finemap_udf_expr.alias("credibleSet"))
            .limit(1)
            .collect()[0]["credibleSet"]
        )
        print(observed)  # failing, nonetype is not iterable

    @pytest.fixture(autouse=True)
    def _setup(self: TestFinemap, mock_study_locus: StudyLocus) -> None:
        """Prepares common fixtures for the tests."""
        credset_schema = ArrayType(
            [
                field.dataType.elementType  # type: ignore
                for field in mock_study_locus.schema
                if field.name == "credibleSet"
            ][0]
        )
        _finemap_udf = f.udf(
            lambda credible_set, neglog_p: PICS._finemap(credible_set, neglog_p),
            credset_schema,
        )
        self._finemap_udf_expr = f.when(
            f.size("credibleSet") > 0,
            _finemap_udf(f.col("credibleSet"), f.col("neglog_pvalue")),
        ).otherwise(f.col("credibleSet"))
