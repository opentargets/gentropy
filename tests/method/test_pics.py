"""Test PICS finemapping."""

from __future__ import annotations

import pyspark.sql.functions as f
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
        observed = (
            df.withColumn(
                "credibleSet",
                f.when(
                    f.size("credibleSet") > 0,
                    _finemap_udf(f.col("credibleSet"), f.col("neglog_pvalue")),
                ).otherwise(f.col("credibleSet")),
            )
            .limit(1)
            .collect()[0]["credibleSet"]
        )

        assert observed == []
