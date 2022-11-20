"""Tests on effect harmonisation."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest

from etl.gwas_ingest.effect_harmonization import get_reverse_complement

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@pytest.fixture
def mock_allele_columns(spark: SparkSession) -> DataFrame:
    """Mock Dataframe to test harmonisation."""
    return spark.createDataFrame(
        [
            {"allele": "A", "reverseComp": "T", "isPalindrome": False},
            {"allele": "C", "reverseComp": "G", "isPalindrome": False},
            {"allele": "T", "reverseComp": "A", "isPalindrome": False},
            {"allele": "G", "reverseComp": "C", "isPalindrome": False},
            {"allele": "AT", "reverseComp": "AT", "isPalindrome": True},
            {"allele": "TTGA", "reverseComp": "TCAA", "isPalindrome": False},
            {"allele": "-", "reverseComp": None, "isPalindrome": False},
            {"allele": None, "reverseComp": None, "isPalindrome": False},
            {"allele": "CATATG", "reverseComp": "CATATG", "isPalindrome": True},
        ]
    ).persist()


@pytest.fixture
def call_get_reverse_complement(mock_allele_columns: DataFrame) -> DataFrame:
    """Test reverse complement on mock data."""
    return mock_allele_columns.transform(
        lambda df: get_reverse_complement(df, "allele")
    )


def test_reverse_complement(call_get_reverse_complement: DataFrame) -> None:
    """Test reverse complement."""
    assert (
        call_get_reverse_complement.filter(
            f.col("reverseComp") != f.col("revcomp_allele")
        ).count()
    ) == 0
