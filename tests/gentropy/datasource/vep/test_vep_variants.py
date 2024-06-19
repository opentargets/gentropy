"""Testing VEP parsing and variant index extraction."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import DataFrame

from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.variant_effect_predictor.variants import (
    VariantEffectPredictorParser,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestVEPParser:
    """Testing VEP parser class.

    Some of the input data:
    - 6_151445307_C_T - complicated variant with numerous annotations.
    - 2_140699625_G_GT - simple variant with no annotations whatsoever.
    """

    @pytest.fixture(autouse=True)
    def _setup(self: TestVEPParser, spark: SparkSession) -> None:
        """Setup fixture."""
        self.raw_vep_output = spark.read.json(
            "tests/gentropy/data_samples/vep_sample.jsonl",
            schema=VariantEffectPredictorParser.get_vep_schema(),
        )
        self.processed_vep_output = VariantEffectPredictorParser.process_vep_output(
            self.raw_vep_output
        )

    def test_process(self: TestVEPParser) -> None:
        """Test process method."""
        df = VariantEffectPredictorParser.process_vep_output(self.raw_vep_output)
        assert isinstance(df, DataFrame), "Processed VEP output is not a DataFrame."
        assert df.count() > 0, "No variant data in processed VEP dataframe."

    def test_conversion(self: TestVEPParser) -> None:
        """Test if processed data can be converted into a VariantIndex object."""
        variant_index = VariantIndex(
            _df=self.processed_vep_output,
            _schema=VariantIndex.get_schema(),
        )

        assert isinstance(
            variant_index, VariantIndex
        ), "VariantIndex object not created."

    def test_variant_count(self: TestVEPParser) -> None:
        """Test if the number of variants is correct.

        It is expected that all rows from the parsed VEP output are present in the processed VEP output.
        """
        assert (
            self.raw_vep_output.count() == self.processed_vep_output.count()
        ), f"Incorrect number of variants in processed VEP output: expected {self.raw_vep_output.count()}, got {self.processed_vep_output.count()}."
