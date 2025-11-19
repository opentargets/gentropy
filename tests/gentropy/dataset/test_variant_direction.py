"""Test variant direction dataset."""

import pytest
from pyspark.sql import Row
from pyspark.sql import types as t

from gentropy import VariantIndex
from gentropy.common.session import Session
from gentropy.dataset.variant_direction import VariantDirection


class TestVariantDirection:
    """Test variant direction dataset."""

    @pytest.fixture
    def variant_index(self, session: Session) -> VariantIndex:
        """VariantIndex fixture."""
        data = [
            Row(
                chromosome="1",
                position=100,
                referenceAllele="A",
                alternateAllele="C",
                variantId="1_100_A_C",
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                position=100,
                referenceAllele="ACT",
                alternateAllele="G",
                variantId="1_100_ACT_G",
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                position=100,
                referenceAllele="A",
                alternateAllele="T",
                variantId="1_100_A_T",
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
        ]
        schema = t.StructType(
            [
                t.StructField("chromosome", t.StringType(), False),
                t.StructField("position", t.IntegerType(), False),
                t.StructField("referenceAllele", t.StringType(), False),
                t.StructField("alternateAllele", t.StringType(), False),
                t.StructField("variantId", t.StringType(), False),
                t.StructField(
                    "alleleFrequencies",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField("populationName", t.StringType(), False),
                                t.StructField("alleleFrequency", t.DoubleType(), False),
                            ]
                        )
                    ),
                ),
            ]
        )
        return VariantIndex(_df=session.spark.createDataFrame(data, schema))

    def test_from_variant_index(
        self, session: Session, variant_index: VariantIndex
    ) -> None:
        """Test from_variant_index."""
        variant_direction = VariantDirection.from_variant_index(
            variant_index=variant_index
        )
        # 3 variants x 2 directions x 2 strands
        assert variant_direction.df.count() == 2 * 4 + 2, "missing variants"
        exp_data = [
            Row(
                chromosome="1",
                originalVariantId="1_100_A_C",
                type=1,
                variantId="1_100_A_C",
                direction=1,
                strand=1,
                isStrandAmbiguous=False,
                originalAlleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_A_C",
                type=1,
                variantId="1_100_C_A",
                direction=-1,
                strand=1,
                isStrandAmbiguous=False,
                originalAlleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_A_C",
                type=1,
                variantId="1_100_T_G",
                direction=1,
                strand=-1,
                isStrandAmbiguous=False,
                originalAlleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_A_C",
                type=1,
                variantId="1_100_G_T",
                direction=-1,
                strand=-1,
                isStrandAmbiguous=False,
                originalAlleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_ACT_G",
                type=3,
                variantId="1_100_ACT_G",
                direction=1,
                strand=1,
                isStrandAmbiguous=False,
                originalAlleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_ACT_G",
                type=3,
                variantId="1_100_G_ACT",
                direction=-1,
                strand=1,
                isStrandAmbiguous=False,
                originalAlleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_ACT_G",
                type=3,
                variantId="1_100_AGT_C",
                direction=1,
                strand=-1,
                isStrandAmbiguous=False,
                originalAlleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_ACT_G",
                type=3,
                variantId="1_100_C_AGT",
                direction=-1,
                strand=-1,
                isStrandAmbiguous=False,
                originalAlleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_A_T",
                type=1,
                variantId="1_100_A_T",
                direction=1,
                strand=1,
                isStrandAmbiguous=True,
                originalAlleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_A_T",
                type=1,
                variantId="1_100_T_A",
                direction=-1,
                strand=1,
                isStrandAmbiguous=True,
                originalAlleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
        ]
        exp_df = session.spark.createDataFrame(exp_data, VariantDirection.get_schema())
        assert (
            variant_direction.df.select(exp_df.columns).collect() == exp_df.collect()
        ), "data does not match expected"
