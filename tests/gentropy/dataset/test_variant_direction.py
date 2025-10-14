"""Test variant direction dataset."""

import pytest
from pyspark.sql import Row
from pyspark.testing import assertDataFrameEqual

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
                reference_allele="A",
                alternate_allele="C",
                variantId="1_100_A_C",
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                position=100,
                reference_allele="ACT",
                alternate_allele="G",
                variantId="1_100_ACT_G",
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                position=100,
                reference_allele="A",
                alternate_allele="T",
                variantId="1_100_A_T",
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
        ]
        return VariantIndex(
            _df=session.spark.createDataFrame(
                data,
                VariantIndex.get_schema(),
            )
        )

    def test_from_variant_index(self, session: Session, variant_index: VariantIndex):
        """Test from_variant_index."""
        variant_direction = VariantDirection.from_variant_index(
            variant_index=variant_index
        )
        # 3 variants x 2 directions x 2 strands
        assert variant_direction.df.count() == 3 * 2 * 2, "missing variants"
        exp_data = [
            Row(
                chromosome="1",
                originalVariantId="1_100_A_C",
                type=1,
                variantId="1_100_A_C",
                direction=1,
                strand=1,
                isPalindromic=False,
                alleleFrequencies=[
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
                isPalindromic=False,
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_A_C",
                type=1,
                variantId="1_100_G_T",
                direction=1,
                strand=-1,
                isPalindromic=False,
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_A_C",
                type=1,
                variantId="1_100_T_G",
                direction=-1,
                strand=-1,
                isPalindromic=False,
                alleleFrequencies=[
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
                isPalindromic=True,
                alleleFrequencies=[
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
                isPalindromic=True,
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_A_T",
                type=1,
                variantId="1_100_T_A",
                direction=1,
                strand=-1,
                isPalindromic=True,
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_A_T",
                type=1,
                variantId="1_100_A_T",
                direction=-1,
                strand=-1,
                isPalindromic=True,
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_ACT_G",
                type=1,
                variantId="1_100_ACT_G",
                direction=1,
                strand=1,
                isPalindromic=False,
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_ACT_G",
                type=1,
                variantId="1_100_G_ACT",
                direction=-1,
                strand=1,
                isPalindromic=False,
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_ACT_G",
                type=1,
                variantId="1_100_CAG_T",
                direction=1,
                strand=-1,
                isPalindromic=False,
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
            Row(
                chromosome="1",
                originalVariantId="1_100_ACT_G",
                type=1,
                variantId="1_100_CAG_T",
                direction=1,
                strand=-1,
                isPalindromic=False,
                alleleFrequencies=[
                    Row(populationName="nfe_adj", alleleFrequency=0.1),
                    Row(populationName="fin_adj", alleleFrequency=0.2),
                ],
            ),
        ]
        exp_df = session.spark.createDataFrame(exp_data, VariantDirection.get_schema())
        assertDataFrameEqual(
            variant_direction.df.select(exp_df.columns).orderBy(
                "variantId", "strand", "direction"
            ),
            exp_df.orderBy("variantId", "strand", "direction"),
        )
