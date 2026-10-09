"""Credible set variant dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


class CredibleSetVariant(Dataset):
    """CredibleSetVariant dataset.

    This dataset represents variant view of the fine-mapped credible set.

    The individual variants have 3 effects:
    * reportedBeta & reportedStandardError - Reflecting marginal association test statistics.
    * conditionalBeta & conditionalStandardError - corresponding to effect of the variant conditioned
        over the other variant effects in the fine-mapped locus, this will be typically `$mu$` and `$omega$` from SuSiE.
    * rescaledAbsBeta, rescaledDirection and rescaledStandardError - Correspond to the effect of the
        variant after rescaling it's value based on the reported pValue & effect direction from marginal test.
        The `rescaled` statistics are only available when original `SummaryStatistics` were annotated after
        clumping, as the process requires the marginal statistics. To get the actual `rescaledBeta` one need to
        multiply the `rescaledDirection` and `rescaledAbsBeta`

    ??? tip "Joining with other datasets"
        This dataset can be joined with:
        * `VariantIndex` by `variantId` to get the variant functional annotations
        * `CredibleSet` by `credibleSetId` to get the fine-mapped locus information

    Examples:
        >>> data = [
        ...     ("0feec5fa23383729341ed71454dc665d", "1_816376_T_C", 1.4784516, -11, 0.21, 5000, 2.1, 0.01, 0.95, 3959.79, None, -0.21, 0.21, 0.21, 0.21, -1, []),
        ... ]
        >>> dataset = CredibleSetVariant(_df=spark.createDataFrame(data, CredibleSetVariant.get_schema()))
        >>> dataset.df.show(truncate=False)
        +--------------------------------+------------+--------------+--------------+---------------------+----------+------------+---------------------+--------------------+----------------+---------+---------------+------------------------+---------------+---------------------+-----------------+---------------+
        |credibleSetId                   |variantId   |pValueMantissa|pValueExponent|effectAlleleFrequency|sampleSize|reportedBeta|reportedStandardError|posteriorProbability|log10BayesFactor|r2Overall|conditionalBeta|conditionalStandardError|rescaledAbsBeta|rescaledStandardError|rescaledDirection|qualityControls|
        +--------------------------------+------------+--------------+--------------+---------------------+----------+------------+---------------------+--------------------+----------------+---------+---------------+------------------------+---------------+---------------------+-----------------+---------------+
        |0feec5fa23383729341ed71454dc665d|1_816376_T_C|1.4784516     |-11           |0.21                 |5000      |2.1         |0.01                 |0.95                |3959.79         |NULL     |-0.21          |0.21                    |0.21           |0.21                 |-1               |[]             |
        +--------------------------------+------------+--------------+--------------+---------------------+----------+------------+---------------------+--------------------+----------------+---------+---------------+------------------------+---------------+---------------------+-----------------+---------------+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[CredibleSetVariant]) -> StructType:
        """Provides the schema for the CredibleSetVariant dataset.

        Returns:
            StructType: Schema for the CredibleSetVariant dataset
        """
        return parse_spark_schema("credible_set_variant.json")
