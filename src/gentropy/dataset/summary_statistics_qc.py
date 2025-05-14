"""Summary statistics QC dataset."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import reduce
from typing import TYPE_CHECKING, Concatenate

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.sumstat_quality_controls import (
    gc_lambda_check,
    mean_beta_check,
    number_of_variants,
    p_z_test,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


@dataclass
class QCTest:
    """Container class for QC tests."""

    columns: list[str]
    function: Callable[Concatenate[DataFrame, ...], DataFrame]

    def call_test(self, df: DataFrame) -> DataFrame:
        """Call the QC test function.

        Args:
            df (DataFrame): The input DataFrame.

        Returns:
            DataFrame: The output DataFrame with the QC test results.
        """
        return self.function(df).select("studyId", *self.columns)


@dataclass
class SummaryStatisticsQC(Dataset):
    """Summary Statistics Quality Controls dataset.

    Examples:
        >>> data = [("S1", 0.45, 6.78, 8.47, 0.55, 2, 1), ("S2", 0.26, -2.15, 4.38, 0.04, 2, 0)]
        >>> df = spark.createDataFrame(data, schema=SummaryStatisticsQC.get_schema())
        >>> qc = SummaryStatisticsQC(_df=df)
        >>> isinstance(qc, SummaryStatisticsQC)
        True
        >>> qc.df.show()
        +-------+---------+------------+----------+---------+----------+--------------+
        |studyId|mean_beta|mean_diff_pz|se_diff_pz|gc_lambda|n_variants|n_variants_sig|
        +-------+---------+------------+----------+---------+----------+--------------+
        |     S1|     0.45|        6.78|      8.47|     0.55|         2|             1|
        |     S2|     0.26|       -2.15|      4.38|     0.04|         2|             0|
        +-------+---------+------------+----------+---------+----------+--------------+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[SummaryStatisticsQC]) -> StructType:
        """Provide the schema for the SummaryStatisticsQC dataset.

        Returns:
            StructType: The schema for the SummaryStatisticsQC dataset.
        """
        return parse_spark_schema("summary_statistics_qc.json")

    @classmethod
    def from_summary_statistics(
        cls: type[SummaryStatisticsQC],
        gwas: SummaryStatistics,
        pval_threshold: float = 1e-8,
    ) -> SummaryStatisticsQC:
        """The function calculates the quality control metrics for the summary statistics.

        Args:
            gwas (SummaryStatistics): The instance of the SummaryStatistics class.
            pval_threshold (float): The p-value threshold for the QC. Default is 1e-8.

        Returns:
            SummaryStatisticsQC: Dataset with quality control metrics for the summary statistics.

        Examples:
            >>> from pyspark.sql import functions as f
            >>> s = 'studyId STRING, variantId STRING, chromosome STRING, position INT, beta DOUBLE, standardError DOUBLE, pValueMantissa FLOAT, pValueExponent INTEGER'
            >>> v1 = [("S1", "1_10000_A_T", "1", 10000, 1.0, 0.2, 9.9, -20), ("S1", "X_10001_C_T", "X", 10001, -0.1, 0.2, 1.0, -1)]
            >>> v2 = [("S2", "1_10001_C_T", "1", 10001, 0.028, 0.2, 1.0, -1), ("S2", "1_10002_G_C", "1", 10002, 0.5, 0.1, 1.0, -1)]
            >>> df = spark.createDataFrame(v1 + v2, s)
            >>> df.show()
            +-------+-----------+----------+--------+-----+-------------+--------------+--------------+
            |studyId|  variantId|chromosome|position| beta|standardError|pValueMantissa|pValueExponent|
            +-------+-----------+----------+--------+-----+-------------+--------------+--------------+
            |     S1|1_10000_A_T|         1|   10000|  1.0|          0.2|           9.9|           -20|
            |     S1|X_10001_C_T|         X|   10001| -0.1|          0.2|           1.0|            -1|
            |     S2|1_10001_C_T|         1|   10001|0.028|          0.2|           1.0|            -1|
            |     S2|1_10002_G_C|         1|   10002|  0.5|          0.1|           1.0|            -1|
            +-------+-----------+----------+--------+-----+-------------+--------------+--------------+
            <BLANKLINE>

            ** This method outputs one value per study, mean beta, mean diff pz, se diff pz, gc lambda, n variants and n variants sig**

            >>> stats = SummaryStatistics(df)
            >>> qc = SummaryStatisticsQC.from_summary_statistics(stats)
            >>> isinstance(qc, SummaryStatisticsQC)
            True
            >>> mean_beta = f.round("mean_beta", 2).alias("mean_beta")
            >>> mean_diff_pz = f.round("mean_diff_pz", 2).alias("mean_diff_pz")
            >>> se_diff_pz = f.round("se_diff_pz", 2).alias("se_diff_pz")
            >>> gc_lambda = f.round("gc_lambda", 2).alias("gc_lambda")
            >>> qc.df.select('studyId', mean_beta, mean_diff_pz, se_diff_pz, gc_lambda, 'n_variants', 'n_variants_sig').show()
            +-------+---------+------------+----------+---------+----------+--------------+
            |studyId|mean_beta|mean_diff_pz|se_diff_pz|gc_lambda|n_variants|n_variants_sig|
            +-------+---------+------------+----------+---------+----------+--------------+
            |     S1|     0.45|        6.78|      8.47|     0.55|         2|             1|
            |     S2|     0.26|       -2.15|      4.38|     0.04|         2|             0|
            +-------+---------+------------+----------+---------+----------+--------------+
            <BLANKLINE>
        """
        n_variants: Callable[[DataFrame], DataFrame] = lambda df: number_of_variants(
            df, pval_threshold
        )
        QC_TESTS = [
            QCTest(["mean_beta"], mean_beta_check),
            QCTest(["mean_diff_pz", "se_diff_pz"], p_z_test),
            QCTest(["gc_lambda"], gc_lambda_check),
            QCTest(["n_variants", "n_variants_sig"], n_variants),
        ]

        qc = reduce(
            lambda qc1, qc2: qc1.join(qc2, on="studyId", how="outer"),
            [test.call_test(gwas.df) for test in QC_TESTS],
        )

        return cls(_df=qc)
