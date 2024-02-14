"""Summary satistics dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

import pyspark.sql.functions as f
from pyspark.sql.utils import AnalysisException

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.utils import parse_region, split_pvalue
from gentropy.dataset.dataset import Dataset
from gentropy.method.window_based_clumping import WindowBasedClumping

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.common.session import Session
    from gentropy.dataset.study_locus import StudyLocus


@dataclass
class SummaryStatistics(Dataset):
    """Summary Statistics dataset.

    A summary statistics dataset contains all single point statistics resulting from a GWAS.
    """

    @classmethod
    def get_schema(cls: type[SummaryStatistics]) -> StructType:
        """Provides the schema for the SummaryStatistics dataset.

        Returns:
            StructType: Schema for the SummaryStatistics dataset
        """
        return parse_spark_schema("summary_statistics.json")

    def pvalue_filter(self: SummaryStatistics, pvalue: float) -> SummaryStatistics:
        """Filter summary statistics based on the provided p-value threshold.

        Args:
            pvalue (float): upper limit of the p-value to be filtered upon.

        Returns:
            SummaryStatistics: summary statistics object containing single point associations with p-values at least as significant as the provided threshold.
        """
        # Converting p-value to mantissa and exponent:
        (mantissa, exponent) = split_pvalue(pvalue)

        # Applying filter:
        df = self._df.filter(
            (f.col("pValueExponent") < exponent)
            | (
                (f.col("pValueExponent") == exponent)
                & (f.col("pValueMantissa") <= mantissa)
            )
        )
        return SummaryStatistics(_df=df, _schema=self._schema)

    def window_based_clumping(
        self: SummaryStatistics,
        distance: int = 500_000,
        gwas_significance: float = 5e-8,
        baseline_significance: float = 0.05,
        locus_collect_distance: int | None = None,
    ) -> StudyLocus:
        """Generate study-locus from summary statistics by distance based clumping + collect locus.

        Args:
            distance (int): Distance in base pairs to be used for clumping. Defaults to 500_000.
            gwas_significance (float, optional): GWAS significance threshold. Defaults to 5e-8.
            baseline_significance (float, optional): Baseline significance threshold for inclusion in the locus. Defaults to 0.05.
            locus_collect_distance (int | None): The distance to collect locus around semi-indices. If not provided, locus is not collected.

        Returns:
            StudyLocus: Clumped study-locus containing variants based on window.
        """
        return (
            WindowBasedClumping.clump_with_locus(
                self,
                window_length=distance,
                p_value_significance=gwas_significance,
                p_value_baseline=baseline_significance,
                locus_window_length=locus_collect_distance,
            )
            if locus_collect_distance
            else WindowBasedClumping.clump(
                self,
                window_length=distance,
                p_value_significance=gwas_significance,
            )
        )

    def exclude_region(self: SummaryStatistics, region: str) -> SummaryStatistics:
        """Exclude a region from the summary stats dataset.

        Args:
            region (str): region given in "chr##:#####-####" format

        Returns:
            SummaryStatistics: filtered summary statistics.
        """
        (chromosome, start_position, end_position) = parse_region(region)

        return SummaryStatistics(
            _df=(
                self.df.filter(
                    ~(
                        (f.col("chromosome") == chromosome)
                        & (
                            (f.col("position") >= start_position)
                            & (f.col("position") <= end_position)
                        )
                    )
                )
            ),
            _schema=SummaryStatistics.get_schema(),
        )

    @staticmethod
    def get_locus_sumstats(
        session: Session,
        window: int,
        locus: Optional[DataFrame] = None,
        pos: Optional[int] = None,
        chr: Optional[str] = None,
        study: Optional[str] = None,
    ) -> DataFrame:
        """Get z-scores for a locus.

        Args:
            session (Session): Session object
            window (int): Window size
            locus (Optional[DataFrame]): Locus DataFrame
            pos (Optional[int]): Position
            chr (Optional[str]): Chromosome
            study (Optional[str]): StudyId to extract z-scores from

        Returns:
            DataFrame: Locus z-scores

        Raises:
            ValueError: If locus is not provided, pos, chr and study must be provided
            ValueError: No valid path found for summary statistics
        """
        if locus is None:
            if pos is None or chr is None or study is None:
                raise ValueError(
                    "If locus is not provided, pos, chr and study must be provided"
                )
            position = pos
            chromosome = chr
        else:
            row = locus.collect()[0]
            position = row["position"]
            chromosome = row["chromosome"]
            study = row["studyId"]
        if study is not None and study.startswith("FINNGEN_"):
            study = study.replace("FINNGEN_", "")

        paths = [
            f"gs://gwas_catalog_data/harmonised_summary_statistics/{study}.parquet",
            f"gs://finngen_data/r10/harmonised_summary_statistics/{study}.parquet",
        ]
        sumstats = None

        for path in paths:
            try:
                sumstats = SummaryStatistics.from_parquet(session=session, path=path)
                break
            except AnalysisException:
                continue
        if sumstats is None:
            raise ValueError("No valid path found for summary statistics")

        return sumstats.df.filter(
            (f.col("chromosome") == chromosome)
            & (f.col("position") >= position - (window / 2))
            & (f.col("position") <= position + (window / 2))
        )
