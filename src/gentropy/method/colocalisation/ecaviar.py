"""eCAVIAR method for colocalisation analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation.model import ColocalisationMethodInterface

if TYPE_CHECKING:
    from typing import Any


class ECaviar(ColocalisationMethodInterface):
    """eCAVIAR-based colocalisation analysis.

    It extends [CAVIAR](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5142122/#bib18) framework to explicitly estimate the posterior probability
    that the same variant is causal in 2 studies while accounting for the uncertainty of LD.

    eCAVIAR computes the colocalization posterior probability (**CLPP**) by utilizing the marginal posterior probabilities.
    This framework allows for **multiple variants to be causal** in a single locus.
    """

    METHOD_NAME: str = "eCAVIAR"
    METHOD_METRICS: list[str] = ["clpp"]

    @classmethod
    def colocalise(
        cls: type[ECaviar],
        overlapping_signals: StudyLocusOverlap,
        **kwargs: Any,
    ) -> Colocalisation:
        """Calculate bayesian colocalisation based on overlapping signals.

        Args:
            overlapping_signals (StudyLocusOverlap): overlapping signals.
            **kwargs (Any): Additional parameters passed to the colocalise method.
                Currently not used for this method.

        Returns:
            Colocalisation: colocalisation results based on eCAVIAR.
        """
        return Colocalisation(
            _df=(
                overlapping_signals.df.withColumns(
                    {
                        "clpp": ECaviar._get_clpp(
                            f.col("statistics.left_posteriorProbability"),
                            f.col("statistics.right_posteriorProbability"),
                        ),
                        "tagVariantSource": cls.get_tag_variant_source(
                            f.col("statistics")
                        ),
                    }
                )
                .groupBy(
                    "leftStudyLocusId",
                    "rightStudyLocusId",
                    "rightStudyType",
                    "chromosome",
                )
                .agg(
                    # Count the number of tag variants that can be found in both loci:
                    f.sum(
                        f.when(f.col("tagVariantSource") == "both", 1).otherwise(0)
                    )
                    .cast(t.LongType())
                    .alias("numberColocalisingVariants"),
                    f.sum(f.col("clpp")).alias("clpp"),
                )
                .withColumn("colocalisationMethod", f.lit(cls.METHOD_NAME))
                .join(
                    overlapping_signals.calculate_beta_ratio(),
                    on=["leftStudyLocusId", "rightStudyLocusId", "chromosome"],
                    how="left",
                )
            ),
            _schema=Colocalisation.get_schema(),
        )

    @staticmethod
    def _get_clpp(left_pp: Column, right_pp: Column) -> Column:
        """Calculate the colocalisation posterior probability (CLPP).

        If the fact that the same variant is found causal for two studies are independent events,
        CLPP is defined as the product of posterior porbabilities that a variant is causal in both studies.

        Args:
            left_pp (Column): left posterior probability
            right_pp (Column): right posterior probability

        Returns:
            Column: CLPP

        Examples:
            >>> d = [{"left_pp": 0.5, "right_pp": 0.5}, {"left_pp": 0.25, "right_pp": 0.75}]
            >>> df = spark.createDataFrame(d)
            >>> df.withColumn("clpp", ECaviar._get_clpp(f.col("left_pp"), f.col("right_pp"))).show()
            +-------+--------+------+
            |left_pp|right_pp|  clpp|
            +-------+--------+------+
            |    0.5|     0.5|  0.25|
            |   0.25|    0.75|0.1875|
            +-------+--------+------+
            <BLANKLINE>

        """
        return left_pp * right_pp
