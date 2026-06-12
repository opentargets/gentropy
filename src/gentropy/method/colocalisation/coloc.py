"""Coloc method for colocalisation analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pydantic import BaseModel

from gentropy.common.stats import get_logsum_column
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation.model import ColocalisationMethodInterface

if TYPE_CHECKING:
    from typing import Any


class _ColocConfig(BaseModel):
    """Configuration for Coloc method."""

    priorc1: float = 1e-4
    """Prior probability of a variant being causal for trait 1."""
    priorc2: float = 1e-4
    """Prior probability of a variant being causal for trait 2."""
    priorc12: float = 1e-5
    """Prior probability of a variant being causal for both traits."""
    overlap_size_cutoff: int = 0
    """Minimum number of overlapping variants before filtering."""
    posterior_cutoff: float = 0.0
    """Minimum overlapping Posterior probability cutoff for small overlaps."""
    pseudocutoff: float = 1e-10
    """Pseudocount to avoid log(0)."""


class Coloc(ColocalisationMethodInterface):
    """Calculate bayesian colocalisation based on overlapping signals from credible sets.

    Based on the [R COLOC package](https://github.com/chr1swallace/coloc/blob/main/R/claudia.R), which uses the Bayes factors from the credible set to estimate the posterior probability of colocalisation. This method makes the simplifying assumption that **only one single causal variant** exists for any given trait in any genomic region.

    | Hypothesis    | Description                                                           |
    | ------------- | --------------------------------------------------------------------- |
    | H<sub>0</sub> | no association with either trait in the region                        |
    | H<sub>1</sub> | association with trait 1 only                                         |
    | H<sub>2</sub> | association with trait 2 only                                         |
    | H<sub>3</sub> | both traits are associated, but have different single causal variants |
    | H<sub>4</sub> | both traits are associated and share the same single causal variant   |

    !!! warning "Bayes factors required"

        Coloc requires the availability of Bayes factors (BF) for each variant in the credible set (`logBF` column).

    """

    METHOD_NAME: str = "COLOC"
    METHOD_METRICS: list[str] = ["h4", "h3", "h2", "h1", "h0"]

    @classmethod
    def colocalise(
        cls: type[Coloc],
        overlapping_signals: StudyLocusOverlap,
        **kwargs: Any,
    ) -> Colocalisation:
        """Calculate bayesian colocalisation based on overlapping signals.

        Args:
            overlapping_signals (StudyLocusOverlap): overlapping peaks
            **kwargs (Any): Additional parameters passed to the colocalise method.

        Keyword Args:
            priorc1 (float): Prior on variant being causal for trait 1. Defaults to 1e-4.
            priorc2 (float): Prior on variant being causal for trait 2. Defaults to 1e-4.
            priorc12 (float): Prior on variant being causal for traits 1 and 2. Defaults to 1e-5.
            overlap_size_cutoff (int): Minimum number of overlapping variants before filtering. Defaults to 0.
            posterior_cutoff (float): Minimum overlapping Posterior probability cutoff for small overlaps. Defaults to 0.0.
            pseudocutoff (float): Pseudocount to avoid log(0). Defaults to 1e-10.

        Returns:
            Colocalisation: Colocalisation results

        Raises:
            TypeError: When passed incorrect keyword argument types.
        """
        config = _ColocConfig(**kwargs)
        return Colocalisation(
            _df=(
                overlapping_signals.df.withColumn(
                    "tagVariantSource", cls.get_tag_variant_source(f.col("statistics"))
                )
                .select("*", "statistics.*")
                # Before summing log_BF columns nulls need to be filled with 0:
                .fillna(
                    0,
                    subset=[
                        "left_logBF",
                        "right_logBF",
                        "left_posteriorProbability",
                        "right_posteriorProbability",
                    ],
                )
                # Sum of log_BFs for each pair of signals
                .withColumn(
                    "sum_log_bf",
                    f.col("left_logBF") + f.col("right_logBF"),
                )
                # Group by overlapping peak and generating dense vectors of log_BF:
                .groupBy(
                    "chromosome",
                    "leftStudyLocusId",
                    "rightStudyLocusId",
                    "rightStudyType",
                )
                .agg(
                    f.size(
                        f.filter(
                            f.collect_list(f.col("tagVariantSource")),
                            lambda x: x == "both",
                        )
                    )
                    .cast(t.LongType())
                    .alias("numberColocalisingVariants"),
                    f.collect_list(f.col("left_logBF")).alias("left_logBF"),
                    f.collect_list(f.col("right_logBF")).alias("right_logBF"),
                    f.collect_list(f.col("left_posteriorProbability")).alias(
                        "left_posteriorProbability"
                    ),
                    f.collect_list(f.col("right_posteriorProbability")).alias(
                        "right_posteriorProbability"
                    ),
                    f.collect_list(f.col("sum_log_bf")).alias("sum_log_bf"),
                    f.collect_list(f.col("tagVariantSource")).alias(
                        "tagVariantSourceList"
                    ),
                )
                .withColumn("logsum1", get_logsum_column(f.col("left_logBF")))
                .withColumn("logsum2", get_logsum_column(f.col("right_logBF")))
                .withColumn("logsum12", get_logsum_column(f.col("sum_log_bf")))
                .drop("left_logBF", "right_logBF", "sum_log_bf")
                # Add priors
                # priorc1 Prior on variant being causal for trait 1
                .withColumn("priorc1", f.lit(config.priorc1))
                # priorc2 Prior on variant being causal for trait 2
                .withColumn("priorc2", f.lit(config.priorc2))
                # priorc12 Prior on variant being causal for traits 1 and 2
                .withColumn("priorc12", f.lit(config.priorc12))
                # h0-h2
                .withColumn("lH0bf", f.lit(0))
                .withColumn("lH1bf", f.log(f.col("priorc1")) + f.col("logsum1"))
                .withColumn("lH2bf", f.log(f.col("priorc2")) + f.col("logsum2"))
                # h3
                .withColumn("sumlogsum", f.col("logsum1") + f.col("logsum2"))
                .withColumn("max", f.greatest("sumlogsum", "logsum12"))
                .withColumn(
                    "anySnpBothSidesHigh",
                    f.aggregate(
                        f.transform(
                            f.arrays_zip(
                                f.col("left_posteriorProbability"),
                                f.col("right_posteriorProbability"),
                                f.col("tagVariantSourceList"),
                            ),
                            # row["left_posteriorProbability"] = left PP, row["right_posteriorProbability"] = right PP
                            lambda row: f.when(
                                (row["tagVariantSourceList"] == "both")
                                & (row["left_posteriorProbability"] > config.posterior_cutoff)
                                & (row["right_posteriorProbability"] > config.posterior_cutoff),
                                1.0,
                            ).otherwise(0.0),
                        ),
                        f.lit(0.0),
                        lambda acc, x: acc + x,
                    )
                    > 0,  # True if sum of these 1.0's > 0
                )
                .filter(
                    (f.col("numberColocalisingVariants") > config.overlap_size_cutoff)
                    | (f.col("anySnpBothSidesHigh"))
                )
                .withColumn(
                    "logdiff",
                    f.when(
                        (f.col("sumlogsum") == f.col("logsum12")),
                        config.pseudocutoff,
                    ).otherwise(
                        f.col("max")
                        + f.log(
                            f.exp(f.col("sumlogsum") - f.col("max"))
                            - f.exp(f.col("logsum12") - f.col("max"))
                        )
                    ),
                )
                .withColumn(
                    "lH3bf",
                    f.log(f.col("priorc1"))
                    + f.log(f.col("priorc2"))
                    + f.col("logdiff"),
                )
                .drop("right_logsum", "left_logsum", "sumlogsum", "max", "logdiff")
                # h4
                .withColumn("lH4bf", f.log(f.col("priorc12")) + f.col("logsum12"))
                # cleaning
                .drop(
                    "priorc1", "priorc2", "priorc12", "logsum1", "logsum2", "logsum12"
                )
                # posteriors: softmax over the 5 hypothesis Bayes factors,
                # computed natively as exp(lH_i - logsumexp(allBF)).
                .withColumn(
                    "coloc_log_denom",
                    get_logsum_column(
                        f.array(
                            f.col("lH0bf"),
                            f.col("lH1bf"),
                            f.col("lH2bf"),
                            f.col("lH3bf"),
                            f.col("lH4bf"),
                        )
                    ),
                )
                .withColumn("h0", f.exp(f.col("lH0bf") - f.col("coloc_log_denom")))
                .withColumn("h1", f.exp(f.col("lH1bf") - f.col("coloc_log_denom")))
                .withColumn("h2", f.exp(f.col("lH2bf") - f.col("coloc_log_denom")))
                .withColumn("h3", f.exp(f.col("lH3bf") - f.col("coloc_log_denom")))
                .withColumn("h4", f.exp(f.col("lH4bf") - f.col("coloc_log_denom")))
                # clean up
                .drop(
                    "coloc_log_denom",
                    "lH0bf",
                    "lH1bf",
                    "lH2bf",
                    "lH3bf",
                    "lH4bf",
                    "left_posteriorProbability",
                    "right_posteriorProbability",
                    "tagVariantSourceList",
                    "anySnpBothSidesHigh",
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
