"""ColocPIP method for colocalisation using PIPs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation.model import ColocalisationMethodInterface

if TYPE_CHECKING:
    from typing import Any


class _ColocPIPConfig(BaseModel):
    """Configuration for ColocPIP method."""

    priorc1: float = 1e-4
    """Prior on variant being causal for trait 1."""
    priorc2: float = 1e-4
    """Prior on variant being causal for trait 2."""
    priorc12: float = 1e-5
    """Prior on variant being causal for both traits."""


class ColocPIP(ColocalisationMethodInterface):
    """Calculate bayesian colocalisation based on overlapping signals from credible sets using PIPs."""

    METHOD_NAME: str = "COLOC_PIP"
    METHOD_METRICS: list[str] = ["h4", "h3", "h2", "h1", "h0"]

    @classmethod
    def colocalise(
        cls: type[ColocPIP],
        overlapping_signals: StudyLocusOverlap,
        **kwargs: Any,
    ) -> Colocalisation:
        """Calculate approximate bayesian colocalisation based on overlapping signals with PIPs.

        Args:
            overlapping_signals (StudyLocusOverlap): overlapping peaks
            **kwargs (Any): Additional parameters passed to the colocalise method.

        Keyword Args:
            priorc1 (float): Prior on variant being causal for trait 1. Defaults to 1e-4.
            priorc2 (float): Prior on variant being causal for trait 2. Defaults to 1e-4.
            priorc12 (float): Prior on variant being causal for traits 1 and 2. Defaults to 1e-5.

        Returns:
            Colocalisation: Colocalisation results

        Raises:
            ValidationError: When passed incorrect prior argument types.
        """
        config = _ColocPIPConfig(**kwargs)

        # Floor priors and per-variant PIPs at a pseudocount, matching the previous
        # per-row implementation (null PIP -> 0 -> pseudocount; priors >= pseudocount).
        pseudocount = 1e-16
        p1 = max(config.priorc1, pseudocount)
        p2 = max(config.priorc2, pseudocount)
        p12 = max(config.priorc12, pseudocount)
        floored_pip1 = f.greatest(
            f.coalesce(f.col("left_posteriorProbability"), f.lit(0.0)),
            f.lit(pseudocount),
        )
        floored_pip2 = f.greatest(
            f.coalesce(f.col("right_posteriorProbability"), f.lit(0.0)),
            f.lit(pseudocount),
        )

        return Colocalisation(
            _df=(
                overlapping_signals.df.withColumn(
                    "tagVariantSource", cls.get_tag_variant_source(f.col("statistics"))
                )
                .select("*", "statistics.*")
                # The overlap is aligned by tag variant, so the PIP approximation reduces
                # to three per-overlap sums over the shared variants -- no per-locus arrays
                # and no Python UDF:
                #   S1 = sum(pip1), S2 = sum(pip2), B = sum(pip1 * pip2)
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
                    f.sum(floored_pip1).alias("sum_pip1"),
                    f.sum(floored_pip2).alias("sum_pip2"),
                    f.sum(floored_pip1 * floored_pip2).alias("sum_pip_prod"),
                )
                # H4 (shared causal) ~ p12 * B; H3 (distinct causal) ~ p1*p2*(S1*S2 - B),
                # where S1*S2 - B = sum_{i!=j} pip1_i*pip2_j. Then normalise H3,H4 to 1
                # (H0=H1=H2=0 in this PIP approximation).
                .withColumn(
                    "_h3_unnorm",
                    f.lit(p1 * p2)
                    * f.greatest(
                        f.col("sum_pip1") * f.col("sum_pip2") - f.col("sum_pip_prod"),
                        f.lit(0.0),
                    ),
                )
                .withColumn("_h4_unnorm", f.lit(p12) * f.col("sum_pip_prod"))
                .withColumn("_denom", f.col("_h3_unnorm") + f.col("_h4_unnorm"))
                .withColumn("h0", f.lit(0.0))
                .withColumn("h1", f.lit(0.0))
                .withColumn("h2", f.lit(0.0))
                .withColumn("h3", f.col("_h3_unnorm") / f.col("_denom"))
                .withColumn("h4", f.col("_h4_unnorm") / f.col("_denom"))
                .drop(
                    "sum_pip1",
                    "sum_pip2",
                    "sum_pip_prod",
                    "_h3_unnorm",
                    "_h4_unnorm",
                    "_denom",
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
