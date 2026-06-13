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

    from pyspark.sql import Column


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
        h3, h4 = cls._pip_posteriors(
            f.col("sum_pip1"),
            f.col("sum_pip2"),
            f.col("sum_pip_prod"),
            config.priorc1,
            config.priorc2,
            config.priorc12,
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
                    f.sum(f.when(f.col("tagVariantSource") == "both", 1).otherwise(0))
                    .cast(t.LongType())
                    .alias("numberColocalisingVariants"),
                    f.sum(cls._floored_pip(f.col("left_posteriorProbability"))).alias(
                        "sum_pip1"
                    ),
                    f.sum(cls._floored_pip(f.col("right_posteriorProbability"))).alias(
                        "sum_pip2"
                    ),
                    f.sum(
                        cls._floored_pip(f.col("left_posteriorProbability"))
                        * cls._floored_pip(f.col("right_posteriorProbability"))
                    ).alias("sum_pip_prod"),
                )
                .withColumn("h0", f.lit(0.0))
                .withColumn("h1", f.lit(0.0))
                .withColumn("h2", f.lit(0.0))
                .withColumn("h3", h3)
                .withColumn("h4", h4)
                .drop("sum_pip1", "sum_pip2", "sum_pip_prod")
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
    def _floored_pip(pip: Column) -> Column:
        """Floor a PIP column at a pseudocount (null -> 0 -> pseudocount).

        Non-zero PIPs are required for the R coloc.pp log-space logic.

        Args:
            pip (Column): posterior inclusion probability column.

        Returns:
            Column: PIP floored at 1e-16.
        """
        return f.greatest(f.coalesce(pip, f.lit(0.0)), f.lit(1e-16))

    @staticmethod
    def _pip_posteriors(
        sum_pip1: Column,
        sum_pip2: Column,
        sum_pip_prod: Column,
        priorc1: float,
        priorc2: float,
        priorc12: float,
    ) -> tuple[Column, Column]:
        """Derive (h3, h4) from the per-overlap PIP sums.

        H4 (shared causal) ~ p12 * B; H3 (distinct causal) ~ p1*p2*(S1*S2 - B), where
        S1*S2 - B = sum_{i!=j} pip1_i*pip2_j. Returned normalised so h3 + h4 = 1
        (H0=H1=H2=0 in this PIP approximation). Shared with ColocPIPECaviar so the two
        cannot numerically diverge.

        Args:
            sum_pip1 (Column): S1 = sum of floored left PIPs per overlap.
            sum_pip2 (Column): S2 = sum of floored right PIPs per overlap.
            sum_pip_prod (Column): B = sum of floored left*right PIP products per overlap.
            priorc1 (float): Prior on variant being causal for trait 1.
            priorc2 (float): Prior on variant being causal for trait 2.
            priorc12 (float): Prior on variant being causal for both traits.

        Returns:
            tuple[Column, Column]: (h3, h4) posterior columns.
        """
        pseudocount = 1e-16
        p1 = max(priorc1, pseudocount)
        p2 = max(priorc2, pseudocount)
        p12 = max(priorc12, pseudocount)
        h3_unnorm = f.lit(p1 * p2) * f.greatest(
            sum_pip1 * sum_pip2 - sum_pip_prod, f.lit(0.0)
        )
        h4_unnorm = f.lit(p12) * sum_pip_prod
        denom = h3_unnorm + h4_unnorm
        return h3_unnorm / denom, h4_unnorm / denom
