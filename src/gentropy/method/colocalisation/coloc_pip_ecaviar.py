"""Joint colocalisation method combining colocPIP and eCAVIAR results."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation.coloc_pip import ColocPIP, _ColocPIPConfig
from gentropy.method.colocalisation.ecaviar import ECaviar
from gentropy.method.colocalisation.model import ColocalisationMethodInterface

if TYPE_CHECKING:
    from typing import Any


class ColocPIPECaviar(ColocalisationMethodInterface):
    """Calculate bayesian colocalisation based on overlapping signals from credible sets using both PIPs and eCAVIAR CLPP."""

    METHOD_NAME: str = "COLOC_PIP_ECAVIAR"
    METHOD_METRICS: list[str] = ["h4", "h3", "clpp"]

    @classmethod
    def colocalise(
        cls: type[ColocPIPECaviar],
        overlapping_signals: StudyLocusOverlap,
        **kwargs: Any,
    ) -> Colocalisation:
        """Colocalise using colocPIP and eCAVIAR metrics in a single fused pass.

        ColocPIP and eCAVIAR group the overlaps by the same keys, so both methods'
        metrics are computed in ONE groupBy and the beta ratio is joined once -- replacing
        the previous two separate groupBys + inner merge join + double beta-ratio (which
        re-shuffled the multi-TiB overlaps ~3 times).

        Args:
            overlapping_signals (StudyLocusOverlap): overlapping peaks
            **kwargs (Any): Additional parameters passed to the colocalise method.

        Keyword Args:
            priorc1 (float): Prior on variant being causal for trait 1. Defaults to 1e-4.
            priorc2 (float): Prior on variant being causal for trait 2. Defaults to 1e-4.
            priorc12 (float): Prior on variant being causal for traits 1 and 2. Defaults to 1e-5.

        Returns:
            Colocalisation: Colocalisation results
        """
        config = _ColocPIPConfig(**kwargs)
        # h3/h4 reuse ColocPIP's exact sums -> posteriors derivation (shared helper) so
        # the fused path cannot numerically diverge from the standalone ColocPIP method.
        h3, h4 = ColocPIP._pip_posteriors(
            f.col("sum_pip1"),
            f.col("sum_pip2"),
            f.col("sum_pip_prod"),
            config.priorc1,
            config.priorc2,
            config.priorc12,
        )

        # One groupBy over the (tag-variant-aligned) overlaps computes every metric for
        # both methods at once: ColocPIP needs S1/S2/B (floored PIP sums); eCAVIAR needs
        # clpp = sum(left_pp * right_pp) (raw); numberColocalisingVariants is identical in
        # both. h0=h1=h2 are not produced for the combined method (matching the original).
        aggregated = (
            overlapping_signals.df.withColumn(
                "tagVariantSource", cls.get_tag_variant_source(f.col("statistics"))
            )
            .select("*", "statistics.*")
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
                f.sum(ColocPIP._floored_pip(f.col("left_posteriorProbability"))).alias(
                    "sum_pip1"
                ),
                f.sum(ColocPIP._floored_pip(f.col("right_posteriorProbability"))).alias(
                    "sum_pip2"
                ),
                f.sum(
                    ColocPIP._floored_pip(f.col("left_posteriorProbability"))
                    * ColocPIP._floored_pip(f.col("right_posteriorProbability"))
                ).alias("sum_pip_prod"),
                f.sum(
                    ECaviar._get_clpp(
                        f.col("left_posteriorProbability"),
                        f.col("right_posteriorProbability"),
                    )
                ).alias("clpp"),
            )
            .withColumn("h3", h3)
            .withColumn("h4", h4)
            .withColumn("colocalisationMethod", f.lit(cls.METHOD_NAME))
        )

        return Colocalisation(
            _df=aggregated.join(
                overlapping_signals.calculate_beta_ratio(),
                on=["leftStudyLocusId", "rightStudyLocusId", "chromosome"],
                how="left",
            ).select(
                "leftStudyLocusId",
                "rightStudyLocusId",
                "rightStudyType",
                "chromosome",
                "colocalisationMethod",
                "numberColocalisingVariants",
                "h3",
                "h4",
                "clpp",
                "betaRatioSignAverage",
            ),
            _schema=Colocalisation.get_schema(),
        )
