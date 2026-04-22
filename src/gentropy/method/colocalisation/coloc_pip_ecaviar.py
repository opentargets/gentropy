"""Joint colocalisation method combining colocPIP and eCAVIAR results."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation.coloc_pip import ColocPIP
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
        """Colocalise using colocPIP and ECaviar methods.

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
        coloc_pip_results = ColocPIP.colocalise(overlapping_signals, **kwargs)
        ecaviar_results = ECaviar.colocalise(overlapping_signals)

        # Merge results: join on key columns and combine metrics
        join_keys = [
            "leftStudyLocusId",
            "rightStudyLocusId",
            "chromosome",
            "rightStudyType",
        ]

        return Colocalisation(
            _df=coloc_pip_results.df.alias("pip")
            .join(
                ecaviar_results.df.alias("ecav").select(
                    *join_keys,
                    f.col("clpp").alias("clpp_ecaviar"),
                    f.col("numberColocalisingVariants").alias(
                        "numberColocalisingVariants_ecaviar"
                    ),
                ),
                on=join_keys,
                how="inner",
            )
            .select(
                f.col("pip.leftStudyLocusId"),
                f.col("pip.rightStudyLocusId"),
                f.col("pip.rightStudyType"),
                f.col("pip.chromosome"),
                # Use a combined method name
                f.lit(cls.METHOD_NAME).alias("colocalisationMethod"),
                # Use the max number of colocalising variants from both methods
                f.greatest(
                    f.col("pip.numberColocalisingVariants"),
                    f.col("numberColocalisingVariants_ecaviar"),
                ).alias("numberColocalisingVariants"),
                # Keep h3 and h4 from ColocPIP
                f.col("pip.h3"),
                f.col("pip.h4"),
                # Add clpp from eCAVIAR
                f.col("clpp_ecaviar").alias("clpp"),
                # Keep beta ratio from ColocPIP
                f.col("pip.betaRatioSignAverage"),
            ),
            _schema=Colocalisation.get_schema(),
        )
