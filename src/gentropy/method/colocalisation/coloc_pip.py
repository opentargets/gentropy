"""ColocPIP method for colocalisation using PIPs."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from pydantic import BaseModel
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.stats import get_logsum
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation.model import ColocalisationMethodInterface

if TYPE_CHECKING:
    from typing import Any

    from numpy.typing import NDArray


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

        priorc1, priorc2, priorc12 = config.priorc1, config.priorc2, config.priorc12

        def _posteriors_batch(
            left_variants: pd.Series,
            left_pips: pd.Series,
            right_variants: pd.Series,
            right_pips: pd.Series,
        ) -> pd.Series:
            return pd.Series(
                [
                    cls._get_posteriors(lv, lp, rv, rp, priorc1, priorc2, priorc12)
                    for lv, lp, rv, rp in zip(
                        left_variants, left_pips, right_variants, right_pips
                    )
                ],
                dtype=object,
            )

        posteriors_udf = f.pandas_udf(_posteriors_batch, t.ArrayType(t.DoubleType()))

        return Colocalisation(
            _df=(
                overlapping_signals.df.withColumn(
                    "tagVariantSource", cls.get_tag_variant_source(f.col("statistics"))
                )
                .select("*", "statistics.*")
                # Before processing, ensure posterior probabilities are not null
                .fillna(
                    0,
                    subset=[
                        "left_posteriorProbability",
                        "right_posteriorProbability",
                    ],
                )
                # Group by overlapping peaks and collect PIPs as arrays
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
                    # Collect variant IDs and PIPs for left study
                    f.collect_list(f.col("tagVariantId")).alias("left_variants"),
                    f.collect_list(f.col("left_posteriorProbability")).alias(
                        "left_pips"
                    ),
                    # Collect variant IDs and PIPs for right study
                    f.collect_list(f.col("tagVariantId")).alias("right_variants"),
                    f.collect_list(f.col("right_posteriorProbability")).alias(
                        "right_pips"
                    ),
                )
                # Calculate posteriors using the PIP approximation
                .withColumn(
                    "posteriors",
                    posteriors_udf(
                        f.col("left_variants").cast("array<string>"),
                        f.col("left_pips").cast("array<double>"),
                        f.col("right_variants").cast("array<string>"),
                        f.col("right_pips").cast("array<double>"),
                    ),
                )
                # Extract individual hypothesis posteriors
                .withColumn("h0", f.col("posteriors").getItem(0))
                .withColumn("h1", f.col("posteriors").getItem(1))
                .withColumn("h2", f.col("posteriors").getItem(2))
                .withColumn("h3", f.col("posteriors").getItem(3))
                .withColumn("h4", f.col("posteriors").getItem(4))
                # Clean up intermediate columns
                .drop(
                    "posteriors",
                    "left_variants",
                    "left_pips",
                    "right_variants",
                    "right_pips",
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
    def _get_posteriors(
        pip1_variants: NDArray[np.str_],
        pip1_values: NDArray[np.float64],
        pip2_variants: NDArray[np.str_],
        pip2_values: NDArray[np.float64],
        p1: float,
        p2: float,
        p12: float,
    ) -> list[float]:
        """Approximate coloc posteriors using only PIPs, following the R coloc.pp logic.

        Args:
            pip1_variants (NDArray[np.str_]): Array of variant names for trait 1
            pip1_values (NDArray[np.float64]): Array of PIP values for trait 1
            pip2_variants (NDArray[np.str_]): Array of variant names for trait 2
            pip2_values (NDArray[np.float64]): Array of PIP values for trait 2
            p1 (float): Prior on variant being causal for trait 1
            p2 (float): Prior on variant being causal for trait 2
            p12 (float): Prior on variant being causal for traits 1 and 2

        Returns:
            list[float]: [H0, H1, H2, H3, H4] posteriors
        """
        # Union of SNPs
        snp_names = np.unique(np.concatenate((pip1_variants, pip2_variants)))
        pip1_dict = dict(zip(pip1_variants, pip1_values))
        pip2_dict = dict(zip(pip2_variants, pip2_values))

        # Ensure priors are never zero to avoid log(0)
        pseudocount = 1e-16
        p1 = max(p1, pseudocount)
        p2 = max(p2, pseudocount)
        p12 = max(p12, pseudocount)
        pip1_vec = np.array([pip1_dict.get(snp, np.nan) for snp in snp_names])
        pip2_vec = np.array([pip2_dict.get(snp, np.nan) for snp in snp_names])

        # Ensure no PIPs are exactly zero by adding pseudocount
        pip1_vec = np.maximum(pip1_vec, pseudocount)
        pip2_vec = np.maximum(pip2_vec, pseudocount)

        # Work in log-space
        log_pip1 = np.log(pip1_vec)
        log_pip2 = np.log(pip2_vec)
        sum_log_pip1 = get_logsum(log_pip1)
        sum_log_pip2 = get_logsum(log_pip2)
        log_sum_both = get_logsum((log_pip1 + log_pip2).astype(np.float64))

        # Compute logdiff as in R coloc:::logdiff
        x = sum_log_pip1 + sum_log_pip2
        y = log_sum_both
        my_max = max(x, y)
        diff_arg = max(np.exp(x - my_max) - np.exp(y - my_max), 0)
        if diff_arg == 0:
            logdiff = -np.inf
        else:
            logdiff = my_max + np.log(diff_arg)

        # H3: distinct causal
        PP3 = np.log(p1) + np.log(p2) + logdiff

        # H4: shared causal
        PP4 = np.log(p12) + log_sum_both

        # Normalize
        denom = get_logsum(np.array([PP3, PP4]))
        PP4 = np.exp(PP4 - denom)
        PP3 = np.exp(PP3 - denom)

        return [0.0, 0.0, 0.0, float(PP3), float(PP4)]
