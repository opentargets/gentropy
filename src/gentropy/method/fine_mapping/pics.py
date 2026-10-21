"""PICS: fine-map a study locus from its lead p-value and the lead's LD.

Probabilistic Identification of Causal SNPs (Farh et al. 2015) needs only the
lead variant's p-value and the variants in LD with it, so it fine-maps loci that
have no full summary statistics, such as GWAS Catalog top hits.

For each tag variant ``i`` with ``r2`` to the lead, PICS asks how likely the
lead's signal is if ``i`` were the causal variant. The lead would then only be a
proxy of ``i``, and a proxy's ``-log10 p`` scales with its ``r2`` to the causal
variant, so the lead's signal ``L`` is expected around

* ``mu = r2 * L``, spread by
* ``sd = sqrt(1 - r ** k) * sqrt(L) / 2``, a curve fitted to permutations, with
  ``k = 6.4``. The paper found any ``k`` between 6 and 8 gives much the same
  result.

The relative likelihood of ``i`` is the two-sided tail of ``N(mu, sd)`` beyond
the observed ``L``. A perfect proxy has ``sd = 0``, which is clamped to 0.001 so
it is as likely as the lead itself. Normalised over the tags, the likelihoods
are the posterior probabilities. The formulas hold only for tags in strong LD,
so tags below ``r2`` 0.5 are left out.

Each fine-mapped locus gives one credible set, led by the lead of the study locus.
Its members are the tags kept by the coverage cut, with the marginal statistics the
study reported for them in ``locus``. PICS gives no Bayes factor, no conditional
effect and no purity, so those columns are null.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from pyspark.sql import Column, Window
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.pandas.functions import pandas_udf
from scipy.stats import norm

from gentropy.common.stats import neglogpval_from_pvalue
from gentropy.dataset.credible_set import (
    CredibleSet,
    CredibleSetVariant,
    CredibleSetVariantQualityCheck,
)
from gentropy.dataset.study_locus import FinemappingMethod, StudyLocusQualityCheck

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import DataFrame

    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.study_locus import LDAnnotatedStudyLocus

# Spread used when PICS gives none, for a tag in perfect LD with the lead.
_ZERO_SPREAD = 0.001

# Smallest ``r2`` to the lead the PICS formulas hold for.
_MIN_R2 = 0.5

# ``ldSet`` fields holding the tag variant and its ``r2`` to the lead.
_TAG, _R2 = "tagVariantId", "r2Overall"

_VARIANT, _R2_OVERALL, _POSTERIOR = "variantId", "r2Overall", "posteriorProbability"

# Column of the members of each credible set, sorted by posterior and cut.
_MEMBERS = "members"

# Temporary columns: the lead's ``-log10 p``, a tag's likelihood, the posteriors before it and its rank.
_NEGLOG_P, _LIKELIHOOD, _PRECEDING, _RANK = "neglogP", "likelihood", "preceding", "rank"


def _likelihood_batch(neglog_p: pd.Series, r2: pd.Series, k: pd.Series) -> pd.Series:
    """The PICS likelihood of each tag being causal, relative to the lead.

    This is the body of the ``_relative_likelihood`` pandas UDF. Each row holds one tag of one
    locus: the lead's ``-log10 p``, the tag's ``r2`` to the lead and the PICS constant, so scipy
    evaluates a whole batch of tags in one call. The caller removes tags the formulas do not hold
    for before calling.

    Args:
        neglog_p (pd.Series): The lead's ``-log10 p``.
        r2 (pd.Series): The tag's ``r2`` to the lead.
        k (pd.Series): The PICS constant.

    Returns:
        pd.Series: The relative likelihood of each tag.

    Examples:
        >>> from pyspark.sql import functions as f
        >>> df = spark.createDataFrame([(10.0, 1.0), (10.0, 0.8)], "neglogP double, r2 double")
        >>> [round(r.likelihood, 4) for r in df.select(
        ...     _relative_likelihood("neglogP", "r2", f.lit(6.4)).alias("likelihood")
        ... ).collect()]
        [1.0, 0.0766]
    """
    lead = neglog_p.to_numpy(dtype=np.float64)
    tag_r2 = r2.to_numpy(dtype=np.float64)
    constant = k.to_numpy(dtype=np.float64)
    mu = tag_r2 * lead
    sd = np.sqrt(1 - np.sqrt(tag_r2) ** constant) * np.sqrt(lead) / 2
    sd = np.where(sd == 0, _ZERO_SPREAD, sd)
    return pd.Series(2 * norm.sf(lead, mu, sd))


# pyspark's stubs only type the deprecated ``functionType`` form, not type hints,
# so the UDF is cast to what it is: a function of columns returning a column.
_relative_likelihood = cast(
    "Callable[..., Column]",
    pandas_udf(_likelihood_batch, t.DoubleType()),
)


class PICSConfig(BaseModel):
    """Parameters of PICS, validated when the method is created.

    Examples:
        >>> PICSConfig()
        PICSConfig(k=6.4, coverage=0.95)
    """

    k: float = Field(default=6.4, gt=0)
    """Empirical constant of the PICS spread; Farh et al. use 6.4."""
    coverage: float = Field(default=0.95, gt=0, le=1)
    """Posterior probability the credible set must reach."""


class PICS:
    """Fine-map LD annotated study loci with PICS.

    The module docstring describes the method.

    Examples:
        >>> PICS().config.coverage
        0.95
    """

    def __init__(self: PICS, k: float = 6.4, coverage: float = 0.95) -> None:
        """Validate the parameters and keep them in ``config``.

        Args:
            k (float): Empirical constant of the PICS spread.
            coverage (float): Posterior probability the credible set must
                reach, between 0 and 1.
        """
        self.config = PICSConfig(k=k, coverage=coverage)

    def finemap(
        self: PICS, loci: LDAnnotatedStudyLocus, study_index: StudyIndex
    ) -> tuple[CredibleSet, CredibleSetVariant]:
        """Fine-map each locus into one credible set and its member variants.

        The credible set is led by the lead of the study locus, carries the study
        type from the study index and the quality controls of the study locus,
        plus ``OUT_OF_SAMPLE_LD``, as the LD comes from a reference panel. A
        study missing from the study index gives a null study type; such credible
        sets are dropped by the credible set validation of the platform
        transformation stage, which also requires the study in the index. A
        lead tied with many perfect proxies can fall outside the cut, so the lead
        is not always a member of its own credible set.

        A member's marginal statistics are its entry in ``locus``; a member without
        one gets null statistics and the ``NOT_IN_LOCUS`` flag. A member with an
        allele frequency from the study gets the ``EAF_FROM_SOURCE`` flag.

        Loci without the lead's p-value, or whose tags all fall below ``r2`` 0.5,
        give nothing. The posteriors are persisted, as both datasets read them.

        Args:
            loci (LDAnnotatedStudyLocus): Loci with ``locus`` and ``ldSet``.
            study_index (StudyIndex): Study index, for the study type.

        Returns:
            tuple[CredibleSet, CredibleSetVariant]: The credible sets and their variants.

        Examples:
            >>> from gentropy.dataset.study_index import StudyIndex
            >>> from gentropy.dataset.study_locus import LDAnnotatedStudyLocus
            >>> locus = [("1_100_A_T", 1.0, -10, 0.3, 1000, 0.1, 0.01)]
            >>> ld_set = [("1_100_A_T", 1.0), ("1_200_C_G", 0.8)]
            >>> loci = LDAnnotatedStudyLocus(
            ...     spark.createDataFrame(
            ...         [("L1", "S1", "1", 1, 1000, "1_100_A_T", [], locus, ld_set)],
            ...         LDAnnotatedStudyLocus.get_schema(),
            ...     ),
            ...     LDAnnotatedStudyLocus.get_schema(),
            ... )
            >>> study_index = StudyIndex(
            ...     spark.createDataFrame([("S1", "P1", "gwas")], "studyId string, projectId string, studyType string")
            ... )
            >>> credible_sets, variants = PICS().finemap(loci, study_index)
            >>> credible_sets.df.select("leadVariantId", "studyType", "fineMappingMethod").show()
            +-------------+---------+-----------------+
            |leadVariantId|studyType|fineMappingMethod|
            +-------------+---------+-----------------+
            |    1_100_A_T|     gwas|             PICS|
            +-------------+---------+-----------------+
            <BLANKLINE>
            >>> credible_sets.df.first().qualityControls
            ['Study locus finemapped without in-sample LD reference']
            >>> variants.df.select("variantId", "pValueExponent", f.round("posteriorProbability", 3).alias("pp"), "qualityControls").show(truncate=False)
            +---------+--------------+-----+------------------------------------------------------------+
            |variantId|pValueExponent|pp   |qualityControls                                             |
            +---------+--------------+-----+------------------------------------------------------------+
            |1_100_A_T|-10           |0.929|[Allele frequency reported by the study]                    |
            |1_200_C_G|NULL          |0.071|[Variant in LD with the lead has no statistics in the locus]|
            +---------+--------------+-----+------------------------------------------------------------+
            <BLANKLINE>
        """
        posteriors = (
            self._posteriors(loci)
            .withColumn("fineMappingMethod", f.lit(FinemappingMethod.PICS.value))
            .withColumn("id", CredibleSet.generate_identifier(CredibleSet.ID_COLUMNS))
            .join(
                study_index.df.select("studyId", "studyType"), on="studyId", how="left"
            )
            .persist()
        )
        return self._credible_sets(posteriors), self._credible_set_variants(posteriors)

    @staticmethod
    def _credible_sets(posteriors: DataFrame) -> CredibleSet:
        """Project the fine-mapped loci to credible sets.

        Args:
            posteriors (DataFrame): Fine-mapped loci with their credible set ``id``,
                ``fineMappingMethod`` and ``studyType``.

        Returns:
            CredibleSet: One row per fine-mapped locus.
        """
        schema = CredibleSet.get_schema()
        return CredibleSet(
            _df=posteriors.withColumns(
                {
                    name: f.lit(None).cast(schema[name].dataType)
                    for name in (
                        "logBayesFactor",
                        "confidence",
                        "purityMeanR2",
                        "purityMinR2",
                    )
                }
            )
            # The LD comes from a reference panel, not from the study's own samples.
            .withColumn(
                "qualityControls",
                CredibleSet.update_quality_flag(
                    f.col("qualityControls"),
                    f.lit(True),
                    StudyLocusQualityCheck.OUT_OF_SAMPLE_LD,
                ),
            )
            .select(*schema.fieldNames()),
            _schema=schema,
        )

    @staticmethod
    def _credible_set_variants(posteriors: DataFrame) -> CredibleSetVariant:
        """Project the members of each credible set, with their marginal statistics.

        Args:
            posteriors (DataFrame): Fine-mapped loci with their credible set ``id``
                and ``members``.

        Returns:
            CredibleSetVariant: One row per member of a credible set.
        """
        schema = CredibleSetVariant.get_schema()
        entry = f.col("entry")
        # Each member gets its locus entry by a join, not by searching ``locus`` per member, which
        # is quadratic on summary-statistics loci. A variant listed twice in ``locus`` keeps its
        # first entry.
        first = Window.partitionBy("credibleSetId", "variantId").orderBy("position")
        entries = (
            posteriors.select(
                f.col("id").alias("credibleSetId"),
                f.posexplode("locus").alias("position", "entry"),
            )
            .withColumn("variantId", entry["variantId"])
            .withColumn("rank", f.row_number().over(first))
            .filter(f.col("rank") == 1)
            .select("credibleSetId", "variantId", "entry")
        )
        members = (
            posteriors.select(
                f.col("id").alias("credibleSetId"), f.explode(_MEMBERS).alias("member")
            )
            .withColumn("variantId", f.col("member")[_VARIANT])
            .join(entries, on=["credibleSetId", "variantId"], how="left")
        )
        variants = members.select(
            "credibleSetId",
            "variantId",
            entry["pValueMantissa"].alias("pValueMantissa"),
            entry["pValueExponent"].alias("pValueExponent"),
            entry["effectAlleleFrequencyFromSource"].alias("effectAlleleFrequency"),
            entry["sampleSize"].alias("sampleSize"),
            entry["beta"].alias("reportedBeta"),
            entry["standardError"].alias("reportedStandardError"),
            f.col("member")[_POSTERIOR]
            .cast(t.FloatType())
            .alias("posteriorProbability"),
            f.col("member")[_R2_OVERALL].alias("r2Overall"),
            f.array_compact(
                f.array(
                    f.when(
                        entry.isNull(),
                        f.lit(CredibleSetVariantQualityCheck.NOT_IN_LOCUS.value),
                    ),
                    f.when(
                        entry["effectAlleleFrequencyFromSource"].isNotNull(),
                        f.lit(CredibleSetVariantQualityCheck.EAF_FROM_SOURCE.value),
                    ),
                )
            ).alias("qualityControls"),
        )
        missing = {
            name: f.lit(None).cast(schema[name].dataType)
            for name in schema.fieldNames()
            if name not in variants.columns
        }
        return CredibleSetVariant(
            _df=variants.withColumns(missing).select(*schema.fieldNames()),
            _schema=schema,
        )

    def _posteriors(self: PICS, loci: LDAnnotatedStudyLocus) -> DataFrame:
        """The loci PICS can fine-map, each with its credible set in ``members``.

        A locus is fine-mapped from its lead's p-value, read from the lead's
        entry in ``locus``, and from the tags in ``ldSet`` with ``r2`` of at
        least 0.5. A locus without the lead's p-value, or whose tags are all
        removed, gives nothing.

        The tags are handled as rows, one per tag of a locus, and the posteriors,
        their order and the coverage cut are window functions over the locus: a
        locus can have thousands of tags, and the same steps as array functions are
        evaluated interpreted and exhaust the memory on the GWAS Catalog top hits.

        The members are sorted by posterior, then by variant id so equal
        posteriors always sort the same way. A member is kept while the
        posteriors before it sum to less than ``coverage``, so the last one kept
        is the one that reaches it. The kept posteriors are not renormalised.

        Args:
            loci (LDAnnotatedStudyLocus): Loci with ``locus`` and ``ldSet``.

        Returns:
            DataFrame: The columns of the loci and ``members``, an array of
                ``variantId``, ``r2Overall`` and ``posteriorProbability``, sorted.
        """
        lead = f.col("leadVariantId")
        entry = f.get(f.filter("locus", lambda e: e["variantId"] == lead), 0)
        candidates = loci.df.withColumn(
            _NEGLOG_P,
            neglogpval_from_pvalue(entry["pValueMantissa"], entry["pValueExponent"]),
        ).filter(f.col(_NEGLOG_P).isNotNull())
        tags = (
            candidates.select("id", _NEGLOG_P, f.inline("ldSet"))
            .filter(f.col(_R2) >= _MIN_R2)
            .withColumn(
                _LIKELIHOOD,
                _relative_likelihood(
                    f.col(_NEGLOG_P),
                    f.col(_R2).cast(t.DoubleType()),
                    f.lit(self.config.k),
                ),
            )
        )
        locus = Window.partitionBy("id")
        ranked = locus.orderBy(f.desc(_POSTERIOR), f.asc(_VARIANT))
        members = (
            tags.select(
                "id",
                f.col(_TAG).alias(_VARIANT),
                f.col(_R2).cast(t.FloatType()).alias(_R2_OVERALL),
                (f.col(_LIKELIHOOD) / f.sum(_LIKELIHOOD).over(locus)).alias(_POSTERIOR),
            )
            .withColumns(
                {
                    _PRECEDING: f.coalesce(
                        f.sum(_POSTERIOR).over(
                            ranked.rowsBetween(Window.unboundedPreceding, -1)
                        ),
                        f.lit(0.0),
                    ),
                    _RANK: f.row_number().over(ranked),
                }
            )
            .filter(f.col(_PRECEDING) < self.config.coverage)
            .groupBy("id")
            .agg(
                # Sorting the (rank, member) structs by their natural order restores the rank order:
                f.array_sort(
                    f.collect_list(
                        f.struct(
                            _RANK,
                            f.struct(_VARIANT, _R2_OVERALL, _POSTERIOR).alias("member"),
                        )
                    )
                ).alias(_MEMBERS)
            )
            .select("id", f.col(_MEMBERS)["member"].alias(_MEMBERS))
        )
        return candidates.drop(_NEGLOG_P).join(members, on="id", how="inner")
