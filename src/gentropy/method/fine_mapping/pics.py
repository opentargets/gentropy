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
from pyspark.sql import Column
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
from gentropy.dataset.study_locus import FinemappingMethod

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import DataFrame

    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.study_locus import LDAnnotatedStudyLocus

_ZERO_SPREAD = 0.001
"""Spread used when PICS gives none, for a tag in perfect LD with the lead."""

_MIN_R2 = 0.5
"""Smallest ``r2`` to the lead the PICS formulas hold for."""

_TAG, _R2 = "tagVariantId", "r2Overall"
"""``ldSet`` fields holding the tag variant and its ``r2`` to the lead."""

_VARIANT, _R2_OVERALL, _POSTERIOR = "variantId", "r2Overall", "posteriorProbability"

_MEMBERS = "members"
"""Column of the members of each credible set, sorted by posterior and cut."""

_MEMBER = t.StructType(
    [
        t.StructField(_VARIANT, t.StringType()),
        t.StructField(_R2_OVERALL, t.FloatType()),
        t.StructField(_POSTERIOR, t.DoubleType()),
    ]
)
"""One member: the variant, its ``r2`` to the lead and its posterior."""

_NEGLOG_P, _TAGS, _LIKELIHOODS = "neglogP", "tags", "likelihoods"
"""Temporary columns: the lead's ``-log10 p``, its usable tags and their likelihoods."""


def _likelihood_batch(neglog_p: pd.Series, r2: pd.Series, k: pd.Series) -> pd.Series:
    """The PICS likelihood of each tag being causal, relative to the lead.

    This is the body of the ``_relative_likelihoods`` pandas UDF. Each row holds
    one locus: the lead's ``-log10 p``, the ``r2`` of its tags and the PICS
    constant. The tags of the whole batch are flattened into one array, so scipy
    evaluates them in a single call, and the result is split back into the rows.
    The caller removes tags the formulas do not hold for before calling.

    Args:
        neglog_p (pd.Series): The lead's ``-log10 p``, one per locus.
        r2 (pd.Series): The ``r2`` of each tag to the lead, an array per locus.
        k (pd.Series): The PICS constant, one per locus.

    Returns:
        pd.Series: The relative likelihood of each tag, in the order of ``r2``.

    Examples:
        >>> from pyspark.sql import functions as f
        >>> df = spark.createDataFrame([(10.0, [1.0, 0.8])], "neglogP double, r2 array<double>")
        >>> row = df.select(
        ...     _relative_likelihoods("neglogP", "r2", f.lit(6.4)).alias("likelihoods")
        ... ).first()
        >>> [round(likelihood, 4) for likelihood in row.likelihoods]
        [1.0, 0.0766]
    """
    sizes = r2.map(len).to_numpy()
    tag_r2 = np.concatenate([np.empty(0), *r2]).astype(np.float64)
    lead = np.repeat(neglog_p.to_numpy(dtype=np.float64), sizes)
    constant = np.repeat(k.to_numpy(dtype=np.float64), sizes)
    mu = tag_r2 * lead
    sd = np.sqrt(1 - np.sqrt(tag_r2) ** constant) * np.sqrt(lead) / 2
    sd = np.where(sd == 0, _ZERO_SPREAD, sd)
    likelihoods = 2 * norm.sf(lead, mu, sd)
    return pd.Series(np.split(likelihoods, np.cumsum(sizes)[:-1]))


# pyspark's stubs only type the deprecated ``functionType`` form, not type hints,
# so the UDF is cast to what it is: a function of columns returning a column.
_relative_likelihoods = cast(
    "Callable[..., Column]",
    pandas_udf(_likelihood_batch, t.ArrayType(t.DoubleType())),  # type: ignore[call-overload]
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
        type from the study index and the quality controls of the study locus. A
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
                        "log10BayesFactor",
                        "confidence",
                        "purityMeanR2",
                        "purityMinR2",
                    )
                }
            ).select(*schema.fieldNames()),
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
        members = posteriors.select(
            f.col("id").alias("credibleSetId"),
            f.inline(
                f.transform(
                    _MEMBERS,
                    lambda m: f.struct(
                        m.alias("member"),
                        f.get(
                            f.filter("locus", lambda e: e["variantId"] == m[_VARIANT]),
                            0,
                        ).alias("entry"),
                    ),
                )
            ),
        )
        variants = members.select(
            "credibleSetId",
            f.col("member")[_VARIANT].alias("variantId"),
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
        least 0.5. A locus without the lead's p-value or without ``ldSet`` is
        removed before the likelihoods are computed: Spark evaluates a Python
        UDF on every row, even inside ``when``. A locus whose tags are all
        removed has no members and is removed too.

        The members are sorted by posterior, then by variant id so equal
        posteriors always sort the same way. A member is kept while the
        posteriors before it sum to less than ``coverage``, so the last one kept
        is the one that reaches it. The kept posteriors are not renormalised.

        Args:
            loci (LDAnnotatedStudyLocus): Loci with ``locus`` and ``ldSet``.

        Returns:
            DataFrame: The columns of the loci and ``members``, an array of
                ``variantId``, ``r2Overall`` and ``posteriorProbability``.
        """
        lead = f.col("leadVariantId")
        entry = f.get(f.filter("locus", lambda e: e["variantId"] == lead), 0)
        candidates = loci.df.withColumns(
            {
                _NEGLOG_P: neglogpval_from_pvalue(
                    entry["pValueMantissa"], entry["pValueExponent"]
                ),
                _TAGS: f.filter("ldSet", lambda e: e[_R2] >= _MIN_R2),
            }
        ).filter(f.col(_NEGLOG_P).isNotNull() & f.col(_TAGS).isNotNull())
        with_likelihoods = candidates.withColumn(
            _LIKELIHOODS,
            _relative_likelihoods(
                f.col(_NEGLOG_P),
                f.transform(f.col(_TAGS), lambda e: e[_R2].cast(t.DoubleType())),
                f.lit(self.config.k),
            ),
        )
        total = f.aggregate(f.col(_LIKELIHOODS), f.lit(0.0), lambda acc, x: acc + x)
        members = f.zip_with(
            f.col(_TAGS),
            f.col(_LIKELIHOODS),
            lambda tag, likelihood: f.struct(
                tag[_TAG].alias(_VARIANT),
                tag[_R2].alias(_R2_OVERALL),
                (likelihood / total).alias(_POSTERIOR),
            ),
        )
        return (
            with_likelihoods.withColumn(_MEMBERS, self._cut(_sorted(members)))
            .drop(_NEGLOG_P, _TAGS, _LIKELIHOODS)
            .filter(f.size(_MEMBERS) > 0)
        )

    def _cut(self: PICS, members: Column) -> Column:
        """Keep sorted members while the posteriors before them are below coverage.

        Args:
            members (Column): Members sorted by posterior, largest first.

        Returns:
            Column: The members of the credible set.
        """
        kept, reached = "kept", "reached"
        start = f.struct(
            f.array().cast(t.ArrayType(_MEMBER)).alias(kept),
            f.lit(0.0).alias(reached),
        )
        return f.aggregate(
            members,
            start,
            lambda acc, member: f.struct(
                f.when(
                    acc[reached] < self.config.coverage,
                    f.concat(acc[kept], f.array(member)),
                )
                .otherwise(acc[kept])
                .alias(kept),
                (acc[reached] + member[_POSTERIOR]).alias(reached),
            ),
            lambda acc: acc[kept],
        )


def _sorted(members: Column) -> Column:
    """Sort members by posterior, largest first, then by variant id.

    Args:
        members (Column): Members of one locus.

    Returns:
        Column: The same members, sorted.
    """

    def compare(left: Column, right: Column) -> Column:
        """Order two members: negative if ``left`` comes first.

        Args:
            left (Column): One member.
            right (Column): Another member.

        Returns:
            Column: -1, 0 or 1.
        """
        return (
            f.when(left[_POSTERIOR] > right[_POSTERIOR], -1)
            .when(left[_POSTERIOR] < right[_POSTERIOR], 1)
            .when(left[_VARIANT] < right[_VARIANT], -1)
            .when(left[_VARIANT] > right[_VARIANT], 1)
            .otherwise(0)
        )

    return f.array_sort(members, compare)
