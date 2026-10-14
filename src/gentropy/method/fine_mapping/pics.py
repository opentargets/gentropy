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
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Self, cast

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.pandas.functions import pandas_udf
from scipy.stats import norm

from gentropy.common.spark import element_struct
from gentropy.common.stats import neglog_pvalue
from gentropy.dataset.credible_set import CredibleSetVariant
from gentropy.model.trait.association import Association
from gentropy.model.trait.locus import LDAnnotated

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import DataFrame

    from gentropy.dataset.study_locus import LDAnnotatedStudyLocus

_ZERO_SPREAD = 0.001
"""Spread used when PICS gives none, for a tag in perfect LD with the lead."""

_MIN_R2 = 0.5
"""Smallest ``r2`` to the lead the PICS formulas hold for."""

_TAG, _R2 = element_struct(LDAnnotated.ld_set.dtype).fieldNames()
"""``ldSet`` fields holding the tag variant and its ``r2`` to the lead."""

_VARIANT = CredibleSetVariant.variant_id.name
_R2_OVERALL = CredibleSetVariant.r2_overall.name
_POSTERIOR = CredibleSetVariant.posterior_probability.name

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

    def __init__(self: Self, k: float = 6.4, coverage: float = 0.95) -> None:
        """Validate the parameters and keep them in ``config``.

        Args:
            k (float): Empirical constant of the PICS spread.
            coverage (float): Posterior probability the credible set must
                reach, between 0 and 1.
        """
        self.config = PICSConfig(k=k, coverage=coverage)

    def _posteriors(self: Self, loci: LDAnnotatedStudyLocus) -> DataFrame:
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
        lead = loci.lead_variant_id
        entry = f.get(
            f.filter(loci.locus, lambda e: e[Association.variant_id.name] == lead), 0
        )
        candidates = loci.df.withColumns(
            {
                _NEGLOG_P: neglog_pvalue(
                    entry[Association.p_value_mantissa.name],
                    entry[Association.p_value_exponent.name],
                ),
                _TAGS: f.filter(loci.ld_set, lambda e: e[_R2] >= _MIN_R2),
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

    def _cut(self: Self, members: Column) -> Column:
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
