"""Window clumping: turn significant variants into study loci, keeping the strongest per window.

Window clumping is the simplest way to find independent signals in summary
statistics. It assumes that two significant variants closer than a fixed
``distance`` belong to the same signal, keeps the more significant of them as a
lead, and marks the other as explained by it. It needs nothing but positions and
p-values: no LD reference, no population, no fine-mapping.

Each lead becomes one study locus, a window of ``distance`` base pairs on each
side of it. A lead and the variants it explains form a clump. By default only
the leads are returned. With ``keep_clumped_loci``, every variant of a clump gets
a row too: it carries the lead's window, and so the same ``id``, and the
``WINDOW_CLUMPED`` quality control flag.

The algorithm
-------------

When ``gwas_significance`` is set, ``clump`` first keeps only the variants at
least as significant as it, with ``SummaryStatistics.pvalue_filter``;
without it, every input variant with a p-value is clumped. A variant missing its
mantissa or exponent cannot be ranked, so it is left out: it is neither a lead
nor clumped. (Legacy ranked it first, so it could clump the measured signal next
to it.) Clumping then runs per study and
chromosome, in four steps:

1. **Cluster.** Variants are sorted by position. A new cluster starts at the
   first variant and wherever the gap to the previous variant is greater than
   ``distance``; every other variant joins the cluster of the variant before it.
   A cluster can be arbitrarily long, because only gaps between adjacent
   variants are compared, but no variant in one cluster can be within
   ``distance`` of a variant in another. Clusters only make step 3 cheaper:
   each one is solved on its own instead of across the whole chromosome.
2. **Rank.** Within a cluster, variants are ranked by p-value, most significant
   first (exponent, then mantissa). Equal p-values are ordered by position and
   then by variant id, so the result is the same on every run.
3. **Prune.** Walking down that ranking, a variant becomes a lead when it is at
   least ``distance`` away from every lead chosen so far; otherwise it is
   clumped. The comparison is strict: a variant exactly ``distance`` away from a
   lead is a lead of its own. The most significant variant of a cluster is always
   a lead. Leads are therefore always at least ``distance`` apart, while a
   clumped variant can sit within ``distance`` of several leads.
4. **Build loci.** Each lead gets the window ``locusStart = max(1, lead
   position - distance)`` and ``locusEnd = lead position + distance``, its
   ``variantId`` as ``leadVariantId``, and an ``id`` hashed from the study,
   chromosome and bounds. The window is cut at 1, the first position of a
   chromosome. With ``keep_clumped_loci``, each clumped variant belongs to the
   clump of the most significant lead within ``distance``, gets that lead's
   window and ``id``, and the ``WINDOW_CLUMPED`` flag in ``qualityControls``.

Some consequences of these steps:

* Leads never share an ``id``, because each lead's window is centred on a
  different position. With ``keep_clumped_loci``, all rows of a clump share
  bounds and ``id``; on a clumped row, ``leadVariantId`` is the clumped variant
  itself, and the lead is the unflagged row with the same ``id``.
* A variant's result depends only on more significant variants. Filtering by
  p-value before clumping therefore removes rows but never changes the rows
  that remain.
* Variants at the same position, such as the alleles of a multi-allelic site,
  are zero base pairs apart, so only the more significant one can lead.
* ``locusEnd`` is not capped at the end of the chromosome.

Example:
    Two signals on chromosome 1, at 1300 and 2000, explain the variants around
    them. A variant far away on the same chromosome, and one on chromosome 2, are
    signals of their own:

    >>> from gentropy.dataset.summary_statistics import SummaryStatistics
    >>> rows = [
    ...     ("GCST1", "1_1000_A_T", "1", 1000, 0.1, 5000, 1.0, -10, 0.3, 0.01),
    ...     ("GCST1", "1_1300_C_G", "1", 1300, 0.2, 5000, 1.0, -12, 0.3, 0.01),
    ...     ("GCST1", "1_1700_G_A", "1", 1700, 0.1, 5000, 1.0, -9, 0.3, 0.01),
    ...     ("GCST1", "1_2000_T_C", "1", 2000, 0.1, 5000, 1.0, -11, 0.3, 0.01),
    ...     ("GCST1", "1_5000_A_G", "1", 5000, 0.1, 5000, 1.0, -8, 0.3, 0.01),
    ...     ("GCST1", "2_1000_C_T", "2", 1000, 0.1, 5000, 1.0, -9, 0.3, 0.01),
    ... ]
    >>> summary_statistics = SummaryStatistics(
    ...     spark.createDataFrame(rows, SummaryStatistics.get_schema())
    ... )
    >>> method = WindowClumping(distance=500, gwas_significance=5e-8)
    >>> leads = method.clump(summary_statistics)
    >>> leads.df.select("leadVariantId", "locusStart", "locusEnd").orderBy(
    ...     "leadVariantId"
    ... ).show()
    +-------------+----------+--------+
    |leadVariantId|locusStart|locusEnd|
    +-------------+----------+--------+
    |   1_1300_C_G|       800|    1800|
    |   1_2000_T_C|      1500|    2500|
    |   1_5000_A_G|      4500|    5500|
    |   2_1000_C_T|       500|    1500|
    +-------------+----------+--------+
    <BLANKLINE>

    Keeping the clumped variants shows what each lead explains:

    >>> loci = WindowClumping(
    ...     distance=500, gwas_significance=5e-8, keep_clumped_loci=True
    ... ).clump(summary_statistics)
    >>> loci.df.select(
    ...     "leadVariantId", "locusStart", "locusEnd", "qualityControls"
    ... ).orderBy("leadVariantId").show(truncate=False)
    +-------------+----------+--------+------------------------------------------------------------+
    |leadVariantId|locusStart|locusEnd|qualityControls                                             |
    +-------------+----------+--------+------------------------------------------------------------+
    |1_1000_A_T   |800       |1800    |[Explained by a more significant variant in the same window]|
    |1_1300_C_G   |800       |1800    |[]                                                          |
    |1_1700_G_A   |800       |1800    |[Explained by a more significant variant in the same window]|
    |1_2000_T_C   |1500      |2500    |[]                                                          |
    |1_5000_A_G   |4500      |5500    |[]                                                          |
    |2_1000_C_T   |500       |1500    |[]                                                          |
    +-------------+----------+--------+------------------------------------------------------------+
    <BLANKLINE>

    The four variants from 1000 to 2000 form one cluster, since no gap between
    adjacent variants exceeds 500, while 5000 starts a cluster of its own. In the
    first cluster, ranked by p-value, the order is 1300, 2000, 1000, 1700:

    * 1300 is the most significant, so it leads;
    * 2000 is 700 from 1300, so it leads too;
    * 1000 is 300 from 1300, so it is clumped and takes the window of 1300;
    * 1700 is 400 from 1300 and 300 from 2000, so it is clumped and takes the
      window of 1300, the more significant of the two leads.

    The clump of 1300 is one locus, so its three rows share an ``id``:

    >>> loci.df.groupBy("id").count().select("count").orderBy("count").collect()[-1]
    Row(count=3)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pydantic import BaseModel, Field

from gentropy.dataset.study_locus import StudyLocus, StudyLocusQualityCheck
from gentropy.method.clumping.cluster import cluster

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from gentropy.dataset.summary_statistics import SummaryStatistics

_LEADS = t.ArrayType(
    t.StructType(
        [
            t.StructField("rank", t.IntegerType()),
            t.StructField("position", t.IntegerType()),
            t.StructField("variantId", t.StringType()),
        ]
    )
)
"""The leads of one cluster: rank in the cluster, from 0, position and variant."""


class WindowClumpingConfig(BaseModel):
    """Parameters of window clumping, validated when the method is created.

    Examples:
        >>> WindowClumpingConfig()
        WindowClumpingConfig(distance=500000, gwas_significance=None, keep_clumped_loci=False)
        >>> WindowClumpingConfig(distance=0)  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        pydantic_core._pydantic_core.ValidationError: 1 validation error for WindowClumpingConfig
        distance
          Input should be greater than 0 ...
    """

    distance: Annotated[int, Field(gt=0)] = 500_000
    """Base pairs on each side of a lead: the clumping window and the locus half-width."""
    gwas_significance: Annotated[float, Field(gt=0, lt=1)] | None = None
    """Largest p-value ``clump`` keeps before clumping; ``None`` keeps every variant."""
    keep_clumped_loci: bool = False
    """Whether clumped variants get rows too, flagged and in their lead's window."""


class WindowClumping:
    """Clump summary statistics into study loci with a fixed window around each lead.

    The module docstring describes the algorithm step by step, with a worked
    example. Create the method once with its parameters, then call ``clump`` on
    each set of summary statistics. Without ``gwas_significance``, filter them to
    significant variants first.

    Examples:
        >>> method = WindowClumping(distance=250_000, gwas_significance=5e-8)
        >>> method.config.distance
        250000
    """

    def __init__(
        self: WindowClumping,
        distance: int,
        gwas_significance: float | None = None,
        keep_clumped_loci: bool = False,
    ) -> None:
        """Validate the parameters and keep them in ``config``.

        Args:
            distance (int): Base pairs on each side of a lead, used both as the
                clumping window and as the half-width of every locus.
            gwas_significance (float | None): Largest p-value to keep before
                clumping. ``None``, the default, clumps every input variant.
            keep_clumped_loci (bool): Whether clumped variants get rows too,
                flagged ``WINDOW_CLUMPED`` and in their lead's window. ``False``,
                the default, returns the leads only.
        """
        self.config = WindowClumpingConfig(
            distance=distance,
            gwas_significance=gwas_significance,
            keep_clumped_loci=keep_clumped_loci,
        )

    def clump(
        self: WindowClumping, summary_statistics: SummaryStatistics
    ) -> StudyLocus:
        """Make one study locus per lead, and optionally a flagged row per clumped variant.

        After the optional significance filter, the steps of the module docstring
        run as three DataFrame transforms, each documented with an example:

        1. ``cluster``, shared with locus-breaker clumping, numbers the
           clusters of each study and chromosome.
        2. ``_assign_leads`` ranks and prunes each cluster. It returns the leads,
           or with ``keep_clumped_loci`` every variant with its clump's lead.
        3. ``_to_study_locus`` builds the windows, ``id`` and flags.

        The data is shuffled once, by study and chromosome; the cluster
        aggregation reuses that partitioning. Everything runs in Spark, with no
        Python UDF.

        Args:
            summary_statistics (SummaryStatistics): The variants to clump. They are
                filtered by ``gwas_significance`` first when it is set; variants
                without a full p-value are left out. ``chromosome`` and
                ``position`` must be filled in; ``variantId`` is not parsed.

        Returns:
            StudyLocus: One row per lead, plus one per clumped variant with
                ``keep_clumped_loci``, with ``ldSet`` and ``locus`` left out.
                Clumped variants carry ``WINDOW_CLUMPED`` in ``qualityControls``;
                leads carry an empty array.

        Examples:
            Two alleles at the same position: the stronger leads, the other is
            clumped, and when kept both rows belong to the lead's locus:

            >>> from gentropy.dataset.summary_statistics import SummaryStatistics
            >>> rows = [
            ...     ("GCST1", "1_1000_A_T", "1", 1000, 0.1, 5000, 1.0, -10, 0.3, 0.01),
            ...     ("GCST1", "1_1000_A_G", "1", 1000, 0.1, 5000, 1.0, -9, 0.3, 0.01),
            ... ]
            >>> summary_statistics = SummaryStatistics(
            ...     spark.createDataFrame(rows, SummaryStatistics.get_schema())
            ... )
            >>> WindowClumping(distance=500).clump(summary_statistics).df.count()
            1
            >>> loci = WindowClumping(distance=500, keep_clumped_loci=True).clump(
            ...     summary_statistics
            ... )
            >>> loci.df.select("leadVariantId", "qualityControls").orderBy(
            ...     "leadVariantId"
            ... ).show(truncate=False)
            +-------------+------------------------------------------------------------+
            |leadVariantId|qualityControls                                             |
            +-------------+------------------------------------------------------------+
            |1_1000_A_G   |[Explained by a more significant variant in the same window]|
            |1_1000_A_T   |[]                                                          |
            +-------------+------------------------------------------------------------+
            <BLANKLINE>
            >>> loci.df.select("id").distinct().count()
            1

            The window is cut at 1, the first position of a chromosome:

            >>> rows = [("GCST1", "1_200_A_T", "1", 200, 0.1, 5000, 1.0, -10, 0.3, 0.01)]
            >>> loci = WindowClumping(distance=500, gwas_significance=5e-8).clump(
            ...     SummaryStatistics(spark.createDataFrame(rows, SummaryStatistics.get_schema()))
            ... )
            >>> loci.df.select("locusStart", "locusEnd").show()
            +----------+--------+
            |locusStart|locusEnd|
            +----------+--------+
            |         1|     700|
            +----------+--------+
            <BLANKLINE>
        """
        if self.config.gwas_significance is not None:
            summary_statistics = summary_statistics.pvalue_filter(
                self.config.gwas_significance
            )
        distance = self.config.distance
        return StudyLocus(
            _df=summary_statistics.df.select(
                "studyId",
                "chromosome",
                "position",
                "variantId",
                "pValueMantissa",
                "pValueExponent",
            )
            # A missing part would rank first and clump measured signals:
            .filter(
                f.col("pValueMantissa").isNotNull()
                & f.col("pValueExponent").isNotNull()
            )
            .transform(cluster, distance)
            .transform(self._assign_leads, distance, self.config.keep_clumped_loci)
            .transform(self._to_study_locus, distance),
            _schema=StudyLocus.get_schema(),
        )

    @staticmethod
    def _assign_leads(
        df: DataFrame, distance: int, keep_clumped_loci: bool = False
    ) -> DataFrame:
        """Prune each cluster into its leads, optionally keeping every variant.

        Each cluster becomes one row holding its variants as an array, sorted by
        p-value, then position, then variant id. Folding over that array keeps
        the leads: a variant joins them unless a lead already chosen is less than
        ``distance`` away. The fold compares each variant with the leads found so
        far, so a cluster costs its size times its number of leads.

        Without ``keep_clumped_loci``, the leads array becomes the rows, and
        nothing more is computed per variant. With it, each variant also takes
        the first lead, in ranking order, within ``distance``: itself if it is a
        lead, otherwise the most significant lead that explains it. That is a
        second pass of the same cost.

        The grouping keys include study and chromosome, so Spark reuses the
        partitioning of ``cluster`` and does not shuffle again.

        Args:
            df (DataFrame): Variants with ``studyId``, ``chromosome``,
                ``clusterId``, ``position``, ``variantId``, ``pValueMantissa`` and
                ``pValueExponent``.
            distance (int): The distance, in base pairs, below which a lead
                explains a variant.
            keep_clumped_loci (bool): Whether to return the clumped variants too.

        Returns:
            DataFrame: One row per lead, or per variant with ``keep_clumped_loci``,
                with ``studyId``, ``chromosome``, ``variantId``, ``isLead`` and
                ``leadPosition``.

        Examples:
            The cluster of the module example has two leads:

            >>> data = [
            ...     ("1_1000_A_T", 1000, 1.0, -10),
            ...     ("1_1300_C_G", 1300, 1.0, -12),
            ...     ("1_1700_G_A", 1700, 1.0, -9),
            ...     ("1_2000_T_C", 2000, 1.0, -11),
            ... ]
            >>> df = spark.createDataFrame(
            ...     [("GCST1", "1", 1, *row) for row in data],
            ...     "studyId string, chromosome string, clusterId long, variantId string, "
            ...     "position int, pValueMantissa float, pValueExponent int",
            ... )
            >>> WindowClumping._assign_leads(df, 500).orderBy("variantId").show()
            +-------+----------+----------+------+------------+
            |studyId|chromosome| variantId|isLead|leadPosition|
            +-------+----------+----------+------+------------+
            |  GCST1|         1|1_1300_C_G|  true|        1300|
            |  GCST1|         1|1_2000_T_C|  true|        2000|
            +-------+----------+----------+------+------------+
            <BLANKLINE>

            Kept, 1700 is within 500 of both leads, and takes 1300, the more
            significant:

            >>> WindowClumping._assign_leads(df, 500, keep_clumped_loci=True).orderBy(
            ...     "variantId"
            ... ).show()
            +-------+----------+----------+------+------------+
            |studyId|chromosome| variantId|isLead|leadPosition|
            +-------+----------+----------+------+------------+
            |  GCST1|         1|1_1000_A_T| false|        1300|
            |  GCST1|         1|1_1300_C_G|  true|        1300|
            |  GCST1|         1|1_1700_G_A| false|        1300|
            |  GCST1|         1|1_2000_T_C|  true|        2000|
            +-------+----------+----------+------+------------+
            <BLANKLINE>

            The comparison is strict: a variant exactly ``distance`` away from a
            lead leads too, and one at the same position is clumped:

            >>> data = [("1_10_A_T", 10, 1.0, -9), ("1_12_C_G", 12, 2.0, -9), ("1_10_A_G", 10, 3.0, -9)]
            >>> df = spark.createDataFrame(
            ...     [("GCST1", "1", 1, *row) for row in data],
            ...     "studyId string, chromosome string, clusterId long, variantId string, "
            ...     "position int, pValueMantissa float, pValueExponent int",
            ... )
            >>> WindowClumping._assign_leads(df, 2, keep_clumped_loci=True).orderBy(
            ...     "variantId"
            ... ).select("variantId", "isLead").show()
            +---------+------+
            |variantId|isLead|
            +---------+------+
            | 1_10_A_G| false|
            | 1_10_A_T|  true|
            | 1_12_C_G|  true|
            +---------+------+
            <BLANKLINE>
        """

        def within_distance(lead: Column, variant: Column) -> Column:
            return f.abs(lead["position"] - variant["position"]) < distance

        ranked = f.array_sort(
            f.collect_list(
                f.struct("pValueExponent", "pValueMantissa", "position", "variantId")
            )
        )
        leads = f.aggregate(
            f.transform(
                "ranked",
                lambda variant, rank: f.struct(
                    rank.alias("rank"),
                    variant["position"].alias("position"),
                    variant["variantId"].alias("variantId"),
                ),
            ),
            f.array().cast(_LEADS),
            lambda chosen, variant: f.when(
                f.exists(chosen, lambda lead: within_distance(lead, variant)), chosen
            ).otherwise(f.concat(chosen, f.array(variant))),
        )
        clusters = (
            df.groupBy("studyId", "chromosome", "clusterId")
            .agg(ranked.alias("ranked"))
            .withColumn("leads", leads)
        )
        if not keep_clumped_loci:
            return clusters.select(
                "studyId",
                "chromosome",
                f.inline(
                    f.transform(
                        "leads",
                        lambda lead: f.struct(
                            lead["variantId"].alias("variantId"),
                            f.lit(True).alias("isLead"),
                            lead["position"].alias("leadPosition"),
                        ),
                    )
                ),
            )
        return clusters.select(
            "studyId",
            "chromosome",
            f.inline(
                f.transform(
                    "ranked",
                    lambda variant, rank: f.struct(
                        variant["variantId"].alias("variantId"),
                        f.exists("leads", lambda lead: lead["rank"] == rank).alias(
                            "isLead"
                        ),
                        f.filter("leads", lambda lead: within_distance(lead, variant))[
                            0
                        ]["position"].alias("leadPosition"),
                    ),
                )
            ),
        )

    @staticmethod
    def _to_study_locus(df: DataFrame, distance: int) -> DataFrame:
        """Build the study locus columns from each variant and its lead.

        The window is ``distance`` base pairs on each side of the lead, cut at 1,
        the first position of a chromosome. ``id`` is hashed from
        ``StudyLocus.ID_COLUMNS``, so every row of a clump shares it. Variants
        that are not leads get the ``WINDOW_CLUMPED`` flag.

        Args:
            df (DataFrame): Variants with ``studyId``, ``chromosome``,
                ``variantId``, ``isLead`` and ``leadPosition``.
            distance (int): Base pairs on each side of the lead.

        Returns:
            DataFrame: The ``StudyLocus`` columns, without ``ldSet`` and ``locus``.

        Examples:
            >>> df = spark.createDataFrame(
            ...     [("GCST1", "1", "1_1300_C_G", True, 1300), ("GCST1", "1", "1_1000_A_T", False, 1300)],
            ...     "studyId string, chromosome string, variantId string, isLead boolean, leadPosition int",
            ... )
            >>> loci = WindowClumping._to_study_locus(df, 500)
            >>> loci.drop("id").orderBy("leadVariantId").show(truncate=False)
            +-------+----------+----------+--------+-------------+------------------------------------------------------------+
            |studyId|chromosome|locusStart|locusEnd|leadVariantId|qualityControls                                             |
            +-------+----------+----------+--------+-------------+------------------------------------------------------------+
            |GCST1  |1         |800       |1800    |1_1000_A_T   |[Explained by a more significant variant in the same window]|
            |GCST1  |1         |800       |1800    |1_1300_C_G   |[]                                                          |
            +-------+----------+----------+--------+-------------+------------------------------------------------------------+
            <BLANKLINE>
            >>> loci.select("id").distinct().count()
            1
        """
        return (
            df.select(
                "studyId",
                "chromosome",
                f.greatest(f.lit(1), f.col("leadPosition") - distance).alias(
                    "locusStart"
                ),
                (f.col("leadPosition") + distance).alias("locusEnd"),
                f.col("variantId").alias("leadVariantId"),
                f.when(f.col("isLead"), f.array().cast(t.ArrayType(t.StringType())))
                .otherwise(f.array(f.lit(StudyLocusQualityCheck.WINDOW_CLUMPED.value)))
                .alias("qualityControls"),
            )
            .withColumn("id", StudyLocus.generate_identifier(StudyLocus.ID_COLUMNS))
            .select(
                "id",
                "studyId",
                "chromosome",
                "locusStart",
                "locusEnd",
                "leadVariantId",
                "qualityControls",
            )
        )
