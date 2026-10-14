"""LD clumping: flag study loci explained by a more significant lead in high LD.

Window clumping keeps leads that are at least a window apart, but two such leads
can still be correlated. LD clumping finds them from each lead's ``ldSet``, the
variants a reference panel says are correlated with it in the study's major
population. It needs both annotations: ``ldSet`` for the linkage, and ``locus``
for the lead's own p-value.

The algorithm, per study and chromosome:

1. **Order the leads** by their p-value, read from the lead's entry in
   ``locus``. Equal p-values are ordered by lead id, so the result does not
   depend on how Spark partitions the rows. A lead missing from its own locus
   has no p-value and comes last.
2. **Collect the more significant leads.** Each lead gets the set of leads up to
   and including itself in that order.
3. **Look for linkage.** A lead is linked when another of those leads is among
   its tags, that is, when a more significant lead is in high LD with it.
4. **Flag or drop.** A linked lead's ``ldSet`` is emptied and it is flagged
   ``LD_CLUMPED``. Without ``keep_clumped_loci``, the default, linked leads are
   dropped instead.

As in the legacy method, every lead counts in step 2, including leads that are
themselves linked. So in a chain A, B, C, from most to least significant, where
A tags B and B tags C but A does not tag C, both B and C are clumped. Rows
already flagged by window clumping take part as well.

The loci keep their bounds and ``id``: LD changes which loci survive, not the
region a locus covers.

Examples:
    B tags the more significant A, so B is clumped; C, on another chromosome,
    is not:

    >>> def locus(lead, exponent):
    ...     return [(lead, 1.0, exponent, 0.3, 1000, 0.1, 0.01)]
    >>> rows = [
    ...     ("A", "S1", "1", 1, 1000, "A", [], locus("A", -20), [("A", 1.0), ("B", 0.8)]),
    ...     ("B", "S1", "1", 1, 1000, "B", [], locus("B", -10), [("B", 1.0), ("A", 0.8)]),
    ...     ("C", "S1", "2", 1, 1000, "C", [], locus("C", -10), [("C", 1.0), ("A", 0.8)]),
    ... ]
    >>> loci = LDAnnotatedStudyLocus(
    ...     spark.createDataFrame(rows, LDAnnotatedStudyLocus.schema)
    ... )
    >>> clumped = LDClumping(keep_clumped_loci=True).clump(loci)
    >>> for row in clumped.df.orderBy(clumped.lead_variant_id).collect():
    ...     print(row.leadVariantId, len(row.ldSet), row.qualityControls)
    A 2 []
    B 0 ['Explained by a more significant variant in high LD']
    C 2 []

    By default the clumped lead is dropped:

    >>> sorted(row.leadVariantId for row in LDClumping().clump(loci).df.collect())
    ['A', 'C']
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

from pydantic import BaseModel
from pyspark.sql import Window
from pyspark.sql import functions as f

from gentropy.common.quality_control import add_flag
from gentropy.common.spark import element_struct
from gentropy.dataset.study_locus import LDAnnotatedStudyLocus, StudyLocusQualityFlag
from gentropy.model.trait.association import Association
from gentropy.model.trait.locus import LDAnnotated

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

_TAG, _ = element_struct(LDAnnotated.ld_set.dtype).fieldNames()
"""``ldSet`` field holding the tag variant."""

_LINKED = "isLinked"
"""Temporary column marking linked leads; a window result is filtered as a column."""

_TAGS = "tags"
"""Temporary column: the lead and the ids of its tags."""


class LDClumpingConfig(BaseModel):
    """Parameters of LD clumping, validated when the method is created.

    Examples:
        >>> LDClumpingConfig()
        LDClumpingConfig(keep_clumped_loci=False)
    """

    keep_clumped_loci: bool = False
    """Whether clumped leads are kept, flagged and with an empty ``ldSet``."""


class LDClumping:
    """Flag or drop study loci whose lead is in LD with a more significant lead.

    The module docstring describes the algorithm with a worked example. The LD
    threshold is applied earlier, by ``annotate_ld``: a tag is in ``ldSet`` only
    if it passed it.

    Examples:
        >>> LDClumping().config.keep_clumped_loci
        False
    """

    def __init__(self: Self, keep_clumped_loci: bool = False) -> None:
        """Validate the parameters and keep them in ``config``.

        Args:
            keep_clumped_loci (bool): Whether clumped leads are kept, flagged
                ``LD_CLUMPED`` and with an empty ``ldSet``. ``False``, the
                default, drops them.
        """
        self.config = LDClumpingConfig(keep_clumped_loci=keep_clumped_loci)

    def clump(self: Self, loci: LDAnnotatedStudyLocus) -> LDAnnotatedStudyLocus:
        """Clump the loci by the LD between their leads.

        Linkage is found on a narrow projection of the loci, so only lead ids,
        p-values and tag ids are shuffled. The linked leads, two short strings
        each, are then broadcast and joined back by ``(id, leadVariantId)``, so
        the rows with their ``locus`` never move. The loci are read twice, once
        narrow and once whole, so pass loci read from storage or persisted:
        otherwise Spark recomputes whatever produced them for each read.

        Args:
            loci (LDAnnotatedStudyLocus): Loci with ``locus`` and ``ldSet``.

        Returns:
            LDAnnotatedStudyLocus: The same loci, with linked leads flagged or
                dropped.
        """
        keys = [
            LDAnnotatedStudyLocus.id.name,
            LDAnnotatedStudyLocus.lead_variant_id.name,
        ]
        # Spark estimates the keys from the window's input, not its output, so it
        # would shuffle both sides; the hint keeps the wide rows where they are.
        linked = f.broadcast(self._linked_leads(loci))
        if not self.config.keep_clumped_loci:
            return LDAnnotatedStudyLocus(loci.df.join(linked, on=keys, how="left_anti"))
        is_linked = f.coalesce(f.col(_LINKED), f.lit(False))
        clumped = loci.df.join(
            linked.withColumn(_LINKED, f.lit(True)), on=keys, how="left"
        ).withColumns(
            {
                LDAnnotatedStudyLocus.ld_set.name: f.when(
                    is_linked, f.array().cast(LDAnnotated.ld_set.dtype)
                ).otherwise(f.col(LDAnnotatedStudyLocus.ld_set.name)),
                LDAnnotatedStudyLocus.quality_controls.name: add_flag(
                    f.col(LDAnnotatedStudyLocus.quality_controls.name),
                    is_linked,
                    StudyLocusQualityFlag.LD_CLUMPED,
                ),
            }
        )
        return LDAnnotatedStudyLocus(
            clumped.select(*LDAnnotatedStudyLocus.schema.fieldNames())
        )

    @staticmethod
    def _linked_leads(loci: LDAnnotatedStudyLocus) -> DataFrame:
        """The ``(id, leadVariantId)`` of every lead linked to a more significant one.

        Only the columns the linkage needs are selected before the window, so
        ``locus`` and the r2 values never reach the shuffle.

        Args:
            loci (LDAnnotatedStudyLocus): Loci with ``locus`` and ``ldSet``.

        Returns:
            DataFrame: One row per linked lead, keyed like the loci.

        Examples:
            >>> def locus(lead, exponent):
            ...     return [(lead, 1.0, exponent, 0.3, 1000, 0.1, 0.01)]
            >>> rows = [
            ...     ("A", "S1", "1", 1, 1000, "A", [], locus("A", -20), [("A", 1.0), ("B", 0.8)]),
            ...     ("B", "S1", "1", 1, 1000, "B", [], locus("B", -10), [("B", 1.0), ("A", 0.8)]),
            ... ]
            >>> loci = LDAnnotatedStudyLocus(
            ...     spark.createDataFrame(rows, LDAnnotatedStudyLocus.schema)
            ... )
            >>> LDClumping._linked_leads(loci).collect()
            [Row(id='B', leadVariantId='B')]
        """
        lead = loci.lead_variant_id
        # The lead's own entry in its locus holds its p-value; read the array once.
        entry = f.get(
            f.filter(loci.locus, lambda e: e[Association.variant_id.name] == lead), 0
        )
        narrow = loci.df.select(
            loci.id,
            loci.study_id,
            loci.chromosome,
            lead,
            entry[Association.p_value_exponent.name].alias(
                Association.p_value_exponent.name
            ),
            entry[Association.p_value_mantissa.name].alias(
                Association.p_value_mantissa.name
            ),
            f.array_union(
                f.array(lead), f.transform(loci.ld_set, lambda e: e[_TAG])
            ).alias(_TAGS),
        )
        more_significant = (
            Window.partitionBy(
                LDAnnotatedStudyLocus.study_id.name,
                LDAnnotatedStudyLocus.chromosome.name,
            )
            .orderBy(
                f.col(Association.p_value_exponent.name).asc_nulls_last(),
                f.col(Association.p_value_mantissa.name).asc_nulls_last(),
                f.col(LDAnnotatedStudyLocus.lead_variant_id.name).asc_nulls_last(),
            )
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        leads_so_far = f.collect_set(LDAnnotatedStudyLocus.lead_variant_id.name).over(
            more_significant
        )
        return (
            narrow.withColumn(
                _LINKED,
                # The lead itself is always in both sets, so linkage needs a second.
                f.size(f.array_intersect(leads_so_far, f.col(_TAGS))) > 1,
            )
            .filter(
                f.col(_LINKED)
                & f.col(LDAnnotatedStudyLocus.lead_variant_id.name).isNotNull()
            )
            .select(
                LDAnnotatedStudyLocus.id.name,
                LDAnnotatedStudyLocus.lead_variant_id.name,
            )
            .distinct()
        )
