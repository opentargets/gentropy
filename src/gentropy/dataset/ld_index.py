"""LD index: one row per variant of the LD reference panel, with the variants it tags.

The index is built from the gnomAD LD matrices. Each variant appears once, with
every variant it is correlated with above the build threshold in any of the
panel's populations. ``ldSet`` keeps ``r`` for each population separately,
because the population that matters depends on the study the variant is looked
up for; the reader picks the population and squares ``r`` itself.

``variantId`` is harmonised to the variant index, so a study locus lead can be
looked up directly. ``chromosome`` sits next to it so the lookup can join on
both and prune by chromosome.

The index has no traits: it is only ever used whole, to annotate study loci with
their LD, so every column is simply required.

Examples:
    >>> LDIndex.print_schema()
    LDIndex
     |-- variantId: string (mandatory)
     |-- chromosome: string (mandatory)
     |-- ldSet: array (mandatory)
     |    |-- element: struct
     |    |    |-- tagVariantId: string
     |    |    |-- rValues: array
     |    |    |    |-- element: struct
     |    |    |    |    |-- population: string
     |    |    |    |    |-- r: double

    A variant that tags itself and one neighbour, measured in two populations:

    >>> self_r = [("nfe", 1.0), ("afr", 1.0)]
    >>> neighbour_r = [("nfe", 0.8), ("afr", 0.4)]
    >>> ld_set = [("1_100_A_T", self_r), ("1_200_C_G", neighbour_r)]
    >>> ld_index = LDIndex(
    ...     spark.createDataFrame([("1_100_A_T", "1", ld_set)], LDIndex.schema)
    ... )
    >>> for tag in ld_index.df.select(ld_index.ld_set).first().ldSet:
    ...     print(tag.tagVariantId, [(r.population, r.r) for r in tag.rValues])
    1_100_A_T [('nfe', 1.0), ('afr', 1.0)]
    1_200_C_G [('nfe', 0.8), ('afr', 0.4)]
"""

from __future__ import annotations

from pyspark.sql import types as t

from gentropy.model.core.dataset import Dataset
from gentropy.model.core.schema import required
from gentropy.model.core.trait import Trait


class LDIndex(Dataset[Trait]):
    """One row per reference panel variant, with the variants it tags."""

    variant_id = required(t.StringType())
    chromosome = required(t.StringType())
    ld_set = required(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField("tagVariantId", t.StringType(), nullable=False),
                    t.StructField(
                        "rValues",
                        t.ArrayType(
                            t.StructType(
                                [
                                    t.StructField(
                                        "population", t.StringType(), nullable=False
                                    ),
                                    t.StructField("r", t.DoubleType(), nullable=False),
                                ]
                            ),
                            containsNull=False,
                        ),
                        nullable=False,
                    ),
                ]
            ),
            containsNull=False,
        )
    )
