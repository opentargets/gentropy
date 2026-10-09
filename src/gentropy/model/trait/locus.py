"""Traits describing where a locus is and which variant leads it.

``Region`` is the window a locus covers on one chromosome. ``LeadVariant`` is
the variant that defines the locus, for example the one clumping picked or the
one with the highest posterior probability in a credible set; the dataset
decides which and documents it.

Example:
    >>> [field.name for field in Region.fields]
    ['chromosome', 'locusStart', 'locusEnd']
    >>> [field.name for field in LeadVariant.fields]
    ['variantId']
"""

from __future__ import annotations

from pyspark.sql import types as t

from gentropy.model.core.trait import Field, Trait


class Region(Trait):
    """A window on one chromosome, from ``locusStart`` to ``locusEnd``."""

    chromosome = Field(t.StringType())
    locus_start = Field(t.IntegerType())
    locus_end = Field(t.IntegerType())


class LeadVariant(Trait):
    """The variant that leads the locus."""

    variant_id = Field(t.StringType())
