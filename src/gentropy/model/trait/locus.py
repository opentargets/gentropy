"""Traits describing where a locus is and which variant leads it.

``Region`` is the window a locus covers on one chromosome. ``LeadVariant`` is
the variant that defines the locus, for example the one clumping picked or the
one with the highest posterior probability in a credible set; the dataset
decides which and documents it. ``VariantLocation`` is where one variant sits on
the genome; the variant's identifier comes from other traits, such as
``HasVariantId``. ``LocusAnnotated`` is the ``locus`` of a region: the statistics
of every variant the study measured inside it, sorted by position.
``LDAnnotated`` is the ``ldSet`` of a lead: the variants correlated with it in
the population that dominates the study, with their ``r2Overall``.

Example:
    >>> [field.name for field in Region.fields]
    ['chromosome', 'locusStart', 'locusEnd']
    >>> [field.name for field in LeadVariant.fields]
    ['variantId']
    >>> [field.name for field in VariantLocation.fields]
    ['chromosome', 'position']
    >>> LocusAnnotated.locus.dtype.elementType.fieldNames()
    ['variantId', 'pValueMantissa', 'pValueExponent', 'effectAlleleFrequencyFromSource', 'sampleSize', 'beta', 'standardError']
    >>> LDAnnotated.ld_set.dtype.simpleString()
    'array<struct<tagVariantId:string,r2Overall:float>>'
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


class VariantLocation(Trait):
    """Where one variant sits: its chromosome and its position on it."""

    chromosome = Field(t.StringType())
    position = Field(t.IntegerType())


class LocusAnnotated(Trait):
    """The statistics of every variant measured inside the region, by position.

    The nested types are those of the ``Association`` fields, so the statistics
    are collected without a cast.
    """

    locus = Field(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField("variantId", t.StringType()),
                    t.StructField("pValueMantissa", t.FloatType()),
                    t.StructField("pValueExponent", t.IntegerType()),
                    t.StructField("effectAlleleFrequencyFromSource", t.FloatType()),
                    t.StructField("sampleSize", t.IntegerType()),
                    t.StructField("beta", t.DoubleType()),
                    t.StructField("standardError", t.DoubleType()),
                ]
            )
        )
    )


class LDAnnotated(Trait):
    """The variants correlated with the lead, with their ``r2Overall``."""

    ld_set = Field(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField("tagVariantId", t.StringType()),
                    t.StructField("r2Overall", t.FloatType()),
                ]
            )
        )
    )
