"""Traits for joining one dataset to another through a shared key.

A ``CanJoinToX`` trait says a dataset holds the key of dataset ``X``, so a method
can join the two without knowing what the column is called on either side. Each
side resolves the key through its own link, so ``StudyLocus`` can keep its key as
``id`` while another dataset stores it as ``studyLocusId``.

A trait here names only the key. Whether a join on that key is meaningful for a
given method, for example whether it also needs the region, is the method's
business.

Example:
    >>> [field.key for field in CanJoinToStudyIndex.fields]
    ['CanJoinToStudyIndex.study_id']
    >>> [field.name for field in CanJoinToVariantIndex.fields]
    ['variantId']
"""

from __future__ import annotations

from pyspark.sql import types as t

from gentropy.model.core.trait import Field, Trait


class CanJoinToStudyIndex(Trait):
    """Holds the study identifier used by the study index."""

    study_id = Field(t.StringType())


class CanJoinToSummaryStatistics(Trait):
    """Holds the study identifier that partitions summary statistics."""

    study_id = Field(t.StringType())


class CanJoinToVariantIndex(Trait):
    """Holds a variant identifier in the variant index format."""

    variant_id = Field(t.StringType())


class CanJoinToStudyLocus(Trait):
    """Holds the key of a study locus."""

    study_locus_id = Field(t.StringType())
