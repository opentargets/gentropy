"""Traits for the identifiers other datasets refer to.

A ``HasXId`` trait says a dataset holds the identifier of ``X``, so a method can
read or join on it without knowing what the column is called. Each dataset
resolves the identifier through its own link, so ``StudyLocus`` can keep its key
as ``id`` while another dataset stores it as ``studyLocusId``.

Example:
    >>> [field.key for field in HasStudyId.fields]
    ['HasStudyId.study_id']
    >>> [field.name for field in HasVariantId.fields]
    ['variantId']
    >>> HasCredibleSetId.credible_set_id.name
    'credibleSetId'
"""

from __future__ import annotations

from pyspark.sql import types as t

from gentropy.model.core.trait import Field, Trait


class HasStudyId(Trait):
    """Holds a study identifier, the key of the study index and the summary statistics."""

    study_id = Field(t.StringType())


class HasVariantId(Trait):
    """Holds a variant identifier harmonised to the variant index, ``chr_pos_ref_alt``."""

    variant_id = Field(t.StringType())


class HasStudyLocusId(Trait):
    """Holds the key of a study locus."""

    study_locus_id = Field(t.StringType())


class HasCredibleSetId(Trait):
    """Holds the key of a credible set."""

    credible_set_id = Field(t.StringType())
