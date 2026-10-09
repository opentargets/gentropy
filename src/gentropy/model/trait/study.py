"""Traits describing a study: which project it belongs to and what kind it is.

``HasStudyType`` holds the study's type, such as ``gwas`` or ``eqtl``. Methods
that treat GWAS and molecular QTL studies differently read it, and datasets
derived from studies, such as credible sets, carry a copy of it. ``HasProjectId``
holds the project or consortium a study comes from.

Example:
    >>> HasStudyType.study_type.name, HasProjectId.project_id.name
    ('studyType', 'projectId')
"""

from __future__ import annotations

from pyspark.sql import types as t

from gentropy.model.core.trait import Field, Trait


class HasStudyType(Trait):
    """Holds the type of study, such as ``gwas`` or ``eqtl``."""

    study_type = Field(t.StringType())


class HasProjectId(Trait):
    """Holds the project or consortium a study comes from."""

    project_id = Field(t.StringType())
