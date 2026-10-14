"""Traits describing a study: which project it belongs to and what kind it is.

``HasStudyType`` holds the study's type, such as ``gwas`` or ``eqtl``. Methods
that treat GWAS and molecular QTL studies differently read it, and datasets
derived from studies, such as credible sets, carry a copy of it. ``HasProjectId``
holds the project or consortium a study comes from. ``LDPopulationStructure``
holds the share of each LD reference population among the study's samples, so
LD can be read from the population that dominates the study.

Example:
    >>> HasStudyType.study_type.name, HasProjectId.project_id.name
    ('studyType', 'projectId')
    >>> LDPopulationStructure.ld_population_structure.dtype.simpleString()
    'array<struct<ldPopulation:string,relativeSampleSize:double>>'
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


class LDPopulationStructure(Trait):
    """Holds the share of each LD reference population among the study's samples."""

    ld_population_structure = Field(
        t.ArrayType(
            t.StructType(
                [
                    t.StructField("ldPopulation", t.StringType()),
                    t.StructField("relativeSampleSize", t.DoubleType()),
                ]
            ),
            containsNull=False,
        )
    )
