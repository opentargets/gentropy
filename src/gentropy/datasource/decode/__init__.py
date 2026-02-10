"""deCODE proteomics datasource module.

This module provides shared constants and metadata for the deCODE proteomics data source.
It defines the project identifiers used across the ingestion pipeline and the bibliographic
metadata for the underlying publication.

The deCODE proteomics dataset originates from the study:
    Eldjarn GH, Ferkingstad E et al.
    "Large-scale plasma proteomics comparisons through genetics and disease associations."
    Nature, 2024. PubMed: 37794188

Two sub-datasets are supported:

- **RAW** (`deCODE-proteomics-raw`): Non-normalised SomaScan measurements from ~36,136 Icelandic
    individuals.
- **SMP** (`deCODE-proteomics-smp`): SMP-normalised SomaScan measurements from ~35,892 Icelandic
    individuals.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class deCODEDataSource(str, Enum):
    """Enumeration of deCODE proteomics data source identifiers.

    These project IDs are embedded in study IDs and used throughout the pipeline
    to distinguish the two SomaScan assay normalisations.
    """

    DECODE_PROTEOMICS_RAW = "deCODE-proteomics-raw"
    """Non-normalised SomaScan measurements."""
    DECODE_PROTEOMICS_SMP = "deCODE-proteomics-smp"
    """SMP-normalised SomaScan measurements."""


class deCODEPublicationMetadata(BaseModel):
    """Bibliographic and cohort metadata for the deCODE proteomics publication.

    All fields carry defaults matching the published study and are used to populate
    the study index with consistent provenance information.
    """

    PUBMED_ID: str = "37794188"
    """PubMed ID for the deCODE proteomics study."""
    PUB_TITLE: str = "Large-scale plasma proteomics comparisons through genetics and disease associations"
    """Title of the deCODE proteomics publication."""
    PUB_FIRST_AUTHOR: str = "Eldjarn GH, Ferkingstad E"
    """First author(s) of the deCODE proteomics publication."""
    PUB_DATE: str = "2024"
    """Publication date of the deCODE proteomics study."""
    PUB_JOURNAL: str = "Nature"
    """Journal where the deCODE proteomics study was published."""
    SMP_SAMPLE_SIZE: int = 35_892
    """Sample size for SMP-normalized proteomics data."""
    SAMPLE_SIZE: int = 36_136
    """Sample size for non-normalized proteomics data."""
    ANCESTRY: str = "Icelandic"
    """Ancestry of the study population."""
    COHORTS: str = "deCODE"
    """Cohorts involved in the deCODE proteomics study."""
    BIOSAMPLE_ID: str = "UBERON_0001969"
    """Biosample ID for deCODE proteomics study - blood plasma."""
