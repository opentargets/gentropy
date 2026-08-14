"""BigBrain brain-tissue molecular-QTL datasource module.

This module provides shared constants and metadata for the BigBrain multi-cohort
brain meta-analysis. It defines the QTL types supported and the bibliographic
metadata for the underlying publication.

The BigBrain dataset originates from the study:
    Réal A, Kailash BP, Dredge W et al.
    "Mapping genetic effects on splicing in ten thousand post-mortem brain samples
    reveals novel mediators of neurological disease risk."
    medRxiv, 2025. Meta-analysis method (mmQTL): PubMed 35058635.

Two sub-datasets are supported, both restricted to the EUR-ancestry release:

- **EQTL** (`BigBrain-eqtl-EUR`): cis-eQTL associations, one study per Ensembl gene.
- **SQTL** (`BigBrain-sqtl-EUR`): cis-sQTL associations, one study per Leafcutter
    intron-cluster feature.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class BigBrainQtlType(str, Enum):
    """Enumeration of BigBrain QTL type identifiers.

    These identifiers are embedded in study IDs and used throughout the pipeline
    to distinguish the two molecular-trait types.
    """

    EQTL = "eqtl"
    """cis-eQTL associations (feature = versioned Ensembl gene ID)."""
    SQTL = "sqtl"
    """cis-sQTL associations (feature = Leafcutter intron-cluster coordinate string)."""


class BigBrainPublicationMetadata(BaseModel):
    """Bibliographic and cohort metadata for the BigBrain publication.

    All fields carry defaults matching the published study and are used to populate
    the study index with consistent provenance information.

    !!! note "Sample size is a backfilled constant"
        BigBrain's full/top association files report no per-variant sample size or
        allele frequency. `SAMPLE_SIZE` is the single overall donor count reported
        for the whole resource (shared by eQTL and sQTL); the README does not break
        this out by ancestry, so it is an approximation even for the EUR-only release.
    """

    PUBMED_ID: str = "35058635"
    """PubMed ID for the mmQTL meta-analysis method used to generate BigBrain."""
    PUB_TITLE: str = "Mapping genetic effects on splicing in ten thousand post-mortem brain samples reveals novel mediators of neurological disease risk"
    """Title of the BigBrain publication."""
    PUB_FIRST_AUTHOR: str = "Réal A, Kailash BP"
    """First author(s) of the BigBrain publication."""
    PUB_DATE: str = "2025"
    """Publication date of the BigBrain study."""
    PUB_JOURNAL: str = "medRxiv"
    """Journal/preprint server where the BigBrain study was published."""
    SAMPLE_SIZE: int = 10_725
    """Backfilled total sample size (10,725 samples from 4,656 donors), shared by eQTL and sQTL."""
    ANCESTRY: str = "EUR"
    """Ancestry of the study population covered by this ingestion."""
    COHORTS: str = "BigBrain"
    """Cohort label for the BigBrain meta-analysis (43 tissue-cohort pairs meta-analysed)."""
    BIOSAMPLE_ID: str = "UBERON_0000955"
    """Biosample ID for the BigBrain study - brain."""
