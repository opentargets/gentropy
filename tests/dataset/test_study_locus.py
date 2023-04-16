"""Test study locus dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame

from otg.dataset.study_locus import (
    CredibleInterval,
    StudyLocus,
    StudyLocusGWASCatalog,
    StudyLocusOverlap,
)

if TYPE_CHECKING:
    from otg.dataset.study_index import StudyIndex, StudyIndexGWASCatalog
    from otg.dataset.variant_annotation import VariantAnnotation


def test_study_locus_creation(mock_study_locus: StudyLocus) -> None:
    """Test study locus creation with mock data."""
    assert isinstance(mock_study_locus, StudyLocus)


def test_study_locus_gwas_catalog_from_source(
    mock_variant_annotation: VariantAnnotation,
    sample_gwas_catalog_associations: DataFrame,
) -> None:
    """Test study locus from gwas catalog mock data."""
    assert isinstance(
        StudyLocusGWASCatalog.from_source(
            sample_gwas_catalog_associations, mock_variant_annotation
        ),
        DataFrame,
    )


def test__map_to_variant_annotation_variants(
    sample_gwas_catalog_associations: DataFrame,
    mock_variant_annotation: VariantAnnotation,
) -> None:
    """Test mapping to variant annotation variants."""
    assert isinstance(
        StudyLocusGWASCatalog._map_to_variant_annotation_variants(
            sample_gwas_catalog_associations.withColumn(
                "studyLocusId", f.monotonically_increasing_id()
            ),
            mock_variant_annotation,
        ),
        DataFrame,
    )


def test_study_locus_gwas_catalog_creation(
    mock_study_locus_gwas_catalog: StudyLocusGWASCatalog,
) -> None:
    """Test study locus creation with mock data."""
    assert isinstance(mock_study_locus_gwas_catalog, StudyLocusGWASCatalog)


def test_study_locus_overlaps(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex
) -> None:
    """Test study locus overlaps."""
    assert isinstance(mock_study_locus.overlaps(mock_study_index), StudyLocusOverlap)


def test_credible_set(mock_study_locus: StudyLocus) -> None:
    """Test credible interval."""
    assert isinstance(mock_study_locus.credible_set(CredibleInterval.IS95), StudyLocus)


def test_unique_lead_tag_variants(mock_study_locus: StudyLocus) -> None:
    """Test unique lead tag variants."""
    assert isinstance(mock_study_locus.unique_lead_tag_variants(), DataFrame)


def test_unique_study_locus_ancestries(
    mock_study_locus: StudyLocus, mock_study_index_gwas_catalog: StudyIndexGWASCatalog
) -> None:
    """Test study locus ancestries."""
    assert isinstance(
        mock_study_locus.unique_study_locus_ancestries(mock_study_index_gwas_catalog),
        DataFrame,
    )


def test_neglog_pvalue(mock_study_locus: StudyLocus) -> None:
    """Test neglog pvalue."""
    assert isinstance(mock_study_locus.neglog_pvalue(), Column)


def test_annotate_credible_sets(mock_study_locus: StudyLocus) -> None:
    """Test annotate credible sets."""
    assert isinstance(mock_study_locus.annotate_credible_sets(), StudyLocus)


def test_clump(mock_study_locus: StudyLocus) -> None:
    """Test clump."""
    assert isinstance(mock_study_locus.clump(), StudyLocus)


def test_qc_ambiguous_study(
    mock_study_locus_gwas_catalog: StudyLocusGWASCatalog,
) -> None:
    """Test qc ambiguous."""
    assert isinstance(
        mock_study_locus_gwas_catalog._qc_ambiguous_study(), StudyLocusGWASCatalog
    )


def test_qc_unresolved_ld(mock_study_locus_gwas_catalog: StudyLocusGWASCatalog) -> None:
    """Test qc unresolved ld."""
    assert isinstance(
        mock_study_locus_gwas_catalog._qc_unresolved_ld(), StudyLocusGWASCatalog
    )


def test_qc_all(sample_gwas_catalog_associations: DataFrame) -> None:
    """Test qc all with some hard-coded values."""
    assert isinstance(
        sample_gwas_catalog_associations.withColumn(
            # Perform all quality control checks:
            "qualityControls",
            StudyLocusGWASCatalog._qc_all(
                f.array().alias("qualityControls"),
                f.col("CHR_ID"),
                f.col("CHR_POS"),
                f.lit("A").alias("referenceAllele"),
                f.lit("T").alias("referenceAllele"),
                f.col("STRONGEST SNP-RISK ALLELE"),
                *StudyLocusGWASCatalog._parse_pvalue(f.col("P-VALUE")),
                5e-8,
            ),
        ),
        DataFrame,
    )
