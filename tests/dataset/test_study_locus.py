"""Test study locus dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from otg.dataset.study_locus import (
    CredibleInterval,
    StudyLocus,
    StudyLocusGWASCatalog,
    StudyLocusOverlap,
)

if TYPE_CHECKING:
    from otg.dataset.ld_index import LDIndex
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
        StudyLocusGWASCatalog,
    )


def test__map_to_variant_annotation_variants(
    sample_gwas_catalog_associations: DataFrame,
    mock_variant_annotation: VariantAnnotation,
) -> None:
    """Test mapping to variant annotation variants."""
    assert isinstance(
        StudyLocusGWASCatalog._map_to_variant_annotation_variants(
            sample_gwas_catalog_associations.withColumn(
                "studyLocusId", f.monotonically_increasing_id().cast(LongType())
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


def test_neglog_pvalue(mock_study_locus: StudyLocus) -> None:
    """Test neglog pvalue."""
    assert isinstance(mock_study_locus.neglog_pvalue(), Column)


def test_annotate_ld(
    mock_study_locus_gwas_catalog: StudyLocusGWASCatalog,
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
    mock_ld_index: LDIndex,
) -> None:
    """Test LD annotation."""
    # Drop ldSet column to avoid duplicated columns
    mock_study_locus_gwas_catalog.df = mock_study_locus_gwas_catalog.df.drop("ldSet")
    assert isinstance(
        mock_study_locus_gwas_catalog.annotate_ld(
            mock_study_index_gwas_catalog, mock_ld_index
        ),
        StudyLocus,
    )


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


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        (
            # Simple case
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadB",
                    [{"tagVariantId": "tagVariantA", "posteriorProbability": 1.0}],
                ),
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadB",
                    [
                        {
                            "tagVariantId": "tagVariantA",
                            "posteriorProbability": 1.0,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        }
                    ],
                )
            ],
        ),
        (
            # Unordered credible set
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadA",
                    [
                        {"tagVariantId": "tagVariantA", "posteriorProbability": 0.44},
                        {"tagVariantId": "tagVariantB", "posteriorProbability": 0.01},
                        {"tagVariantId": "tagVariantC", "posteriorProbability": 0.04},
                        {"tagVariantId": "tagVariantD", "posteriorProbability": 0.01},
                        {"tagVariantId": "tagVariantE", "posteriorProbability": 0.5},
                    ],
                )
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadA",
                    [
                        {
                            "tagVariantId": "tagVariantE",
                            "posteriorProbability": 0.5,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        },
                        {
                            "tagVariantId": "tagVariantA",
                            "posteriorProbability": 0.44,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        },
                        {
                            "tagVariantId": "tagVariantC",
                            "posteriorProbability": 0.04,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        },
                        {
                            "tagVariantId": "tagVariantB",
                            "posteriorProbability": 0.01,
                            "is95CredibleSet": False,
                            "is99CredibleSet": True,
                        },
                        {
                            "tagVariantId": "tagVariantD",
                            "posteriorProbability": 0.01,
                            "is95CredibleSet": False,
                            "is99CredibleSet": False,
                        },
                    ],
                )
            ],
        ),
        (
            # Null credible set
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadB",
                    None,
                ),
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadB",
                    None,
                )
            ],
        ),
        (
            # Empty credible set
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadB",
                    [],
                ),
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadB",
                    [],
                )
            ],
        ),
    ],
)
def test_annotate_credible_sets(
    spark: SparkSession, observed: list, expected: list
) -> None:
    """Test annotate_credible_sets."""
    schema = StructType(
        [
            StructField("studyLocusId", LongType(), True),
            StructField("studyId", StringType(), True),
            StructField("variantId", StringType(), True),
            StructField(
                "credibleSet",
                ArrayType(
                    StructType(
                        [
                            StructField("tagVariantId", StringType(), True),
                            StructField("posteriorProbability", DoubleType(), True),
                            StructField("is95CredibleSet", BooleanType(), True),
                            StructField("is99CredibleSet", BooleanType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    data_sl = StudyLocus(
        _df=spark.createDataFrame(observed, schema)
    ).annotate_credible_sets()
    expected_sl = StudyLocus(_df=spark.createDataFrame(expected, schema))
    assert data_sl.annotate_credible_sets().df.collect() == expected_sl.df.collect()
