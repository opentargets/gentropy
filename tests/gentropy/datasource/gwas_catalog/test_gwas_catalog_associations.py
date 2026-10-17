"""Test GWAS Catalog associations import."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from gentropy.dataset.study_locus import AnnotatedStudyLocus, StudyLocusQualityCheck
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.gwas_catalog.associations import (
    GWASCatalogCuratedAssociationsParser,
)


def test_qc_all(sample_gwas_catalog_associations: DataFrame) -> None:
    """Test qc all with some hard-coded values."""
    assert isinstance(
        sample_gwas_catalog_associations.withColumn(
            # Perform all quality control checks:
            "qualityControls",
            GWASCatalogCuratedAssociationsParser._qc_all(
                f.array().alias("qualityControls"),
                f.col("CHR_ID"),
                f.col("CHR_POS"),
                f.lit("A").alias("referenceAllele"),
                f.lit("T").alias("referenceAllele"),
                f.col("STRONGEST SNP-RISK ALLELE"),
                *GWASCatalogCuratedAssociationsParser._split_pvalue_column(
                    f.col("P-VALUE")
                ),
                5e-8,
            ),
        ),
        DataFrame,
    )


def test_from_source(
    mock_variant_index: VariantIndex,
    sample_gwas_catalog_associations: DataFrame,
) -> None:
    """The parsed associations keep what study splitting and the study loci need."""
    associations = GWASCatalogCuratedAssociationsParser.from_source(
        sample_gwas_catalog_associations, mock_variant_index
    )
    assert set(associations.columns) == {
        "variantId",
        "chromosome",
        "position",
        "studyId",
        "pValueMantissa",
        "pValueExponent",
        "subStudyDescription",
        "qualityControls",
        "beta",
        "standardError",
    }
    assert associations.count() == sample_gwas_catalog_associations.count()


def test_map_variants_to_variant_index(
    sample_gwas_catalog_associations: DataFrame,
    mock_variant_index: VariantIndex,
) -> None:
    """Test mapping to variant annotation variants."""
    assert isinstance(
        GWASCatalogCuratedAssociationsParser._map_variants_to_gnomad_variants(
            sample_gwas_catalog_associations.withColumn(
                "rowId", f.monotonically_increasing_id().cast(StringType())
            ),
            mock_variant_index,
        ),
        DataFrame,
    )


class TestToStudyLocus:
    """Each placed curated association becomes a point study locus."""

    SCHEMA = (
        "variantId string, chromosome string, position int, studyId string, "
        "pValueMantissa float, pValueExponent int, qualityControls array<string>, "
        "beta double, standardError double"
    )
    COMPOSITE = StudyLocusQualityCheck.COMPOSITE_FLAG.value
    TOP_HIT = StudyLocusQualityCheck.TOP_HIT.value

    def test_loci(self, spark: SparkSession) -> None:
        """Unplaced hits go, flags stay, and one hit per position of a study survives."""
        associations = spark.createDataFrame(
            [
                # Two alleles at one position: the more significant one is kept.
                ("1_100_A_T", "1", 100, "S1", 1.0, -10, [], 0.2, 0.01),
                ("1_100_A_G", "1", 100, "S1", 1.0, -9, [], 0.1, 0.01),
                # Close to the first, but its own locus: no window clumping.
                ("1_150_C_G", "1", 150, "S1", 2.0, -9, [self.COMPOSITE], None, None),
                # The same position in another study is another locus.
                ("1_100_A_T", "1", 100, "S2", 3.0, -8, [], 0.3, 0.02),
                # Not mapped to a variant: dropped.
                (None, None, None, "S1", 1.0, -12, [], None, None),
            ],
            self.SCHEMA,
        )

        loci = GWASCatalogCuratedAssociationsParser.to_study_locus(associations)

        assert isinstance(loci, AnnotatedStudyLocus)
        rows = {(row.studyId, row.leadVariantId): row for row in loci.df.collect()}
        assert set(rows) == {
            ("S1", "1_100_A_T"),
            ("S1", "1_150_C_G"),
            ("S2", "1_100_A_T"),
        }
        lead = rows[("S1", "1_100_A_T")]
        assert (lead.locusStart, lead.locusEnd) == (100, 100)
        assert lead.qualityControls == [self.TOP_HIT]
        assert [entry.variantId for entry in lead.locus] == ["1_100_A_T"]
        assert (lead.locus[0].pValueExponent, lead.locus[0].beta) == (-10, 0.2)
        assert lead.ldSet is None
        assert sorted(rows[("S1", "1_150_C_G")].qualityControls) == sorted(
            [self.COMPOSITE, self.TOP_HIT]
        )
        assert rows[("S1", "1_100_A_T")].id != rows[("S2", "1_100_A_T")].id
