"""Tests for finngen meta summary statistics dataset."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pyspark.sql.types as t
import pytest
from pyspark.sql import DataFrame, Row
from pyspark.testing import assertDataFrameEqual

from gentropy import Session, VariantIndex
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.datasource.finngen_meta.study_index import (
    FinnGenMetaManifest,
    MetaAnalysisDataSource,
)
from gentropy.datasource.finngen_meta.summary_statistics import (
    FinnGenMetaSummaryStatistics,
)


class TestFinnGenMetaSummaryStatistics:
    """Test FinnGenMetaSummaryStatistics dataset."""

    @pytest.fixture
    def raw_sumstat_required_schema(self) -> t.StructType:
        """Create a mock schema for raw_summary_statistics DataFrame."""
        return t.StructType(
            [
                t.StructField("studyId", t.StringType(), True),
                t.StructField("#CHR", t.StringType(), True),
                t.StructField("POS", t.LongType(), True),
                t.StructField("REF", t.StringType(), True),
                t.StructField("ALT", t.StringType(), True),
                t.StructField("SNP", t.StringType(), True),
                # FinnGen
                t.StructField("fg_af_alt", t.DoubleType(), True),
                # MVP_EUR
                t.StructField("MVP_EUR_af_alt", t.DoubleType(), True),
                t.StructField("MVP_EUR_r2", t.DoubleType(), True),
                # MVP_AFR
                t.StructField("MVP_AFR_af_alt", t.DoubleType(), True),
                t.StructField("MVP_AFR_r2", t.DoubleType(), True),
                # MVP_HIS
                t.StructField("MVP_HIS_af_alt", t.DoubleType(), True),
                t.StructField("MVP_HIS_r2", t.DoubleType(), True),
                # UKBB
                t.StructField("ukbb_af_alt", t.DoubleType(), True),
                # Meta
                t.StructField("all_inv_var_meta_beta", t.DoubleType(), True),
                t.StructField("all_inv_var_meta_sebeta", t.DoubleType(), True),
                t.StructField("all_inv_var_meta_mlogp", t.DoubleType(), True),
            ]
        )

    @pytest.fixture
    def raw_sumstat_input_df(
        self, raw_sumstat_required_schema: t.StructType, session: Session
    ) -> DataFrame:
        """Create a mock raw_summary_statistics DataFrame for testing."""
        # fmt: off
        raw_summary_statistics_data = [
            # Row 1: Missing all_inv_var_meta_mlogp (should be filtered out)
            ("FINNGEN_TEST", "1", 1000, "A", "G", "rs1",
            0.3,  # FinnGen AF
            0.4, 0.9,  # MVP_EUR AF, r2
            0.35, 0.85,  # MVP_AFR AF, r2
            0.39, 0.87,  # MVP_HIS AF, r2
            0.45,  # UKBB AF
            0.16, 0.016, None),  # Meta (missing mlogp)

            # Row 2: Missing all_inv_var_meta_beta (should be filtered out)
            ("FINNGEN_TEST", "1", 1001, "A", "C", "rs2",
            0.32,  # FinnGen AF
            0.42, 0.92,  # MVP_EUR AF, r2
            0.37, 0.87,  # MVP_AFR AF, r2
            0.39, 0.89,  # MVP_HIS AF, r2
            0.47,  # UKBB AF
            None, 0.018, 5.2),  # Meta (missing beta)

            # Row 3: Missing all_inv_var_meta_sebeta (should be filtered out)
            ("FINNGEN_TEST", "1", 1002, "A", "C", "rs3",
            0.33,  # FinnGen AF
            0.43, 0.93,  # MVP_EUR AF, r2
            0.35, 0.85,  # MVP_AFR AF, r2
            0.39, 0.87,  # MVP_HIS AF, r2
            0.48,  # UKBB AF
            0.18, None, 5.3),  # Meta (missing sebeta)

            # Row 4: Only one cohort (FinnGen) - should be filtered out (not meta-analyzed)
            ("FINNGEN_TEST", "1", 1003, "G", "T", "rs4",
            0.34,  # FinnGen AF
            None, None,  # MVP_EUR (missing)
            None, None,  # MVP_AFR (missing)
            None, None,  # MVP_HIS (missing)
            None,  # UKBB (missing)
            0.19, 0.019, 5.4),  # Meta

            # Row 5: Two cohorts but both from MVP - should be filtered out (not meta-analyzed)
            ("FINNGEN_TEST", "1", 1004, "C", "T", "rs5",
            None,  # FinnGen (missing)
            0.44, 0.94,  # MVP_EUR AF, r2
            0.38, 0.88,  # MVP_AFR AF, r2
            None, None,  # MVP_HIS (missing)
            None,  # UKBB (missing)
            0.20, 0.020, 5.5),  # Meta

            # Row 6: Low imputation score (MVP_EUR r2 = 0.7 < 0.8) - should be filtered out
            ("FINNGEN_TEST", "1", 1005, "G", "A", "rs6",
            0.35,  # FinnGen AF
            0.45, 0.7,  # MVP_EUR AF, r2 (low r2)
            None, None,  # MVP_AFR (missing)
            None, None,  # MVP_HIS (missing)
            0.49,  # UKBB AF
            0.21, 0.021, 5.6),  # Meta

            # Row 7: Very low allele frequency (0.00001) - should be filtered out due to low MAC
            ("FINNGEN_TEST", "1", 1006, "T", "C", "rs7",
            0.00001,  # FinnGen AF (very low)
            0.00001, 0.95,  # MVP_EUR AF (very low), r2
            None, None,  # MVP_AFR (missing)
            None, None,  # MVP_HIS (missing)
            0.00001,  # UKBB AF (very low)
            0.22, 0.022, 5.7),  # Meta

            # Row 8: Palindromic variant A>T - should be filtered out
            ("FINNGEN_TEST", "1", 999, "A", "T", "rs8",
            0.36,  # FinnGen AF
            0.46, 0.96,  # MVP_EUR AF, r2
            None, None,  # MVP_AFR (missing)
            None, None,  # MVP_HIS (missing)
            0.50,  # UKBB AF
            0.23, 0.023, 5.8),  # Meta

            # Row 9: Variant with flipped direction - should pass but beta flipped
            ("FINNGEN_TEST", "1", 1000, "A", "G", "rs9",
            0.37,  # FinnGen AF
            0.47, 0.97,  # MVP_EUR AF, r2
            None, None,  # MVP_AFR (missing)
            None, None,  # MVP_HIS (missing)
            0.51,  # UKBB AF
            0.24, 0.024, 5.9),  # Meta

            # Row 10: Variant with direct direction - should pass with original beta
            ("FINNGEN_TEST", "1", 1001, "A", "C", "rs10",
            0.38,  # FinnGen AF
            0.48, 0.98,  # MVP_EUR AF, r2
            None, None,  # MVP_AFR (missing)
            None, None,  # MVP_HIS (missing)
            0.52,  # UKBB AF
            0.25, 0.025, 6.0),  # Meta

            # Row 11: Variant missing from variant direction dataset - should be filtered out
            ("FINNGEN_TEST", "1", 1002, "A", "C", "rs11",
            0.39,  # FinnGen AF
            0.49, 0.99,  # MVP_EUR AF, r2
            None, None,  # MVP_AFR (missing)
            None, None,  # MVP_HIS (missing)
            0.53,  # UKBB AF
            0.26, 0.026, 6.1),  # Meta
        ]
        # fmt: on
        return session.spark.createDataFrame(
            raw_summary_statistics_data, raw_sumstat_required_schema
        )

    @pytest.fixture
    def variant_index(self, session: Session) -> VariantIndex:
        """Mock of the variant index DataFrame."""
        variant_index_schema = t.StructType(
            [
                t.StructField("variantId", t.StringType(), False),
                t.StructField("chromosome", t.StringType(), False),
                t.StructField("position", t.IntegerType(), False),
                t.StructField("referenceAllele", t.StringType(), False),
                t.StructField("alternateAllele", t.StringType(), False),
                t.StructField(
                    "alleleFrequencies",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField("populationName", t.StringType(), True),
                                t.StructField("alleleFrequency", t.DoubleType(), True),
                            ]
                        ),
                        True,
                    ),
                    True,
                ),
            ]
        )

        variant_index_data = [
            # Flipped variant: 1_1000_G_A (original was 1_1000_A_G)
            ("1_1000_G_A", "1", 1000, "G", "A", None),
            # Direct variant: 1_1001_A_C
            ("1_1001_A_C", "1", 1001, "A", "C", None),
            # Palindromic variant: 1_999_A_T (will be filtered out due to strand ambiguity)
            ("1_999_A_T", "1", 999, "A", "T", None),
        ]

        return VariantIndex(
            session.spark.createDataFrame(variant_index_data, variant_index_schema)
        )

    @pytest.fixture
    def variant_direction(self, variant_index: VariantIndex) -> VariantDirection:
        """Mock of the variant direction DataFrame."""
        return VariantDirection.from_variant_index(variant_index)

    @pytest.fixture
    @patch("gentropy.datasource.finngen_meta.summary_statistics.FinnGenMetaManifest")
    def finngen_manifest(
        self, mock_manifest: MagicMock, session: Session
    ) -> FinnGenMetaManifest:
        """Mock of the FinnGenMetaManifest."""
        # Mock FinnGen manifest
        finngen_manifest_data = [
            (
                "FINNGEN_TEST",
                1000,
                [
                    {"cohort": "FinnGen", "nCases": 500},
                    {"cohort": "MVP_EUR", "nCases": 300},
                    {"cohort": "MVP_AFR", "nCases": 200},
                    {"cohort": "MVP_AMR", "nCases": 150},
                    {"cohort": "UKBB", "nCases": 400},
                ],
            )
        ]

        finngen_manifest_schema = "studyId STRING, nSamples INT, nCasesPerCohort ARRAY<STRUCT<cohort:STRING,nCases:INT>>"

        mock_manifest.df = session.spark.createDataFrame(
            finngen_manifest_data, finngen_manifest_schema
        )
        return mock_manifest

    def test_bgzip_from_parquet(self, tmp_path: Path, session: Session) -> None:
        """Test bgzip from parquet conversion."""
        input_path = "tests/gentropy/data_samples/*_meta_out.tsv.gz"
        output_path = tmp_path / "output"
        with pytest.raises(KeyError) as e:
            FinnGenMetaSummaryStatistics.bgzip_to_parquet(
                session,
                summary_statistics_glob=input_path,
                datasource=MetaAnalysisDataSource.FINNGEN_UKBB_MVP,
                raw_summary_statistics_output_path=output_path.as_posix(),
            )

            assert "session.spark.use_enhanced_bgzip_codec" in str(e.value)

    def test_from_source(
        self,
        finngen_manifest: FinnGenMetaManifest,
        variant_direction: VariantDirection,
        raw_sumstat_input_df: DataFrame,
    ) -> None:
        """Test summary statistics from source."""
        sumstat = FinnGenMetaSummaryStatistics.from_source(
            raw_sumstat_input_df,
            finngen_manifest,
            variant_direction,
        )

        assert sumstat.df.count() == 2, "wrong number of variants"
        expected_data = [
            Row(
                studyId="FINNGEN_TEST",
                variantId="1_1000_G_A",
                chromosome="1",
                position=1000,
                beta=-0.24,
                sampleSize=1000,
                pValueMantissa=1.258925437927246,
                pValueExponent=-6,
                effectAlleleFrequencyFromSource=None,
                standardError=0.024,
            ),
            Row(
                studyId="FINNGEN_TEST",
                variantId="1_1001_A_C",
                chromosome="1",
                position=1001,
                beta=0.25,
                sampleSize=1000,
                pValueMantissa=1.0,
                pValueExponent=-6,
                effectAlleleFrequencyFromSource=None,
                standardError=0.025,
            ),
        ]
        assertDataFrameEqual(
            actual=sumstat.df.collect(), expected=expected_data, checkRowOrder=False
        )
