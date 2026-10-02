"""Validate locus-breaker clumping parameter choices for cis-only BigBrain summary statistics.

BigBrain eQTL/sQTL studies are cis-only (one gene, one ~2Mb window per study), unlike
GWAS or UKB-PPP which scan the whole genome per phenotype. Two settings that are safe
defaults for genome-wide studies are not safe defaults here:

- `remove_mhc=True` (used by both GWAS Catalog and UKB-PPP) exists to guard against
  LD-driven false trans signals in the MHC region. For BigBrain, a gene whose cis
  window overlaps the MHC is a real study, not a confound - excluding it would just
  delete that gene's results.
- UKB-PPP's `lbc_pvalue_threshold=1.7e-11` is 5e-8 Bonferroni-corrected across its
  ~2,900 genome-wide-scanned proteins. Applied to a cis-only window with a realistic
  cis-eQTL effect size, it is far stricter than the field convention for cis
  significance (~1e-5, matching this repo's own SuSiE/credible-set-QC defaults) and
  would call zero loci.

This test builds small, realistic-scale synthetic BigBrain-shaped studies (including
one gene whose window overlaps the MHC) and demonstrates both points before these
parameters are used against real production data.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.genomic_region import GenomicRegion, KnownGenomicRegions
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.summary_statistics import SummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# BigBrain-appropriate cis significance, matching this repo's existing SuSiE
# lead-variant / credible-set-QC threshold (`FinemapperConfig.lead_pval_threshold`,
# `CredibleSetQCStepConfig.p_value_threshold`), rather than a genome-wide value.
CIS_BASELINE_PVALUE = 1e-4
CIS_PVALUE_THRESHOLD = 1e-5

# UKB-PPP's production locus-breaker threshold: 5e-8 Bonferroni-corrected across its
# ~2,900 genome-wide-scanned proteins. Included here only as the contrasting case.
GENOME_WIDE_PVALUE_THRESHOLD = 1.7e-11

LEAD_VARIANT_PVALUE_EXPONENT = -7  # a realistic strong cis-eQTL lead p-value (1e-7)

MHC_REGION = GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC)


@pytest.fixture(scope="module")
def bigbrain_two_gene_summary_statistics(
    spark: SparkSession,
) -> SummaryStatistics:
    """Two cis-eQTL studies: one autosomal, one whose window overlaps the MHC region."""
    autosomal_lead_position = 1_000_000
    mhc_lead_position = MHC_REGION.start + 10_000

    def cis_window(
        study_id: str, chromosome: str, lead_position: int
    ) -> list[tuple[str, str, str, int, float, int]]:
        # Lead variant at CIS-significant p=1e-7, flanked by weaker LD-decay signals,
        # surrounded by non-significant noise (p=0.5) spanning the ~2Mb cis window.
        return [
            (
                study_id,
                f"{lead_position - 1_000_000}_A_G",
                chromosome,
                lead_position - 1_000_000,
                0.5,
                -1,
            ),
            (
                study_id,
                f"{lead_position - 20_000}_A_G",
                chromosome,
                lead_position - 20_000,
                1.0,
                -4,
            ),
            (
                study_id,
                f"{lead_position}_A_G",
                chromosome,
                lead_position,
                1.0,
                LEAD_VARIANT_PVALUE_EXPONENT,
            ),
            (
                study_id,
                f"{lead_position + 20_000}_A_G",
                chromosome,
                lead_position + 20_000,
                1.0,
                -4,
            ),
            (
                study_id,
                f"{lead_position + 1_000_000}_A_G",
                chromosome,
                lead_position + 1_000_000,
                0.5,
                -1,
            ),
        ]

    data = cis_window(
        "BigBrain_eqtl_EUR_ENSG00000000001", "1", autosomal_lead_position
    ) + cis_window("BigBrain_eqtl_EUR_ENSG00000000002", "6", mhc_lead_position)

    df = (
        spark.createDataFrame(
            data,
            [
                "studyId",
                "variantId",
                "chromosome",
                "position",
                "pValueMantissa",
                "pValueExponent",
            ],
        )
        .withColumn("position", f.col("position").cast(t.IntegerType()))
        .withColumn("pValueMantissa", f.col("pValueMantissa").cast(t.FloatType()))
        .withColumn("pValueExponent", f.col("pValueExponent").cast(t.IntegerType()))
        .withColumn("beta", f.lit(0.1).cast(t.DoubleType()))
        .withColumn("standardError", f.lit(0.02).cast(t.DoubleType()))
        .withColumn("sampleSize", f.lit(10_725).cast(t.IntegerType()))
        .withColumn("effectAlleleFrequencyFromSource", f.lit(None).cast(t.FloatType()))
    )
    return SummaryStatistics(_df=df, _schema=SummaryStatistics.get_schema())


class TestBigBrainMhcHandling:
    """`remove_mhc=True` (the GWAS/UKB-PPP default) would silently drop the MHC-window gene."""

    @pytest.fixture(scope="class")
    def clumped_loci(
        self, bigbrain_two_gene_summary_statistics: SummaryStatistics
    ) -> StudyLocus:
        """Locus-breaker clumping with BigBrain-appropriate (non-genome-wide) thresholds."""
        return bigbrain_two_gene_summary_statistics.locus_breaker_clumping(
            baseline_pvalue_cutoff=CIS_BASELINE_PVALUE,
            distance_cutoff=250_000,
            pvalue_cutoff=CIS_PVALUE_THRESHOLD,
            flanking_distance=100_000,
        )

    def test_both_genes_called_before_mhc_filter(
        self, clumped_loci: StudyLocus
    ) -> None:
        """Both the autosomal and MHC-overlapping genes should produce a locus."""
        assert clumped_loci.df.count() == 2

    def test_remove_mhc_true_drops_the_mhc_gene(self, clumped_loci: StudyLocus) -> None:
        """Reusing UKB-PPP's `remove_mhc=True` would delete the MHC-window gene's locus."""
        filtered = clumped_loci.exclude_region(MHC_REGION, exclude_overlap=True)
        assert filtered.df.count() == 1
        first_row = filtered.df.first()
        assert first_row is not None
        assert first_row["studyId"] == "BigBrain_eqtl_EUR_ENSG00000000001"

    def test_remove_mhc_false_keeps_both_genes(self, clumped_loci: StudyLocus) -> None:
        """With `remove_mhc=False`, the MHC-window gene's real cis signal is retained."""
        study_ids = {
            row["studyId"] for row in clumped_loci.df.select("studyId").collect()
        }
        assert study_ids == {
            "BigBrain_eqtl_EUR_ENSG00000000001",
            "BigBrain_eqtl_EUR_ENSG00000000002",
        }


class TestBigBrainSignificanceThreshold:
    """A genome-wide-scale Bonferroni threshold (UKB-PPP's 1.7e-11) misses realistic cis-eQTL signals."""

    def test_cis_threshold_calls_the_locus(
        self, bigbrain_two_gene_summary_statistics: SummaryStatistics
    ) -> None:
        """At the cis-appropriate threshold (1e-5), the p=1e-7 lead variant is called."""
        loci = bigbrain_two_gene_summary_statistics.locus_breaker_clumping(
            baseline_pvalue_cutoff=CIS_BASELINE_PVALUE,
            distance_cutoff=250_000,
            pvalue_cutoff=CIS_PVALUE_THRESHOLD,
            flanking_distance=100_000,
        )
        assert loci.df.count() == 2

    def test_genome_wide_threshold_misses_the_locus(
        self, bigbrain_two_gene_summary_statistics: SummaryStatistics
    ) -> None:
        """UKB-PPP's phenome-wide-Bonferroni threshold (1.7e-11) is stricter than our p=1e-7 cis lead variant, so no loci survive despite this being a real, strong cis-eQTL effect."""
        loci = bigbrain_two_gene_summary_statistics.locus_breaker_clumping(
            baseline_pvalue_cutoff=CIS_BASELINE_PVALUE,
            distance_cutoff=250_000,
            pvalue_cutoff=GENOME_WIDE_PVALUE_THRESHOLD,
            flanking_distance=100_000,
        )
        assert loci.df.count() == 0
