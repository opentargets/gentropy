# pylint: disable=too-many-arguments
"""Step to ingest pre-computed FinnGen SuSIE finemapping results."""

from __future__ import annotations

from dataclasses import dataclass

from gentropy.common.session import Session
from gentropy.config import FinngenFinemappingConfig
from gentropy.datasource.finngen.finemapping import FinnGenFinemapping
from gentropy.datasource.finngen.study_index import FinnGenStudyIndex


@dataclass
class FinnGenFinemappingIngestionStep(FinnGenFinemapping):
    """FinnGen fine-mapping ingestion step."""

    def __init__(
        self,
        session: Session,
        finngen_annotated_variants_path: str,
        finngen_finemapping_out: str,
        finngen_finemapping_credible_set_variant_out: str,
        finngen_susie_finemapping_snp_files: str = FinngenFinemappingConfig().finngen_susie_finemapping_snp_files,
        finngen_susie_finemapping_cs_summary_files: str = FinngenFinemappingConfig().finngen_susie_finemapping_cs_summary_files,
        finngen_finemapping_lead_pvalue_threshold: float = FinngenFinemappingConfig().finngen_finemapping_lead_pvalue_threshold,
        finngen_release_prefix: str = FinngenFinemappingConfig().finngen_release_prefix,
    ) -> None:
        """Run FinnGen finemapping ingestion step.

        Args:
            session (Session): Session object, with `use_enhanced_bgzip_codec` to read the block gzipped inputs.
            finngen_annotated_variants_path (str): FinnGen annotated variants file of the same release, for the allele frequencies.
            finngen_finemapping_out (str): Output path for the credible sets.
            finngen_finemapping_credible_set_variant_out (str): Output path for the credible set variants.
            finngen_susie_finemapping_snp_files(str): Path to the FinnGen SuSIE finemapping results.
            finngen_susie_finemapping_cs_summary_files (str): FinnGen SuSIE summaries for CS filters(LBF>2).
            finngen_finemapping_lead_pvalue_threshold (float): Credible sets whose lead p-value is above it are dropped.
            finngen_release_prefix (str): Finngen project release prefix. Should look like FINNGEN_R*.
        """
        finngen_release_prefix = FinnGenStudyIndex.validate_release_prefix(
            finngen_release_prefix
        )["prefix"]
        credible_sets, credible_set_variants = (
            FinnGenFinemapping.from_finngen_susie_finemapping(
                spark=session.spark,
                finngen_susie_finemapping_snp_files=finngen_susie_finemapping_snp_files,
                finngen_susie_finemapping_cs_summary_files=finngen_susie_finemapping_cs_summary_files,
                finngen_annotated_variants_path=finngen_annotated_variants_path,
                finngen_release_prefix=finngen_release_prefix,
                lead_pvalue_threshold=finngen_finemapping_lead_pvalue_threshold,
            )
        )
        credible_sets.df.write.mode(session.write_mode).parquet(finngen_finemapping_out)
        credible_set_variants.df.write.mode(session.write_mode).parquet(
            finngen_finemapping_credible_set_variant_out
        )
