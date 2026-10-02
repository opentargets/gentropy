"""Step to ingest FinnGen multiome single-cell eQTL fine-mapping results."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.finngen_multiome.finemapping import FinnGenMultiomeFinemapping
from gentropy.datasource.finngen_multiome.study_index import FinnGenMultiomeStudyIndex


class FinnGenMultiomeIngestionStep:
    """FinnGen multiome single-cell eQTL ingestion step.

    Builds a study index (one study per cell type and gene) and SuSiE credible sets from the
    FinnGen multiome atlas cis-eQTL fine-mapping, with effect allele frequencies taken from the
    cis-nominal summary statistics. Credible sets with a lead p-value above
    `lead_pvalue_threshold` are flagged as sub-significant.
    """

    def __init__(
        self,
        session: Session,
        snp_files: str,
        cs_summary_files: str,
        nominal_files: str,
        summary_stats_location_template: str,
        study_index_output_path: str,
        credible_set_output_path: str,
        project_prefix: str,
        lead_pvalue_threshold: float,
        credset_lbf_threshold: float,
        purity_min_r2_threshold: float,
    ) -> None:
        """Run FinnGen multiome ingestion step.

        Args:
            session (Session): Session object.
            snp_files (str): Glob of the `*.SUSIE.snp.tsv.gz` files.
            cs_summary_files (str): Glob of the `*.SUSIE.cred.tsv.gz` files (95% credible sets).
            nominal_files (str): Glob of the `*.cis_nominal.tsv.gz` files.
            summary_stats_location_template (str): Location of the cis-nominal summary statistics with `{cell_type}` placeholder.
            study_index_output_path (str): Output path for the study index.
            credible_set_output_path (str): Output path for the credible sets.
            project_prefix (str): Prefix for study identifiers.
            lead_pvalue_threshold (float): Lead p-value threshold to flag sub-significant credible sets.
            credset_lbf_threshold (float): Minimum log10 Bayes factor for credible sets other than the first.
            purity_min_r2_threshold (float): Minimum pairwise r2 within the credible set.
        """
        (
            FinnGenMultiomeStudyIndex.from_source(
                session=session,
                cs_summary_files=cs_summary_files,
                project_prefix=project_prefix,
                summary_stats_location_template=summary_stats_location_template,
                credset_lbf_threshold=credset_lbf_threshold,
                purity_min_r2_threshold=purity_min_r2_threshold,
            )
            .df.coalesce(1)
            .write.mode(session.write_mode)
            .parquet(study_index_output_path)
        )
        (
            FinnGenMultiomeFinemapping.from_source(
                session=session,
                snp_files=snp_files,
                cs_summary_files=cs_summary_files,
                nominal_files=nominal_files,
                project_prefix=project_prefix,
                credset_lbf_threshold=credset_lbf_threshold,
                purity_min_r2_threshold=purity_min_r2_threshold,
            )
            .validate_lead_pvalue(pvalue_cutoff=lead_pvalue_threshold)
            .df.write.mode(session.write_mode)
            .parquet(credible_set_output_path)
        )
