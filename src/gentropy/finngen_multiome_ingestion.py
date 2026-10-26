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
    `lead_pvalue_threshold` are dropped with their variants.

    ## Data flow

    ```mermaid
    flowchart TD
      subgraph INPUTS
        A1["*.SUSIE.cred.tsv.gz (credible set summaries)"]
        A2["*.SUSIE.snp.tsv.gz (every variant tested per gene)"]
        A3["*.cis_nominal.tsv.gz (marginal statistics)"]
      end

      subgraph OUTPUTS
        O1[study_index]
        O2[credible_set]
        O3[credible_set_variant]
      end

      A1 --> F["keep first credible set of a gene or cs_log10bf > log10(e^2), and min r2 >= 0.25"]
      F --> SI["one study per cell type and gene: Cell Ontology term and donors per cell type"]
      SI --> O1
      A2 --> BF["per gene and effect: Bayes factor over all tested variants, locus bounds"]
      F --> BF
      A2 --> M["credible set members: marginal and conditional effects of their single effect"]
      F --> M
      BF --> M
      A3 --> EAF["alternative allele frequency AF_Allele2 of the members"]
      M --> EAF
      EAF --> CS["CredibleSet.from_members: lead variant, drop leads with p above lead_pvalue_threshold"]
      CS --> O2
      CS --> O3
    ```

    ## Rationale

    - The studies are defined per cell type and gene, as for the single-cell eQTL studies of the
      eQTL Catalogue, so colocalisation and the locus-to-gene features treat them the same way.
    - The purity filter matches FinnGen's `low_purity` flag; the Bayes factor filter matches the
      FinnGen GWAS ingestion.
    - The effect allele frequency comes from the cis-nominal files, as `maf` of the fine-mapping
      files is the minor allele frequency.
    - The conditional effects follow the FinnGen GWAS ingestion, which shares the SuSiE output
      layout (see `FinnGenMultiomeFinemapping.from_source`).
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
        credible_set_variant_output_path: str,
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
            credible_set_variant_output_path (str): Output path for the credible set variants.
            project_prefix (str): Prefix for study identifiers.
            lead_pvalue_threshold (float): Credible sets whose lead p-value is above it are dropped.
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
        credible_sets, credible_set_variants = FinnGenMultiomeFinemapping.from_source(
            session=session,
            snp_files=snp_files,
            cs_summary_files=cs_summary_files,
            nominal_files=nominal_files,
            project_prefix=project_prefix,
            credset_lbf_threshold=credset_lbf_threshold,
            purity_min_r2_threshold=purity_min_r2_threshold,
            lead_pvalue_threshold=lead_pvalue_threshold,
        )
        credible_sets.df.write.mode(session.write_mode).parquet(credible_set_output_path)
        credible_set_variants.df.write.mode(session.write_mode).parquet(
            credible_set_variant_output_path
        )
