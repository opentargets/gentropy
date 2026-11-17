"""Step to ingest the SLE-map single-cell eQTL fine-mapping results."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.sle_map.finemapping import SleMapFinemapping
from gentropy.datasource.sle_map.study_index import SleMapStudyIndex


class SleMapIngestionStep:
    """SLE-map single-cell eQTL ingestion step.

    Builds the study index and the SuSiE credible sets of SLE-map: one study per cell type and
    gene, of 15 peripheral blood cell types and all cells together.

    ## Data flow

    ```mermaid
    flowchart TD
      subgraph INPUTS
        A1[all_memberships.tsv]
        A2[all_components.tsv]
        A3[all_diagnostics.tsv]
      end

      subgraph OUTPUTS
        O1[study_index]
        O2[credible_set]
        O3[credible_set_variant]
      end

      A2 --> P["credible sets with purity min r2 >= purity_min_r2_threshold"]
      A1 --> M["members: marginal statistics of the alternative allele, posterior probability and Bayes factor of the single effect"]
      P --> M
      A3 --> M
      M --> CS["CredibleSet.from_members: lead variant, drop leads with p above lead_pvalue_threshold"]
      CS --> O2
      CS --> O3
      A3 --> SI["one study per cell type and gene with credible sets; sample size split over the cohort ancestries"]
      CS --> SI
      SI --> O1
    ```

    ## Rationale

    - The credible sets are the 95% credible sets of SuSiE reported by SLE-map, with the region
      log Bayes factor computed by SLE-map over every variant tested for the gene.
    - The effect allele is the alternative allele of `variantId`.
    - SLE-map gives the ancestries of the whole cohort only, so each study's sample size is split
      over them in proportion, for its LD population structure.
    """

    def __init__(
        self,
        session: Session,
        memberships_path: str,
        components_path: str,
        diagnostics_path: str,
        study_index_output_path: str,
        credible_set_output_path: str,
        credible_set_variant_output_path: str,
        purity_min_r2_threshold: float | None = 0.25,
        lead_pvalue_threshold: float | None = 1e-3,
    ) -> None:
        """Run the SLE-map ingestion step.

        Args:
            session (Session): Session object.
            memberships_path (str): Path to `all_memberships.tsv`.
            components_path (str): Path to `all_components.tsv`.
            diagnostics_path (str): Path to `all_diagnostics.tsv`.
            study_index_output_path (str): Output path for the study index.
            credible_set_output_path (str): Output path for the credible sets.
            credible_set_variant_output_path (str): Output path for the credible set variants.
            purity_min_r2_threshold (float | None): Credible sets whose minimum pairwise r2 is below it are dropped.
            lead_pvalue_threshold (float | None): Credible sets whose lead p-value is above it are dropped.
        """
        credible_sets, credible_set_variants = SleMapFinemapping.from_source(
            session,
            memberships_path,
            components_path,
            diagnostics_path,
            purity_min_r2_threshold=purity_min_r2_threshold,
            lead_pvalue_threshold=lead_pvalue_threshold,
        )
        credible_sets.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(credible_set_output_path)
        credible_set_variants.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(credible_set_variant_output_path)
        (
            SleMapStudyIndex.from_source(session, diagnostics_path, credible_sets)
            .df.coalesce(1)
            .write.mode(session.write_mode)
            .parquet(study_index_output_path)
        )
