"""Step to ingest the eQTLGen single-cell eQTL and co-eQTL fine-mapping results."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.eqtlgen.finemapping import EqtlGenDataset, EqtlGenFinemapping
from gentropy.datasource.eqtlgen.study_index import EqtlGenStudyIndex


class EqtlGenIngestionStep:
    """eQTLGen single-cell eQTL and co-eQTL ingestion step.

    Builds the study index and the SuSiE credible sets of one eQTLGen dataset: the single-cell
    eQTLs (one study per cell type and gene) or the co-eQTLs (one study per cell type and gene
    pair).

    ## Data flow

    ```mermaid
    flowchart TD
      subgraph INPUTS
        A1["finemap_<cell type>/*.txt or joint_co_eQTL_fine_mapping_<cell type>_*/*.txt"]
        A2["target index (co-eQTLs only)"]
      end

      subgraph OUTPUTS
        O1[study_index]
        O2[credible_set]
        O3[credible_set_variant]
      end

      A1 --> R["every variant tested per feature; effects turned to the alternative allele"]
      R --> M["members of each SusieRss_CS credible set: posterior probability and Bayes factor of its single effect over every variant tested"]
      M --> F["keep credible sets with log10 Bayes factor >= min_credible_set_log10_bayes_factor"]
      F --> CS["CredibleSet.from_members: lead variant, drop leads with p above lead_pvalue_threshold"]
      CS --> O2
      CS --> O3
      R --> SI["one study per feature with credible sets; eGene symbol to gene by location"]
      A2 --> SI
      CS --> SI
      SI --> O1
    ```

    ## Rationale

    - The credible sets are those reported by eQTLGen (`SusieRss_CS`), and the default filters
      are those of the eQTLGen release `260326`.
    - `assessed_allele` is the reference allele, so the effects are turned to the alternative
      allele of `variantId`.
    - The co-eQTL gene symbols are resolved by location, as a symbol can name several genes.
    """

    def __init__(
        self,
        session: Session,
        dataset: str,
        finemapping_files: str,
        study_index_output_path: str,
        credible_set_output_path: str,
        credible_set_variant_output_path: str,
        target_index_path: str | None = None,
        lead_pvalue_threshold: float = 1e-2,
        min_credible_set_log10_bayes_factor: float = 0.8685889638065036,
    ) -> None:
        """Run the eQTLGen ingestion step.

        Args:
            session (Session): Session object.
            dataset (str): `single_cell_eqtl` or `coeqtl`.
            finemapping_files (str): Glob of the fine-mapping files of the dataset.
            study_index_output_path (str): Output path for the study index.
            credible_set_output_path (str): Output path for the credible sets.
            credible_set_variant_output_path (str): Output path for the credible set variants.
            target_index_path (str | None): Target index, to map the co-eQTL gene symbols.
            lead_pvalue_threshold (float): Credible sets whose lead p-value is above it are dropped.
            min_credible_set_log10_bayes_factor (float): Minimum log10 Bayes factor of a credible set.
        """
        eqtlgen_dataset = EqtlGenDataset(dataset)
        if eqtlgen_dataset is EqtlGenDataset.COEQTL and target_index_path is None:
            raise ValueError("The co-eQTLs need a target index to map eGene.")
        # Read twice, for the credible sets and the study index: persist, so the files are read once.
        variants = EqtlGenFinemapping.read(
            session, eqtlgen_dataset, finemapping_files
        ).persist()
        credible_sets, credible_set_variants = EqtlGenFinemapping.from_source(
            variants,
            lead_pvalue_threshold=lead_pvalue_threshold,
            min_credible_set_log10_bayes_factor=min_credible_set_log10_bayes_factor,
        )
        credible_sets.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(credible_set_output_path)
        credible_set_variants.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(credible_set_variant_output_path)
        (
            EqtlGenStudyIndex.from_source(
                session,
                eqtlgen_dataset,
                variants,
                credible_sets,
                target_index_path,
            )
            .df.coalesce(1)
            .write.mode(session.write_mode)
            .parquet(study_index_output_path)
        )
