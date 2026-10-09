"""Step to run eQTL Catalogue credible set and study index ingestion."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.eqtl_catalogue.finemapping import EqtlCatalogueFinemapping
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex


class EqtlCatalogueStep:
    r"""eQTL Catalogue ingestion step.

    ## Process overview

    From SuSIE fine mapping results (available at [their FTP](https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/susie/) ),
    this step extracts credible sets and a study index for gene-expression and molecular-trait
    QTL studies.

    1. **Read study metadata** from the eQTL Catalogue TSV manifest using
       `EqtlCatalogueStudyIndex.read_studies_from_source`, optionally excluding quantification
       methods listed in `mqtl_quantification_methods_blacklist`.
    2. **Read raw credible sets** using `EqtlCatalogueFinemapping.read_credible_set_from_source`.
    3. **Read raw log Bayes factors** using `EqtlCatalogueFinemapping.read_lbf_from_source`.
    4. **Parse and correct the SuSIE results** using `EqtlCatalogueFinemapping.parse_susie_results`,
       joining credible sets, log Bayes factors and study metadata.
    5. **Build the study index** using `EqtlCatalogueStudyIndex.from_susie_results`.
    6. **Build the credible sets** using `EqtlCatalogueFinemapping.from_susie_results`, flagging
       loci whose lead p-value is above `lead_pvalue_threshold` as sub-significant.

    !!! note "eQTL Catalogue data"
        The data needs to be downloaded to hadoop compatible filesystem before
        running the step.

    !!! note "Non-ATGC variant removal"
        Some source studies (e.g. AFR_LCL/QTS000044, MAGE/QTS000055) report large indels and
        structural variants with imprecise coordinates, using CNV notation (`<DEL>`, `<DUP>`) for
        `alt`, which produced duplicate credible-set entries. These variants are removed in
        `read_credible_set_from_source` by requiring `ref`/`alt` to contain only A/T/G/C bases.

    !!! note "PIP to alpha correction"
        Some source studies (e.g. CommonMind/QTS000008, Kim-Hellmuth/QTS000042) report the
        per-variant `pip` computed by SuSiE instead of the per-(credible-set x variant) alpha
        value, which makes the posterior probabilities within a credible set sum to more than
        1.0. `parse_susie_results` detects and corrects this via
        [`scale_pip`][gentropy.datasource.eqtl_catalogue.finemapping.EqtlCatalogueFinemapping.scale_pip],
        which recomputes alpha from the log Bayes factors using a log-sum-exp normalisation.

    ## Data flow

    ```mermaid
    flowchart TD
      subgraph INPUTS
        A1[eqtl_catalogue_dataset_metadata]
        A2[raw credible_set.parquet]
        A3[raw lbf_variable.parquet]
      end

      subgraph OUTPUTS
        O1[study_index]
        O2[credible_set]
      end

        A1 --> META[read_studies_from_source]
        META --> P1[studies_metadata]

        A2 --> CS["read_credible_set_from_source"]
        CS --> P2[credible_sets]

        A3 --> LBF[read_lbf_from_source]
        LBF --> P3[lbf]

        P1 --> PARSE["parse_susie_results"]
        P2 --> PARSE
        P3 --> PARSE
        PARSE --> P4[processed_susie_results]

        P4 --> SI[EqtlCatalogueStudyIndex.from_susie_results]
        SI --> O1

        P4 --> CSB[EqtlCatalogueFinemapping.from_susie_results]
        CSB --> O2

        classDef parquet fill:#bd757c,stroke:#73343A,color:#333
        class A1,A2,A3,P1,P2,P3,P4,O1,O2 parquet
    ```

    ??? tip "Inputs"
        - [x] **eqtl_catalogue_dataset_metadata** — TSV manifest of eQTL Catalogue studies/datasets.
        - [x] **credible_set.parquet** — raw SuSIE credible-set files, one per dataset (`QTS*/QTD*/QTD*.credible_set.parquet`).
        - [x] **lbf_variable.parquet** — raw SuSIE log-Bayes-factor files, one per dataset (`QTS*/QTD*/QTD*.lbf_variable.parquet`).

    ??? tip "Outputs"
        This pipeline produces 2 artefacts:

        - [x] **study_index** — study index in Parquet format.
        - [x] **credible_set** — credible sets in Parquet format, flagged for sub-significant lead loci.
    """

    def __init__(
        self,
        session: Session,
        eqtl_catalogue_dataset_metadata_path: str,
        credible_set_input_glob: str,
        lbf_variable_input_glob: str,
        study_index_output_path: str,
        credible_set_output_path: str,
        lead_pvalue_threshold: float,
        mqtl_quantification_methods_blacklist: list[str] | None = None,
    ) -> None:
        """Run eQTL Catalogue ingestion step.

        Args:
            session (Session): Session object.
            eqtl_catalogue_dataset_metadata_path (str): Path to the eQTL Catalogue dataset metadata file.
            credible_set_input_glob (str): Glob pattern to read SuSIE credible set parquet files.
                example of files in `gs://bucket/susie/QTS*/QTD*/QTD*.credible_set.parquet`
            lbf_variable_input_glob (str): Glob pattern to read SuSIE LBF parquet files.
                example of files in `gs://bucket/susie/QTS*/QTD*/QTD*.lbf_variable.parquet`
            study_index_output_path (str): Path to write the study index parquet file.
                Written as a coalesce(1) parquet dataset.
            credible_set_output_path (str): Path to write the credible set parquet file.
                Written as a partitioned by studyId parquet dataset.
            lead_pvalue_threshold (float): P-value threshold to flag sub-significant loci.
            mqtl_quantification_methods_blacklist (list[str] | None): List of quantification methods to exclude from the study index.
                Can be used to exclude mQTL studies from the study index. Allowed values are
                in gentropy.datasource.eqtl_catalogue.QuantificationMethod.


        !!! note "glob patterns"
            The glob patterns need to point to a compatible hadoop filesystem (ex. S3, gcs, etc.), using the
            ebi ftp server directly will not work with spark.
        """
        # Extract
        studies_metadata = EqtlCatalogueStudyIndex.read_studies_from_source(
            eqtl_catalogue_dataset_metadata_path,
            mqtl_quantification_methods_blacklist or [],
            session=session,
        ).persist()

        # Read all parquet files from the nested QTS*/QTD*/ structure via glob.
        # The metadata join in parse_susie_results drops any QTDs absent from the metadata.
        credible_sets_df = EqtlCatalogueFinemapping.read_credible_set_from_source(
            credible_set_input_glob,
            session=session,
        ).persist()

        lbf_df = EqtlCatalogueFinemapping.read_lbf_from_source(
            lbf_variable_input_glob,
            session=session,
        ).persist()

        # Transform
        processed_susie_df = EqtlCatalogueFinemapping.parse_susie_results(
            credible_sets_df, lbf_df, studies_metadata
        ).persist()
        processed_susie_df.count()

        studies_metadata.unpersist()
        credible_sets_df.unpersist()
        lbf_df.unpersist()

        (
            EqtlCatalogueStudyIndex.from_susie_results(processed_susie_df)
            .coalesce(1)
            .df.write.mode(session.write_mode)
            .parquet(study_index_output_path)
        )

        (
            EqtlCatalogueFinemapping.from_susie_results(processed_susie_df)
            # Flagging sub-significant loci:
            .validate_lead_pvalue(pvalue_cutoff=lead_pvalue_threshold)
            .df.repartition(session.output_partitions, "studyId", "chromosome")
            .sortWithinPartitions("chromosome", "variantId")
            .write.mode(session.write_mode)
            .option("maxRecordsPerFile", 50_000_000)
            .parquet(credible_set_output_path)
        )
        processed_susie_df.unpersist()
