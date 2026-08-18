"""Step to run eQTL Catalogue credible set and study index ingestion."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.eqtl_catalogue.finemapping import EqtlCatalogueFinemapping
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex


class EqtlCatalogueStep:
    """eQTL Catalogue ingestion step.

    From SuSIE fine mapping results (available at [their FTP](https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/susie/) ),
    we extract credible sets and study index datasets from gene expression QTL studies.

    !!! note "eQTL Catalogue data"
        The data needs to be downloaded to hadoop compatible filesystem before
        running the step.
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
