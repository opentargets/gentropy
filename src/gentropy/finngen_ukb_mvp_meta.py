"""Step to run FinnGen UKBB MVP meta-analysis data ingestion."""

from __future__ import annotations

from gentropy import StudyIndex
from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.finngen.efo_mapping import EFOMapping
from gentropy.datasource.finngen_meta import FinnGenMetaManifest
from gentropy.datasource.finngen_meta.study_index import FinnGenMetaStudyIndex
from gentropy.datasource.finngen_meta.summary_statistics import (
    FinnGenUkbMvpMetaSummaryStatistics,
)


class FinngenUkbMvpMetaSummaryStatisticsIngestionStep:
    """FinnGen UK Biobank and Million Veteran Program meta-analysis summary statistics ingestion step.

    !!! warning "Inputs"
        - [x] This step requires the gnomAD variant index to perform the allele flipping during harmonisation.
        - [x] The `source_manifest_path` should point to a manifest that includes paths to the summary statistics files.

    !!! note "Outputs"
        This step outputs 4 artifacts:

        - [x] Raw summary statistics in Parquet format.
        - [x] Harmonised summary statistics in Parquet format.
        - [x] Summary statistics QC results in Parquet format.
        - [x] Study Index in parquet format (updated with QC results).

    !!! tip "Process Overview"
        The step performs the following operations:

        1. Reads the FinnGen meta-analysis manifest and EFO curation files.
        2. Builds the study index and writes it to the specified output path.
        3. Reads the raw summary statistics based on paths provided in the study index.
        3. Converts the raw summary statistics into Parquet format and writes them as raw output.
        4. Reads the gnomAD variant index and builds the variant direction for allele flipping.
        5. Harmonises the summary statistics with variant direction and manifest.
        6. Writes the harmonised summary statistics to the specified output path.
        7. Performs quality control on the harmonised summary statistics based on a p-value threshold.
        8. Writes the QC results to the specified output path.
        9. Updates the study index with QC results and overwrites the existing study index file.

    !!! tip "Mermaid Diagram"

        ``` mermaid
        %% --- INPUTS ---
        A1([source_manifest_path]) --> B1
        A2([efo_curation_path]) --> B2
        A3([gnomad_variant_index_path]) --> G1

        %% --- STEP 1: MANIFEST AND EFO ---
        subgraph "Manifest & EFO Loading"
        B1["FinnGenMetaManifest.from_path()"] --> C1["finngen_manifest"]
        B2["EFOMapping.from_path()"] --> C2["efo_mapping"]
        C1 --> D1["FinnGenMetaStudyIndex.from_finngen_manifest()"]
        C2 --> D1
        end

        %% --- STEP 2: STUDY INDEX ---
        D1 --> D2["Write study_index → study_index_output_path"]
        D1 --> D3["Get summary_statistics_paths()"]

        %% --- STEP 3: SUMMARY STATS CONVERSION ---
        D3 --> E1["FinnGenUkbMvpMetaSummaryStatistics.bgzip_to_parquet()"]
        E1 --> E2["raw_summary_statistics_output_path"]

        %% --- STEP 4: VARIANT ANNOTATIONS ---
        G1["VariantIndex.from_parquet()"] --> H1["VariantDirection.from_variant_index()"]

        %% --- STEP 5: HARMONISATION ---
        E2 --> F1["Read raw_summary_statistics (Spark)"]
        F1 --> F2["FinnGenUkbMvpMetaSummaryStatistics.from_source()"]
        H1 --> F2
        F2 --> F3["Write harmonised_summary_statistics → harmonised_summary_statistics_output_path"]

        %% --- STEP 6: QC ---
        F3 --> Q1["SummaryStatistics.from_parquet()"]
        Q1 --> Q2["SummaryStatisticsQC.from_summary_statistics(qc_threshold)"]
        Q2 --> Q3["Write QC results → harmonised_summary_statistics_qc_output_path"]

        %% --- STEP 7: UPDATE STUDY INDEX ---
        Q2 --> S1["StudyIndex.from_parquet()"]
        S1 --> S2["annotate_sumstats_qc()"]
        S2 --> S3["Write updated study_index → study_index_output_path"]

        %% --- OUTPUTS ---
        S3 --> Z1([Final Study Index])
        Q3 --> Z2([QC Results])
        F3 --> Z3([Harmonised Summary Statistics])
        E2 --> Z4([Raw Summary Statistics])

        %% --- STYLING ---
        classDef input fill:#f8f8ff,stroke:#555,stroke-width:1px,color:#000;
        classDef output fill:#e7ffe7,stroke:#555,stroke-width:1px,color:#000;
        classDef process fill:#f0f0f0,stroke:#888,stroke-width:1px,color:#000;

        class A1,A2,A3 input;
        class Z1,Z2,Z3,Z4 output;
        class B1,B2,C1,C2,D1,D2,D3,E1,E2,F1,F2,F3,G1,H1,Q1,Q2,Q3,S1,S2,S3 process;
        ```
    """

    def __init__(
        self,
        session: Session,
        # Inputs
        source_manifest_path: str,
        efo_curation_path: str,
        gnomad_variant_index_path: str,
        # Outputs
        study_index_output_path: str,
        raw_summary_statistics_output_path: str,
        harmonised_summary_statistics_output_path: str,
        harmonised_summary_statistics_qc_output_path: str,
        # Harmonisation config
        perform_meta_analysis_filter: bool = True,
        imputation_score_threshold: float = 0.8,
        perform_imputation_score_filter: bool = True,
        min_allele_count_threshold: int = 20,
        perform_min_allele_count_filter: bool = True,
        min_allele_frequency_threshold: float = 1e-4,
        perform_min_allele_frequency_filter: bool = False,
        filter_out_ambiguous_variants: bool = False,
        # QC config
        qc_threshold: float = 1e-8,
    ) -> None:
        """Data ingestion and harmonisation step for FinnGen UKB meta-analysis.

        Args:
            session (Session): Session object.
            source_manifest_path (str): Path to the manifest file.
            efo_curation_path (str): Path to the EFO curation file.
            gnomad_variant_index_path (str): Path to the gnomAD variant index file.
            study_index_output_path (str): Output path for the study index.
            raw_summary_statistics_output_path (str): Output path for raw summary statistics.
            harmonised_summary_statistics_output_path (str): Output path for harmonised summary statistics.
            harmonised_summary_statistics_qc_output_path (str): Output path for harmonised summary statistics QC results.
            perform_meta_analysis_filter (bool, optional): Whether to filter non-meta analyzed variants. Defaults to True.
            imputation_score_threshold (float, optional): Imputation score threshold. Defaults to 0.8.
            perform_imputation_score_filter (bool, optional): Whether to filter low imputation scores. Defaults to True.
            min_allele_count_threshold (int, optional): Minimum allele count threshold. Defaults to 20.
            perform_min_allele_count_filter (bool, optional): Whether to filter low allele counts. Defaults to True.
            min_allele_frequency_threshold (float, optional): Minimum allele frequency threshold. Defaults to 1e-4.
            perform_min_allele_frequency_filter (bool, optional): Whether to filter low allele frequencies. Defaults to False.
            filter_out_ambiguous_variants (bool, optional): Whether to filter out ambiguous variants. Defaults to False.
            qc_threshold (float, optional): P-value threshold for QC. Defaults to 1e-8.

        Raises:
            AssertionError: If no summary statistics paths are found in the study index.
        """
        sumstat_harmonisation_config = {
            "perform_meta_analysis_filter": perform_meta_analysis_filter,
            "imputation_score_threshold": imputation_score_threshold,
            "perform_imputation_score_filter": perform_imputation_score_filter,
            "min_allele_count_threshold": min_allele_count_threshold,
            "perform_min_allele_count_filter": perform_min_allele_count_filter,
            "min_allele_frequency_threshold": min_allele_frequency_threshold,
            "perform_min_allele_frequency_filter": perform_min_allele_frequency_filter,
            "filter_out_ambiguous_variants": filter_out_ambiguous_variants,
        }

        session.logger.info(f"Reading Finngen manifest from {source_manifest_path}.")
        finngen_manifest = FinnGenMetaManifest.from_path(
            session=session, manifest_path=source_manifest_path
        )
        session.logger.info(f"Building study index for: {finngen_manifest.meta.value}")
        session.logger.info(f"Reading EFO curation from {efo_curation_path}.")
        efo_mapping = EFOMapping.from_path(
            session=session, efo_curation_path=efo_curation_path
        )

        session.logger.info("Creating study index.")
        study_index = FinnGenMetaStudyIndex.from_finngen_manifest(
            manifest=finngen_manifest, efo_mapping=efo_mapping
        )

        session.logger.info("Writing study index.")
        study_index.df.write.mode(session.write_mode).parquet(study_index_output_path)
        session.logger.info(f"Study index written to {study_index_output_path}.")

        session.logger.info("Reading summary statistics paths from manifest.")
        # NOTE: we can rely on the study index to extract the raw summary statistics paths
        # to make sure to only process these summary statistics which are part of the study index.
        # this may not be accurate if the summary statistics source paths were not found in the
        # source manifest.
        source_summary_statistics_paths = study_index.get_summary_statistics_paths()
        assert (
            len(source_summary_statistics_paths) > 0
        ), "No summary statistics paths found in study index."
        session.logger.info(
            f"Found {len(source_summary_statistics_paths)} summary statistics files."
        )

        session.logger.info("Converting raw summary statistics to Parquet format.")
        FinnGenUkbMvpMetaSummaryStatistics.bgzip_to_parquet(
            session=session,
            summary_statistics_list=source_summary_statistics_paths,
            datasource=finngen_manifest.meta,
            raw_summary_statistics_output_path=raw_summary_statistics_output_path,
            n_threads=FinnGenUkbMvpMetaSummaryStatistics.N_THREAD_OPTIMAL,
        )
        session.logger.info("Raw summary statistics conversion completed.")
        session.logger.info(f"Output path: {raw_summary_statistics_output_path}.")

        session.logger.info("Reading gnomAD variant index.")
        gnomad_variant_index = VariantIndex.from_parquet(
            session=session, path=gnomad_variant_index_path
        )

        session.logger.info("Building variant direction annotations.")
        variant_direction = VariantDirection.from_variant_index(
            variant_index=gnomad_variant_index
        )

        session.logger.info("Reading raw summary statistics.")
        raw_summary_statistics = session.spark.read.parquet(
            raw_summary_statistics_output_path
        )

        session.logger.info("Harmonising summary statistics.")
        session.logger.info("Applying the following harmonisation configuration:")
        for key, value in sumstat_harmonisation_config.items():
            session.logger.info(f"  - {key}: {value}")
        harmonised_summary_statistics = FinnGenUkbMvpMetaSummaryStatistics.from_source(
            raw_summary_statistics=raw_summary_statistics,
            finngen_manifest=finngen_manifest,
            variant_annotations=variant_direction,
            **sumstat_harmonisation_config,
        )

        session.logger.info("Writing harmonised summary statistics.")
        harmonised_summary_statistics.df.write.mode(session.write_mode).parquet(
            harmonised_summary_statistics_output_path
        )
        session.logger.info(
            f"Harmonised summary statistics written to {harmonised_summary_statistics_output_path}."
        )

        session.logger.info("Reading harmonised summary statistics for QC.")
        harmonised_summary_statistics = SummaryStatistics.from_parquet(
            session, harmonised_summary_statistics_output_path
        )
        session.logger.info("Running summary statistics QC.")
        summary_statistics_qc = SummaryStatisticsQC.from_summary_statistics(
            gwas=harmonised_summary_statistics,
            pval_threshold=qc_threshold,
        )

        session.logger.info("Writing summary statistics QC results.")
        summary_statistics_qc.df.repartition(1).write.mode(session.write_mode).parquet(
            harmonised_summary_statistics_qc_output_path
        )
        session.logger.info(
            f"Summary statistics QC results written to {harmonised_summary_statistics_qc_output_path}."
        )

        session.logger.info("Adding qc to the study index.")
        study_index = StudyIndex.from_parquet(session, study_index_output_path)
        study_index = study_index.annotate_sumstats_qc(summary_statistics_qc)

        session.logger.info("Writing updated study index.")
        study_index.df.repartition(1).write.mode("overwrite").parquet(
            study_index_output_path
        )
        session.logger.info("Updated study index with qc flags.")
