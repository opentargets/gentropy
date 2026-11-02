"""Step to run FinnGen UKBB MVP meta-analysis data ingestion."""

from __future__ import annotations

from typing import TYPE_CHECKING

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

if TYPE_CHECKING:
    from typing import Any


class FinngenUkbMvpMetaSummaryStatisticsIngestionStep:
    """FinnGen UK Biobank and Million Veteran Program meta-analysis summary statistics ingestion step.

    # Process overview

    The step performs the following operations:

    1. Prepares `FinnGenManifest` and `EFOCuration`.
    2. Builds the `StudyIndex`.
    3. Reads the source summary statistics listed in the `StudyIndex`, one read per distinct header.
    4. Prepares `VariantDirection` for allele flipping.
    5. Harmonises `SummaryStatistics` from the source summary statistics.
    6. Performs quality control on the harmonised `SummaryStatistics`.
    7. Writes the `StudyIndex` with the QC results.

    ``` mermaid
    graph TD
        %% --- INPUTS ---
        A1([source_manifest_path]) --> B1
        A2([efo_curation_path]) --> B2
        A3([gnomad_variant_index_path]) --> G1
        A4([Source Summary Statistics in BGZIP format]) --> C3

        %% --- STEP 1: StudyIndex ---
        subgraph "Building studyIndex"
        B1["FinnGenMetaManifest"] --> C1["StudyIndex"]
        B2["EFOMapping"] --> C1
        end

        %% --- STEP 2: Source Summary Statistics ---
        subgraph "Reading summary statistics"
        C1 --> C2["List of summary statistics paths"]
        C2 --> C3["Source summary statistics, grouped by header"]
        end

        %% --- STEP 3: Variant Annotations ---
        subgraph "Variant Annotations"
        G1["VariantIndex"] --> G2["VariantDirection"]
        end

        %% --- STEP 4: Harmonised Summary Statistics ---
        subgraph "Harmonising summary statistics"
        C3 --> D1["Allele flipping"]
        B1 --> D1
        G2 --> D1
        D1 --> D2["Removal of not meta-analysed variants"]
        D2 --> D3["Removal of low imputation score variants"]
        D3 --> D4["Removal of low allele count variants"]
        D4 --> E1["Harmonised summary statistics, partitioned by studyId and chromosome"]
        end

        %% --- STEP 5: QC ---
        subgraph "Summary Statistics QC"
        E1 --> Q1["SummaryStatistics QC"]
        Q1 --> Q2["StudyIndex annotated with QC"]
        C1 --> Q2
        end

        %% --- STYLING ---
        classDef input fill:#f8f8ff,stroke:#555,stroke-width:1px,color:#000;
        classDef output fill:#e7ffe7,stroke:#555,stroke-width:1px,color:#000;

        class A1,A2,A3,A4 input;
        class Q2,E1,Q1 output;
    ```

    ??? tip "Inputs"
        - [x] This step requires the gnomAD variant index to perform the allele flipping during harmonisation.
        - [x] The `source_manifest_path` should point to a manifest that includes paths to the summary statistics files.

    ??? tip "Outputs"
        This step outputs 3 artifacts:

        - [x] Harmonised summary statistics in Parquet format, partitioned by `studyId` and `chromosome`, sorted by `position` within each partition.
        - [x] Summary statistics QC results in Parquet format.
        - [x] Study Index in parquet format, with the QC results.

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
            harmonised_summary_statistics_output_path (str): Output path for harmonised summary statistics.
            harmonised_summary_statistics_qc_output_path (str): Output path for harmonised summary statistics QC results.
            perform_meta_analysis_filter (bool, optional): Whether to filter non-meta analyzed variants.
            imputation_score_threshold (float, optional): Imputation score threshold.
            perform_imputation_score_filter (bool, optional): Whether to filter low imputation scores.
            min_allele_count_threshold (int, optional): Minimum allele count threshold.
            perform_min_allele_count_filter (bool, optional): Whether to filter low allele counts.
            min_allele_frequency_threshold (float, optional): Minimum allele frequency threshold.
            perform_min_allele_frequency_filter (bool, optional): Whether to filter low allele frequencies.
            filter_out_ambiguous_variants (bool, optional): Whether to filter out ambiguous variants.
            qc_threshold (float, optional): P-value threshold for QC.

        Raises:
            AssertionError: If no summary statistics paths are found in the study index.
        """
        assert qc_threshold < 1.0, "QC threshold should be a p-value less than 1.0."
        sumstat_harmonisation_config: dict[str, Any] = {
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

        session.logger.info("Reading summary statistics paths from study index.")
        # NOTE: The study index lists the summary statistics paths, so only the summary
        # statistics of the studies in the study index are read.
        source_summary_statistics_paths = study_index.get_summary_statistics_paths()
        assert len(source_summary_statistics_paths) > 0, (
            "No summary statistics paths found in study index."
        )
        session.logger.info(
            f"Found {len(source_summary_statistics_paths)} summary statistics files."
        )
        source_summary_statistics = FinnGenUkbMvpMetaSummaryStatistics.from_source_files(
            session=session,
            summary_statistics_paths=source_summary_statistics_paths,
            datasource=finngen_manifest.meta,
        )

        session.logger.info("Reading gnomAD variant index.")
        gnomad_variant_index = VariantIndex.from_parquet(
            session=session, path=gnomad_variant_index_path
        )

        session.logger.info("Building variant direction annotations.")
        variant_direction = VariantDirection.from_variant_index(
            variant_index=gnomad_variant_index
        )

        session.logger.info("Harmonising summary statistics.")
        session.logger.info("Applying the following harmonisation configuration:")
        for key, value in sumstat_harmonisation_config.items():
            session.logger.info(f"  - {key}: {value}")
        harmonised_summary_statistics = FinnGenUkbMvpMetaSummaryStatistics.from_source(
            raw_summary_statistics=source_summary_statistics,
            finngen_manifest=finngen_manifest,
            variant_annotations=variant_direction,
            **sumstat_harmonisation_config,
        )

        session.logger.info("Writing harmonised summary statistics.")
        # One file per study and chromosome, with the variants in genomic order. The sort
        # starts with the partition columns, so the write adds no sort of its own.
        partitions = ["studyId", "chromosome"]
        (
            harmonised_summary_statistics.df.repartition(*partitions)
            .sortWithinPartitions(*partitions, "position")
            .write.mode(session.write_mode)
            .partitionBy(*partitions)
            .parquet(harmonised_summary_statistics_output_path)
        )
        session.logger.info(
            f"Harmonised summary statistics written to {harmonised_summary_statistics_output_path}."
        )

        session.logger.info("Running summary statistics QC.")
        summary_statistics_qc = SummaryStatisticsQC.from_summary_statistics(
            gwas=SummaryStatistics.from_parquet(
                session=session, path=harmonised_summary_statistics_output_path
            ),
            pval_threshold=qc_threshold,
        )
        summary_statistics_qc.df.repartition(1).write.mode(session.write_mode).parquet(
            harmonised_summary_statistics_qc_output_path
        )
        session.logger.info(
            f"Summary statistics QC results written to {harmonised_summary_statistics_qc_output_path}."
        )

        session.logger.info("Writing study index with the QC results.")
        # The QC is read back from its output, so it is computed once.
        study_index = study_index.annotate_sumstats_qc(
            SummaryStatisticsQC.from_parquet(
                session=session, path=harmonised_summary_statistics_qc_output_path
            )
        )
        study_index.df.repartition(1).write.mode(session.write_mode).parquet(
            study_index_output_path
        )
        session.logger.info(f"Study index written to {study_index_output_path}.")
