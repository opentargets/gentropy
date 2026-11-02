"""Steps to run FinnGen UKBB MVP meta-analysis data ingestion."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy.common.session import Session
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

    1. Prepares the `FinnGenMetaManifest`.
    2. Reads the source summary statistics listed in the manifest, one read per distinct header.
    3. Prepares `VariantDirection` for allele flipping.
    4. Harmonises `SummaryStatistics` from the source summary statistics.

    The QC of the harmonised summary statistics is the `summary_statistics_qc` step, and the
    study index is the `finngen_ukb_mvp_meta_study_index` step.

    ``` mermaid
    graph TD
        %% --- INPUTS ---
        A1([source_manifest_path]) --> B1
        A3([gnomad_variant_index_path]) --> G1
        A4([Source Summary Statistics in BGZIP format]) --> C3

        %% --- STEP 1: Source Summary Statistics ---
        subgraph "Reading summary statistics"
        B1["FinnGenMetaManifest"] --> C2["List of summary statistics paths"]
        C2 --> C3["Source summary statistics, grouped by header"]
        end

        %% --- STEP 2: Variant Annotations ---
        subgraph "Variant Annotations"
        G1["VariantIndex"] --> G2["VariantDirection"]
        end

        %% --- STEP 3: Harmonised Summary Statistics ---
        subgraph "Harmonising summary statistics"
        C3 --> D1["Allele flipping"]
        B1 --> D1
        G2 --> D1
        D1 --> D2["Removal of not meta-analysed variants"]
        D2 --> D3["Removal of low imputation score variants"]
        D3 --> D4["Removal of low allele count variants"]
        D4 --> E1["Harmonised summary statistics, partitioned by studyId and chromosome"]
        end

        %% --- STYLING ---
        classDef input fill:#f8f8ff,stroke:#555,stroke-width:1px,color:#000;
        classDef output fill:#e7ffe7,stroke:#555,stroke-width:1px,color:#000;

        class A1,A3,A4 input;
        class E1 output;
    ```

    ??? tip "Inputs"
        - [x] This step requires the gnomAD variant index to perform the allele flipping during harmonisation.
        - [x] The `source_manifest_path` should point to a manifest that includes paths to the summary statistics files.

    ??? tip "Outputs"
        - [x] Harmonised summary statistics in Parquet format, partitioned by `studyId` and `chromosome`, sorted by `position` within each partition.

    """

    def __init__(
        self,
        session: Session,
        # Inputs
        source_manifest_path: str,
        gnomad_variant_index_path: str,
        # Outputs
        harmonised_summary_statistics_output_path: str,
        # Harmonisation config
        perform_meta_analysis_filter: bool = True,
        imputation_score_threshold: float = 0.8,
        perform_imputation_score_filter: bool = True,
        min_allele_count_threshold: int = 20,
        perform_min_allele_count_filter: bool = True,
        min_allele_frequency_threshold: float = 1e-4,
        perform_min_allele_frequency_filter: bool = False,
        filter_out_ambiguous_variants: bool = False,
    ) -> None:
        """Data ingestion and harmonisation step for FinnGen UKB meta-analysis.

        Args:
            session (Session): Session object.
            source_manifest_path (str): Path to the manifest file.
            gnomad_variant_index_path (str): Path to the gnomAD variant index file.
            harmonised_summary_statistics_output_path (str): Output path for harmonised summary statistics.
            perform_meta_analysis_filter (bool, optional): Whether to filter non-meta analyzed variants.
            imputation_score_threshold (float, optional): Imputation score threshold.
            perform_imputation_score_filter (bool, optional): Whether to filter low imputation scores.
            min_allele_count_threshold (int, optional): Minimum allele count threshold.
            perform_min_allele_count_filter (bool, optional): Whether to filter low allele counts.
            min_allele_frequency_threshold (float, optional): Minimum allele frequency threshold.
            perform_min_allele_frequency_filter (bool, optional): Whether to filter low allele frequencies.
            filter_out_ambiguous_variants (bool, optional): Whether to filter out ambiguous variants.

        Raises:
            AssertionError: If no summary statistics paths are found in the manifest.
        """
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

        session.logger.info("Reading summary statistics paths from the manifest.")
        source_summary_statistics_paths = finngen_manifest.get_summary_statistics_paths()
        assert len(source_summary_statistics_paths) > 0, (
            "No summary statistics paths found in the manifest."
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


class FinngenUkbMvpMetaStudyIndexStep:
    """FinnGen UK Biobank and Million Veteran Program meta-analysis study index step.

    The study index holds one study per manifest row, with the ontology terms of the EFO
    curation. When `sumstats_qc_path` is given, the studies are annotated with the summary
    statistics QC, the output of the `summary_statistics_qc` step, in `sumstatQCValues`
    and `qualityControls`.

    The study index depends only on the manifest, the EFO curation and the QC, so a new
    curation or new QC thresholds need this step only, and no new harmonisation.
    """

    def __init__(
        self,
        session: Session,
        source_manifest_path: str,
        efo_curation_path: str,
        study_index_output_path: str,
        sumstats_qc_path: str | None = None,
    ) -> None:
        """Build the study index of the FinnGen UKB MVP meta-analysis.

        Args:
            session (Session): Session object.
            source_manifest_path (str): Path to the manifest file.
            efo_curation_path (str): Path to the EFO curation file.
            study_index_output_path (str): Output path for the study index.
            sumstats_qc_path (str | None): Path to the summary statistics QC. Optional.
        """
        session.logger.info(f"Reading Finngen manifest from {source_manifest_path}.")
        finngen_manifest = FinnGenMetaManifest.from_path(
            session=session, manifest_path=source_manifest_path
        )
        session.logger.info(f"Reading EFO curation from {efo_curation_path}.")
        efo_mapping = EFOMapping.from_path(
            session=session, efo_curation_path=efo_curation_path
        )
        study_index = FinnGenMetaStudyIndex.from_finngen_manifest(
            manifest=finngen_manifest, efo_mapping=efo_mapping
        )
        if sumstats_qc_path:
            session.logger.info(f"Annotating the study index with {sumstats_qc_path}.")
            study_index = study_index.annotate_sumstats_qc(
                SummaryStatisticsQC.from_parquet(session=session, path=sumstats_qc_path)
            )
        study_index.df.repartition(1).write.mode(session.write_mode).parquet(
            study_index_output_path
        )
        session.logger.info(f"Study index written to {study_index_output_path}.")
