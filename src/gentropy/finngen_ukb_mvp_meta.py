r"""Steps to run FinnGen UKBB MVP meta-analysis data ingestion.

# Pipeline overview

The ingestion pipeline consists of four steps that must be executed in order:

1. **`FinngenUkbMvpMetaStudyIndexStep`** — builds `StudyIndex` from the manifest and EFO curation.
2. **`FinngenUkbMvpMetaSumstatConversionStep`** — converts BGZIP summary statistics to Parquet, partitioned by ``studyId``.
3. **`FinngenUkbMvpMetaSumstatHarmonisationStep`** — harmonises summary statistics using gnomAD allele directions.
4. **`FinngenUkbMvpMetaStudyIndexQCAnnotationStep`** — runs summary-statistics QC and annotates the study index.

`FinngenUkbMvpMetaSummaryStatisticsIngestionStep` is a convenience façade that chains all four steps.

``` mermaid
graph TD
    %% --- INPUTS ---
    A1([source_manifest_path]) --> B1
    A2([efo_curation_path]) --> B2
    A3([finngen_release]) --> C1
    A4([gnomad_variant_index_path]) --> G1
    A5([Source Summary Statistics BGZIP]) --> C3

    %% --- STEP 1: StudyIndex ---
    subgraph S1["① FinngenUkbMvpMetaStudyIndexStep"]
        B1["FinnGenMetaManifest"] --> C1["StudyIndex"]
        B2["EFOMapping"] --> C1
    end

    %% --- STEP 2: BGZIP to Parquet ---
    subgraph S2["② FinngenUkbMvpMetaSumstatConversionStep"]
        C1 --> C2["Summary statistics paths"]
        C2 --> C3["Raw summary statistics\n(Parquet · partitioned by studyId)"]
    end

    %% --- STEP 3: Harmonisation ---
    subgraph S3["③ FinngenUkbMvpMetaSumstatHarmonisationStep"]
        G1["VariantIndex"] --> G2["VariantDirection"]
        C3 --> D1["Allele flipping & filtering"]
        B1 --> D1
        G2 --> D1
        D1 --> E1["Harmonised summary statistics\n(Parquet · partitioned by studyId)"]
    end

    %% --- STEP 4: QC ---
    subgraph S4["④ FinngenUkbMvpMetaStudyIndexQCAnnotationStep"]
        E1 --> Q1["SummaryStatisticsQC"]
        Q1 --> Q2["StudyIndex annotated with QC"]
        C1 --> Q2
    end

    %% --- STYLING ---
    classDef input fill:#f8f8ff,stroke:#555,stroke-width:1px,color:#000;
    classDef output fill:#e7ffe7,stroke:#555,stroke-width:1px,color:#000;

    class A1,A2,A3,A4,A5 input;
    class C3,E1,Q1,Q2 output;
```

??? tip "Inputs"
    - [x] `source_manifest_path`: manifest with summary statistics file paths and study metadata.
    - [x] `efo_curation_path`: EFO curation file for disease mapping.
    - [x] `gnomad_variant_index_path`: gnomAD variant index used for allele-flip direction.
    - [x] `finngen_release`: FinnGen release identifier used to filter EFO mappings (default ``"R12"``).

??? tip "Outputs"
    - [x] Raw summary statistics in Parquet format, partitioned by ``studyId``.
    - [x] Harmonised summary statistics in Parquet format, partitioned by ``studyId``.
    - [x] Summary statistics QC results in Parquet format.
    - [x] Study index in Parquet format (updated with QC flags).
"""

from __future__ import annotations

from pydantic import BaseModel, Field

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


class _SumstatHarmonisationConfig(BaseModel):
    """Validated configuration for summary-statistics harmonisation."""

    perform_meta_analysis_filter: bool = True
    """Whether to remove variants that were not meta-analysed."""
    imputation_score_threshold: float = Field(default=0.8, ge=0.0, le=1.0)
    """Minimum INFO/imputation score to retain a variant. Must be in [0, 1]."""
    perform_imputation_score_filter: bool = True
    """Whether to apply the imputation score filter."""
    min_allele_count_threshold: int = Field(default=20, ge=1)
    """Minimum allele count (AC) to retain a variant. Must be >= 1."""
    perform_min_allele_count_filter: bool = True
    """Whether to apply the minimum allele count filter."""
    min_allele_frequency_threshold: float = Field(default=1e-4, gt=0.0, lt=1.0)
    """Minimum allele frequency (AF) to retain a variant. Must be in (0, 1)."""
    perform_min_allele_frequency_filter: bool = False
    """Whether to apply the minimum allele frequency filter."""
    filter_out_ambiguous_variants: bool = False
    """Whether to remove strand-ambiguous variants (A/T or C/G)."""


class FinngenUkbMvpMetaStudyIndexStep:
    """Step 1 of 4: build and write the FinnGen UKB MVP meta-analysis study index."""

    def __init__(
        self,
        session: Session,
        # Inputs
        source_manifest_path: str,
        efo_curation_path: str,
        # Output
        study_index_output_path: str,
        # Config
        finngen_release: str = "R12",
    ) -> None:
        """Build and write the FinnGen UKB MVP meta-analysis study index.

        Args:
            session (Session): Session object.
            source_manifest_path (str): Path to the FinnGen manifest file.
            efo_curation_path (str): Path to the EFO curation file.
            study_index_output_path (str): Output path for the study index.
            finngen_release (str): FinnGen release identifier used to filter EFO mappings (e.g. ``"R12"``). Defaults to ``"R12"``.
        """
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
            manifest=finngen_manifest,
            efo_mapping=efo_mapping,
            finngen_release=finngen_release,
        )

        session.logger.info("Writing study index.")
        study_index.df.write.mode(session.write_mode).parquet(study_index_output_path)
        session.logger.info(f"Study index written to {study_index_output_path}.")


class FinngenUkbMvpMetaSumstatConversionStep:
    """Step 2 of 4: convert BGZIP summary statistics to Parquet, partitioned by ``studyId``."""

    def __init__(
        self,
        session: Session,
        # Inputs
        source_manifest_path: str,
        study_index_output_path: str,
        # Output
        raw_summary_statistics_output_path: str,
    ) -> None:
        """Convert FinnGen UKB MVP meta-analysis summary statistics from BGZIP to Parquet.

        Args:
            session (Session): Session object.
            source_manifest_path (str): Path to the FinnGen manifest file.
            study_index_output_path (str): Path to the study index produced by the study index step.
            raw_summary_statistics_output_path (str): Output path for raw summary statistics.

        Raises:
            AssertionError: If no summary statistics paths are found in the study index.
        """
        session.logger.info(f"Reading Finngen manifest from {source_manifest_path}.")
        finngen_manifest = FinnGenMetaManifest.from_path(
            session=session, manifest_path=source_manifest_path
        )

        session.logger.info("Reading summary statistics paths from study index.")
        study_index = StudyIndex.from_parquet(
            session=session, path=study_index_output_path
        )
        source_summary_statistics_paths = study_index.get_summary_statistics_paths()
        assert len(source_summary_statistics_paths) > 0, (
            "No summary statistics paths found in study index."
        )
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


class FinngenUkbMvpMetaSumstatHarmonisationStep:
    """Step 3 of 4: harmonise summary statistics using gnomAD allele directions, partitioned by ``studyId``."""

    def __init__(
        self,
        session: Session,
        # Inputs
        source_manifest_path: str,
        gnomad_variant_index_path: str,
        raw_summary_statistics_output_path: str,
        # Output
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
        """Harmonise FinnGen UKB MVP meta-analysis summary statistics.

        Args:
            session (Session): Session object.
            source_manifest_path (str): Path to the FinnGen manifest file.
            gnomad_variant_index_path (str): Path to the gnomAD variant index file.
            raw_summary_statistics_output_path (str): Path to raw summary statistics produced by the conversion step.
            harmonised_summary_statistics_output_path (str): Output path for harmonised summary statistics, partitioned by studyId.
            perform_meta_analysis_filter (bool, optional): Whether to remove variants not included in the meta-analysis.
            imputation_score_threshold (float, optional): Minimum INFO score in [0, 1]. Defaults to 0.8.
            perform_imputation_score_filter (bool, optional): Whether to apply the imputation score filter.
            min_allele_count_threshold (int, optional): Minimum allele count (>= 1). Defaults to 20.
            perform_min_allele_count_filter (bool, optional): Whether to apply the minimum allele count filter.
            min_allele_frequency_threshold (float, optional): Minimum allele frequency in (0, 1). Defaults to 1e-4.
            perform_min_allele_frequency_filter (bool, optional): Whether to apply the minimum allele frequency filter.
            filter_out_ambiguous_variants (bool, optional): Whether to remove strand-ambiguous variants.
        """
        config = _SumstatHarmonisationConfig(
            perform_meta_analysis_filter=perform_meta_analysis_filter,
            imputation_score_threshold=imputation_score_threshold,
            perform_imputation_score_filter=perform_imputation_score_filter,
            min_allele_count_threshold=min_allele_count_threshold,
            perform_min_allele_count_filter=perform_min_allele_count_filter,
            min_allele_frequency_threshold=min_allele_frequency_threshold,
            perform_min_allele_frequency_filter=perform_min_allele_frequency_filter,
            filter_out_ambiguous_variants=filter_out_ambiguous_variants,
        )

        session.logger.info(f"Reading Finngen manifest from {source_manifest_path}.")
        finngen_manifest = FinnGenMetaManifest.from_path(
            session=session, manifest_path=source_manifest_path
        )

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
        for key, value in config.model_dump().items():
            session.logger.info(f"  - {key}: {value}")
        harmonised_summary_statistics = FinnGenUkbMvpMetaSummaryStatistics.from_source(
            raw_summary_statistics=raw_summary_statistics,
            finngen_manifest=finngen_manifest,
            variant_annotations=variant_direction,
            **config.model_dump(),
        )

        session.logger.info("Writing harmonised summary statistics.")
        harmonised_summary_statistics.df.write.mode(session.write_mode).partitionBy(
            "studyId"
        ).parquet(harmonised_summary_statistics_output_path)
        session.logger.info(
            f"Harmonised summary statistics written to {harmonised_summary_statistics_output_path}."
        )


class FinngenUkbMvpMetaStudyIndexQCAnnotationStep:
    """Step 4 of 4: run summary-statistics QC and annotate the study index with QC flags."""

    def __init__(
        self,
        session: Session,
        # Inputs
        study_index_output_path: str,
        harmonised_summary_statistics_output_path: str,
        # Output
        harmonised_summary_statistics_qc_output_path: str,
        # QC config
        qc_threshold: float = 1e-8,
    ) -> None:
        """Run QC on harmonised summary statistics and annotate the study index.

        Args:
            session (Session): Session object.
            study_index_output_path (str): Path to the study index (will be overwritten with QC flags).
            harmonised_summary_statistics_output_path (str): Path to harmonised summary statistics produced by the harmonisation step.
            harmonised_summary_statistics_qc_output_path (str): Output path for harmonised summary statistics QC results.
            qc_threshold (float, optional): P-value threshold for QC. Defaults to 1e-8.

        Raises:
            AssertionError: If qc_threshold is not less than 1.0.
        """
        assert qc_threshold < 1.0, "QC threshold should be a p-value less than 1.0."

        session.logger.info("Reading harmonised summary statistics for QC.")
        harmonised_summary_statistics = SummaryStatistics.from_parquet(
            session=session, path=harmonised_summary_statistics_output_path
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
        study_index = StudyIndex.from_parquet(
            session=session, path=study_index_output_path
        )
        study_index = study_index.annotate_sumstats_qc(summary_statistics_qc)

        session.logger.info("Writing updated study index.")
        study_index.df.repartition(1).write.mode("overwrite").parquet(
            study_index_output_path
        )
        session.logger.info("Updated study index with qc flags.")


class FinngenUkbMvpMetaSummaryStatisticsIngestionStep:
    """Convenience façade that chains all four FinnGen UKB MVP meta-analysis ingestion steps.

    Runs `FinngenUkbMvpMetaStudyIndexStep`, `FinngenUkbMvpMetaSumstatConversionStep`,
    `FinngenUkbMvpMetaSumstatHarmonisationStep`, and
    `FinngenUkbMvpMetaStudyIndexQCAnnotationStep` in order.
    See the module docstring for the full pipeline diagram.
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
        FinngenUkbMvpMetaStudyIndexStep(
            session=session,
            source_manifest_path=source_manifest_path,
            efo_curation_path=efo_curation_path,
            study_index_output_path=study_index_output_path,
        )
        FinngenUkbMvpMetaSumstatConversionStep(
            session=session,
            source_manifest_path=source_manifest_path,
            study_index_output_path=study_index_output_path,
            raw_summary_statistics_output_path=raw_summary_statistics_output_path,
        )
        FinngenUkbMvpMetaSumstatHarmonisationStep(
            session=session,
            source_manifest_path=source_manifest_path,
            gnomad_variant_index_path=gnomad_variant_index_path,
            raw_summary_statistics_output_path=raw_summary_statistics_output_path,
            harmonised_summary_statistics_output_path=harmonised_summary_statistics_output_path,
            perform_meta_analysis_filter=perform_meta_analysis_filter,
            imputation_score_threshold=imputation_score_threshold,
            perform_imputation_score_filter=perform_imputation_score_filter,
            min_allele_count_threshold=min_allele_count_threshold,
            perform_min_allele_count_filter=perform_min_allele_count_filter,
            min_allele_frequency_threshold=min_allele_frequency_threshold,
            perform_min_allele_frequency_filter=perform_min_allele_frequency_filter,
            filter_out_ambiguous_variants=filter_out_ambiguous_variants,
        )
        FinngenUkbMvpMetaStudyIndexQCAnnotationStep(
            session=session,
            study_index_output_path=study_index_output_path,
            harmonised_summary_statistics_output_path=harmonised_summary_statistics_output_path,
            harmonised_summary_statistics_qc_output_path=harmonised_summary_statistics_qc_output_path,
            qc_threshold=qc_threshold,
        )
