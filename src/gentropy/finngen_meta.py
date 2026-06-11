r"""Steps for ingesting FinnGen two-way and three-way meta-analyses.

# Pipeline overview

The ingestion workflow is split into independently runnable steps:

1. `FinngenMetaStudyIndexStep` builds a meta-analysis study index from a manifest.
2. `FinngenUkbMvpMetaSumstatConversionStep` converts three-way BGZIP files to
   Parquet partitioned by ``studyId``.
3. `TwoWayMetaSumstatHarmonisationStep` and
   `ThreeWayMetaSumstatHarmonisationStep` harmonise the corresponding inputs
   against a precomputed `VariantDirection` dataset.
4. `FinngenMetaStudyIndexQCAnnotationStep` computes summary-statistics QC and
   writes an annotated standard study index.
"""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.study_index import MetaAnalysisStudyIndex
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.datasource.finngen.efo_mapping import EFOMapping
from gentropy.datasource.finngen_meta import (
    FinnGenMetaRelease,
    MetaAnalysisHarmonisationConfig,
    MetaAnalysisType,
)
from gentropy.datasource.finngen_meta.study_index import FinnGenMetaManifest
from gentropy.datasource.finngen_meta.three_way import (
    ThreeWaySummaryStatistics,
)
from gentropy.datasource.finngen_meta.two_way import TwoWaySummaryStatistics


class FinngenMetaStudyIndexStep:
    """Build and write a FinnGen meta-analysis study index."""

    def __init__(
        self,
        session: Session,
        # Inputs
        manifest_path: str,
        efo_curation_path: str,
        # Output
        study_index_output_path: str,
        # Config
        finngen_release: str = "R12",
        meta_analysis_type: str = "three_way",
    ) -> None:
        """Build and write a two-way or three-way FinnGen meta-analysis study index.

        Args:
            session (Session): Session object.
            manifest_path (str): Path to the FinnGen manifest file.
            efo_curation_path (str): Path to the EFO curation file.
            study_index_output_path (str): Output path for the study index.
            finngen_release (str): FinnGen release identifier used in generated study IDs
                (e.g. ``"R12"``). Defaults to ``"R12"``.
            meta_analysis_type (str): Meta-analysis type, case-insensitively matching
                ``"TWO_WAY"`` or ``"THREE_WAY"``. Defaults to ``"three_way"``.
        """
        if finngen_release != "R12":
            raise NotImplementedError("Only FinnGen R12 is currently supported.")
        release = FinnGenMetaRelease(release=finngen_release)
        session.logger.info(f"Reading FinnGen manifest from {manifest_path}.")

        session.logger.info(f"Reading EFO curation from {efo_curation_path}.")
        efo_mapping = EFOMapping.from_path(
            session=session, efo_curation_path=efo_curation_path
        )

        session.logger.info("Creating study index.")
        study_index = FinnGenMetaManifest.from_source(
            session=session,
            manifest_path=manifest_path,
            meta_analysis_type=MetaAnalysisType(meta_analysis_type.upper()),
            release=release,
            efo_mapping=efo_mapping,
        )

        session.logger.info("Writing study index.")
        study_index.df.write.mode(session.write_mode).parquet(study_index_output_path)
        session.logger.info(f"Study index written to {study_index_output_path}.")


class FinngenUkbMvpMetaSumstatConversionStep:
    """Convert three-way FinnGen-UKBB-MVP BGZIP summary statistics to Parquet."""

    def __init__(
        self,
        session: Session,
        # Inputs
        summary_statistics_glob: str,
        # Output
        raw_summary_statistics_output_path: str,
        # Config
        finngen_release: str = "R12",
    ) -> None:
        """Convert FinnGen UKB MVP meta-analysis summary statistics from BGZIP to Parquet.

        Args:
            session (Session): Session object.
            summary_statistics_glob (str): Hadoop-compatible path or glob for source
                summary-statistics files.
            raw_summary_statistics_output_path (str): Output path for raw summary statistics.
            finngen_release (str): FinnGen release identifier used in generated study IDs
                (e.g. ``"R12"``). Defaults to ``"R12"``.

        Raises:
            AssertionError: If the glob does not resolve to any files.
        """
        session.logger.info("Resolving source summary statistics paths.")
        ssp = session.list_hadoop_paths(summary_statistics_glob)
        assert len(ssp) > 1, (
            f"Expected more than one summary statistics file, found {len(ssp)} for '{summary_statistics_glob}'."
        )
        session.logger.info(f"Found {len(ssp)} summary statistics files.")

        session.logger.info("Converting raw summary statistics to Parquet format.")
        ThreeWaySummaryStatistics.bgzip_to_parquet(
            session=session,
            summary_statistics_list=ssp,
            raw_summary_statistics_output_path=raw_summary_statistics_output_path,
            n_threads=ThreeWaySummaryStatistics.N_THREAD_OPTIMAL,
            meta_analysis_type=MetaAnalysisType.THREE_WAY,
            finngen_release=FinnGenMetaRelease(release=finngen_release),
        )
        session.logger.info("Raw summary statistics conversion completed.")
        session.logger.info(f"Output path: {raw_summary_statistics_output_path}.")


class TwoWayMetaSumstatHarmonisationStep:
    """Harmonise two-way FinnGen-UKBB summary statistics."""

    def __init__(
        self,
        session: Session,
        # Inputs
        meta_analysis_study_index_path: str,
        variant_direction_path: str,
        raw_summary_statistics_output_path: str,
        # Output
        harmonised_summary_statistics_output_path: str,
        # Harmonisation config
        perform_meta_analysis_filter: bool = True,
        min_allele_count_threshold: int = 20,
        perform_min_allele_count_filter: bool = True,
        min_allele_frequency_threshold: float = 1e-4,
        perform_min_allele_frequency_filter: bool = False,
        remove_ambiguous_alleles: bool = False,
        verify_atgc: bool = True,
        remove_monomorphic_alleles: bool = True,
        finngen_release: str = "R12",
    ) -> None:
        """Harmonise FinnGen meta-analysis summary statistics.

        Args:
            session (Session): Session object.
            meta_analysis_study_index_path (str): Path to the meta-analysis study index.
            variant_direction_path (str): Path to the variant direction file.
            raw_summary_statistics_output_path (str): Path to raw summary statistics produced by the conversion step.
            harmonised_summary_statistics_output_path (str): Output path for harmonised summary statistics, partitioned by studyId.
            perform_meta_analysis_filter (bool, optional): Whether to remove variants not included in the meta-analysis.
            min_allele_count_threshold (int, optional): Minimum allele count (>= 1). Defaults to 20.
            perform_min_allele_count_filter (bool, optional): Whether to apply the minimum allele count filter.
            min_allele_frequency_threshold (float, optional): Minimum allele frequency in (0, 0.5). Defaults to 1e-4.
            perform_min_allele_frequency_filter (bool, optional): Whether to apply the minimum allele frequency filter.
            remove_ambiguous_alleles (bool, optional): Whether to remove strand-ambiguous variants.
            verify_atgc (bool, optional): Whether to verify that reference and alternate alleles are valid (A, T, G, C).
            remove_monomorphic_alleles (bool, optional): Whether to remove monomorphic variants (i.e. variants where all alleles are the same).
            finngen_release (str): FinnGen release identifier used in generated study IDs
                (e.g. ``"R12"``). Defaults to ``"R12"``.
        """
        config = MetaAnalysisHarmonisationConfig(
            perform_meta_analysis_filter=perform_meta_analysis_filter,
            # MAC filter
            perform_min_allele_count_filter=perform_min_allele_count_filter,
            min_allele_count_threshold=min_allele_count_threshold,
            # MAF filter
            perform_min_allele_frequency_filter=perform_min_allele_frequency_filter,
            min_allele_frequency_threshold=min_allele_frequency_threshold,
            # Remove ambiguous variants filter
            remove_ambiguous_alleles=remove_ambiguous_alleles,
            # Remove non-ATGC alleles
            verify_atgc=verify_atgc,
            # Remove monoallelic variants
            remove_monomorphic_alleles=remove_monomorphic_alleles,
        )

        session.logger.info(
            f"Reading Meta analysis Study Index from {meta_analysis_study_index_path}."
        )
        msi = MetaAnalysisStudyIndex.from_parquet(
            session=session,
            path=meta_analysis_study_index_path,
        )
        session.logger.info("Reading variant direction annotations.")
        vd = VariantDirection.from_parquet(session=session, path=variant_direction_path)

        session.logger.info("Reading raw summary statistics.")
        rss = session.spark.read.parquet(raw_summary_statistics_output_path)
        session.logger.info("Harmonising summary statistics.")
        hss = TwoWaySummaryStatistics.from_source(
            raw_summary_statistics=rss,
            meta_analysis_study_index=msi,
            variant_direction=vd,
            config=config,
            finngen_release=FinnGenMetaRelease(release=finngen_release),
        )

        session.logger.info("Writing harmonised summary statistics.")
        (
            hss.df.write.mode(session.write_mode)
            .partitionBy("studyId", "chromosome")
            .option("maxRecordsPerFile", 50_000_000)
            .parquet(harmonised_summary_statistics_output_path)
        )
        session.logger.info(
            f"Harmonised summary statistics written to {harmonised_summary_statistics_output_path}."
        )


class ThreeWayMetaSumstatHarmonisationStep:
    """Harmonise three-way FinnGen-UKBB-MVP summary statistics."""

    def __init__(
        self,
        session: Session,
        # Inputs
        meta_analysis_study_index_path: str,
        variant_direction_path: str,
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
        remove_ambiguous_alleles: bool = False,
        verify_atgc: bool = True,
        remove_monomorphic_alleles: bool = True,
    ) -> None:
        """Harmonise FinnGen meta-analysis summary statistics.

        Args:
            session (Session): Session object.
            meta_analysis_study_index_path (str): Path to the meta-analysis study index.
            variant_direction_path (str): Path to the variant direction file.
            raw_summary_statistics_output_path (str): Path to raw summary statistics produced by the conversion step.
            harmonised_summary_statistics_output_path (str): Output path for harmonised summary statistics, partitioned by studyId.
            perform_meta_analysis_filter (bool, optional): Whether to remove variants not included in the meta-analysis.
            imputation_score_threshold (float, optional): Minimum INFO score in [0, 1]. Defaults to 0.8.
            perform_imputation_score_filter (bool, optional): Whether to apply the imputation score filter.
            min_allele_count_threshold (int, optional): Minimum allele count (>= 1). Defaults to 20.
            perform_min_allele_count_filter (bool, optional): Whether to apply the minimum allele count filter.
            min_allele_frequency_threshold (float, optional): Minimum allele frequency in (0, 0.5). Defaults to 1e-4.
            perform_min_allele_frequency_filter (bool, optional): Whether to apply the minimum allele frequency filter.
            remove_ambiguous_alleles (bool, optional): Whether to remove strand-ambiguous variants.
            verify_atgc (bool, optional): Whether to verify that reference and alternate alleles are valid (A, T, G, C).
            remove_monomorphic_alleles (bool, optional): Whether to remove monomorphic variants (i.e. variants where all alleles are the same).
        """
        config = MetaAnalysisHarmonisationConfig(
            perform_meta_analysis_filter=perform_meta_analysis_filter,
            # Imputation filter
            perform_imputation_score_filter=perform_imputation_score_filter,
            imputation_score_threshold=imputation_score_threshold,
            # MAC filter
            perform_min_allele_count_filter=perform_min_allele_count_filter,
            min_allele_count_threshold=min_allele_count_threshold,
            # MAF filter
            perform_min_allele_frequency_filter=perform_min_allele_frequency_filter,
            min_allele_frequency_threshold=min_allele_frequency_threshold,
            # Remove ambiguous variants filter
            remove_ambiguous_alleles=remove_ambiguous_alleles,
            # Remove non-ATGC alleles
            verify_atgc=verify_atgc,
            # Remove monoallelic variants
            remove_monomorphic_alleles=remove_monomorphic_alleles,
        )

        session.logger.info(
            f"Reading Meta analysis Study Index from {meta_analysis_study_index_path}."
        )
        msi = MetaAnalysisStudyIndex.from_parquet(
            session=session,
            path=meta_analysis_study_index_path,
        )

        session.logger.info("Reading variant direction annotations.")
        vd = VariantDirection.from_parquet(session=session, path=variant_direction_path)

        session.logger.info("Reading raw summary statistics.")
        rss = session.spark.read.parquet(raw_summary_statistics_output_path)

        session.logger.info("Harmonising summary statistics.")
        hss = ThreeWaySummaryStatistics.from_source(
            raw_summary_statistics=rss,
            meta_analysis_study_index=msi,
            variant_direction=vd,
            config=config,
        )

        session.logger.info("Writing harmonised summary statistics.")
        (
            hss.df.write.mode(session.write_mode)
            .partitionBy("studyId", "chromosome")
            .option("maxRecordsPerFile", 50_000_000)
            .parquet(harmonised_summary_statistics_output_path)
        )
        session.logger.info(
            f"Harmonised summary statistics written to {harmonised_summary_statistics_output_path}."
        )


class FinngenMetaStudyIndexQCAnnotationStep:
    """Run summary-statistics QC and write an annotated standard study index."""

    def __init__(
        self,
        session: Session,
        # Inputs
        study_index_output_path: str,
        harmonised_summary_statistics_output_path: str,
        # Output
        harmonised_summary_statistics_qc_output_path: str,
        study_index_with_qc_output_path: str,
        # QC config
        qc_threshold: float = 1e-8,
    ) -> None:
        """Run QC on harmonised summary statistics and annotate the study index.

        Args:
            session (Session): Session object.
            study_index_output_path (str): Path to the meta-analysis study index.
            harmonised_summary_statistics_output_path (str): Path to harmonised summary statistics produced by the harmonisation step.
            harmonised_summary_statistics_qc_output_path (str): Output path for harmonised summary statistics QC results.
            study_index_with_qc_output_path (str): Output path for the study index annotated with QC flags.
            qc_threshold (float, optional): P-value threshold for QC. Defaults to 1e-8.

        Raises:
            AssertionError: If ``qc_threshold`` is not between 0.0 and 1.0.
        """
        assert 0.0 < qc_threshold < 1.0, (
            "QC threshold should be a p-value greater than 0.0 and less than 1.0."
        )

        session.logger.info("Reading harmonised summary statistics for QC.")
        hss = SummaryStatistics.from_parquet(
            session=session,
            path=harmonised_summary_statistics_output_path,
        )

        session.logger.info("Running summary statistics QC.")
        ssqc = SummaryStatisticsQC.from_summary_statistics(
            gwas=hss,
            pval_threshold=qc_threshold,
        )

        session.logger.info("Annotating study index.")
        si = MetaAnalysisStudyIndex.from_parquet(
            session=session, path=study_index_output_path
        ).to_study()
        (
            hss.annotate_study_with_sumstat_location(si)
            .annotate_sumstats_qc(ssqc)
            .df.repartition(1)
            .write.mode(session.write_mode)
            .parquet(study_index_with_qc_output_path)
        )

        session.logger.info("Writing summary statistics QC results.")
        ssqc.df.repartition(1).write.mode(session.write_mode).parquet(
            harmonised_summary_statistics_qc_output_path
        )

        session.logger.info(
            f"Summary statistics QC results written to {harmonised_summary_statistics_qc_output_path}."
        )
