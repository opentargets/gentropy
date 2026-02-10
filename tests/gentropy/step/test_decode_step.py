"""Test deCODE ingestion steps."""

import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame, Row

from gentropy import Session
from gentropy.decode_ingestion import (
    deCODEManifestGenerationStep,
    deCODEStudyIndexGenerationStep,
    deCODESummaryStatisticsHarmonisationStep,
    deCODESummaryStatisticsIngestionStep,
    deCODESummaryStatisticsQCStep,
)


@pytest.fixture
def decode_manifest_df(session: Session) -> DataFrame:
    """Manifest example DataFrame fixture."""
    data = [
        Row(
            projectId="deCODE-proteomics-raw",
            studyId="deCODE-proteomics-raw_Proteomics_PC0_10006_7_RAF1_c_Raf_05314415",
            hasSumstats=True,
            summarystatsLocation="s3a://some_bucket/some_folder/Proteomics_PC0_10006_7_RAF1_c_Raf_05314415.txt.gz",
            size="925.8 MiB",
            accessionTimestamp=datetime.datetime(2024, 5, 29, 7, 39, 17),
        ),
        Row(
            projectId="deCODE-proteomics-raw",
            studyId="deCODE-proteomics-raw_Proteomics_PC0_10003_15_ZNF41_ZNF41_05242112",
            hasSumstats=True,
            summarystatsLocation="s3a://some_bucket/some_folder/Proteomics_PC0_10003_15_ZNF41_ZNF41_05242112.txt.gz",
            size="926.9 MiB",
            accessionTimestamp=datetime.datetime(2024, 5, 29, 7, 39, 21),
        ),
    ]
    return session.spark.createDataFrame(data)


@pytest.fixture()
def decode_study_index_df(session: Session) -> DataFrame:
    """StudyIndex example mock."""
    data = [
        Row(
            studyId="deCODE-proteomics-raw_Proteomics_PC0_10006_7_RAF1_c_Raf_05314415",
            projectId="deCODE-proteomics-raw",
            studyType="pqtl",
            traitFromSource="Proteomics_PC0_10006_7_RAF1_c_Raf",
            geneId="ENSG00000132155",
            biosampleFromSourceId="UBERON_0001969",
            pubmedId="37794188",
            publicationTitle="Large-scale plasma proteomics comparisons through genetics and disease associations",
            publicationFirstAuthor="Eldjarn GH, Ferkingstad E",
            publicationDate="2024",
            publicationJournal="Nature",
            initialSampleSize="36,136 Icelandic individuals",
            nSamples=36136,
            cohorts=["deCODE"],
            ldPopulationStructure=[Row(ldPopulation="nfe", relativeSampleSize=1.0)],
            discoverySamples=[Row(sampleSize=36136, ancestry="Icelandic")],
            summarystatsLocation="s3a://some_bucket/some_folder/Proteomics_PC0_10001_7_RAF1_c_Raf_07082019.txt.gz",
            hasSumstats=True,
        ),
        Row(
            studyId="deCODE-proteomics-raw_Proteomics_PC0_10003_15_ZNF41_ZNF41_05242112",
            projectId="deCODE-proteomics-raw",
            studyType="pqtl",
            traitFromSource="Proteomics_PC0_10003_15_ZNF41_ZNF41",
            geneId="ENSG00000147124",
            biosampleFromSourceId="UBERON_0001969",
            pubmedId="37794188",
            publicationTitle="Large-scale plasma proteomics comparisons through genetics and disease associations",
            publicationFirstAuthor="Eldjarn GH, Ferkingstad E",
            publicationDate="2024",
            publicationJournal="Nature",
            initialSampleSize="36,136 Icelandic individuals",
            nSamples=36136,
            cohorts=["deCODE"],
            ldPopulationStructure=[Row(ldPopulation="nfe", relativeSampleSize=1.0)],
            discoverySamples=[Row(sampleSize=36136, ancestry="Icelandic")],
            summarystatsLocation="s3a://some_bucket/some_folder/Proteomics_PC0_10003_15_ZNF41_ZNF41_07082019.txt.gz",
            hasSumstats=True,
        ),
    ]
    return session.spark.createDataFrame(data)


@pytest.fixture
def decode_summary_statistics_df(session: Session) -> DataFrame:
    """SummaryStatistics example DataFrame fixture."""
    data = [
        Row(
            studyId="deCODE-proteomics-raw_Proteomics_PC0_10006_7_RAF1_c_Raf_05314415",
            variantId="1_100_A_G",
            chromosome="1",
            position=100,
            beta=0.5,
            standardError=0.1,
            pValueMantissa=1.5,
            pValueExponent=-8,
            effectAlleleFrequencyFromSource=0.3,
        ),
        Row(
            studyId="deCODE-proteomics-raw_Proteomics_PC0_10003_15_ZNF41_ZNF41_05242112",
            variantId="2_200_C_T",
            chromosome="2",
            position=200,
            beta=-0.3,
            standardError=0.15,
            pValueMantissa=2.5,
            pValueExponent=-7,
            effectAlleleFrequencyFromSource=0.45,
        ),
    ]
    return session.spark.createDataFrame(data)


class TestdeCODEIngestionStep:
    """Test deCODEManifestGenerationStep."""

    @patch("gentropy.decode_ingestion.S3Config")
    @patch("gentropy.decode_ingestion.deCODEManifest")
    def test_decode_manifest_generation_step(
        self,
        manifest_mock: MagicMock,
        s3_config_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        decode_manifest_df: DataFrame,
    ):
        """Test deCODEManifestGenerationStep."""
        s3_config_path = (tmp_path / "s3_config.json").as_posix()
        bucket_listing_path = (tmp_path / "bucket_listing.txt").as_posix()
        output_path = (tmp_path / "manifest_output").as_posix()

        # Mock the instance of the manifest
        manifest_instance = MagicMock()
        manifest_instance.df = decode_manifest_df

        # Link the instance to return_value from the constructor
        manifest_mock.from_bucket_listing.return_value = manifest_instance

        config_instance = MagicMock()
        s3_config_mock.from_file = MagicMock(return_value=config_instance)

        deCODEManifestGenerationStep(
            session=session,
            s3_config_path=s3_config_path,
            bucket_listing_path=bucket_listing_path,
            output_path=output_path,
        )

        s3_config_mock.from_file.assert_called_once_with(s3_config_path)
        manifest_mock.from_bucket_listing.assert_called_once_with(
            session=session, config=config_instance, path=bucket_listing_path
        )
        assert Path(output_path).exists()

    @patch("gentropy.decode_ingestion.deCODEManifest")
    @patch("gentropy.decode_ingestion.TargetIndex")
    @patch("gentropy.decode_ingestion.deCODEStudyIndex")
    def test_decode_study_index_generation_step(
        self,
        study_index_mock: MagicMock,
        target_index_mock: MagicMock,
        manifest_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        decode_study_index_df: DataFrame,
    ):
        """Test deCODEStudyIndexGenerationStep."""
        manifest_path = (tmp_path / "manifest.parquet").as_posix()
        target_index_path = (tmp_path / "target_index.parquet").as_posix()
        output_path = (tmp_path / "study_index_output").as_posix()

        # Mock the instances
        manifest_instance = MagicMock()
        target_index_instance = MagicMock()
        study_index_instance = MagicMock()
        study_index_instance.df = decode_study_index_df

        # Link the instances to return_value from the constructors
        manifest_mock.from_path.return_value = manifest_instance
        target_index_mock.from_parquet.return_value = target_index_instance
        study_index_mock.from_manifest.return_value = study_index_instance

        deCODEStudyIndexGenerationStep(
            session=session,
            manifest_path=manifest_path,
            target_index_path=target_index_path,
            output_path=output_path,
        )

        manifest_mock.from_path.assert_called_once_with(
            session=session, path=manifest_path
        )
        target_index_mock.from_parquet.assert_called_once_with(
            session=session, path=target_index_path
        )
        study_index_mock.from_manifest.assert_called_once_with(
            manifest=manifest_instance, target_index=target_index_instance
        )
        assert Path(output_path).exists()


class TestdeCODESummaryStatisticsIngestionStep:
    """Test deCODESummaryStatisticsIngestionStep."""

    @patch("gentropy.decode_ingestion.deCODESummaryStatistics")
    @patch("gentropy.decode_ingestion.StudyIndex")
    def test_decode_summary_statistics_ingestion_step(
        self,
        study_index_mock: MagicMock,
        summary_statistics_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        decode_study_index_df: DataFrame,
    ):
        """Test deCODESummaryStatisticsIngestionStep."""
        study_index_path = (tmp_path / "study_index.parquet").as_posix()
        raw_summary_statistics_path = (
            tmp_path / "raw_summary_statistics.parquet"
        ).as_posix()

        # Mock the study index instance
        study_index_instance = MagicMock()
        summary_statistics_paths = [
            "s3a://some_bucket/some_folder/Proteomics_PC0_10006_7_RAF1_c_Raf_05314415.txt.gz",
            "s3a://some_bucket/some_folder/Proteomics_PC0_10003_15_ZNF41_ZNF41_05242112.txt.gz",
        ]
        study_index_instance.get_summary_statistics_paths.return_value = (
            summary_statistics_paths
        )

        # Link the instance to return_value from the constructor
        study_index_mock.from_parquet.return_value = study_index_instance

        # Mock the txtgz_to_parquet method
        summary_statistics_mock.txtgz_to_parquet = MagicMock()
        summary_statistics_mock.N_THREAD_MAX = 4

        deCODESummaryStatisticsIngestionStep(
            session=session,
            study_index_path=study_index_path,
            raw_summary_statistics_path=raw_summary_statistics_path,
        )

        study_index_mock.from_parquet.assert_called_once_with(session, study_index_path)
        study_index_instance.get_summary_statistics_paths.assert_called_once()
        summary_statistics_mock.txtgz_to_parquet.assert_called_once_with(
            session=session,
            summary_statistics_list=summary_statistics_paths,
            raw_summary_statistics_output_path=raw_summary_statistics_path,
            n_threads=4,
        )


class TestdeCODESummaryStatisticsHarmonisationStep:
    """Test deCODESummaryStatisticsHarmonisationStep."""

    @patch("gentropy.decode_ingestion.deCODESummaryStatistics")
    @patch("gentropy.decode_ingestion.VariantDirection")
    def test_decode_summary_statistics_harmonisation_step(
        self,
        variant_direction_mock: MagicMock,
        summary_statistics_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        decode_summary_statistics_df: DataFrame,
    ):
        """Test deCODESummaryStatisticsHarmonisationStep."""
        raw_summary_statistics_path = (
            tmp_path / "raw_summary_statistics.parquet"
        ).as_posix()
        gnomad_variant_direction_path = (
            tmp_path / "gnomad_variant_direction.parquet"
        ).as_posix()
        harmonised_summary_statistics_path = (
            tmp_path / "harmonised_summary_statistics"
        ).as_posix()

        # Create mock raw summary statistics file
        decode_summary_statistics_df.write.mode("overwrite").parquet(
            raw_summary_statistics_path
        )

        # Mock the variant direction instance
        variant_direction_instance = MagicMock()
        variant_direction_mock.from_parquet.return_value = variant_direction_instance

        # Mock the harmonised summary statistics instance
        harmonised_instance = MagicMock()
        harmonised_instance.df = decode_summary_statistics_df
        summary_statistics_mock.from_source.return_value = harmonised_instance

        deCODESummaryStatisticsHarmonisationStep(
            session=session,
            raw_summary_statistics_path=raw_summary_statistics_path,
            gnomad_variant_direction_path=gnomad_variant_direction_path,
            harmonised_summary_statistics_path=harmonised_summary_statistics_path,
            min_mac_threshold=50,
            min_sample_size_threshold=30_000,
            flipping_window_size=10_000_000,
        )

        variant_direction_mock.from_parquet.assert_called_once_with(
            session, gnomad_variant_direction_path
        )
        summary_statistics_mock.from_source.assert_called_once()
        assert Path(harmonised_summary_statistics_path).exists()


class TestdeCODESummaryStatisticsQCStep:
    """Test deCODESummaryStatisticsQCStep."""

    @patch("gentropy.decode_ingestion.SummaryStatistics")
    @patch("gentropy.decode_ingestion.SummaryStatisticsQC")
    @patch("gentropy.decode_ingestion.StudyIndex")
    def test_decode_summary_statistics_qc_step(
        self,
        study_index_mock: MagicMock,
        summary_statistics_qc_mock: MagicMock,
        summary_statistics_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        decode_summary_statistics_df: DataFrame,
        decode_study_index_df: DataFrame,
    ):
        """Test deCODESummaryStatisticsQCStep."""
        harmonised_summary_statistics_path = (
            tmp_path / "harmonised_summary_statistics.parquet"
        ).as_posix()
        harmonised_summary_statistics_qc_output_path = (
            tmp_path / "harmonised_summary_statistics_qc"
        ).as_posix()
        qc_summary_statistics_path = (tmp_path / "qc_summary_statistics").as_posix()
        study_index_path = (tmp_path / "study_index.parquet").as_posix()
        study_index_with_qc_output_path = (tmp_path / "study_index_with_qc").as_posix()

        # Mock the harmonised summary statistics instance
        harmonised_ss_instance = MagicMock()
        harmonised_ss_instance.df = decode_summary_statistics_df
        summary_statistics_mock.from_parquet.return_value = harmonised_ss_instance

        # Mock the summary statistics QC instance
        qc_instance = MagicMock()
        qc_instance.df = decode_summary_statistics_df
        summary_statistics_qc_mock.from_summary_statistics.return_value = qc_instance

        # Mock the study index instances
        study_index_instance = MagicMock()
        study_index_instance.df = decode_study_index_df
        study_index_instance.annotate_sumstats_qc.return_value = study_index_instance
        study_index_instance.persist.return_value = study_index_instance
        study_index_mock.from_parquet.return_value = study_index_instance

        deCODESummaryStatisticsQCStep(
            session=session,
            harmonised_summary_statistics_path=harmonised_summary_statistics_path,
            harmonised_summary_statistics_qc_output_path=harmonised_summary_statistics_qc_output_path,
            qc_summary_statistics_path=qc_summary_statistics_path,
            study_index_path=study_index_path,
            study_index_with_qc_output_path=study_index_with_qc_output_path,
            qc_threshold=1e-6,
        )

        summary_statistics_mock.from_parquet.assert_called_once_with(
            session=session,
            path=harmonised_summary_statistics_path,
        )
        summary_statistics_qc_mock.from_summary_statistics.assert_called_once_with(
            gwas=harmonised_ss_instance,
            pval_threshold=1e-6,
        )
        study_index_mock.from_parquet.assert_called_once_with(
            session=session, path=study_index_path
        )
        study_index_instance.annotate_sumstats_qc.assert_called_once_with(qc_instance)
        assert Path(harmonised_summary_statistics_qc_output_path).exists()
        assert Path(qc_summary_statistics_path).exists()
        assert Path(study_index_with_qc_output_path).exists()
