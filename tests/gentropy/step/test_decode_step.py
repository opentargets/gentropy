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

    @pytest.mark.skip(reason="Not implemented yet")
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


# class TestdeCODESummaryStatisticsIngestionStep:
#     """Test deCODESummaryStatisticsIngestionStep."""

#     @pytest.mark.skip(reason="Not implemented yet")
#     def test_decode_summary_statistics_ingestion_step(self):
#         """Test deCODESummaryStatisticsIngestionStep."""
#         pass


# class TestdeCODESummaryStatisticsHarmonisationStep:
#     """Test deCODESummaryStatisticsHarmonizationStep."""

#     @pytest.mark.skip(reason="Not implemented yet")
#     def test_decode_summary_statistics_harmonization_step(self):
#         """Test deCODESummaryStatisticsHarmonizationStep."""
#         pass
