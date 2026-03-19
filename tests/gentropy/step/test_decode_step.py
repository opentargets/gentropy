"""Test deCODE ingestion steps."""

import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame, Row

from gentropy import Session
from gentropy.decode_ingestion import (
    deCODEManifestGenerationStep,
    deCODESummaryStatisticsHarmonisationStep,
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

    @patch("gentropy.decode_ingestion.deCODEManifest")
    def test_decode_manifest_generation_step(
        self,
        manifest_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        decode_manifest_df: DataFrame,
    ) -> None:
        """Test deCODEManifestGenerationStep."""
        s3_config_path = (tmp_path / "s3_config.json").as_posix()
        bucket_listing_path = (tmp_path / "bucket_listing.txt").as_posix()
        output_path = (tmp_path / "manifest_output").as_posix()

        # Mock the manifest instance returned by from_bucket_listing
        manifest_instance = MagicMock()
        manifest_instance.df = decode_manifest_df
        manifest_mock.from_bucket_listing.return_value = manifest_instance

        deCODEManifestGenerationStep(
            session=session,
            s3_config_path=s3_config_path,
            bucket_listing_path=bucket_listing_path,
            output_path=output_path,
        )

        manifest_mock.from_bucket_listing.assert_called_once_with(
            session=session,
            s3_config_path=s3_config_path,
            path=bucket_listing_path,
        )
        assert Path(output_path).exists()


class TestdeCODESummaryStatisticsHarmonisationStep:
    """Test deCODESummaryStatisticsHarmonisationStep."""

    @patch("gentropy.decode_ingestion.SummaryStatisticsQC")
    @patch("gentropy.decode_ingestion.deCODEStudyIndex")
    @patch("gentropy.decode_ingestion.AptamerMetadata")
    @patch("gentropy.decode_ingestion.MolecularComplex")
    @patch("gentropy.decode_ingestion.deCODESummaryStatistics")
    @patch("gentropy.decode_ingestion.VariantDirection")
    @patch("gentropy.decode_ingestion.deCODEManifest")
    def test_decode_summary_statistics_harmonisation_step(
        self,
        manifest_mock: MagicMock,
        variant_direction_mock: MagicMock,
        summary_statistics_mock: MagicMock,
        molecular_complex_mock: MagicMock,
        aptamer_metadata_mock: MagicMock,
        study_index_mock: MagicMock,
        qc_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        decode_summary_statistics_df: DataFrame,
    ) -> None:
        """Test deCODESummaryStatisticsHarmonisationStep."""
        raw_summary_statistics_path = (
            tmp_path / "raw_summary_statistics.parquet"
        ).as_posix()
        manifest_path = (tmp_path / "manifest.parquet").as_posix()
        aptamer_metadata_path = (tmp_path / "aptamer_metadata.tsv").as_posix()
        variant_direction_path = (tmp_path / "variant_direction.parquet").as_posix()
        molecular_complex_path = (tmp_path / "molecular_complex.parquet").as_posix()
        harmonised_summary_statistics_path = (
            tmp_path / "harmonised_summary_statistics"
        ).as_posix()
        protein_qtl_study_index_path = (tmp_path / "protein_qtl_study_index").as_posix()
        qc_summary_statistics_path = (tmp_path / "qc_summary_statistics").as_posix()

        # Write raw summary statistics parquet (step reads it via spark.read.parquet)
        decode_summary_statistics_df.write.mode("overwrite").parquet(
            raw_summary_statistics_path
        )

        # Mock deCODEManifest
        manifest_instance = MagicMock()
        manifest_instance.persist.return_value = manifest_instance
        manifest_mock.from_parquet.return_value = manifest_instance

        # Mock MolecularComplex
        mc_instance = MagicMock()
        mc_instance.persist.return_value = mc_instance
        molecular_complex_mock.from_parquet.return_value = mc_instance

        # Mock AptamerMetadata
        am_instance = MagicMock()
        am_instance.persist.return_value = am_instance
        aptamer_metadata_mock.from_source.return_value = am_instance

        # Mock deCODEStudyIndex
        si_instance = MagicMock()
        si_instance.persist.return_value = si_instance
        study_index_mock.from_manifest.return_value = si_instance

        # Mock VariantDirection
        vd_instance = MagicMock()
        vd_instance.persist.return_value = vd_instance
        variant_direction_mock.from_parquet.return_value = vd_instance

        # Mock deCODESummaryStatistics.from_source — returns (hss, pqtl_si) tuple
        harmonised_instance = MagicMock()
        harmonised_instance.df = decode_summary_statistics_df
        harmonised_instance.persist.return_value = harmonised_instance

        pqtl_si_instance = MagicMock()
        pqtl_si_instance.persist.return_value = pqtl_si_instance
        pqtl_si_instance.annotate_sumstats_qc.return_value.df = (
            decode_summary_statistics_df
        )
        summary_statistics_mock.from_source.return_value = (
            harmonised_instance,
            pqtl_si_instance,
        )

        # Mock SummaryStatisticsQC
        qc_instance = MagicMock()
        qc_instance.df = decode_summary_statistics_df
        qc_instance.persist.return_value = qc_instance
        qc_mock.from_summary_statistics.return_value = qc_instance

        deCODESummaryStatisticsHarmonisationStep(
            session=session,
            raw_summary_statistics_path=raw_summary_statistics_path,
            manifest_path=manifest_path,
            aptamer_metadata_path=aptamer_metadata_path,
            variant_direction_path=variant_direction_path,
            molecular_complex_path=molecular_complex_path,
            harmonised_summary_statistics_path=harmonised_summary_statistics_path,
            protein_qtl_study_index_path=protein_qtl_study_index_path,
            qc_summary_statistics_path=qc_summary_statistics_path,
            min_mac_threshold=50,
            min_sample_size_threshold=30_000,
            flipping_window_size=10_000_000,
        )

        variant_direction_mock.from_parquet.assert_called_once_with(
            session, variant_direction_path
        )
        summary_statistics_mock.from_source.assert_called_once()
        assert Path(harmonised_summary_statistics_path).exists()
        assert Path(protein_qtl_study_index_path).exists()
        assert Path(qc_summary_statistics_path).exists()
