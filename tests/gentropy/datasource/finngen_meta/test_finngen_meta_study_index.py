"""Tests for finngen meta study index generation."""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row
from pyspark.sql import types as t

from gentropy import Session
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC
from gentropy.datasource.finngen.efo_mapping import EFOMapping
from gentropy.datasource.finngen_meta import FinnGenMetaManifest, MetaAnalysisDataSource
from gentropy.datasource.finngen_meta.study_index import FinnGenMetaStudyIndex


class TestFinnGenMetaStudyIndex:
    """Test FinnGenMetaStudyIndex class."""

    @pytest.fixture(autouse=True)
    def _setup(self, session: Session) -> None:
        """Setup fixture."""
        data = [
            Row(
                studyId="A",
                projectId="P",
                traitFromSource="Asthma",
                discoverySamples=[
                    Row(sampleSize=10, ancestry="Finnish"),
                    Row(sampleSize=11, ancestry="European"),
                    Row(sampleSize=12, ancestry="African"),
                    Row(sampleSize=13, ancestry="Admixed American"),
                ],
                nSamples=10 + 11 + 12 + 13,
                nCases=23,
                nCasesPerCohort=[
                    Row(cohort="FinnGen", nCases=5),
                    Row(cohort="UKBB", nCases=3),
                    Row(cohort="MVP-EUR", nCases=3),
                    Row(cohort="MVP-AFR", nCases=5),
                    Row(cohort="MVP-AMR", nCases=5),
                ],
                nControls=23,
                summarystatsLocation="gs://bucket/path/to/sumstats",
                hasSumstats=True,
            )
        ]
        schema = t.StructType(
            [
                t.StructField("studyId", t.StringType(), True),
                t.StructField("projectId", t.StringType(), True),
                t.StructField("traitFromSource", t.StringType(), True),
                t.StructField(
                    "discoverySamples",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField("sampleSize", t.IntegerType(), True),
                                t.StructField("ancestry", t.StringType(), True),
                            ]
                        )
                    ),
                    True,
                ),
                t.StructField("nSamples", t.IntegerType(), True),
                t.StructField("nCases", t.IntegerType(), True),
                t.StructField(
                    "nCasesPerCohort",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField("cohort", t.StringType(), True),
                                t.StructField("nCases", t.IntegerType(), True),
                            ]
                        )
                    ),
                    True,
                ),
                t.StructField("nControls", t.IntegerType(), True),
                t.StructField("summarystatsLocation", t.StringType(), True),
                t.StructField("hasSumstats", t.BooleanType(), True),
            ]
        )
        df = session.spark.createDataFrame(data, schema)
        manifest_mock = MagicMock(spec=FinnGenMetaManifest)
        manifest_mock.df = df
        manifest_mock.meta = MetaAnalysisDataSource.FINNGEN_UKBB_MVP
        self.manifest_mock = manifest_mock
        self.session = session

        efo_data = [
            Row(STUDY="FinnGen R12", SEMANTIC_TAG="EFO_A", PROPERTY_VALUE="Asthma")
        ]
        efo_schema = t.StructType(
            [
                t.StructField("STUDY", t.StringType(), True),
                t.StructField("SEMANTIC_TAG", t.StringType(), True),
                t.StructField("PROPERTY_VALUE", t.StringType(), True),
            ]
        )
        efo_df = session.spark.createDataFrame(efo_data, efo_schema)
        efo_mapping_mock = EFOMapping(df=efo_df)
        self.efo_mapping_mock = efo_mapping_mock

    def test_build_study_index(self) -> None:
        """Test building study index from manifest and EFO mapping."""
        study_index = FinnGenMetaStudyIndex.from_finngen_manifest(
            self.manifest_mock, self.efo_mapping_mock
        )
        assert study_index.df.count() == 1
        row = study_index.df.collect()[0]
        assert row["studyId"] == "A"
        assert row["projectId"] == "P"
        assert row["traitFromSource"] == "Asthma"
        assert row["nSamples"] == 46
        assert row["nCases"] == 23
        assert row["nControls"] == 23
        assert row["summarystatsLocation"] == "gs://bucket/path/to/sumstats"
        assert row["hasSumstats"] is True
        assert {c["ldPopulation"] for c in row["ldPopulationStructure"]} == {
            "fin",
            "nfe",
            "afr",
            "amr",
        }
        assert set(row["traitFromSourceMappedIds"]) == {"EFO_A"}
        assert (
            row["initialSampleSize"]
            == "1,550,147 (MVP: nEUR=449,042, nAFR=121,177, nAMR=59,048; FinnGenR12: nNFE=500,349; pan-UKBB-EUR: nEUR=420,531)"
        )
        assert row["publicationDate"] == "2024-11-01"
        assert set(row["cohorts"]) == {"MVP", "FinnGen", "pan-UKBB-EUR"}
        assert row["qualityControls"] == []

    @pytest.mark.parametrize(
        ("n_variants", "expected_quality_controls"),
        [
            pytest.param(11_597_970, [], id="passes the QC"),
            pytest.param(
                1_000_000,
                ["The number of SNPs in the study is below the expected threshold"],
                id="too few variants",
            ),
        ],
    )
    def test_annotate_sumstats_qc(
        self,
        session: Session,
        n_variants: int,
        expected_quality_controls: list[str],
    ) -> None:
        """Test that the built study index can be annotated with the summary statistics QC."""
        study_index = FinnGenMetaStudyIndex.from_finngen_manifest(
            self.manifest_mock, self.efo_mapping_mock
        )
        qc = SummaryStatisticsQC(
            _df=session.spark.createDataFrame(
                [("A", 0.0, 0.0, 0.0, 1.0, n_variants, 10)],
                SummaryStatisticsQC.get_schema(),
            )
        )
        row = study_index.annotate_sumstats_qc(qc).df.collect()[0]
        assert {v["QCCheckName"] for v in row["sumstatQCValues"]} == {
            "mean_beta",
            "mean_diff_pz",
            "se_diff_pz",
            "gc_lambda",
            "n_variants",
            "n_variants_sig",
        }
        assert row["qualityControls"] == expected_quality_controls
