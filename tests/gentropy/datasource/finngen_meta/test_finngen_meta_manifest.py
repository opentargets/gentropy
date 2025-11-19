"""Test FinnGen Meta Manifest."""

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session
from gentropy.datasource.finngen_meta import FinnGenMetaManifest, MetaAnalysisDataSource


class TestFinnGenMetaManifest:
    """Test FinnGenMetaManifest class.

    Test cases are based on slices of the original manifests files from:
    - gs://finngen-public-data-r12/meta_analysis/mvp_ukbb/FinnGen_R12_MVP_UKBB_manifest.tsv
    - gs://finngen-public-data-r12/meta_analysis/ukbb/finngen_R12_meta_analysis_mapping_with_definitions.tsv
    """

    @pytest.mark.parametrize(
        ("manifest_path", "expected_meta"),
        [
            (
                "tests/gentropy/data_samples/finngen_ukbb_meta_manifest.tsv",
                MetaAnalysisDataSource.FINNGEN_UKBB,
            ),
            (
                "tests/gentropy/data_samples/finngen_ukbb_mvp_meta_manifest.tsv",
                MetaAnalysisDataSource.FINNGEN_UKBB_MVP,
            ),
        ],
    )
    def test_from_path(
        self,
        manifest_path: str,
        expected_meta: MetaAnalysisDataSource,
        session: Session,
    ) -> None:
        """Test from path method using slices of the original manifests."""
        manifest = FinnGenMetaManifest.from_path(session, manifest_path)
        assert isinstance(manifest, FinnGenMetaManifest)
        assert manifest.meta == expected_meta
        assert manifest.df.count() == 4

    @pytest.mark.parametrize(
        ("manifest_path", "expected_study_ids"),
        [
            (
                "tests/gentropy/data_samples/finngen_ukbb_meta_manifest.tsv",
                [
                    Row(studyId="FINNGEN_R12_UKB_META_AB1_AMOEBIASIS"),
                    Row(studyId="FINNGEN_R12_UKB_META_AB1_ASPERGILLOSIS"),
                    Row(studyId="FINNGEN_R12_UKB_META_AB1_CANDIDIASIS"),
                    Row(studyId="FINNGEN_R12_UKB_META_AB1_DERMATOPHYTOSIS"),
                ],
            ),
            (
                "tests/gentropy/data_samples/finngen_ukbb_mvp_meta_manifest.tsv",
                [
                    Row(studyId="FINNGEN_R12_UKB_MVP_META_AB1_BACT_INTEST_OTH"),
                    Row(studyId="FINNGEN_R12_UKB_MVP_META_AB1_CANDIDIASIS"),
                    Row(studyId="FINNGEN_R12_UKB_MVP_META_AB1_INTESTINAL_INFECTIONS"),
                    Row(studyId="FINNGEN_R12_UKB_MVP_META_AB1_OTHER_SUPERF_MYCOSIS"),
                ],
            ),
        ],
    )
    def test_study_ids(
        self,
        manifest_path: str,
        expected_study_ids: list[Row],
        session: Session,
    ) -> None:
        """Test study_ids property."""
        manifest = FinnGenMetaManifest.from_path(session, manifest_path)
        assert manifest.df.select("studyId").distinct().count() == 4
        expected_study_df = session.spark.createDataFrame(
            expected_study_ids,
            schema="studyId: STRING",
        )
        assert (
            manifest.df.select("studyId").distinct().collect()
            == expected_study_df.collect()
        ), "data does not match expected"

    @pytest.mark.parametrize(
        ("manifest_path", "expected_project_id"),
        [
            (
                "tests/gentropy/data_samples/finngen_ukbb_meta_manifest.tsv",
                "FINNGEN_R12_UKB_META",
            ),
            (
                "tests/gentropy/data_samples/finngen_ukbb_mvp_meta_manifest.tsv",
                "FINNGEN_R12_UKB_MVP_META",
            ),
        ],
    )
    def test_project_id(
        self,
        manifest_path: str,
        expected_project_id: str,
        session: Session,
    ) -> None:
        """Test project_id property."""
        manifest = FinnGenMetaManifest.from_path(session, manifest_path)
        assert manifest.df.select("projectId").distinct().count() == 1
        expected_study_df = session.spark.createDataFrame(
            [(sid,) for sid in [expected_project_id] * 4],
            schema="projectId: STRING",
        )
        assert (
            manifest.df.select("projectId").distinct().collect()
            == expected_study_df.distinct().collect()
        ), "data does not match expected"

    @pytest.mark.parametrize(
        ("manifest_path", "expected_trait_from_source"),
        [
            (
                "tests/gentropy/data_samples/finngen_ukbb_meta_manifest.tsv",
                [
                    Row(traitFromSource="Amoebiasis"),
                    Row(traitFromSource="Aspergillosis"),
                    Row(traitFromSource="Candidiasis"),
                    Row(traitFromSource="Dermatophytosis"),
                ],
            ),
            (
                "tests/gentropy/data_samples/finngen_ukbb_mvp_meta_manifest.tsv",
                [
                    Row(traitFromSource="Other bacterial intestinal infections"),
                    Row(traitFromSource="Candidiasis"),
                    Row(traitFromSource="Intestinal infectious diseases"),
                    Row(traitFromSource="Other superficial mycoses"),
                ],
            ),
        ],
    )
    def test_trait_from_source(
        self,
        manifest_path: str,
        expected_trait_from_source: list[Row],
        session: Session,
    ) -> None:
        """Test project_id property."""
        manifest = FinnGenMetaManifest.from_path(session, manifest_path)
        assert manifest.df.select("traitFromSource").distinct().count() == 4
        expected_study_df = session.spark.createDataFrame(
            expected_trait_from_source,
            schema="traitFromSource: STRING",
        )
        assert (
            manifest.df.select("traitFromSource").distinct().collect()
            == expected_study_df.collect()
        ), "data does not match expected"

    @pytest.mark.parametrize(
        ("manifest_path", "expected_discovery_samples"),
        [
            (
                "tests/gentropy/data_samples/finngen_ukbb_meta_manifest.tsv",
                [
                    Row(
                        discoverySamples=[
                            Row(sampleSize=444489, ancestry="fin"),
                            Row(sampleSize=389150, ancestry="nfe"),
                        ]
                    ),
                    Row(
                        discoverySamples=[
                            Row(sampleSize=489237, ancestry="fin"),
                            Row(sampleSize=380751, ancestry="nfe"),
                        ]
                    ),
                    Row(
                        discoverySamples=[
                            Row(sampleSize=494318, ancestry="fin"),
                            Row(sampleSize=390987, ancestry="nfe"),
                        ]
                    ),
                    Row(
                        discoverySamples=[
                            Row(sampleSize=494048, ancestry="fin"),
                            Row(sampleSize=408123, ancestry="nfe"),
                        ]
                    ),
                ],
            ),
            (
                "tests/gentropy/data_samples/finngen_ukbb_mvp_meta_manifest.tsv",
                [
                    Row(
                        discoverySamples=[
                            Row(sampleSize=451963, ancestry="Finnish"),
                            Row(sampleSize=452656, ancestry="European"),
                            Row(sampleSize=120127, ancestry="African"),
                            Row(sampleSize=50446, ancestry="Admixed American"),
                        ]
                    ),
                    Row(
                        discoverySamples=[
                            Row(sampleSize=494318, ancestry="Finnish"),
                            Row(sampleSize=831793, ancestry="European"),
                            Row(sampleSize=116003, ancestry="African"),
                            Row(sampleSize=49308, ancestry="Admixed American"),
                        ]
                    ),
                    Row(
                        discoverySamples=[
                            Row(sampleSize=500348, ancestry="Finnish"),
                            Row(sampleSize=441932, ancestry="European"),
                            Row(sampleSize=116116, ancestry="African"),
                            Row(sampleSize=48874, ancestry="Admixed American"),
                        ]
                    ),
                    Row(
                        discoverySamples=[
                            Row(sampleSize=489719, ancestry="Finnish"),
                            Row(sampleSize=452016, ancestry="European"),
                            Row(sampleSize=118458, ancestry="African"),
                            Row(sampleSize=50189, ancestry="Admixed American"),
                        ]
                    ),
                ],
            ),
        ],
    )
    def test_discovery_samples(
        self,
        manifest_path: str,
        expected_discovery_samples: list[Row],
        session: Session,
    ) -> None:
        """Test discoverySamples property."""
        manifest = FinnGenMetaManifest.from_path(session, manifest_path)
        assert manifest.df.select("discoverySamples").distinct().count() == 4
        expected_study_df = session.spark.createDataFrame(
            expected_discovery_samples,
            schema="discoverySamples: ARRAY<STRUCT<sampleSize: INT, ancestry: STRING>>",
        )

        assert (
            manifest.df.select("discoverySamples").distinct().collect()
            == expected_study_df.collect()
        ), "data does not match expected"

    @pytest.mark.parametrize(
        ("manifest_path", "expected_n_stats"),
        [
            (
                "tests/gentropy/data_samples/finngen_ukbb_meta_manifest.tsv",
                [
                    Row(nSamples=833639, nCases=289, nControls=833350),
                    Row(nSamples=869988, nCases=643, nControls=869345),
                    Row(nSamples=885305, nCases=15960, nControls=869345),
                    Row(nSamples=902171, nCases=32826, nControls=869345),
                ],
            ),
            (
                "tests/gentropy/data_samples/finngen_ukbb_mvp_meta_manifest.tsv",
                [
                    Row(nSamples=1075192, nCases=15125, nControls=1060067),
                    Row(nSamples=1491422, nCases=28822, nControls=1462600),
                    Row(nSamples=1107270, nCases=68854, nControls=1038416),
                    Row(nSamples=1110382, nCases=6019, nControls=1104363),
                ],
            ),
        ],
    )
    def test_n_stats(
        self,
        manifest_path: str,
        expected_n_stats: list[Row],
        session: Session,
    ) -> None:
        """Test nStats property."""
        manifest = FinnGenMetaManifest.from_path(session, manifest_path)
        assert (
            manifest.df.select("nSamples", "nCases", "nControls").distinct().count()
            == 4
        )
        expected_study_df = session.spark.createDataFrame(
            expected_n_stats,
            schema="nSamples: INT, nCases: INT, nControls: INT",
        )
        assert (
            manifest.df.select("nSamples", "nCases", "nControls").distinct().collect()
            == expected_study_df.collect()
        ), "data does not match expected"

    @pytest.mark.parametrize(
        ("manifest_path", "expected_n_cases_per_cohort"),
        [
            (
                "tests/gentropy/data_samples/finngen_ukbb_meta_manifest.tsv",
                [
                    Row(
                        nCasesPerCohort=[
                            Row(cohort="FinnGen", nCases=197),
                            Row(cohort="UKBB", nCases=92),
                        ]
                    ),
                    Row(
                        nCasesPerCohort=[
                            Row(cohort="FinnGen", nCases=260),
                            Row(cohort="UKBB", nCases=383),
                        ]
                    ),
                    Row(
                        nCasesPerCohort=[
                            Row(cohort="FinnGen", nCases=5341),
                            Row(cohort="UKBB", nCases=10619),
                        ]
                    ),
                    Row(
                        nCasesPerCohort=[
                            Row(cohort="FinnGen", nCases=5071),
                            Row(cohort="UKBB", nCases=27755),
                        ]
                    ),
                ],
            ),
            (
                "tests/gentropy/data_samples/finngen_ukbb_mvp_meta_manifest.tsv",
                [
                    Row(
                        nCasesPerCohort=[
                            Row(cohort="FinnGen", nCases=7671),
                            Row(cohort="UKBB", nCases=0),
                            Row(cohort="MVP_EUR", nCases=5606),
                            Row(cohort="MVP_AFR", nCases=1274),
                            Row(cohort="MVP_AMR", nCases=574),
                        ]
                    ),
                    Row(
                        nCasesPerCohort=[
                            Row(cohort="FinnGen", nCases=5341),
                            Row(cohort="UKBB", nCases=10619),
                            Row(cohort="MVP_EUR", nCases=8526),
                            Row(cohort="MVP_AFR", nCases=3459),
                            Row(cohort="MVP_AMR", nCases=877),
                        ]
                    ),
                    Row(
                        nCasesPerCohort=[
                            Row(cohort="FinnGen", nCases=56056),
                            Row(cohort="UKBB", nCases=0),
                            Row(cohort="MVP_EUR", nCases=9346),
                            Row(cohort="MVP_AFR", nCases=2444),
                            Row(cohort="MVP_AMR", nCases=1008),
                        ]
                    ),
                    Row(
                        nCasesPerCohort=[
                            Row(cohort="FinnGen", nCases=742),
                            Row(cohort="UKBB", nCases=0),
                            Row(cohort="MVP_EUR", nCases=2644),
                            Row(cohort="MVP_AFR", nCases=2089),
                            Row(cohort="MVP_AMR", nCases=544),
                        ]
                    ),
                ],
            ),
        ],
    )
    def test_n_cases_per_cohort(
        self,
        manifest_path: str,
        expected_n_cases_per_cohort: list[Row],
        session: Session,
    ) -> None:
        """Test nCasesPerCohort property."""
        manifest = FinnGenMetaManifest.from_path(session, manifest_path)
        assert manifest.df.select("nCasesPerCohort").distinct().count() == 4
        expected_study_df = session.spark.createDataFrame(
            expected_n_cases_per_cohort,
            schema="nCasesPerCohort: ARRAY<STRUCT<cohort: STRING, nCases: INT>>",
        )
        assert (
            manifest.df.select("nCasesPerCohort").distinct().collect()
            == expected_study_df.collect()
        ), "data does not match expected"

    @pytest.mark.parametrize(
        ("manifest_path", "expected_sumstats_stats"),
        [
            (
                "tests/gentropy/data_samples/finngen_ukbb_meta_manifest.tsv",
                [
                    Row(summarystatsLocation=None, hasSumstats=True),
                    Row(summarystatsLocation=None, hasSumstats=True),
                    Row(summarystatsLocation=None, hasSumstats=True),
                    Row(summarystatsLocation=None, hasSumstats=True),
                ],
            ),
            (
                "tests/gentropy/data_samples/finngen_ukbb_mvp_meta_manifest.tsv",
                [
                    Row(
                        summarystatsLocation="gs://finngen-public-data-r12/meta_analysis/mvp_ukbb/summary_stats/AB1_BACT_INTEST_OTH_meta_out.tsv.gz",
                        hasSumstats=True,
                    ),
                    Row(
                        summarystatsLocation="gs://finngen-public-data-r12/meta_analysis/mvp_ukbb/summary_stats/AB1_CANDIDIASIS_meta_out.tsv.gz",
                        hasSumstats=True,
                    ),
                    Row(
                        summarystatsLocation="gs://finngen-public-data-r12/meta_analysis/mvp_ukbb/summary_stats/AB1_INTESTINAL_INFECTIONS_meta_out.tsv.gz",
                        hasSumstats=True,
                    ),
                    Row(
                        summarystatsLocation="gs://finngen-public-data-r12/meta_analysis/mvp_ukbb/summary_stats/AB1_OTHER_SUPERF_MYCOSIS_meta_out.tsv.gz",
                        hasSumstats=True,
                    ),
                ],
            ),
        ],
    )
    def test_sumstat_stats(
        self,
        manifest_path: str,
        expected_sumstats_stats: list[Row],
        session: Session,
    ) -> None:
        """Test sumstat stats property."""
        manifest = FinnGenMetaManifest.from_path(session, manifest_path)
        assert manifest.df.select("summarystatsLocation", "hasSumstats").count() == 4
        expected_study_df = session.spark.createDataFrame(
            expected_sumstats_stats,
            schema="summarystatsLocation: STRING, hasSumstats: BOOLEAN",
            # Make sure summarystatsLocation is of type STRING even if None
            # as the manifest may contain null values
        ).withColumn(
            "summarystatsLocation", f.col("summarystatsLocation").cast(t.StringType())
        )

        assert (
            manifest.df.select("summarystatsLocation", "hasSumstats").collect()
            == expected_study_df.collect()
        ), "data does not match expected"
