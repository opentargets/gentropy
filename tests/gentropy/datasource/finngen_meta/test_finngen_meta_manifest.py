"""Test FinnGen Meta Manifest."""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row

from gentropy import Session
from gentropy.dataset.study_index import MetaAnalysisStudyIndex
from gentropy.datasource.finngen.efo_mapping import EFOMapping
from gentropy.datasource.finngen_meta import FinnGenMetaRelease, MetaAnalysisType
from gentropy.datasource.finngen_meta.study_index import FinnGenMetaManifest

_RELEASE = FinnGenMetaRelease(release="R12")


class TestFinnGenMetaManifest:
    """Test FinnGenMetaManifest class.

    Test cases are based on slices of the original manifests files from:
    - gs://finngen-public-data-r12/meta_analysis/mvp_ukbb/FinnGen_R12_MVP_UKBB_manifest.tsv
    - gs://finngen-public-data-r12/meta_analysis/ukbb/finngen_R12_meta_analysis_mapping_with_definitions.tsv
    """

    @pytest.fixture
    def mock_efo_mapping(self) -> EFOMapping:
        """Pass-through EFO mapping mock; returns the study index unchanged."""
        mock = MagicMock(spec=EFOMapping)
        mock.annotate_study_index.side_effect = lambda study_index, **kwargs: (
            study_index
        )
        return mock

    @pytest.fixture(
        params=[
            (
                "tests/gentropy/data_samples/finngen_ukbb_meta_manifest.tsv",
                MetaAnalysisType.TWO_WAY,
            ),
            (
                "tests/gentropy/data_samples/finngen_ukbb_mvp_meta_manifest.tsv",
                MetaAnalysisType.THREE_WAY,
            ),
        ],
        ids=["TWO_WAY", "THREE_WAY"],
    )
    def manifest(
        self,
        request: pytest.FixtureRequest,
        session: Session,
        mock_efo_mapping: EFOMapping,
    ) -> tuple[MetaAnalysisStudyIndex, MetaAnalysisType]:
        """Load FinnGenMetaManifest once per variant (TWO_WAY and THREE_WAY)."""
        manifest_path, meta_analysis_type = request.param
        return (
            FinnGenMetaManifest.from_source(
                session,
                manifest_path,
                meta_analysis_type=meta_analysis_type,
                release=_RELEASE,
                efo_mapping=mock_efo_mapping,
            ),
            meta_analysis_type,
        )

    def test_from_source(
        self, manifest: tuple[MetaAnalysisStudyIndex, MetaAnalysisType]
    ) -> None:
        """Test from_source method using slices of the original manifests."""
        loaded, _ = manifest
        assert isinstance(loaded, MetaAnalysisStudyIndex)
        assert loaded.df.count() == 4

    def test_study_ids(
        self,
        manifest: tuple[MetaAnalysisStudyIndex, MetaAnalysisType],
        session: Session,
    ) -> None:
        """Test that study IDs are computed correctly for both manifest variants."""
        loaded, meta_analysis_type = manifest
        expected_study_ids = {
            MetaAnalysisType.TWO_WAY: [
                Row(studyId="FINNGEN_R12_UKB_META_AB1_AMOEBIASIS"),
                Row(studyId="FINNGEN_R12_UKB_META_AB1_ASPERGILLOSIS"),
                Row(studyId="FINNGEN_R12_UKB_META_AB1_CANDIDIASIS"),
                Row(studyId="FINNGEN_R12_UKB_META_AB1_DERMATOPHYTOSIS"),
            ],
            MetaAnalysisType.THREE_WAY: [
                Row(studyId="FINNGEN_R12_UKB_MVP_META_AB1_BACT_INTEST_OTH"),
                Row(studyId="FINNGEN_R12_UKB_MVP_META_AB1_CANDIDIASIS"),
                Row(studyId="FINNGEN_R12_UKB_MVP_META_AB1_INTESTINAL_INFECTIONS"),
                Row(studyId="FINNGEN_R12_UKB_MVP_META_AB1_OTHER_SUPERF_MYCOSIS"),
            ],
        }
        expected = expected_study_ids[meta_analysis_type]
        expected_df = session.spark.createDataFrame(expected, schema="studyId: STRING")
        assert loaded.df.select("studyId").distinct().count() == 4
        assert (
            loaded.df.select("studyId").distinct().collect() == expected_df.collect()
        ), "data does not match expected"

    def test_project_id(
        self,
        manifest: tuple[MetaAnalysisStudyIndex, MetaAnalysisType],
        session: Session,
    ) -> None:
        """Test that projectId is set correctly for both manifest variants."""
        loaded, meta_analysis_type = manifest
        expected_project_id = {
            MetaAnalysisType.TWO_WAY: "FINNGEN_R12_UKB_META",
            MetaAnalysisType.THREE_WAY: "FINNGEN_R12_UKB_MVP_META",
        }[meta_analysis_type]
        expected_df = session.spark.createDataFrame(
            [(expected_project_id,)] * 4, schema="projectId: STRING"
        )
        assert loaded.df.select("projectId").distinct().count() == 1
        assert (
            loaded.df.select("projectId").distinct().collect()
            == expected_df.distinct().collect()
        ), "data does not match expected"

    def test_trait_from_source(
        self,
        manifest: tuple[MetaAnalysisStudyIndex, MetaAnalysisType],
        session: Session,
    ) -> None:
        """Test traitFromSource is mapped from the manifest name column."""
        loaded, meta_analysis_type = manifest
        expected_traits = {
            MetaAnalysisType.TWO_WAY: [
                Row(traitFromSource="Amoebiasis"),
                Row(traitFromSource="Aspergillosis"),
                Row(traitFromSource="Candidiasis"),
                Row(traitFromSource="Dermatophytosis"),
            ],
            MetaAnalysisType.THREE_WAY: [
                Row(traitFromSource="Other bacterial intestinal infections"),
                Row(traitFromSource="Candidiasis"),
                Row(traitFromSource="Intestinal infectious diseases"),
                Row(traitFromSource="Other superficial mycoses"),
            ],
        }
        expected = expected_traits[meta_analysis_type]
        expected_df = session.spark.createDataFrame(
            expected, schema="traitFromSource: STRING"
        )
        assert loaded.df.select("traitFromSource").distinct().count() == 4
        assert (
            loaded.df.select("traitFromSource").distinct().collect()
            == expected_df.collect()
        ), "data does not match expected"

    def test_discovery_samples(
        self,
        manifest: tuple[MetaAnalysisStudyIndex, MetaAnalysisType],
        session: Session,
    ) -> None:
        """Test discoverySamples are assembled from per-cohort manifest columns."""
        loaded, meta_analysis_type = manifest
        expected_discovery_samples = {
            MetaAnalysisType.TWO_WAY: [
                Row(
                    discoverySamples=[
                        Row(sampleSize=444489, ancestry="Finnish"),
                        Row(sampleSize=389150, ancestry="European"),
                    ]
                ),
                Row(
                    discoverySamples=[
                        Row(sampleSize=489237, ancestry="Finnish"),
                        Row(sampleSize=380751, ancestry="European"),
                    ]
                ),
                Row(
                    discoverySamples=[
                        Row(sampleSize=494318, ancestry="Finnish"),
                        Row(sampleSize=390987, ancestry="European"),
                    ]
                ),
                Row(
                    discoverySamples=[
                        Row(sampleSize=494048, ancestry="Finnish"),
                        Row(sampleSize=408123, ancestry="European"),
                    ]
                ),
            ],
            MetaAnalysisType.THREE_WAY: [
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
        }
        expected = expected_discovery_samples[meta_analysis_type]
        expected_df = session.spark.createDataFrame(
            expected,
            schema="discoverySamples: ARRAY<STRUCT<sampleSize: INT, ancestry: STRING>>",
        )
        assert loaded.df.select("discoverySamples").distinct().count() == 4
        assert (
            loaded.df.select("discoverySamples").distinct().collect()
            == expected_df.collect()
        ), "data does not match expected"

    def test_n_stats(
        self,
        manifest: tuple[MetaAnalysisStudyIndex, MetaAnalysisType],
        session: Session,
    ) -> None:
        """Test nSamples, nCases and nControls are computed from manifest columns."""
        loaded, meta_analysis_type = manifest
        expected_n_stats = {
            MetaAnalysisType.TWO_WAY: [
                Row(nSamples=833639, nCases=289, nControls=833350),
                Row(nSamples=869988, nCases=643, nControls=869345),
                Row(nSamples=885305, nCases=15960, nControls=869345),
                Row(nSamples=902171, nCases=32826, nControls=869345),
            ],
            MetaAnalysisType.THREE_WAY: [
                Row(nSamples=1075192, nCases=15125, nControls=1060067),
                Row(nSamples=1491422, nCases=28822, nControls=1462600),
                Row(nSamples=1107270, nCases=68854, nControls=1038416),
                Row(nSamples=1110382, nCases=6019, nControls=1104363),
            ],
        }
        expected = expected_n_stats[meta_analysis_type]
        expected_df = session.spark.createDataFrame(
            expected, schema="nSamples: INT, nCases: INT, nControls: INT"
        )
        assert (
            loaded.df.select("nSamples", "nCases", "nControls").distinct().count() == 4
        )
        assert (
            loaded.df.select("nSamples", "nCases", "nControls").distinct().collect()
            == expected_df.collect()
        ), "data does not match expected"

    def test_n_cases_per_cohort(
        self,
        manifest: tuple[MetaAnalysisStudyIndex, MetaAnalysisType],
        session: Session,
    ) -> None:
        """Test nCasesPerCohort contains per-cohort case counts."""
        loaded, meta_analysis_type = manifest
        expected_n_cases_per_cohort = {
            MetaAnalysisType.TWO_WAY: [
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
            MetaAnalysisType.THREE_WAY: [
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
        }
        expected = expected_n_cases_per_cohort[meta_analysis_type]
        expected_df = session.spark.createDataFrame(
            expected,
            schema="nCasesPerCohort: ARRAY<STRUCT<cohort: STRING, nCases: INT>>",
        )
        assert loaded.df.select("nCasesPerCohort").distinct().count() == 4
        assert (
            loaded.df.select("nCasesPerCohort").distinct().collect()
            == expected_df.collect()
        ), "data does not match expected"

    def test_sumstat_stats(
        self, manifest: tuple[MetaAnalysisStudyIndex, MetaAnalysisType]
    ) -> None:
        """Test that hasSumstats and summarystatsLocation are initialised to False/None at manifest stage.

        Both columns are only populated during the QC annotation step after summary statistics
        have been processed; the manifest itself cannot determine whether sumstats are valid.
        """
        loaded, _ = manifest
        rows = loaded.df.select("summarystatsLocation", "hasSumstats").collect()
        assert len(rows) == 4
        for row in rows:
            assert row["hasSumstats"] is False, (
                "hasSumstats should be False at manifest stage"
            )
            assert row["summarystatsLocation"] is None, (
                "summarystatsLocation should be None at manifest stage"
            )
