"""Tests for AnnotateSumstatQCStep."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.annotate_sumstat_qc_step import AnnotateSumstatQCStep
from gentropy.dataset.study_index import StudyIndex, StudyQualityCheck
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from gentropy.common.session import Session


def _make_study_index(spark: SparkSession, study_ids: list[str]) -> StudyIndex:
    """Create a minimal StudyIndex containing exactly the given study IDs."""
    schema = StudyIndex.get_schema()
    df = spark.createDataFrame(
        [(sid, "PROJ", "gwas") for sid in study_ids],
        "studyId STRING, projectId STRING, studyType STRING",
    )
    for field in schema:
        if field.name not in df.columns:
            df = df.withColumn(field.name, f.lit(None).cast(field.dataType))
    # Re-order columns to match schema
    df = df.select([field.name for field in schema])
    return StudyIndex(_df=df, _schema=schema)


def _make_sumstats_qc(spark: SparkSession) -> SummaryStatisticsQC:
    """QC metrics: S1 passes all thresholds, S2 fails mean_beta."""
    data = [
        # studyId, mean_beta, mean_diff_pz, se_diff_pz, gc_lambda, n_variants, n_variants_sig
        ("S1", 0.01, 0.01, 0.01, 1.0, 3_000_000, 500),  # all pass
        ("S2", 0.9, 0.01, 0.01, 1.0, 3_000_000, 500),  # mean_beta fails
    ]
    df = spark.createDataFrame(data, SummaryStatisticsQC.get_schema())
    return SummaryStatisticsQC(_df=df)


class TestAnnotateSumstatQCStep:
    """Tests for AnnotateSumstatQCStep."""

    def test_output_is_valid_study_index(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
    ) -> None:
        """Step writes a Parquet file that loads back as a valid StudyIndex."""
        study_path = str(tmp_path / "study_index")
        qc_path = str(tmp_path / "sumstats_qc")
        out_path = str(tmp_path / "output")

        _make_study_index(spark, ["S1", "S2", "S3"]).df.write.parquet(study_path)
        _make_sumstats_qc(spark).df.write.parquet(qc_path)

        AnnotateSumstatQCStep(
            session=session,
            study_index_path=study_path,
            sumstats_qc_path=qc_path,
            output_path=out_path,
        )

        result = StudyIndex.from_parquet(session, out_path)
        assert isinstance(result, StudyIndex)
        assert result.df.count() == 3

    def test_study_without_qc_flagged_sumstats_not_available(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
    ) -> None:
        """Study with no matching QC row is flagged SUMSTATS_NOT_AVAILABLE."""
        study_path = str(tmp_path / "study_index")
        qc_path = str(tmp_path / "sumstats_qc")
        out_path = str(tmp_path / "output")

        _make_study_index(spark, ["S1", "S2", "S3"]).df.write.parquet(study_path)
        _make_sumstats_qc(spark).df.write.parquet(qc_path)

        AnnotateSumstatQCStep(
            session=session,
            study_index_path=study_path,
            sumstats_qc_path=qc_path,
            output_path=out_path,
        )

        result = StudyIndex.from_parquet(session, out_path)
        s3_flags = (
            result.df.filter(f.col("studyId") == "S3")
            .select("qualityControls")
            .collect()[0]["qualityControls"]
        )
        assert s3_flags is not None
        assert StudyQualityCheck.SUMSTATS_NOT_AVAILABLE.value in s3_flags

    def test_failing_study_flagged_mean_beta(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
    ) -> None:
        """Study with high mean_beta is flagged FAILED_MEAN_BETA_CHECK."""
        study_path = str(tmp_path / "study_index")
        qc_path = str(tmp_path / "sumstats_qc")
        out_path = str(tmp_path / "output")

        _make_study_index(spark, ["S1", "S2", "S3"]).df.write.parquet(study_path)
        _make_sumstats_qc(spark).df.write.parquet(qc_path)

        AnnotateSumstatQCStep(
            session=session,
            study_index_path=study_path,
            sumstats_qc_path=qc_path,
            output_path=out_path,
        )

        result = StudyIndex.from_parquet(session, out_path)
        s2_flags = (
            result.df.filter(f.col("studyId") == "S2")
            .select("qualityControls")
            .collect()[0]["qualityControls"]
        )
        assert s2_flags is not None
        assert StudyQualityCheck.FAILED_MEAN_BETA_CHECK.value in s2_flags

    def test_passing_study_has_no_qc_flags(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
    ) -> None:
        """Study with all QC metrics within thresholds carries no qualityControls flags."""
        study_path = str(tmp_path / "study_index")
        qc_path = str(tmp_path / "sumstats_qc")
        out_path = str(tmp_path / "output")

        _make_study_index(spark, ["S1", "S2", "S3"]).df.write.parquet(study_path)
        _make_sumstats_qc(spark).df.write.parquet(qc_path)

        AnnotateSumstatQCStep(
            session=session,
            study_index_path=study_path,
            sumstats_qc_path=qc_path,
            output_path=out_path,
        )

        result = StudyIndex.from_parquet(session, out_path)
        s1_flags = (
            result.df.filter(f.col("studyId") == "S1")
            .select("qualityControls")
            .collect()[0]["qualityControls"]
        )
        assert not s1_flags  # empty list or None — no flags
