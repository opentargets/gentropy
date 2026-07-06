"""Test Interval dataset."""

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from gentropy import Session
from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.contig_index import ContigIndex
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.target_index import TargetIndex


@pytest.fixture
def interval_dataframe(session: Session) -> DataFrame:
    """Get the interval dataframe for testing."""
    data = [
        ("1", 100, 200, "ENSG1", "E2G", "promoter", "1", "biosample1", 0.1, "1"),
        ("1", 150, 250, "ENSG2", "E2G", "enhancer", "2", "biosample2", 0.2, "1"),
        ("2", 300, 400, "ENSG3", "E2G", "intragenic", "3", "biosample1", 0.3, "1"),
        ("2", 300, 400, "ENSG3", "E2G", "promoter", "4", "biosample2", 0.4, "1"),
        ("2", 400, 500, "ENSG4", "epiraction", "other", "5", "biosample1", 0.5, "1"),
        ("3", 450, 550, "ENSG5", "other", "6", "interval6", "biosample2", 0.6, "1"),
    ]
    schema = "chromosome STRING, start LONG, end LONG, geneId STRING, datasourceId STRING, intervalType STRING, intervalId STRING, biosampleId STRING, score DOUBLE, studyId STRING"
    return session.spark.createDataFrame(data, schema=schema)


@pytest.fixture
def contig_index(session: Session) -> ContigIndex:
    """Get a mock contig index."""
    data = [("1", 0, 200), ("2", 0, 300)]
    schema = "id STRING, start LONG, end LONG"
    df = session.spark.createDataFrame(data, schema=schema)
    return ContigIndex(df)


@pytest.fixture
def target_index(session: Session) -> TargetIndex:
    """Get a mock target index."""
    data = [
        ("ENSG1", "Gene1", ("t1", "1", 0, 1000, "+")),
        ("ENSG2", "Gene2", ("t2", "1", 0, 1000, "+")),
        ("ENSG3", "Gene3", ("t3", "2", 0, 1000, "+")),
        ("ENSG4", "Gene4", ("t4", "2", 0, 1000, "+")),
        ("ENSG5", "Gene5", ("t5", "3", 0, 1000, "+")),
    ]
    schema = "id STRING, approvedSymbol STRING, canonicalTranscript STRUCT<id: STRING, chromosome: STRING, start: LONG, end: LONG, strand: STRING>"
    df = session.spark.createDataFrame(data, schema=schema)
    return TargetIndex(df)


@pytest.fixture
def biosample_index(session: Session) -> BiosampleIndex:
    """Get a mock biosample index."""
    data = [("biosample1", "CellType1"), ("biosample2", "CellType2")]
    schema = "biosampleId STRING, biosampleName STRING"
    df = session.spark.createDataFrame(data, schema=schema)
    return BiosampleIndex(df)


class TestIntervalDataset:
    """Test Interval dataset functionalities."""

    def test_qc(
        self,
        interval_dataframe: DataFrame,
        contig_index: ContigIndex,
        target_index: TargetIndex,
        biosample_index: BiosampleIndex,
    ) -> None:
        """Test QC method of Interval dataset."""
        intervals = Intervals(interval_dataframe)

        valid, invalid = intervals.qc(
            contig_index=contig_index,
            target_index=target_index,
            biosample_index=biosample_index,
            min_valid_score=0.5,
            max_valid_score=1.0,
            invalid_qc_reasons=["INVALID_CHROMOSOME"],
        )

        # Validate the results
        assert isinstance(valid, Intervals)
        assert isinstance(invalid, Intervals)

        # Assert the total count remains the same
        assert valid.df.count() + invalid.df.count() == interval_dataframe.count()

        # Assert there are no INVALID_CHROMOSOME in valid intervals
        assert valid.df.count() == 5, "Expected 5 valid intervals"
        assert invalid.df.count() == 1, "Expected 1 interval with INVALID_CHROMOSOME"

    def test_validate_target_flags_trans_chromosomal_effect(
        self,
        session: Session,
    ) -> None:
        """Validate that chromosome mismatches between interval and gene are flagged."""
        target_data = [
            ("ENSG1", ("t1", "1", 0, 1000, "+")),
            ("ENSG2", ("t2", "2", 0, 1000, "+")),
        ]
        target_schema = "id STRING, canonicalTranscript STRUCT<id: STRING, chromosome: STRING, start: LONG, end: LONG, strand: STRING>"
        target_index = TargetIndex(
            session.spark.createDataFrame(target_data, schema=target_schema)
        )

        interval_data = [
            ("1", 100, 200, "ENSG1", "E2G", "promoter", "i1"),  # chr match
            ("3", 150, 250, "ENSG1", "E2G", "enhancer", "i2"),  # chr mismatch
            ("2", 300, 400, "ENSG2", "E2G", "intragenic", "i3"),  # chr match
        ]
        interval_schema = "chromosome STRING, start LONG, end LONG, geneId STRING, datasourceId STRING, intervalType STRING, intervalId STRING"
        intervals = Intervals(
            session.spark.createDataFrame(interval_data, schema=interval_schema)
        )

        result = intervals.validate_target(target_index)
        flagged = result.df.filter(
            f.array_contains(
                f.col("qualityControls"),
                "Interval chromosome does not match associated gene chromosome",
            )
        )
        assert flagged.count() == 1, (
            "Expected exactly 1 interval flagged as TRANS_CHROMOSOMAL_EFFECT"
        )
        assert flagged.first()["intervalId"] == "i2"
