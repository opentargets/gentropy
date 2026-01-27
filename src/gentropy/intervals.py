"""Step to generate interval annotation dataset."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.contig_index import ContigIndex
from gentropy.dataset.target_index import TargetIndex
from gentropy.datasource.intervals.e2g import IntervalsE2G
from gentropy.datasource.intervals.epiraction import IntervalsEpiraction


class IntervalE2GStep:
    """Interval E2G step.

    This step generates a dataset that contains interval evidence supporting the functional associations of variants with genes.
    """

    def __init__(
        self,
        session: Session,
        target_index_path: str,
        biosample_mapping_path: str,
        biosample_index_path: str,
        chromosome_contig_index_path: str,
        interval_source: str,
        valid_output_path: str,
        invalid_output_path: str,
        min_valid_score: float = 0.6,
        max_valid_score: float = 1.0,
        invalid_qc_reasons: list[str] | None = None,
    ) -> None:
        """Run intervals step.

        Args:
            session (Session): Session object.
            target_index_path (str): Input target index path.
            biosample_mapping_path (str): Input biosample mapping path.
            biosample_index_path (str): Input biosample index path.
            chromosome_contig_index_path (str): Input chromosome contig index path.
            interval_source (str): Input intervals source path.
            valid_output_path (str): Output valid intervals path.
            invalid_output_path (str): Output invalid intervals path.
            min_valid_score (float): Minimum valid score for interval QC.
            max_valid_score (float): Maximum valid score for interval QC.
            invalid_qc_reasons (list[str] | None): List of invalid quality check reason names from `IntervalQualityCheck` (e.g. ['INVALID_CHROMOSOME']).
        """
        invalid_qc_reasons = invalid_qc_reasons or []

        biosample_mapping = session.spark.read.csv(biosample_mapping_path, header=True)
        target_index = TargetIndex.from_parquet(session, target_index_path).persist()
        biosample_index = BiosampleIndex.from_parquet(session, biosample_index_path)
        contig_index = ContigIndex.from_parquet(session, chromosome_contig_index_path)
        data = IntervalsE2G.read(session.spark, interval_source)
        interval_e2g = IntervalsE2G.parse(data, biosample_mapping, target_index)
        valid, invalid = interval_e2g.qc(
            contig_index=contig_index,
            target_index=target_index,
            biosample_index=biosample_index,
            min_valid_score=min_valid_score,
            max_valid_score=max_valid_score,
            invalid_qc_reasons=invalid_qc_reasons,
        )
        valid.df.write.mode(session.write_mode).parquet(valid_output_path)
        invalid.df.write.mode(session.write_mode).parquet(invalid_output_path)


class IntervalEpiractionStep:
    """Interval epiraction step.

    This step generates a dataset that contains interval evidence supporting the functional associations of variants with genes.

    """

    def __init__(
        self,
        session: Session,
        target_index_path: str,
        biosample_index_path: str,
        chromosome_contig_index_path: str,
        interval_source: str,
        valid_output_path: str,
        invalid_output_path: str,
        min_valid_score: float = 0.6,
        max_valid_score: float = 1.0,
        invalid_qc_reasons: list[str] | None = None,
    ) -> None:
        """Run intervals step.

        Args:
            session (Session): Session object.
            target_index_path (str): Input target index path.
            biosample_index_path (str): Input biosample index path.
            chromosome_contig_index_path (str): Input chromosome contig index path.
            interval_source (str): Input intervals source path.
            valid_output_path (str): Output valid intervals path.
            invalid_output_path (str): Output invalid intervals path.
            min_valid_score (float): Minimum valid score for interval QC.
            max_valid_score (float): Maximum valid score for interval QC.
            invalid_qc_reasons (list[str] | None): List of invalid quality check reason names from `IntervalQualityCheck` (e.g. ['INVALID_CHROMOSOME']).
        """
        invalid_qc_reasons = invalid_qc_reasons or []
        target_index = TargetIndex.from_parquet(session, target_index_path).persist()
        data = IntervalsEpiraction.read(session.spark, interval_source)
        interval_epiraction = IntervalsEpiraction.parse(data, target_index)
        biosample_index = BiosampleIndex.from_parquet(session, biosample_index_path)
        contig_index = ContigIndex.from_parquet(session, chromosome_contig_index_path)
        valid, invalid = interval_epiraction.qc(
            contig_index=contig_index,
            target_index=target_index,
            biosample_index=biosample_index,
            min_valid_score=min_valid_score,
            max_valid_score=max_valid_score,
            invalid_qc_reasons=invalid_qc_reasons,
        )
        valid.df.write.mode(session.write_mode).parquet(valid_output_path)
        invalid.df.write.mode(session.write_mode).parquet(invalid_output_path)
