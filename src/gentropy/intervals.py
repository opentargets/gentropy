"""Step to generate interval annotation dataset."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.target_index import TargetIndex
from gentropy.datasource.intervals.epiraction import IntervalsEpiraction


class IntervalIndexStep:
    """Interval index step.

    This step generates a dataset that contains interval evidence supporting the functional associations of variants with genes.

    """

    def __init__(
        self,
        session: Session,
        target_index_path: str,
        interval_source: str,
        interval_index_path: str,
    ) -> None:
        """Run intervals step.

        Args:
            session (Session): Session object.
            target_index_path (str): Input target index path.
            interval_source (str): Input intervals source path.
            interval_index_path (str): Output interval index path.
        """
        target_index = TargetIndex.from_parquet(
            session,
            target_index_path,
        ).persist()
        data = IntervalsEpiraction.read(session.spark, interval_source)
        interval_index = IntervalsEpiraction.parse(data, target_index)

        interval_index.df.write.mode(session.write_mode).parquet(interval_index_path)
