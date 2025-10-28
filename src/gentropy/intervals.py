"""Step to generate interval annotation dataset."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.biosample_index import BiosampleIndex
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
        interval_source: str,
        interval_e2g_path: str,
    ) -> None:
        """Run intervals step.

        Args:
            session (Session): Session object.
            target_index_path (str): Input target index path.
            biosample_mapping_path (str): Input biosample mapping path.
            biosample_index_path (str): Input biosample index path.
            interval_source (str): Input intervals source path.
            interval_e2g_path (str): Output processed e2g intervals path.
        """
        target_index = TargetIndex.from_parquet(
            session,
            target_index_path,
        ).persist()
        biosample_mapping = session.spark.read.option("header", "true").csv(
            biosample_mapping_path
        )
        biosample_index = BiosampleIndex.from_parquet(session, biosample_index_path)
        data = IntervalsE2G.read(session.spark, interval_source)
        interval_e2g = IntervalsE2G.parse(
            data, biosample_mapping, target_index, biosample_index
        )

        interval_e2g.df.write.mode(session.write_mode).parquet(interval_e2g_path)


class IntervalEpiractionStep:
    """Interval epiraction step.

    This step generates a dataset that contains interval evidence supporting the functional associations of variants with genes.

    """

    def __init__(
        self,
        session: Session,
        target_index_path: str,
        interval_source: str,
        interval_epiraction_path: str,
    ) -> None:
        """Run intervals step.

        Args:
            session (Session): Session object.
            target_index_path (str): Input target index path.
            interval_source (str): Input intervals source path.
            interval_epiraction_path (str): Output processed interval epiraction path.
        """
        target_index = TargetIndex.from_parquet(
            session,
            target_index_path,
        ).persist()
        data = IntervalsEpiraction.read(session.spark, interval_source)
        interval_epiraction = IntervalsEpiraction.parse(data, target_index)

        interval_epiraction.df.write.mode(session.write_mode).parquet(
            interval_epiraction_path
        )
