"""Step to generate interval annotation dataset."""

from __future__ import annotations

from functools import reduce

from gentropy.common.Liftover import LiftOverSpark
from gentropy.common.session import Session
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex


class IntervalStep:
    """Interval index step.

    This step generates a dataset that contains interval evidence supporting the functional associations of variants with genes.

     1. Chromatin interaction experiments, e.g. Promoter Capture Hi-C (PCHi-C).
     2. Enhancer to gene promoter regional correlations.
     3. DNase hypersensitive site (DHS) to gene promoter associations.
    """

    def __init__(
        self,
        session: Session,
        variant_index_path: str,
        target_index_path: str,
        liftover_chain_file_path: str,
        interval_sources: dict[str, str],
        interval_index_path: str,
        liftover_max_length_difference: int = 100,
    ) -> None:
        """Run intervals step.

        Args:
            session (Session): Session object.
            variant_index_path (str): Input variant index path.
            target_index_path (str): Input target index path.
            liftover_chain_file_path (str): Path to GRCh37 to GRCh38 chain file.
            interval_sources (dict[str, str]): Dictionary of raw interval sources.
            interval_index_path (str): Output intervals path.
            liftover_max_length_difference (int): Maximum length difference for liftover.
        """
        target_index = TargetIndex.from_parquet(
            session,
            target_index_path,
        ).persist()

        lift = LiftOverSpark(
            # lift over variants to hg38
            liftover_chain_file_path,
            liftover_max_length_difference,
        )

        intervals = Intervals(
            _df=reduce(
                lambda x, y: x.unionByName(y, allowMissingColumns=True),
                # create interval instances by parsing each source
                [
                    Intervals.from_source(
                        session.spark, source_name, source_path, target_index, lift
                    ).df
                    for source_name, source_path in interval_sources.items()
                ],
            ),
            _schema=Intervals.get_schema(),
        )

        vi = VariantIndex.from_parquet(session, variant_index_path).persist()
        interval_index = intervals.overlap_variant_index(vi)

        interval_index.df.write.mode(session.write_mode).parquet(interval_index_path)
