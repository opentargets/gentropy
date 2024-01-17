"""Step to run FinnGen study index generation."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.finngen.study_index import FinnGenStudyIndex


class FinnGenStudiesStep:
    """FinnGen study index generation step."""

    def __init__(self, session: Session, finngen_study_index_out: str) -> None:
        """Run FinnGen study index generation step.

        Args:
            session (Session): Session object.
            finngen_study_index_out (str): Output FinnGen study index path.
        """
        # Fetch study index.
        FinnGenStudyIndex.from_source(session.spark).df.write.mode(
            session.write_mode
        ).parquet(finngen_study_index_out)
