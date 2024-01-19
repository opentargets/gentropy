"""Step to dump a filtered version of a LD matrix (block matrix) as Parquet files."""
from __future__ import annotations

import hail as hl

from gentropy.common.session import Session
from gentropy.datasource.gnomad.ld import GnomADLDMatrix


class LDIndexStep:
    """LD index step.

    !!! warning "This step is resource intensive"

        Suggested params: high memory machine, 5TB of boot disk, no SSDs.
    """

    def __init__(self, session: Session, min_r2: float, ld_index_out: str) -> None:
        """Run step.

        Args:
            session (Session): Session object.
            min_r2 (float): Minimum r2 to consider when considering variants within a window.
            ld_index_out (str): Output LD index path.
        """
        hl.init(sc=session.spark.sparkContext, log="/dev/null")
        (
            GnomADLDMatrix()
            .as_ld_index(min_r2)
            .df.write.partitionBy("chromosome")
            .mode(session.write_mode)
            .parquet(ld_index_out)
        )
        session.logger.info(ld_index_out)
