"""EPIMAP signal intensities.

These are input files for tissue enrichment in CHEERS.
Signal values are average across samples of the same cell type.

"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession
    from etl.v2g.intervals.Liftover import LiftOverSpark


class ParseEPIMAP:
    """Parser EPIMAP dataset.

    :param EPIMAP_parquet: path to the parquet file containing the EPIMAP signal data.
    :param lift: LiftOverSpark object

    **Summary of the logic:**

    - Lifting over coordinates to GRCh38
    """

    # Constants:
    DATASET_NAME = "EPIMAP"
    DATA_TYPE = "interval"
    EXPERIMENT_TYPE = "H3K27ac"
    PMID = "11111111"
    BIO_FEATURE = "many"

    def __init__(
        self: ParseEPIMAP,
        etl: ETLSession,
        epimap_datafile: str,
        lift: LiftOverSpark,
    ) -> None:
        """Initialise EPIMAP parser.

        Args:
            etl (ETLSession): current ETL session
            epimap_datafile (str): filepath to EPIMAP dataset
            lift (LiftOverSpark): LiftOverSpark object
        """
        self.etl = etl

        etl.logger.info("Parsing EPIMAP data...")
        etl.logger.info(f"Reading data from {epimap_datafile}")

        # Process EPIMAP data in a single step:
        self.EPIMAP_intervals = (
            etl.spark
            # Read table then do some modifications:
            .read.parquet(epimap_datafile)
            .withColumn("chr", f.regexp_replace(f.col("chr"), "chr", ""))
            # Lift over to the GRCh38 build:
            .transform(lambda df: lift.convert_intervals(df, "chr", "start", "end"))
            .distinct()
            .persist()
        )

    def get_intervals(self: ParseEPIMAP) -> DataFrame:
        """Get EPIMAP intervals."""
        return self.EPIMAP_intervals
