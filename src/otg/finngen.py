"""Step to run FinnGen study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.request import urlopen

from pyspark.sql import functions as f

from otg.common.session import Session
from otg.config import FinnGenStepConfig


@dataclass
class FinnGenStep(FinnGenStepConfig):
    """FinnGen study table ingestion step.

    The following information is aggregated/extracted:
    - Study ID in the special format (FINNGEN_R7_*)
    - Trait name (for example, Amoebiasis)
    - Number of cases and controls
    - Link to the summary statistics location

    Some fields are also populated as constants, such as study type and the initial sample size.
    """

    session: Session = Session()

    def run(self: FinnGenStep) -> None:
        """Run FinnGen study table ingestion step."""
        # Read the JSON data from the URL.
        json_data = urlopen(self.finngen_phenotype_table_url).read().decode("utf-8")
        rdd = self.session.spark.sparkContext.parallelize([json_data])
        df = self.session.spark.read.json(rdd)

        # Select the desired columns.
        df = df.select("phenocode", "phenostring", "num_cases", "num_controls")

        # Rename the columns.
        df = df.withColumnRenamed("phenocode", "studyId")
        df = df.withColumnRenamed("phenostring", "traitFromSource")
        df = df.withColumnRenamed("num_cases", "nCases")
        df = df.withColumnRenamed("num_controls", "nControls")

        # Transform the column values.
        df = df.withColumn(
            "studyId", f.concat(f.lit(self.finngen_release_prefix), df["studyId"])
        )
        df = df.withColumn("nSamples", df["nCases"] + df["nControls"])
        df = df.withColumn(
            "summarystatsLocation",
            f.concat(
                f.lit(self.finngen_sumstat_url_prefix),
                df["studyId"],
                f.lit(self.finngen_sumstat_url_suffix),
            ),
        )

        # Set constant value columns.
        # Then f.when(f.lit(True)) trick makes sure that the column is created as nullable, to ensure that it is not flagged as incorrect by validate_df_schema. See: https://stackoverflow.com/a/68578278.
        df = df.withColumn(
            "initialSampleSize",
            f.when(f.lit(True), f.lit("342,499 (190,879 females and 151,620 males)")),
        )
        df = df.withColumn("hasSumstats", f.when(f.lit(True), f.lit(True)))

        # Write the output.
        df.write.mode(self.session.write_mode).parquet(self.finngen_study_index_out)
