"""Step to generate UKBiobank summary statistics dataset."""
from __future__ import annotations

import subprocess
from dataclasses import dataclass

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.session import Session
from otg.config import UKBiobankSumstatsMergeConfig


@dataclass
class UKBiobankSumstatsMergeStep(UKBiobankSumstatsMergeConfig):
    """Step to merge Neale Lab and SAIGE summsary statistics (n=3,419) from UKBiobank."""

    session: Session = Session()

    def get_gsutil_file_list(
        self: UKBiobankSumstatsMergeStep, directory: str
    ) -> list[str]:
        """Get a list of file names from the specified directory in GCP using gsutil."""
        cmd = f"gsutil ls {directory}/*.tsv.gz"
        output = subprocess.check_output(cmd, shell=True).decode("utf-8").strip()
        file_list = output.split("\n")
        return file_list

    def run(self: UKBiobankSumstatsMergeStep) -> None:
        """Run Step."""
        # Extract
        self.session.logger.info(self.ukbiobank_sumstats_merged_out)
        self.session.logger.info(str(self.ukbiobank_file_batch_size))
        self.session.logger.info(str(self.ukbiobank_sumstats_dirs))

        # Define a consistent schema for the merged data between Neale and SAIGE
        merged_schema = """
            studyId string,
            chromosome string,
            base_pair_location integer,
            other_allele string,
            effect_allele string,
            effect_allele_frequency double,
            `p-value` double,
            beta double,
            odds_ratio double,
            standard_error double
        """

        # Create an empty DataFrame with the merged schema
        merged_df = self.session.spark.createDataFrame([], merged_schema)

        # Process files in batches from each directory
        for ukbiobank_sumstats_dir in self.ukbiobank_sumstats_dirs:
            # Get the list of files to process per directory
            file_list = self.get_gsutil_file_list(directory=ukbiobank_sumstats_dir)
            self.session.logger.info(str(file_list))
            total_files = len(file_list)

            # Process files in batches
            for i in range(0, total_files, self.ukbiobank_file_batch_size):
                start_index = i
                end_index = min(i + self.ukbiobank_file_batch_size, total_files)
                batch_files = file_list[start_index:end_index]

                # Read the batch of files into a DataFrame
                df = (
                    self.session.spark.read.option("header", "true")
                    .option("delimiter", "\t")
                    .csv(batch_files)
                )

                # Create studyId column from the file name
                if "neale" in ukbiobank_sumstats_dir:
                    df = df.withColumn(
                        "studyId",
                        f.expr(
                            "concat('NEALE2_', split(regexp_extract(input_file_name(), r'([^/]+)\\.gwas', 1), '_')[0], '_RAW')"
                        ),
                    )
                elif "saige" in ukbiobank_sumstats_dir:
                    df = df.withColumn(
                        "studyId",
                        f.expr(
                            "concat('SAIGE_', replace(regexp_extract(input_file_name(), r'(?<=PheCode_)(\\d+\\.\\d+)_SAIGE', 1), '.', '_'))"
                        ),
                    )

                # Convert data types and select columns
                df = df.select(
                    "studyId",
                    f.col("chromosome").cast(t.StringType()),
                    f.col("base_pair_location").cast(t.IntegerType()),
                    f.col("other_allele").cast(t.StringType()),
                    f.col("effect_allele").cast(t.StringType()),
                    f.col("effect_allele_frequency").cast(t.DoubleType()),
                    f.col("`p-value`").cast(t.DoubleType()),
                    f.col("beta").cast(t.DoubleType()),
                    f.col("odds_ratio").cast(t.DoubleType()),
                    f.col("standard_error").cast(t.DoubleType()),
                )

                # Union the batch DataFrame with the merged DataFrame
                merged_df = merged_df.union(df)

        # Write the merged DataFrame as Parquet files
        merged_df.write.mode(self.session.write_mode).parquet(
            self.ukbiobank_sumstats_merged_out
        )
        self.session.logger.info("Merging UKBiobank studies successfully completed.")
