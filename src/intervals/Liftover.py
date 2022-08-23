from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING

import gcsfs
import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyliftover import LiftOver

if TYPE_CHECKING:
    from pyspark.sql import dataframe


class LiftOverSpark:
    """
    LiftOver class for mapping genomic coordinates to an other genome build.

    The input is a Spark DataFrame with a chromosome and position column. This classs can
    also map regions, if a start and end positions are provided.

    **Logic**:

    - The mapping is dropped if the mapped chromosome is not on the same as the source.
    - The mapping is dropped if the mapping is ambiguous (more than one mapping is available).
    - If regions are provided, the mapping is dropped if the new region is reversed (mapped_start > mapped_end).
    - If regions are provided, the mapping is dropped if the difference of the lenght of the mapped region and original is larger than a threshold.
    - When lifting over intervals, only unique coordinates are lifted, they joined back to the original dataframe.
    """

    def __init__(
        self: LiftOverSpark, chain_file: str, max_difference: int = None
    ) -> None:
        """
        :param chain_file: Path to the chain file. Local or google bucket. Chainfile is not gzipped!
        :param max_difference: Maximum difference between the length of the mapped region and the original region.
        """

        self.chain_file = chain_file

        # Initializing liftover object by opening the chain file:
        if chain_file.startswith("gs://"):
            with gcsfs.GCSFileSystem().open(chain_file) as chain_file_object:
                self.lo = LiftOver(chain_file_object)
        else:
            self.lo = LiftOver(chain_file)

        # If no maximum difference is provided, set it to 100:
        self.max_difference = 100 if max_difference is None else max_difference

        # UDF to do map genomic coordinates to liftover coordinates:
        self.liftover_udf = f.udf(
            lambda chrom, pos: self.lo.convert_coordinate(chrom, pos),
            t.ArrayType(t.ArrayType(t.StringType())),
        )

    def convert_intervals(
        self: LiftOverSpark,
        df: dataframe,
        chrom_col: str,
        start_col: str,
        end_col: str,
        filter: bool = True,
    ) -> dataframe:
        """
        Convert genomic intervals to liftover coordinates

        :param df: spark Dataframe with chromosome, start and end columns.
        :param chrom_col: Name of the chromosome column.
        :param start_col: Name of the start column.
        :param end_col: Name of the end column.
        :param filter: If True, filter is applied on the mapped data, otherwise return everything. Default: True.

        :return: filtered Spark Dataframe with the mapped start and end coordinates.
        """

        # Lift over start coordinates, changing to 1-based coordinates:
        start_df = (
            df.withColumn(start_col, f.col(start_col) + 1)
            .select(chrom_col, start_col)
            .distinct()
        )
        start_df = self.convert_coordinates(
            start_df, chrom_col, start_col
        ).withColumnRenamed("mapped_pos", "mapped_" + start_col)

        # Lift over end coordinates:
        end_df = df.select(chrom_col, end_col).distinct()
        end_df = self.convert_coordinates(end_df, chrom_col, end_col).withColumnRenamed(
            "mapped_pos", "mapped_" + end_col
        )

        # Join dataframe with mappings (we have to account for the +1 position shift of the start coordinates):
        mapped_df = df.join(
            start_df.withColumn(start_col, f.col(start_col) - 1),
            on=[chrom_col, start_col],
            how="left",
        ).join(end_df, on=[chrom_col, end_col], how="left")

        # The filter option allows to get all the data and filter it afterwards.
        if filter:
            return (
                mapped_df
                # Select only rows where the start is smaller than the end:
                .filter(
                    # Drop rows with no mappings:
                    f.col(f"mapped_{start_col}").isNotNull()
                    & f.col(f"mapped_{end_col}").isNotNull()
                    # Drop rows where the start is larger than the end:
                    & (f.col(f"mapped_{end_col}") >= f.col(f"mapped_{start_col}"))
                    # Drop rows where the difference of the length of the regions are larger than the threshold:
                    & (
                        f.abs(
                            (f.col(end_col) - f.col(start_col))
                            - (
                                f.col(f"mapped_{end_col}")
                                - f.col(f"mapped_{start_col}")
                            )
                        )
                        <= self.max_difference
                    )
                ).persist()
            )
        else:
            return mapped_df.persist()

    def convert_coordinates(
        self: LiftOverSpark, df: dataframe, chrom_name: str, pos_name: str
    ) -> list:
        """
        Converts genomic coordinates to coordinates on an other build

        :param df: Spark Dataframe with chromosome and position columns.
        :param chrom_name: Name of the chromosome column.
        :param pos_name: Name of the position column.

        :return: Spark Dataframe with the mapped position column.
        """

        mapped = (
            df.withColumn(
                "mapped", self.liftover_udf(f.col(chrom_name), f.col(pos_name))
            )
            .filter((f.col("mapped").isNotNull()) & (f.size(f.col("mapped")) == 1))
            # Extracting mapped corrdinates:
            .withColumn("mapped_" + chrom_name, f.col("mapped")[0][0])
            .withColumn("mapped_" + pos_name, f.col("mapped")[0][1])
            # Drop rows that mapped to the other chromosomes:
            .filter(
                f.col("mapped_" + chrom_name)
                == f.concat(f.lit("chr"), f.col(chrom_name))
            )
            # Dropping unused columns:
            .drop("mapped", "mapped_" + chrom_name)
            .persist()
        )

        return mapped


def generate_genomic_coordinate_data(
    chrom: str = "chrom", start: str = "start", end: str = "end"
) -> dataframe:
    """
    Generate small data for liftover column.
    """

    # These entries already contains the mapped coordinates.
    coordinates = """
        chr20:48400001-48500000_chr20:49783463-49883463
        chrX:1000001-1000100_chrX:1039265-1039365
        chr1:10000001-10001000_chr1:9939942-9940942
        chr2:5000001-5001000_chr2:4952410-4953410
    """
    return (
        spark.createDataFrame(
            [{"coordinates": coordinate} for coordinate in coordinates.split()]
        )
        .withColumn("old_coordinates", f.split(f.col("coordinates"), "_")[0])
        .withColumn("new_coordinates", f.split(f.col("coordinates"), "_")[1])
        .withColumn("old_coordinates_split", f.split(f.col("old_coordinates"), ":|-"))
        .withColumn(
            chrom, f.regexp_replace(f.col("old_coordinates_split")[0], "chr", "")
        )
        .withColumn(start, f.col("old_coordinates_split")[1].cast(t.IntegerType()))
        .withColumn(end, f.col("old_coordinates_split")[2].cast(t.IntegerType()))
        .select(chrom, start, end, "new_coordinates")
        .persist()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Wrapper for the the LiftOver module.")
    parser.add_argument(
        "--chain_file", type=str, help="Path to the chain file (.chain)"
    )
    parser.add_argument(
        "--max_distance",
        type=int,
        help="Maximum distance between the length of the mapped region and the original region.",
    )
    parser.add_argument(
        "--data_file",
        type=str,
        help="Path to a dataset (parquet) to lift over.",
        required=False,
    )
    parser.add_argument(
        "--coordinate_columns",
        type=str,
        help="Comma separated list of columns to lift over eg. chr,start,end",
        required=False,
    )
    parser.add_argument(
        "--output_file", type=str, help="Output parquet file.", required=False
    )
    args = parser.parse_args()

    # Initialize spark session:
    spark = pyspark.sql.SparkSession.builder.master("local[*]").getOrCreate()

    # Initialize logging:
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Parse column names:
    (chrom, start, end) = (
        args.coordinate_columns.split(",")
        if args.coordinate_columns
        else ("chr", "start", "end")
    )

    # Just print out some of the arguments:
    logging.info(f"Data file (parquet): {args.data_file}")
    logging.info(f"Chain file: {args.chain_file}")
    logging.info(f"Max distance: {args.max_distance}")
    logging.info(f"Chromosome column name: {chrom}")
    logging.info(f"Start column name: {start}")
    logging.info(f"End column name: {end}")

    # If data file is not given generate data:
    if args.data_file is None:
        data = generate_genomic_coordinate_data(chrom, start, end)
    else:
        data = spark.read.parquet(args.data_file).persist()
    logging.info(f"Dataframe has {data.count()} rows.")
    logging.info(f"Data: \n{data.show(2, truncate=False, vertical=True)}")

    # Initialize LiftOver object:
    lift = LiftOverSpark(args.chain_file, args.max_distance)

    # Lift over the data:
    new_data = lift.convert_intervals(data, chrom, start, end, filter=False)

    if args.output_file is not None:
        logging.info(f"Saving data: {args.output_file}")
        new_data.write.mode("overwrite").parquet(args.output_file)

    logging.info(f"New data: \n{new_data.show(2, truncate=False, vertical=True)}")
