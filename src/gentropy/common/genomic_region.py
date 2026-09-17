"""Genomic Region handling and LiftOver functionality."""

from __future__ import annotations

import tempfile
from enum import Enum
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from google.cloud import storage
from pyliftover import LiftOver

if TYPE_CHECKING:
    from hail.table import Table
    from pyspark.sql import DataFrame



class KnownGenomicRegions(Enum):
    """Known genomic regions in the human genome in string format."""

    MHC = "chr6:25726063-33400556"


class GenomicRegion:
    """Genomic regions of interest."""

    def __init__(self, chromosome: str, start: int, end: int) -> None:
        """Class constructor.

        Args:
            chromosome (str): Chromosome.
            start (int): Start position.
            end (int): End position.
        """
        self.chromosome = chromosome
        self.start = start
        self.end = end

    def __str__(self) -> str:
        """String representation of the genomic region.

        Returns:
            str: Genomic region in chr:start-end format.
        """
        return f"{self.chromosome}:{self.start}-{self.end}"

    @classmethod
    def from_string(cls: type[GenomicRegion], region: str) -> GenomicRegion:
        """Parse region string to chr:start-end.

        Args:
            region (str): Genomic region expected to follow chr##:#,###-#,### format or ##:####-#####.

        Returns:
            GenomicRegion: Genomic region object.

        Raises:
            ValueError: If the end and start positions cannot be casted to integer or not all three values value error is raised.

        Examples:
            >>> print(GenomicRegion.from_string('chr6:28,510,120-33,480,577'))
            6:28510120-33480577
            >>> print(GenomicRegion.from_string('6:28510120-33480577'))
            6:28510120-33480577
            >>> print(GenomicRegion.from_string('6:28510120'))
            Traceback (most recent call last):
                ...
            ValueError: Genomic region should follow a ##:####-#### format.
            >>> print(GenomicRegion.from_string('6:28510120-foo'))
            Traceback (most recent call last):
                ...
            ValueError: Start and the end position of the region has to be integer.
        """
        region = region.replace(":", "-").replace(",", "")
        try:
            chromosome, start_position, end_position = region.split("-")
        except ValueError as err:
            raise ValueError(
                "Genomic region should follow a ##:####-#### format."
            ) from err

        try:
            return cls(
                chromosome=chromosome.replace("chr", ""),
                start=int(start_position),
                end=int(end_position),
            )
        except ValueError as err:
            raise ValueError(
                "Start and the end position of the region has to be integer."
            ) from err

    @classmethod
    def from_known_genomic_region(
        cls: type[GenomicRegion], region: KnownGenomicRegions
    ) -> GenomicRegion:
        """Get known genomic region.

        Args:
            region (KnownGenomicRegions): Known genomic region.

        Returns:
            GenomicRegion: Genomic region object.

        Examples:
            >>> print(GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC))
            6:25726063-33400556
        """
        return GenomicRegion.from_string(region.value)


class LiftOverSpark:
    """LiftOver class for mapping genomic coordinates to an other genome build.

    The input is a Spark DataFrame with a chromosome and position column. This classs can
    also map regions, if a start and end positions are provided.

    **Logic**:

    - The mapping is dropped if the mapped chromosome is not on the same as the source.
    - The mapping is dropped if the mapping is ambiguous (more than one mapping is available).
    - If regions are provided, the mapping is dropped if the new region is reversed (mapped_start > mapped_end).
    - If regions are provided, the mapping is dropped if the difference of the length of the mapped region and original is larger than a threshold.
    - When lifting over intervals, only unique coordinates are lifted, they joined back to the original dataframe.
    """

    def __init__(
        self: LiftOverSpark, chain_file: str, max_difference: int = 100
    ) -> None:
        """Initialize the LiftOver object.

        Args:
            chain_file (str): Path to the chain file
            max_difference (int, optional): The maximum tolerated difference in the resulting length. Defaults to 100.
        """
        self.chain_file = chain_file
        self.max_difference = max_difference

        # Initializing liftover object by opening the chain file - LiftOver only supports local files:
        if chain_file.startswith("gs://"):
            bucket_name = chain_file.split("/")[2]
            blob_name = "/".join(chain_file.split("/")[3:])
            with tempfile.NamedTemporaryFile(delete=True) as temp_file:
                storage.Client().bucket(bucket_name).blob(
                    blob_name
                ).download_to_filename(temp_file.name)
                self.lo = LiftOver(temp_file.name)
        else:
            self.lo = LiftOver(chain_file)

        # UDF to do map genomic coordinates to liftover coordinates:
        self.liftover_udf = f.udf(
            lambda chrom, pos: self.lo.convert_coordinate(chrom, pos),
            t.ArrayType(t.ArrayType(t.StringType())),
        )

    def convert_intervals(
        self: LiftOverSpark,
        df: DataFrame,
        chrom_col: str,
        start_col: str,
        end_col: str,
        filter: bool = True,
    ) -> DataFrame:
        """Convert genomic intervals to liftover coordinates.

        Args:
            df (DataFrame): spark Dataframe with chromosome, start and end columns.
            chrom_col (str): Name of the chromosome column.
            start_col (str): Name of the start column.
            end_col (str): Name of the end column.
            filter (bool): If True, filter is applied on the mapped data, otherwise return everything. Defaults to True.

        Returns:
            DataFrame: Liftovered intervals
        """
        # Lift over start coordinates, changing to 1-based coordinates:
        start_df = (
            df.withColumn(start_col, f.col(start_col) + 1)
            .select(chrom_col, start_col)
            .distinct()
        )
        start_df = self.convert_coordinates(
            start_df, chrom_col, start_col
        ).withColumnRenamed("mapped_pos", f"mapped_{start_col}")

        # Lift over end coordinates:
        end_df = df.select(chrom_col, end_col).distinct()
        end_df = self.convert_coordinates(end_df, chrom_col, end_col).withColumnRenamed(
            "mapped_pos", f"mapped_{end_col}"
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
        self: LiftOverSpark, df: DataFrame, chrom_name: str, pos_name: str
    ) -> DataFrame:
        """Converts genomic coordinates to coordinates on an other build.

        Args:
            df (DataFrame): Spark Dataframe with chromosome and position columns.
            chrom_name (str): Name of the chromosome column.
            pos_name (str): Name of the position column.

        Returns:
            DataFrame: Spark Dataframe with the mapped position column.
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



def liftover_loci(
    variant_index: Table, chain_path: str, dest_reference_genome: str
) -> Table:
    """Liftover a Hail table containing variant information from GRCh37 to GRCh38 or vice versa.

    Args:
        variant_index (Table): Variants to be lifted over
        chain_path (str): Path to chain file for liftover
        dest_reference_genome (str): Destination reference genome. It can be either GRCh37 or GRCh38.

    Returns:
        Table: LD variant index with coordinates in the new reference genome

    Warning:
        This function assumes hail is initialized in Session.
    """
    import hail as hl
    if not hl.get_reference("GRCh37").has_liftover(
        "GRCh38"
    ):  # True when a chain file has already been registered
        rg37 = hl.get_reference("GRCh37")
        rg38 = hl.get_reference("GRCh38")
        if dest_reference_genome == "GRCh38":
            rg37.add_liftover(chain_path, rg38)
        elif dest_reference_genome == "GRCh37":
            rg38.add_liftover(chain_path, rg37)
    # Dynamically create the new field with transmute
    new_locus = f"locus_{dest_reference_genome}"
    return variant_index.transmute(
        **{new_locus: hl.liftover(variant_index.locus, dest_reference_genome)}
    )
