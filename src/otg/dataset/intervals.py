"""Interval dataset."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset
from otg.dataset.v2g import V2G
from otg.json import schemas

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.common.Liftover import LiftOverSpark
    from otg.common.session import Session
    from otg.dataset.gene_index import GeneIndex
    from otg.dataset.variant_index import VariantIndex


@dataclass
class Intervals(Dataset):
    """Intervals dataset links genes to genomic regions based on genome interaction studies."""

    _schema: StructType = parse_spark_schema("intervals.json")

    @classmethod
    def from_parquet(cls: type[Intervals], session: Session, path: str) -> Intervals:
        """Initialise Intervals from parquet file.

        Args:
            session (Session): ETL session
            path (str): Path to parquet file

        Returns:
            Intervals: Intervals dataset
        """
        return super().from_parquet(session, path, cls._schema)

    @classmethod
    def parse_andersson(
        cls: type[Intervals],
        session: Session,
        path: str,
        gene_index: GeneIndex,
        lift: LiftOverSpark,
    ) -> Intervals:
        """Parse Andersson et al. 2014 dataset.

        Args:
            session (Session): session
            path (str): Path to dataset
            gene_index (GeneIndex): Gene index
            lift (LiftOverSpark): LiftOverSpark instance

        Returns:
            Intervals: Intervals dataset
        """
        # Constant values:
        dataset_name = "andersson2014"
        experiment_type = "fantom5"
        pmid = "24670763"
        bio_feature = "aggregate"
        twosided_threshold = 2.45e6  # <-  this needs to phased out. Filter by percentile instead of absolute value.

        session.logger.info("Parsing Andersson 2014 data...")
        session.logger.info(f"Reading data from {path}")

        # Expected andersson et al. schema:
        input_schema = t.StructType.fromJson(
            json.loads(
                pkg_resources.read_text(schemas, "andersson2014.json", encoding="utf-8")
            )
        )

        # Read the anderson file:
        parsed_anderson_df = (
            session.spark.read.option("delimiter", "\t")
            .option("header", "true")
            .schema(input_schema)
            .csv(path)
            # Parsing score column and casting as float:
            .withColumn("score", f.col("score").cast("float") / f.lit(1000))
            # Parsing the 'name' column:
            .withColumn("parsedName", f.split(f.col("name"), ";"))
            .withColumn("gene_symbol", f.col("parsedName")[2])
            .withColumn("location", f.col("parsedName")[0])
            .withColumn(
                "chrom",
                f.regexp_replace(f.split(f.col("location"), ":|-")[0], "chr", ""),
            )
            .withColumn(
                "start", f.split(f.col("location"), ":|-")[1].cast(t.IntegerType())
            )
            .withColumn(
                "end", f.split(f.col("location"), ":|-")[2].cast(t.IntegerType())
            )
            # Select relevant columns:
            .select("chrom", "start", "end", "gene_symbol", "score")
            # Drop rows with non-canonical chromosomes:
            .filter(
                f.col("chrom").isin([str(x) for x in range(1, 23)] + ["X", "Y", "MT"])
            )
            # For each region/gene, keep only one row with the highest score:
            .groupBy("chrom", "start", "end", "gene_symbol")
            .agg(f.max("score").alias("resourceScore"))
            .orderBy("chrom", "start")
        )

        return cls(
            df=(
                # Lift over the intervals:
                lift.convert_intervals(parsed_anderson_df, "chrom", "start", "end")
                .drop("start", "end")
                .withColumnRenamed("mapped_start", "start")
                .withColumnRenamed("mapped_end", "end")
                .distinct()
                # Joining with the gene index
                .alias("intervals")
                .join(
                    gene_index.symbols_lut().alias("genes"),
                    on=[f.col("intervals.gene_symbol") == f.col("genes.geneSymbol")],
                    how="left",
                )
                .filter(
                    # Drop rows where the gene is not on the same chromosome
                    (f.col("chrom") == f.col("chromosome"))
                    # Drop rows where the TSS is far from the start of the region
                    & (
                        f.abs((f.col("start") + f.col("end")) / 2 - f.col("tss"))
                        <= twosided_threshold
                    )
                )
                # Select relevant columns:
                .select(
                    "chromosome",
                    "start",
                    "end",
                    "geneId",
                    "resourceScore",
                    f.lit(dataset_name).alias("datasourceId"),
                    f.lit(experiment_type).alias("datatypeId"),
                    f.lit(pmid).alias("pmid"),
                    f.lit(bio_feature).alias("biofeature"),
                )
            )
        )

    @classmethod
    def parse_javierre(
        cls: type[Intervals],
        session: Session,
        path: str,
        gene_index: GeneIndex,
        lift: LiftOverSpark,
    ) -> Intervals:
        """Parse Javierre et al. 2016 dataset.

        Args:
            session (Session): session
            path (str): Path to dataset
            gene_index (GeneIndex): Gene index
            lift (LiftOverSpark): LiftOverSpark instance

        Returns:
            Intervals: Javierre et al. 2016 interval data
        """
        # Constant values:
        dataset_name = "javierre2016"
        experiment_type = "pchic"
        pmid = "27863249"
        twosided_threshold = 2.45e6

        session.logger.info("Parsing Javierre 2016 data...")
        session.logger.info(f"Reading data from {path}")

        # Read Javierre data:
        javierre_raw = (
            session.spark.read.parquet(path)
            # Splitting name column into chromosome, start, end, and score:
            .withColumn("name_split", f.split(f.col("name"), r":|-|,"))
            .withColumn(
                "name_chr",
                f.regexp_replace(f.col("name_split")[0], "chr", "").cast(
                    t.StringType()
                ),
            )
            .withColumn("name_start", f.col("name_split")[1].cast(t.IntegerType()))
            .withColumn("name_end", f.col("name_split")[2].cast(t.IntegerType()))
            .withColumn("name_score", f.col("name_split")[3].cast(t.FloatType()))
            # Cleaning up chromosome:
            .withColumn(
                "chrom",
                f.regexp_replace(f.col("chrom"), "chr", "").cast(t.StringType()),
            )
            .drop("name_split", "name", "annotation")
            # Keep canonical chromosomes and consistent chromosomes with scores:
            .filter(
                (f.col("name_score").isNotNull())
                & (f.col("chrom") == f.col("name_chr"))
                & f.col("name_chr").isin(
                    [f"{x}" for x in range(1, 23)] + ["X", "Y", "MT"]
                )
            )
        )

        # Lifting over intervals:
        javierre_remapped = (
            javierre_raw
            # Lifting over to GRCh38 interval 1:
            .transform(lambda df: lift.convert_intervals(df, "chrom", "start", "end"))
            .drop("start", "end")
            .withColumnRenamed("mapped_chrom", "chrom")
            .withColumnRenamed("mapped_start", "start")
            .withColumnRenamed("mapped_end", "end")
            # Lifting over interval 2 to GRCh38:
            .transform(
                lambda df: lift.convert_intervals(
                    df, "name_chr", "name_start", "name_end"
                )
            )
            .drop("name_start", "name_end")
            .withColumnRenamed("mapped_name_chr", "name_chr")
            .withColumnRenamed("mapped_name_start", "name_start")
            .withColumnRenamed("mapped_name_end", "name_end")
        )

        # Once the intervals are lifted, extracting the unique intervals:
        unique_intervals_with_genes = (
            javierre_remapped.alias("intervals")
            .select(
                f.col("chrom"),
                f.col("start").cast(t.IntegerType()),
                f.col("end").cast(t.IntegerType()),
            )
            .distinct()
            .join(
                gene_index.locations_lut().alias("genes"),
                on=[f.col("intervals.chrom") == f.col("genes.chromosome")],
                how="left",
            )
            # TODO: add filter as part of the join condition
            .filter(
                (
                    (f.col("start") >= f.col("genomicLocation.start"))
                    & (f.col("start") <= f.col("genomicLocation.end"))
                )
                | (
                    (f.col("end") >= f.col("genomicLocation.start"))
                    & (f.col("end") <= f.col("genomicLocation.end"))
                )
            )
            .select("chrom", "start", "end", "geneId", "tss")
        )

        # Joining back the data:
        return cls(
            df=(
                javierre_remapped.join(
                    unique_intervals_with_genes,
                    on=["chrom", "start", "end"],
                    how="left",
                )
                .filter(
                    # Drop rows where the TSS is far from the start of the region
                    f.abs((f.col("start") + f.col("end")) / 2 - f.col("tss"))
                    <= twosided_threshold
                )
                # For each gene, keep only the highest scoring interval:
                .groupBy(
                    "name_chr", "name_start", "name_end", "genes.geneId", "bio_feature"
                )
                .agg(f.max(f.col("name_score")).alias("resourceScore"))
                # Create the output:
                .select(
                    f.col("name_chr").alias("chromosome"),
                    f.col("name_start").alias("start"),
                    f.col("name_end").alias("end"),
                    f.col("resourceScore"),
                    f.col("genes.geneId").alias("geneId"),
                    f.col("bio_feature").alias("biofeature"),
                    f.lit(dataset_name).alias("datasourceId"),
                    f.lit(experiment_type).alias("datatypeId"),
                    f.lit(pmid).alias("pmid"),
                )
            )
        )

    @classmethod
    def parse_jung(
        cls: type[Intervals],
        session: Session,
        path: str,
        gene_index: GeneIndex,
        lift: LiftOverSpark,
    ) -> Intervals:
        """Parse the Jung et al. 2019 dataset.

        Args:
            session (Session): session
            path (str): path to the Jung et al. 2019 dataset
            gene_index (GeneIndex): gene index
            lift (LiftOverSpark): LiftOverSpark instance

        Returns:
            Intervals: _description_
        """
        dataset_name = "javierre2016"
        experiment_type = "pchic"
        pmid = "27863249"

        session.logger.info("Parsing Jung 2019 data...")
        session.logger.info(f"Reading data from {path}")

        # Read Jung data:
        jung_raw = (
            session.spark.read.csv(path, sep=",", header=True)
            .withColumn("interval", f.split(f.col("Interacting_fragment"), r"\."))
            .select(
                # Parsing intervals:
                f.regexp_replace(f.col("interval")[0], "chr", "").alias("chrom"),
                f.col("interval")[1].cast(t.IntegerType()).alias("start"),
                f.col("interval")[2].cast(t.IntegerType()).alias("end"),
                # Extract other columns:
                f.col("Promoter").alias("gene_name"),
                f.col("Tissue_type").alias("tissue"),
            )
        )

        # Lifting over the coordinates:
        return cls(
            df=(
                jung_raw
                # Lifting over to GRCh38 interval 1:
                .transform(
                    lambda df: lift.convert_intervals(df, "chrom", "start", "end")
                )
                .select(
                    "chrom",
                    f.col("mapped_start").alias("start"),
                    f.col("mapped_end").alias("end"),
                    f.explode(f.split(f.col("gene_name"), ";")).alias("gene_name"),
                    "tissue",
                )
                .alias("intervals")
                # Joining with genes:
                .join(
                    gene_index.symbols_lut().alias("genes"),
                    on=[f.col("intervals.gene_name") == f.col("genes.geneSymbol")],
                    how="inner",
                )
                # Finalize dataset:
                .select(
                    "chromosome",
                    "start",
                    "end",
                    "geneId",
                    f.col("tissue").alias("biofeature"),
                    f.lit(1.0).alias("score"),
                    f.lit(dataset_name).alias("datasourceId"),
                    f.lit(experiment_type).alias("datatypeId"),
                    f.lit(pmid).alias("pmid"),
                )
                .drop_duplicates()
            )
        )

    @classmethod
    def parse_thurman(
        cls: type[Intervals],
        session: Session,
        path: str,
        gene_index: GeneIndex,
        lift: LiftOverSpark,
    ) -> Intervals:
        """Parse the Thurman et al. 2019 dataset.

        Args:
            session (Session): session
            path (str): path to the Thurman et al. 2019 dataset
            gene_index (GeneIndex): gene index
            lift (LiftOverSpark): LiftOverSpark instance

        Returns:
            Intervals: _description_
        """
        dataset_name = "thurman2012"
        experiment_type = "dhscor"
        pmid = "22955617"

        session.logger.info("Parsing Jung 2019 data...")
        session.logger.info(f"Reading data from {path}")

        # Read Jung data:
        jung_raw = (
            session.spark.read.csv(path, sep=",", header=True)
            .withColumn("interval", f.split(f.col("Interacting_fragment"), r"\."))
            .select(
                # Parsing intervals:
                f.regexp_replace(f.col("interval")[0], "chr", "").alias("chrom"),
                f.col("interval")[1].cast(t.IntegerType()).alias("start"),
                f.col("interval")[2].cast(t.IntegerType()).alias("end"),
                # Extract other columns:
                f.col("Promoter").alias("gene_name"),
                f.col("Tissue_type").alias("tissue"),
            )
        )

        return cls(
            df=(
                jung_raw
                # Lifting over to GRCh38 interval 1:
                .transform(
                    lambda df: lift.convert_intervals(df, "chrom", "start", "end")
                )
                .select(
                    "chrom",
                    f.col("mapped_start").alias("start"),
                    f.col("mapped_end").alias("end"),
                    f.explode(f.split(f.col("gene_name"), ";")).alias("gene_name"),
                    "tissue",
                )
                .alias("intervals")
                # Joining with genes:
                .join(
                    gene_index.symbols_lut().alias("genes"),
                    on=[f.col("intervals.gene_name") == f.col("genes.geneSymbol")],
                    how="inner",
                )
                # Finalize dataset:
                .select(
                    "chromosome",
                    "start",
                    "end",
                    "geneId",
                    f.col("tissue").alias("biofeature"),
                    f.lit(1.0).alias("score"),
                    f.lit(dataset_name).alias("datasourceId"),
                    f.lit(experiment_type).alias("datatypeId"),
                    f.lit(pmid).alias("pmid"),
                )
                .drop_duplicates()
            )
        )

    def v2g(self: Intervals, variant_index: VariantIndex) -> V2G:
        """Convert intervals into V2G by intersecting with a variant index.

        Args:
            variant_index (VariantIndex): Variant index dataset

        Returns:
            V2G: Variant-to-gene evidence dataset
        """
        return V2G(
            df=(
                # TODO: We can include the start and end position as part of the `on` clause in the join
                self.df.join(variant_index.df, on="chromosome", how="inner")
                .filter(f.col("position").between(f.col("start"), f.col("end")))
                .drop("start", "end")
            )
        )
