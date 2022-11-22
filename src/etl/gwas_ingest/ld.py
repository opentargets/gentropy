"""Performing linkage disequilibrium (LD) operations."""
from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f

if TYPE_CHECKING:
    from omegaconf.listconfig import ListConfig
    from hail.table import Table
    from pyspark.sql import Column
    from etl.common.ETLSession import ETLSession

import hail as hl
from hail.linalg import BlockMatrix


def _liftover_loci(variant_index: Table) -> Table:
    """Liftover hail table with LD variant index.

    Args:
        variant_index (Table): LD variant indexes

    Returns:
        Table: LD variant index with locus 38 coordinates
    """
    if not hl.get_reference("GRCh37").has_liftover("GRCh38"):
        rg37 = hl.get_reference("GRCh37")
        rg38 = hl.get_reference("GRCh38")
        rg37.add_liftover(
            "gs://hail-common/references/grch37_to_grch38.over.chain.gz", rg38
        )

    return variant_index.annotate(locus38=hl.liftover(variant_index.locus, "GRCh38"))


def _interval_start(contig: Column, position: Column, ld_radius: int) -> Column:
    """Start position of the interval based on available positions.

    Args:
        contig (Column): genomic contigs
        position (Column): genomic positions
        ld_radius (int): bp around locus

    Returns:
        Column: Position of the locus starting the interval

    Examples:
        >>> d = [
        ...     {"contig": "21", "pos": 100},
        ...     {"contig": "21", "pos": 200},
        ...     {"contig": "21", "pos": 300},
        ... ]
        >>> df = spark.createDataFrame(d)
        >>> df.withColumn("start", _interval_start(f.col("contig"), f.col("pos"), 100)).show()
        +------+---+-----+
        |contig|pos|start|
        +------+---+-----+
        |    21|100|  100|
        |    21|200|  100|
        |    21|300|  200|
        +------+---+-----+
        <BLANKLINE>

    """
    w = Window.partitionBy(contig).orderBy(position).rangeBetween(-ld_radius, ld_radius)
    return f.min(position).over(w)


def _interval_stop(contig: Column, position: Column, ld_radius: int) -> Column:
    """Stop position of the interval based on available positions.

    Args:
        contig (Column): genomic contigs
        position (Column): genomic positions
        ld_radius (int): bp around locus

    Returns:
        Column: Position of the locus at the end of the interval

    Examples:
        >>> d = [
        ...     {"contig": "21", "pos": 100},
        ...     {"contig": "21", "pos": 200},
        ...     {"contig": "21", "pos": 300},
        ... ]
        >>> df = spark.createDataFrame(d)
        >>> df.withColumn("start", _interval_stop(f.col("contig"), f.col("pos"), 100)).show()
        +------+---+-----+
        |contig|pos|start|
        +------+---+-----+
        |    21|100|  200|
        |    21|200|  300|
        |    21|300|  300|
        +------+---+-----+
        <BLANKLINE>

    """
    w = Window.partitionBy(contig).orderBy(position).rangeBetween(-ld_radius, ld_radius)
    return f.max(position).over(w)


def _annotate_index_intervals(index: DataFrame, ld_radius: int) -> DataFrame:
    """Annotate LD index with indexes starting and stopping a given interval.

    Args:
        index (DataFrame): LD index
        ld_radius (int): radius around each position

    Returns:
        DataFrame: including `start_idx` and `stop_idx`
    """
    index_with_positions = index.select(
        "*",
        _interval_start(
            contig=f.col("chromosome"),
            position=f.col("position"),
            ld_radius=ld_radius,
        ).alias("start_pos"),
        _interval_stop(
            contig=f.col("chromosome"),
            position=f.col("position"),
            ld_radius=ld_radius,
        ).alias("stop_pos"),
    )

    return (
        index_with_positions.join(
            index_with_positions.select(
                "chromosome",
                f.col("position").alias("start_pos"),
                f.col("idx").alias("start_idx"),
            ),
            on=["chromosome", "start_pos"],
        )
        .join(
            index_with_positions.select(
                "chromosome",
                f.col("position").alias("stop_pos"),
                f.col("idx").alias("stop_idx"),
            ),
            on=["chromosome", "stop_pos"],
        )
        .drop("start_pos", "stop_pos")
    )


def _query_block_matrix(
    bm: BlockMatrix, idxs: list[int], starts: list[int], stops: list[int], min_r2: float
) -> DataFrame:
    """Query block matrix for idxs rows sparsified by start/stop columns.

    Args:
        bm (BlockMatrix): LD matrix
        idxs (List[int]): Row indexes to query (distinct and incremenetal)
        starts (List[int]): Interval start column indexes (same size as idxs)
        stops (List[int]): Interval start column indexes (same size as idxs)
        min_r2 (float): Minimum r2 to keep

    Returns:
        DataFrame: i,j,r where i and j are the row and column indexes and r is the LD
    """
    bm_sparsified = bm.filter_rows(idxs).sparsify_row_intervals(
        starts, stops, blocks_only=True
    )
    entries = bm_sparsified.entries(keyed=False)

    return entries.rename({"entry": "r"}).to_spark().filter(f.col("r") ** 2 >= min_r2)


def lead_coordinates_in_ld(
    leads_df: DataFrame,
) -> tuple[list[int], list[int], list[int]]:
    """Idxs for lead, first variant in the region and last variant in the region.

    Args:
        leads_df (DataFrame): Lead variants from `_annotate_index_intervals` output

    Returns:
        Tuple[List[int], List[int], List[int]]: Lead, start and stop indexes
    """
    intervals = (
        leads_df
        # start idx > stop idx in rare occasions due to liftover
        .filter(f.col("start_idx") < f.col("stop_idx"))
        .groupBy("chromosome", "idx")
        .agg(f.min("start_idx").alias("start_idx"), f.max("stop_idx").alias("stop_idx"))
        .sort(f.col("idx"))
        .persist()
    )

    idxs = intervals.select("idx").rdd.flatMap(lambda x: x).collect()
    starts = intervals.select("start_idx").rdd.flatMap(lambda x: x).collect()
    stops = intervals.select("stop_idx").rdd.flatMap(lambda x: x).collect()

    return idxs, starts, stops


def precompute_ld_index(pop_ldindex_path: str, ld_radius: int) -> DataFrame:
    """Parse LD index and annotate with interval start and stop.

    Args:
        pop_ldindex_path (str): path to gnomAD LD index
        ld_radius (int): radius

    Returns:
        DataFrame: Parsed LD iindex
    """
    ld_index = hl.read_table(pop_ldindex_path).naive_coalesce(400)
    ld_index_38 = _liftover_loci(ld_index)

    ld_index_spark = (
        ld_index_38.to_spark()
        .filter(f.col("`locus38.position`").isNotNull())
        .select(
            f.regexp_replace("`locus38.contig`", "chr", "").alias("chromosome"),
            f.col("`locus38.position`").alias("position"),
            f.col("`alleles`").getItem(0).alias("referenceAllele"),
            f.col("`alleles`").getItem(1).alias("alternateAllele"),
            f.col("idx"),
        )
        .repartition(400, "chromosome")
        .sortWithinPartitions("position")
        .persist()
    )

    parsed_ld_index = _annotate_index_intervals(ld_index_spark, ld_radius)
    return parsed_ld_index


def variants_in_ld_in_gnomad_pop(
    etl: ETLSession,
    variants_df: DataFrame,
    ld_path: str,
    parsed_ld_index_path: str,
    min_r2: float,
) -> DataFrame:
    """Return lead variants with LD annotation.

    Args:
        etl (ETLSession): Session
        variants_df (DataFrame): variants to annotate
        ld_path (str): path to LD matrix
        parsed_ld_index_path (str): path to LD index
        min_r2 (float): minimum r2 to keep

    Returns:
        DataFrame: lead variants with LD annotation
    """
    # LD blockmatrix and indexes from gnomAD
    bm = BlockMatrix.read(ld_path)
    bm = bm + bm.T

    parsed_ld_index = etl.spark.read.parquet(parsed_ld_index_path).persist()

    leads_with_idxs = variants_df.join(
        parsed_ld_index,
        on=["chromosome", "position", "referenceAllele", "alternateAllele"],
    )

    # idxs for lead, first variant in the region and last variant in the region
    idxs, starts, stops = lead_coordinates_in_ld(leads_with_idxs)

    etl.logger.info("Querying block matrix...")
    entries = _query_block_matrix(bm, idxs, starts, stops, min_r2)

    i_position_lut = etl.spark.createDataFrame(list(enumerate(idxs))).toDF("i", "idx")

    lead_tag = (
        entries.join(
            f.broadcast(leads_with_idxs.join(f.broadcast(i_position_lut), on="idx")),
            on="i",
            how="inner",
        )
        .drop("i", "idx", "start_idx", "stop_idx")
        .alias("leads")
        .join(
            parsed_ld_index.select(
                f.col("chromosome"),
                f.concat_ws(
                    "_",
                    f.col("chromosome"),
                    f.col("position"),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
                ).alias("tagVariantId"),
                f.col("idx").alias("tag_idx"),
            ).alias("tags"),
            on=[
                f.col("leads.chromosome") == f.col("tags.chromosome"),
                f.col("leads.j") == f.col("tags.tag_idx"),
            ],
        )
        .select("variantId", "leads.chromosome", "gnomad_ancestry", "tagVariantId", "r")
    )

    return lead_tag


def ld_annotation_by_locus_ancestry(
    etl: ETLSession,
    association_ancestry: DataFrame,
    ld_populations: ListConfig,
    min_r2: float,
) -> DataFrame:
    """LD information for all locus and ancestries.

    Args:
        etl (ETLSession): Session
        association_ancestry (DataFrame): variant-ancestry information
        ld_populations (ListConfig): list of populations to annotate
        min_r2 (float): minimum r2 to keep

    Returns:
        DataFrame: lead variants with LD annotation by relevant ancestry
    """
    # All gnomad populations captured in associations:
    assoc_populations = (
        association_ancestry.select("gnomad_ancestry")
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    # Retrieve LD information from gnomAD
    ld_annotated_assocs = []
    for pop in assoc_populations:
        pop_parsed_ldindex_path = ""
        pop_matrix_path = ""
        for popobj in ld_populations:
            if popobj.id == pop:
                pop_parsed_ldindex_path = popobj.parsed_index
                pop_matrix_path = popobj.matrix
                variants_in_pop = association_ancestry.filter(
                    f.col("gnomad_ancestry") == pop
                ).distinct()
                etl.logger.info(f"[{pop}] - Annotating LD information...")
                ld_annotated_assocs.append(
                    variants_in_ld_in_gnomad_pop(
                        etl=etl,
                        variants_df=variants_in_pop,
                        ld_path=pop_matrix_path,
                        parsed_ld_index_path=pop_parsed_ldindex_path,
                        min_r2=min_r2,
                    )
                )

    return reduce(DataFrame.unionByName, ld_annotated_assocs)
