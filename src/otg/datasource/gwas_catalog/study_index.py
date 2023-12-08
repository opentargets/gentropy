"""Study Index for GWAS Catalog data source."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.utils import parse_efos
from otg.dataset.study_index_gwas_catalog import (
    StudyIndexGWASCatalog as StudyIndexGWASCatalogDataset,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


@dataclass
class GWASCatalogStudyIndex:
    """Study index from GWAS Catalog.

    The following information is harmonised from the GWAS Catalog:

    - All publication related information retained.
    - Mapped measured and background traits parsed.
    - Flagged if harmonized summary statistics datasets available.
    - If available, the ftp path to these files presented.
    - Ancestries from the discovery and replication stages are structured with sample counts.
    - Case/control counts extracted.
    - The number of samples with European ancestry extracted.

    """

    @staticmethod
    def _parse_discovery_samples(discovery_samples: Column) -> Column:
        """Parse discovery sample sizes from GWAS Catalog.

        This is a curated field. From publication sometimes it is not clear how the samples were split
        across the reported ancestries. In such cases we are assuming the ancestries were evenly presented
        and the total sample size is split:

        ["European, African", 100] -> ["European, 50], ["African", 50]

        Args:
            discovery_samples (Column): Raw discovery sample sizes

        Returns:
            Column: Parsed and de-duplicated list of discovery ancestries with sample size.

        Examples:
            >>> data = [('s1', "European", 10), ('s1', "African", 10), ('s2', "European, African, Asian", 100), ('s2', "European", 50)]
            >>> df = (
            ...    spark.createDataFrame(data, ['studyId', 'ancestry', 'sampleSize'])
            ...    .groupBy('studyId')
            ...    .agg(
            ...        f.collect_set(
            ...            f.struct('ancestry', 'sampleSize')
            ...        ).alias('discoverySampleSize')
            ...    )
            ...    .orderBy('studyId')
            ...    .withColumn('discoverySampleSize', GWASCatalogStudyIndex._parse_discovery_samples(f.col('discoverySampleSize')))
            ...    .select('discoverySampleSize')
            ...    .show(truncate=False)
            ... )
            +--------------------------------------------+
            |discoverySampleSize                         |
            +--------------------------------------------+
            |[{African, 10}, {European, 10}]             |
            |[{European, 83}, {African, 33}, {Asian, 33}]|
            +--------------------------------------------+
            <BLANKLINE>
        """
        # To initialize return objects for aggregate functions, schema has to be definied:
        schema = t.ArrayType(
            t.StructType(
                [
                    t.StructField("ancestry", t.StringType(), True),
                    t.StructField("sampleSize", t.IntegerType(), True),
                ]
            )
        )

        # Splitting comma separated ancestries:
        exploded_ancestries = f.transform(
            discovery_samples,
            lambda sample: f.split(sample.ancestry, r",\s(?![^()]*\))"),
        )

        # Initialize discoverySample object from unique list of ancestries:
        unique_ancestries = f.transform(
            f.aggregate(
                exploded_ancestries,
                f.array().cast(t.ArrayType(t.StringType())),
                lambda x, y: f.array_union(x, y),
                f.array_distinct,
            ),
            lambda ancestry: f.struct(
                ancestry.alias("ancestry"),
                f.lit(0).cast(t.LongType()).alias("sampleSize"),
            ),
        )

        # Computing sample sizes for ancestries when splitting is needed:
        resolved_sample_count = f.transform(
            f.arrays_zip(
                f.transform(exploded_ancestries, lambda pop: f.size(pop)).alias(
                    "pop_size"
                ),
                f.transform(discovery_samples, lambda pop: pop.sampleSize).alias(
                    "pop_count"
                ),
            ),
            lambda pop: (pop.pop_count / pop.pop_size).cast(t.IntegerType()),
        )

        # Flattening out ancestries with sample sizes:
        parsed_sample_size = f.aggregate(
            f.transform(
                f.arrays_zip(
                    exploded_ancestries.alias("ancestries"),
                    resolved_sample_count.alias("sample_count"),
                ),
                GWASCatalogStudyIndex._merge_ancestries_and_counts,
            ),
            f.array().cast(schema),
            lambda x, y: f.array_union(x, y),
        )

        # Normalize ancestries:
        return f.aggregate(
            parsed_sample_size,
            unique_ancestries,
            GWASCatalogStudyIndex._normalize_ancestries,
        )

    @staticmethod
    def _normalize_ancestries(merged: Column, ancestry: Column) -> Column:
        """Normalize ancestries from a list of structs.

        As some ancestry label might be repeated with different sample counts,
        these counts need to be collected.

        Args:
            merged (Column): Resulting list of struct with unique ancestries.
            ancestry (Column): One ancestry object coming from raw.

        Returns:
            Column: Unique list of ancestries with the sample counts.
        """
        # Iterating over the list of unique ancestries and adding the sample size if label matches:
        return f.transform(
            merged,
            lambda a: f.when(
                a.ancestry == ancestry.ancestry,
                f.struct(
                    a.ancestry.alias("ancestry"),
                    (a.sampleSize + ancestry.sampleSize)
                    .cast(t.LongType())
                    .alias("sampleSize"),
                ),
            ).otherwise(a),
        )

    @staticmethod
    def _merge_ancestries_and_counts(ancestry_group: Column) -> Column:
        """Merge ancestries with sample sizes.

        After splitting ancestry annotations, all resulting ancestries needs to be assigned
        with the proper sample size.

        Args:
            ancestry_group (Column): Each element is a struct with `sample_count` (int) and `ancestries` (list)

        Returns:
            Column: a list of structs with `ancestry` and `sampleSize` fields.

        Examples:
            >>> data = [(12, ['African', 'European']),(12, ['African'])]
            >>> (
            ...     spark.createDataFrame(data, ['sample_count', 'ancestries'])
            ...     .select(GWASCatalogStudyIndex._merge_ancestries_and_counts(f.struct('sample_count', 'ancestries')).alias('test'))
            ...     .show(truncate=False)
            ... )
            +-------------------------------+
            |test                           |
            +-------------------------------+
            |[{African, 12}, {European, 12}]|
            |[{African, 12}]                |
            +-------------------------------+
            <BLANKLINE>
        """
        # Extract sample size for the ancestry group:
        count = ancestry_group.sample_count

        # We need to loop through the ancestries:
        return f.transform(
            ancestry_group.ancestries,
            lambda ancestry: f.struct(
                ancestry.alias("ancestry"),
                count.alias("sampleSize"),
            ),
        )

    @staticmethod
    def parse_cohorts(raw_cohort: Column) -> Column:
        """Return a list of unique cohort labels from pipe separated list if provided.

        Args:
            raw_cohort (Column): Cohort list column, where labels are separated by `|` sign.

        Returns:
            Column: an array colun with string elements.

        Examples:
        >>> data = [('BioME|CaPS|Estonia|FHS|UKB|GERA|GERA|GERA',),(None,),]
        >>> spark.createDataFrame(data, ['cohorts']).select(GWASCatalogStudyIndex.parse_cohorts(f.col('cohorts')).alias('parsedCohorts')).show(truncate=False)
        +--------------------------------------+
        |parsedCohorts                         |
        +--------------------------------------+
        |[BioME, CaPS, Estonia, FHS, UKB, GERA]|
        |[null]                                |
        +--------------------------------------+
        <BLANKLINE>
        """
        return f.when(
            (raw_cohort.isNull()) | (raw_cohort == ""),
            f.array(f.lit(None).cast(t.StringType())),
        ).otherwise(f.array_distinct(f.split(raw_cohort, r"\|")))

    @classmethod
    def _parse_study_table(
        cls: type[GWASCatalogStudyIndex], catalog_studies: DataFrame
    ) -> StudyIndexGWASCatalogDataset:
        """Harmonise GWASCatalog study table with `StudyIndex` schema.

        Args:
            catalog_studies (DataFrame): GWAS Catalog study table

        Returns:
            StudyIndexGWASCatalogDataset: Parsed and annotated GWAS Catalog study table.
        """
        return StudyIndexGWASCatalogDataset(
            _df=catalog_studies.select(
                f.coalesce(
                    f.col("STUDY ACCESSION"), f.monotonically_increasing_id()
                ).alias("studyId"),
                f.lit("GCST").alias("projectId"),
                f.lit("gwas").alias("studyType"),
                f.col("PUBMED ID").alias("pubmedId"),
                f.col("FIRST AUTHOR").alias("publicationFirstAuthor"),
                f.col("DATE").alias("publicationDate"),
                f.col("JOURNAL").alias("publicationJournal"),
                f.col("STUDY").alias("publicationTitle"),
                f.coalesce(f.col("DISEASE/TRAIT"), f.lit("Unreported")).alias(
                    "traitFromSource"
                ),
                f.col("INITIAL SAMPLE SIZE").alias("initialSampleSize"),
                parse_efos(f.col("MAPPED_TRAIT_URI")).alias("traitFromSourceMappedIds"),
                parse_efos(f.col("MAPPED BACKGROUND TRAIT URI")).alias(
                    "backgroundTraitFromSourceMappedIds"
                ),
            ),
            _schema=StudyIndexGWASCatalogDataset.get_schema(),
        )

    @classmethod
    def from_source(
        cls: type[GWASCatalogStudyIndex],
        catalog_studies: DataFrame,
        ancestry_file: DataFrame,
        sumstats_lut: DataFrame,
    ) -> StudyIndexGWASCatalogDataset:
        """Ingests study level metadata from the GWAS Catalog.

        Args:
            catalog_studies (DataFrame): GWAS Catalog raw study table
            ancestry_file (DataFrame): GWAS Catalog ancestry table.
            sumstats_lut (DataFrame): GWAS Catalog summary statistics list.

        Returns:
            StudyIndexGWASCatalogDataset: Parsed and annotated GWAS Catalog study table.
        """
        # Read GWAS Catalogue raw data
        return (
            cls._parse_study_table(catalog_studies)
            .annotate_ancestries(ancestry_file)
            .annotate_sumstats_info(sumstats_lut)
            .annotate_discovery_sample_sizes()
        )
