"""GWAS Catalog study ingestion."""
from __future__ import annotations

import re
from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql import types as t

from etl.json import validate_df_schema

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from etl.common.ETLSession import ETLSession

STUDY_COLUMNS_MAP = {
    # 'DATE ADDED TO CATALOG': 'date_added_to_catalog',
    "PUBMED ID": "pubmedId",
    "FIRST AUTHOR": "firstAuthor",
    "DATE": "publicationDate",
    "JOURNAL": "journal",
    "LINK": "link",
    "STUDY": "study",
    "DISEASE/TRAIT": "diseaseTrait",
    "INITIAL SAMPLE SIZE": "initialSampleSize",
    # 'REPLICATION SAMPLE SIZE': 'replication_sample_size',
    # 'PLATFORM [SNPS PASSING QC]': 'platform',
    # 'ASSOCIATION COUNT': 'association_count',
    # "MAPPED_TRAIT": "mappedTrait",
    "MAPPED_TRAIT_URI": "mappedTraitUri",
    "STUDY ACCESSION": "studyAccession",
    # 'GENOTYPING TECHNOLOGY': 'genotyping_technology',
    # "SUMMARY STATS LOCATION": "summaryStatsLocation",
    # 'SUBMISSION DATE': 'submission_date',
    # 'STATISTICAL MODEL': 'statistical_model',
    # "BACKGROUND TRAIT": "backgroundTrait",
    # "MAPPED BACKGROUND TRAIT": "mappedBackgroundTrait",
    "MAPPED BACKGROUND TRAIT URI": "mappedBackgroundTraitUri",
}


def get_sumstats_location(etl: ETLSession, summarystats_list: str) -> DataFrame:
    """Get summary stat locations.

    Args:
        etl (ETLSession): current ETL session
        summarystats_list (str): filepath of table listing summary stats

    Returns:
        DataFrame: dataframe with each GCST with summary stats and its location
    """
    gwas_sumstats_base_uri = "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics"

    sumstats = (
        etl.spark.read.csv(summarystats_list, sep="\t", header=False)
        .select(
            f.concat(
                f.lit(gwas_sumstats_base_uri),
                f.regexp_replace(f.col("_c0"), r"^\.\/", ""),
            ).alias("summarystatsLocation")
        )
        .select(
            f.regexp_extract(f.col("summarystatsLocation"), r"\/(GCST\d+)\/", 1).alias(
                "studyAccession"
            ),
            "summarystatsLocation",
        )
        .persist()
    )

    print(f"Number of studies with harmonized summary stats: {sumstats.count()}")
    return sumstats


def annotate_summary_stats(
    etl: ETLSession, df: DataFrame, summarystats_list: str
) -> DataFrame:
    """This function adds ftp location of harmonized summary statistics when available.

    Args:
        etl (ETLSession): ETLSession
        df (DataFrame): GWAS Catalog study table
        summarystats_list (str): path to table listing harmonized summary stats

    Returns:
        DataFrame: the sumstats annotation contains the path and a flag indicating if
        sumstats are available.
    """
    # TODO perform a join
    sumstats = get_sumstats_location(etl, summarystats_list)

    return df.join(sumstats, on="studyAccession", how="left").withColumn(
        "hasSumstats",
        f.when(f.col("summarystatsLocation").isNotNull(), True).otherwise(False),
    )


def read_study_table(etl: ETLSession, gwas_study_file: str) -> DataFrame:
    """Read GWASCatalog study table.

    Args:
        etl (ETLSession): ETL session
        gwas_study_file (str): GWAS studies filepath

    Returns:
        DataFrame: Study table.
    """
    return etl.spark.read.csv(gwas_study_file, sep="\t", header=True).select(
        *[
            f.col(old_name).alias(new_name)
            for old_name, new_name in STUDY_COLUMNS_MAP.items()
        ]
    )


def extract_discovery_sample_sizes(df: DataFrame) -> DataFrame:
    """Extract the sample size of the discovery stage of the study as annotated in the GWAS Catalog.

    Args:
        df (DataFrame): Df with a column called `initial_sample_size`

    Returns:
        DataFrame: df with columns `n_cases`, `n_controls`, and `n_samples`
    """
    columns = df.columns

    return (
        df.withColumn(
            "samples", f.explode_outer(f.split(f.col("initialSampleSize"), r",\s+"))
        )
        # Extracting the sample size from the string:
        .withColumn(
            "sampleSize",
            f.regexp_extract(
                f.regexp_replace(f.col("samples"), ",", ""), r"[0-9,]+", 0
            ).cast(t.IntegerType()),
        )
        # Extracting number of cases:
        .withColumn(
            "nCases",
            f.when(f.col("samples").contains("cases"), f.col("sampleSize")).otherwise(
                f.lit(0)
            ),
        )
        .withColumn(
            "nControls",
            f.when(
                f.col("samples").contains("controls"), f.col("sampleSize")
            ).otherwise(f.lit(0)),
        )
        # Aggregating sample sizes for all ancestries:
        .groupBy(columns)
        .agg(
            f.sum("nCases").alias("nCases"),
            f.sum("nControls").alias("nControls"),
            f.sum("sampleSize").alias("nSamples"),
        )
        .persist()
    )


def parse_efos(c: Column) -> Column:
    """Extracting EFO identifiers.

    This function parses EFO identifiers from a comma-separated list of EFO URIs.

    Args:
        c (Column): column with a list of EFO IDs

    Returns:
        Column: column with a list of parsed EFO IDs
    """
    return f.expr(f"regexp_extract_all({c}, '([A-Z]+_[0-9]+)')")


def column2camel_case(s: str) -> str:
    """A helper function to convert column names to camel cases.

    Args:
        s (str): a single column name

    Returns:
        str: spark expression to select and rename the column
    """

    def string2camelcase(s: str) -> str:
        """Converting a string to camelcase.

        Args:
            s (str): a random string

        Returns:
            str: Camel cased string
        """
        # Removing a bunch of unwanted characters from the column names:
        s = re.sub(r"[\/\(\)\-]+", " ", s)

        first, *rest = s.split(" ")
        return "".join([first.lower(), *map(str.capitalize, rest)])

    return f"`{s}` as {string2camelcase(s)}"


def parse_ancestries(etl: ETLSession, ancestry_file: str) -> DataFrame:
    """Extracing sample sizes and ancestry information.

    This function parses the ancestry data. Also get counts for the europeans in the same
    discovery stage.

    Args:
        etl (ETLSession): ETL session
        ancestry_file (str): File name of the ancestry table as downloaded from the GWAS Catalog

    Returns:
        DataFrame: Slimmed and cleaned version of the ancestry annotation.
    """
    # Reading ancestry data:
    ancestry = (
        etl.spark.read.csv(ancestry_file, sep="\t", header=True)
        # Convert column headers to camelcase:
        .transform(
            lambda df: df.select(*[f.expr(column2camel_case(x)) for x in df.columns])
        )
    )

    # Get a high resolution dataset on experimental stage:
    ancestry_stages = (
        ancestry.groupBy("studyAccession")
        .pivot("stage")
        .agg(
            f.collect_set(
                f.struct(
                    f.col("numberOfIndividuals").alias("sampleSize"),
                    f.col("broadAncestralCategory").alias("ancestry"),
                )
            )
        )
        .withColumnRenamed("initial", "discoverySamples")
        .withColumnRenamed("replication", "replicationSamples")
    )

    # Generate information on the ancestry composition of the discovery stage, and calculate
    # the proportion of the Europeans:
    europeans_deconvoluted = (
        ancestry
        # Focus on discovery stage:
        .filter(f.col("stage") == "initial")
        # Sorting ancestries if European:
        .withColumn(
            "ancestryFlag",
            # Excluding finnish:
            f.when(
                f.col("initialSampleDescription").contains("Finnish"), f.lit("other")
            )
            # Excluding Icelandic population:
            .when(
                f.col("initialSampleDescription").contains("Icelandic"), f.lit("other")
            )
            # Including European ancestry:
            .when(f.col("broadAncestralCategory") == "European", f.lit("european"))
            # Exclude all other population:
            .otherwise("other"),
        )
        # Grouping by study accession and initial sample description:
        .groupBy("studyAccession")
        .pivot("ancestryFlag")
        .agg(
            # Summarizing sample sizes for all ancestries:
            f.sum(f.col("numberOfIndividuals"))
        )
        # Do aritmetics to make sure we have the right proportion of european in the set:
        .withColumn(
            "initialSampleCountEuropean",
            f.when(f.col("european").isNull(), f.lit(0)).otherwise(f.col("european")),
        )
        .withColumn(
            "other", f.when(f.col("other").isNull(), f.lit(0)).otherwise(f.col("other"))
        )
        .withColumn(
            "initialSampleCount", f.col("initialSampleCountEuropean") + f.col("other")
        )
        .drop("european", "other")
    )

    return ancestry_stages.join(
        europeans_deconvoluted, on="studyAccession", how="outer"
    )


def ingest_gwas_catalog_studies(
    etl: ETLSession, study_file: str, ancestry_file: str, summary_stats_list: str
) -> DataFrame:
    """This function ingests study level metadata from the GWAS Catalog.

    The following information is aggregated/extracted:
    - All publication related information retained.
    - Mapped measured and background traits parsed.
    - Flagged if harmonized summary statistics datasets available.
    - If available, the ftp path to these files presented.
    - Ancestries from the discovery and replication stages are structured with sample counts.
    - Case/control counts extracted.
    - The number of samples with European ancestry extracted.

    Args:
        etl (ETLSession): ETLSession
        study_file (str): path to the GWAS Catalog study table v1.0.3.
        ancestry_file (str): path to the GWAS Catalog ancestry table.
        summary_stats_list (str): path to the GWAS Catalog harmonized summary statistics list.

    Returns:
        DataFrame: Parsed and annotated GWAS Catalog study table.
    """
    study_data = (
        # reading study table:
        read_study_table(etl, study_file)
        # Parse sample size:
        .transform(extract_discovery_sample_sizes)
        # Adding summary stats location:
        .transform(
            lambda df: annotate_summary_stats(etl, df, summary_stats_list)
        ).select(
            "*",
            parse_efos(f.col("mappedTraitUri")).alias("efos"),
            parse_efos(f.col("mappedBackgroundTraitUri")).alias("backgroundEfos"),
        )
    )

    ancestry_data = parse_ancestries(etl, ancestry_file)

    studies = study_data.join(ancestry_data, on="studyAccession", how="left")

    validate_df_schema(studies, "studies.json")

    return studies
