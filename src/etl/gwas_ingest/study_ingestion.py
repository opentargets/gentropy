from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql import types as t

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession

STUDY_COLUMNS_MAP = {
    # 'DATE ADDED TO CATALOG': 'date_added_to_catalog',
    "PUBMED ID": "pubmed_id",
    "FIRST AUTHOR": "first_author",
    "DATE": "publication_date",
    "JOURNAL": "journal",
    "LINK": "link",
    "STUDY": "study",
    "DISEASE/TRAIT": "disease_trait",
    "INITIAL SAMPLE SIZE": "initial_sample_size",
    # 'REPLICATION SAMPLE SIZE': 'replication_sample_size',
    # 'PLATFORM [SNPS PASSING QC]': 'platform',
    # 'ASSOCIATION COUNT': 'association_count',
    "MAPPED_TRAIT": "mapped_trait",
    "MAPPED_TRAIT_URI": "mapped_trait_uri",
    "STUDY ACCESSION": "study_accession",
    # 'GENOTYPING TECHNOLOGY': 'genotyping_technology',
    "SUMMARY STATS LOCATION": "summary_stats_location",
    # 'SUBMISSION DATE': 'submission_date',
    # 'STATISTICAL MODEL': 'statistical_model',
    "BACKGROUND TRAIT": "background_trait",
    "MAPPED BACKGROUND TRAIT": "mapped_background_trait",
    "MAPPED BACKGROUND TRAIT URI": "mapped_background_trait_uri",
}


def get_sumstats_location(etl: ETLSession, summarystats_list: str) -> DataFrame:

    gwas_sumstats_location = "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics"

    return (
        etl.spark.read.csv(summarystats_list, sep="\t", header=False)
        .withColumn("has_summarystats", f.lit(True))
        .withColumn(
            "summarystats_location",
            f.concat(
                f.lit(gwas_sumstats_location),
                f.col("_c0"),
            ),
        )
        .withColumn(
            "study_accession",
            f.regexp_extract(f.col("summarystats_location"), r"\/(GCST\d+)\/", 1),
        )
        .select("study_accession", "has_summarystats", "summarystats_location")
        .persist()
    )


def read_study_table(
    etl: ETLSession,
    gwas_study_file: str,
    unpublished_gwas_study_file: str,
    read_unpublished: bool,
) -> DataFrame:
    """
    This function reads the study table and returns a Spark DataFrame with the columns renamed to match the names
    in the `STUDY_COLUMNS_MAP` dictionary

    Args:
      read_unpublished (bool): bool=False. Defaults to False. Indicating if unpublished studies should be read.

    Returns:
      A dataframe with the columns specified in STUDY_COLUMNS_MAP.
    """

    # Reading the study table:
    study_table = etl.spark.read.csv(gwas_study_file, sep="\t", header=True)

    if read_unpublished:

        # Reading the unpublished study table:
        unpublished_study_table = (
            etl.spark.read.csv(unpublished_gwas_study_file, sep="\t", header=True)
            # The column names are expected to be the same in both tables, so this should be gone at some point:
            .withColumnRenamed("MAPPED TRAIT", "MAPPED_TRAIT").withColumnRenamed(
                "MAPPED TRAIT URI", "MAPPED_TRAIT_URI"
            )
        )

        # Expression to replace "not yet cureated" string with nulls:
        expressions = [
            (
                column,
                f.when(f.col(column) == "not yet curated", f.lit(None)).otherwise(
                    f.col(column)
                ),
            )
            for column in STUDY_COLUMNS_MAP.keys()
        ]
        unpublished_study_table = reduce(
            lambda DF, value: DF.withColumn(*value),
            expressions,
            unpublished_study_table,
        )

        study_table = study_table.union(unpublished_study_table)

    # Selecting and renaming relevant columns:
    return study_table.select(
        *[
            f.col(old_name).alias(new_name)
            for old_name, new_name in STUDY_COLUMNS_MAP.items()
        ]
    ).persist()


def extract_sample_sizes(df: DataFrame) -> DataFrame:
    """
    It takes a dataframe with a column called `initial_sample_size` and returns a dataframe with columns
    `n_cases`, `n_controls`, and `n_samples`

    Args:
      df (pyspark.sql.DataFrame): pyspark.sql.DataFrame

    Returns:
      A dataframe with the columns:
        - n_cases
        - n_controls
        - n_samples
    """

    columns = df.columns

    return (
        df
        # .withColumn('samples', F.explode_outer(F.col('initial_sample_size')))
        .withColumn(
            "samples", f.explode_outer(f.split(f.col("initial_sample_size"), r",\s+"))
        )
        # Extracting the sample size from the string:
        .withColumn(
            "sample_size",
            f.regexp_extract(
                f.regexp_replace(f.col("samples"), ",", ""), r"[0-9,]+", 0
            ).cast(t.IntegerType()),
        )
        # Extracting number of cases:
        .withColumn(
            "n_cases",
            f.when(f.col("samples").contains("cases"), f.col("sample_size")).otherwise(
                f.lit(0)
            ),
        )
        .withColumn(
            "n_controls",
            f.when(
                f.col("samples").contains("controls"), f.col("sample_size")
            ).otherwise(f.lit(0)),
        )
        .groupBy(columns)
        .agg(
            f.sum("n_cases").alias("n_cases"),
            f.sum("n_controls").alias("n_controls"),
            f.sum("sample_size").alias("n_samples"),
        )
        .persist()
    )
