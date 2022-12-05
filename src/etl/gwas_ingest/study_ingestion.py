"""GWAS Catalog study ingestion."""
from __future__ import annotations

import re
from itertools import chain
from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from etl.common.ETLSession import ETLSession


STUDY_COLUMNS_MAP = {
    # 'DATE ADDED TO CATALOG': 'date_added_to_catalog',
    "PUBMED ID": "pubmedId",
    "FIRST AUTHOR": "firstAuthor",
    "DATE": "publicationDate",
    "JOURNAL": "journal",
    # "LINK": "link", # Link not read: links are generated on the front end
    "STUDY": "study",
    "DISEASE/TRAIT": "studyDiseaseTrait",
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
        .withColumn(
            "summarystatsLocation",
            f.concat(
                f.lit(gwas_sumstats_base_uri),
                f.regexp_replace(f.col("_c0"), r"^\.\/", ""),
            ),
        )
        .select(
            f.regexp_extract(f.col("summarystatsLocation"), r"\/(GCST\d+)\/", 1).alias(
                "studyAccession"
            ),
            "summarystatsLocation",
            f.lit(True).alias("hasSumstats"),
        )
        .persist()
    )

    etl.logger.info(
        f"Number of studies with harmonized summary stats: {sumstats.count()}"
    )
    return sumstats


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
        df (DataFrame): gwas studies table with a column called `initialSampleSize`

    Returns:
        DataFrame: df with columns `nCases`, `nControls`, and `nSamples` per `studyAccession`
    """
    return (
        df.select(
            "studyAccession",
            f.explode_outer(f.split(f.col("initialSampleSize"), r",\s+")).alias(
                "samples"
            ),
        )
        # Extracting the sample size from the string:
        .withColumn(
            "sampleSize",
            f.regexp_extract(
                f.regexp_replace(f.col("samples"), ",", ""), r"[0-9,]+", 0
            ).cast(t.IntegerType()),
        )
        .select(
            "studyAccession",
            "sampleSize",
            f.when(f.col("samples").contains("cases"), f.col("sampleSize"))
            .otherwise(f.lit(0))
            .alias("nCases"),
            f.when(f.col("samples").contains("controls"), f.col("sampleSize"))
            .otherwise(f.lit(0))
            .alias("nControls"),
        )
        # Aggregating sample sizes for all ancestries:
        .groupBy("studyAccession")
        .agg(
            f.sum("nCases").alias("nCases"),
            f.sum("nControls").alias("nControls"),
            f.sum("sampleSize").alias("nSamples"),
        )
        .persist()
    )


def spliting_gwas_studies(study_association: DataFrame) -> DataFrame:
    """Splitting studies and consolidating disease annotation.

    Processing disease annotation of the joined study/association table. If assigned disease
    of the study and the association don't agree, we assume the study needs to be split.
    Then disease EFOs, trait names and study i are consolidated

    Args:
        study_association (DataFrame): DataFrame

    Returns:
        A dataframe with the studyAccession, studyId, diseaseTrait, and efos columns.
    """
    print(study_association.columns)
    # A window to aid study splitting:
    study_split_window = Window.partitionBy("studyAccession").orderBy("splitField")

    # A window to detect ambiguous associations:
    assoc_ambiguity_window = Window.partitionBy("studyId", "variantId")
    return (
        study_association
        # As some studies needs to be split by not only the p-value text, but the EFO as well, we need to do this thing:
        .withColumn(
            "splitField",
            f.concat_ws(
                "_",
                f.col("pValueText"),
                f.array_join(f.col("associationEfos"), "_"),
            ),
        )
        # Windowing over the groups:
        .withColumn("row_number", f.dense_rank().over(study_split_window) - 1)
        # Study identifiers are split when there are more than one type of associationEfos:
        .withColumn(
            "studyId",
            f.when(f.col("row_number") == 0, f.col("studyAccession")).otherwise(
                f.concat_ws("_", "studyAccession", "row_number")
            ),
        )
        # Disese traits are generated based on p-value text when splitting study:
        .withColumn(
            "diseaseTrait",
            # When study is split:
            f.when(
                f.col("row_number") != 0,
                f.when(
                    f.col("pValueText").isNotNull(),
                    f.concat(
                        "associationDiseaseTrait", f.lit(" ["), "pValueText", f.lit("]")
                    ),
                ).otherwise("associationDiseaseTrait"),
            )
            # When there's association disease trait:
            .when(
                f.col("associationDiseaseTrait").isNotNull(),
                f.col("associationDiseaseTrait"),
            )
            # When no association disease trait is present we get from study:
            .otherwise(f.col("studyDiseaseTrait")),
        )
        # The EFO field is also consolidated:
        .withColumn(
            "efos",
            # When available, EFOs are pulled from associations:
            f.when(f.col("associationEfos").isNotNull(), f.col("associationEfos"))
            # When no association is given, the study level EFOs are used:
            .otherwise(f.col("studyEfos")),
        )
        # Flagging ambiguous associations:
        .withColumn(
            "variantCountInStudy",
            f.count(f.col("variantId")).over(assoc_ambiguity_window),
        )
        .withColumn(
            "qualityControl",
            f.when(
                f.col("variantCountInStudy") > 1,
                f.array_union(
                    f.col("qualityControl"), f.array(f.lit("ambiguous association"))
                ),
            ).otherwise(f.col("qualityControl")),
        )
        .drop(
            "row_number",
            "studyAccession",
            "studyEfos",
            "studyDiseaseTrait",
            "associationEfos",
            "associationDiseaseTrait",
            "pValueText",
            # 'full_description'
        )
    )


def parse_efos(col_name: str) -> Column:
    """Extracting EFO identifiers.

    This function parses EFO identifiers from a comma-separated list of EFO URIs.

    Args:
        col_name (str): name of column with a list of EFO IDs

    Returns:
        Column: column with a sorted list of parsed EFO IDs
    """
    return f.array_sort(f.expr(f"regexp_extract_all({col_name}, '([A-Z]+_[0-9]+)')"))


def column2camel_case(col_name: str) -> str:
    """A helper function to convert column names to camel cases.

    Args:
        col_name (str): a single column name

    Returns:
        str: spark expression to select and rename the column
    """

    def string2camelcase(col_name: str) -> str:
        """Converting a string to camelcase.

        Args:
            col_name (str): a random string

        Returns:
            str: Camel cased string
        """
        # Removing a bunch of unwanted characters from the column names:
        col_name = re.sub(r"[\/\(\)\-]+", " ", col_name)

        first, *rest = col_name.split(" ")
        return "".join([first.lower(), *map(str.capitalize, rest)])

    return f"`{col_name}` as {string2camelcase(col_name)}"


def parse_gnomad_ancestries(study_ancestry: DataFrame) -> DataFrame:
    """Generate sample fractions of GnomAD pouplation for studies.

    It takes a dataframe with study accessions and discovery ancestries and returns a dataframe with study ancestries mapped to
    gnomAD ancestries + fractional sample sizes.

    # Available GnomAD populations:
    afr - African/African american
    amr - Latino/Admixed American
    asj - Ashkenazi Jewish
    eas - East Asian
    fin - Finnish
    nfe - Non-Finnish European
    est - Estonian
    nwe - North-West European
    seu - Southern European

    Args:
        study_ancestry (DataFrame): DataFrame

    Returns:
        A dataframe with study accession, gnomad population, sample size, and relative sample size.
    """
    pop_map = {
        "European": "nfe",
        "African American or Afro-Caribbean": "afr",
        "Native American": "amr",
        "Asian unspecified": "eas",
        "Hispanic or Latin American": "amr",
        "East Asian": "eas",
        "Central Asian": "seu",  # Was EUR
        "Oceanian": "eas",
        "South East Asian": "eas",
        "Other admixed ancestry": "nfe",
        "African unspecified": "afr",
        "Sub-Saharan African": "afr",
        "Greater Middle Eastern (Middle Eastern North African or Persian)": "seu",  # Was EUR
        "Aboriginal Australian": "eas",
        "Other": "nfe",
        "South Asian": "eas",  # Was SAS
        "NR": "nfe",
    }

    # Expression to map GWAS Catalog ancestries to GnomAD reference panels:
    pop_mapping_expr = f.create_map(*[f.lit(x) for x in chain(*pop_map.items())])

    # Windowing throught all study accessions, while ordering by association EFOs:
    window_spec = Window.partitionBy("studyAccession")

    return (
        study_ancestry.filter(f.col("discoverySamples").isNotNull())
        # Exploding discoverSample object:
        .select(
            "studyAccession",
            f.explode_outer(f.col("discoverySamples")).alias("discoverySample"),
        )
        # Splitting ancestry further:
        .withColumn(
            "ancestry",
            f.split(
                f.regexp_replace(
                    f.col("discoverySample.ancestry"), "ern, Nor", "ern Nor"
                ),
                ", ",
            ),
        )
        # Splitting sample sizes evenly for composite ancestries:
        .withColumn(
            "sampleSize",
            f.col("discoverySample.sampleSize") / f.size(f.col("ancestry")),
        )
        # Exploding ancestries:
        .withColumn("ancestry", f.explode_outer(f.col("ancestry")))
        .withColumn("mapped_population", pop_mapping_expr.getItem(f.col("ancestry")))
        .groupBy("studyAccession", "mapped_population")
        .agg(f.sum(f.col("sampleSize")).alias("sampleSize"))
        # Get relative sample sizes:
        .withColumn(
            "relativeSampleSize",
            f.col("sampleSize") / f.sum(f.col("sampleSize")).over(window_spec),
        )
        # Group by studies and get a list of stucts with ancestries:
        .groupBy("studyAccession")
        .agg(
            f.collect_set(
                f.struct(
                    f.col("relativeSampleSize").alias("relativeSampleSize"),
                    f.col("mapped_population").alias("gnomadPopulation"),
                )
            ).alias("gnomadSamples")
        )
        .orderBy("studyAccession")
        .persist()
    )


def parse_ancestries(etl: ETLSession, ancestry_file: str) -> DataFrame:
    """Extracting sample sizes and ancestry information.

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
        .persist()
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
            "initialSampleCountOther",
            f.when(f.col("other").isNull(), f.lit(0)).otherwise(f.col("other")),
        )
        .withColumn(
            "initialSampleCount", f.col("initialSampleCountEuropean") + f.col("other")
        )
        .drop("european", "other", "initialSampleCountOther")
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
    # Read GWAS Catalogue raw data
    gwas_studies = read_study_table(etl, study_file)
    gwas_ancestries = parse_ancestries(etl, ancestry_file)
    ss_studies = get_sumstats_location(etl, summary_stats_list)

    # Sample size extraction is a separate process:
    study_size_df = extract_discovery_sample_sizes(gwas_studies)

    # Mapping ancestries of the discover sampe description to GnomAD:
    # gnomad_ancestries = parse_gnomad_ancestries(
    #     gwas_ancestries.select("studyAccession", "discoverySamples").distinct()
    # )

    return (
        gwas_studies
        # Add study sizes:
        .join(study_size_df, on="studyAccession", how="left")
        # Adding GnomAD ancestry mapping:
        # .join(gnomad_ancestries, on="studyAccession", how="left")
        # Adding summary stats location:
        .join(ss_studies, on="studyAccession", how="left")
        .withColumn("hasSumstats", f.coalesce(f.col("hasSumstats"), f.lit(False)))
        .join(gwas_ancestries, on="studyAccession", how="left")
        # Select relevant columns:
        .select(
            "studyAccession",
            # Publication related fields:
            "pubmedId",
            "firstAuthor",
            "publicationDate",
            "journal",
            "study",
            # Disease related fields:
            "studyDiseaseTrait",
            parse_efos("mappedTraitUri").alias("studyEfos"),
            parse_efos("mappedBackgroundTraitUri").alias("backgroundEfos"),
            # Sample related fields:
            "initialSampleSize",
            "nCases",
            "nControls",
            "nSamples",
            # Ancestry related labels:
            "discoverySamples",
            "replicationSamples",
            # "gnomadSamples",
            # Summary stats fields:
            "summarystatsLocation",
            "hasSumstats",
        )
        .persist()
    )


def generate_study_table(association_study: DataFrame) -> DataFrame:
    """Extracting studies from the joined study/association table.

    Args:
        association_study (DataFrame): DataFrame with both associations and studies.

    Returns:
        A dataframe with the columns specified in the study_columns list.
    """
    study_columns = [
        "studyId",
        "diseaseTrait",
        "efos",
        "pubmedId",
        "firstAuthor",
        "publicationDate",
        "journal",
        "study",
        "backgroundEfos",
        "initialSampleSize",
        "nCases",
        "nControls",
        "nSamples",
        "discoverySamples",
        "replicationSamples",
        # "gnomadSamples",
        "summarystatsLocation",
        "hasSumstats",
    ]

    return association_study.select(*study_columns).distinct().persist()
