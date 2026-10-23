"""One-off conversion of released SuSiE-inf credible sets to CredibleSet and CredibleSetVariant.

Temporary: delete once the GWAS Catalog and UKB PPP SuSiE-inf credible sets are converted.

The input is the credible set QC output of gentropy 2.x/3.x: a `StudyLocus` with one row per
credible set and its members in the `locus` array. The output is the two datasets of the
current schema, written to `<output>/credible_set` and `<output>/credible_set_variant`.

The job depends on PySpark only, so it runs on a plain Dataproc cluster. The target schemas are
copied below from `src/gentropy/assets/schemas/credible_set{,_variant}.json`, and the
identifier is computed as `CredibleSet.generate_identifier(CredibleSet.ID_COLUMNS)` does.

Mapping:

- From the old credible set: `studyId`, `chromosome`, `locusStart`, `locusEnd`, the purity,
  `qualityControls` (`OUT_OF_SAMPLE_LD`), and `logBayesFactor` = `credibleSetlog10BF` · ln 10
  (SuSiE-inf's `lbf_l`, already over the whole region).
- From each `locus` member: `posteriorProbability` (alpha), `logBayesFactor` (the variant's
  natural log Bayes factor) and `conditionalBeta` (mu). `conditionalStandardError` (1/sqrt(omega))
  and `r2Overall` were never stored, so they stay null.
- From the study index: `studyType`. A credible set of a study missing from it is dropped, as the
  fine-mapper writes nothing for such a study.
- From the harmonised summary statistics, joined on study and variant: `pValueMantissa`,
  `pValueExponent`, `reportedBeta`, `reportedStandardError`, `effectAlleleFrequency` and
  `sampleSize`. A variant with several rows in a study counts as missing, as the fine-mapper drops
  such variants. A member without statistics is flagged `NOT_IN_LOCUS`; one with an allele
  frequency, `EAF_FROM_SOURCE`.
- The lead is recomputed as the member with the highest posterior probability, ties broken by
  variant id, and `id` hashes the new key.

The job ends with checks and fails when one does not hold.
"""

# ruff: noqa: T201  (command-line script: prints are its report)
from __future__ import annotations

import argparse
import math
import sys

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql import types as t

FINE_MAPPING_METHOD = "SuSiE-inf"
NOT_IN_LOCUS = "Variant in LD with the lead has no statistics in the locus"
EAF_FROM_SOURCE = "Allele frequency reported by the study"
ID_COLUMNS = (
    "studyId",
    "chromosome",
    "locusStart",
    "locusEnd",
    "leadVariantId",
    "fineMappingMethod",
)

# src/gentropy/assets/schemas/credible_set.json
CREDIBLE_SET_SCHEMA = t.StructType(
    [
        t.StructField("id", t.StringType(), False),
        t.StructField("chromosome", t.StringType(), False),
        t.StructField("locusStart", t.IntegerType(), False),
        t.StructField("locusEnd", t.IntegerType(), False),
        t.StructField("studyId", t.StringType(), False),
        t.StructField("studyType", t.StringType(), False),
        t.StructField("leadVariantId", t.StringType(), False),
        t.StructField("fineMappingMethod", t.StringType(), False),
        t.StructField("logBayesFactor", t.DoubleType(), True),
        t.StructField("confidence", t.ByteType(), True),
        t.StructField("purityMeanR2", t.DoubleType(), True),
        t.StructField("purityMinR2", t.DoubleType(), True),
        t.StructField("qualityControls", t.ArrayType(t.StringType(), False), False),
    ]
)

# src/gentropy/assets/schemas/credible_set_variant.json
CREDIBLE_SET_VARIANT_SCHEMA = t.StructType(
    [
        t.StructField("credibleSetId", t.StringType(), False),
        t.StructField("variantId", t.StringType(), False),
        t.StructField("pValueMantissa", t.FloatType(), True),
        t.StructField("pValueExponent", t.IntegerType(), True),
        t.StructField("effectAlleleFrequency", t.FloatType(), True),
        t.StructField("sampleSize", t.IntegerType(), True),
        t.StructField("reportedBeta", t.DoubleType(), True),
        t.StructField("reportedStandardError", t.DoubleType(), True),
        t.StructField("posteriorProbability", t.FloatType(), False),
        t.StructField("logBayesFactor", t.FloatType(), True),
        t.StructField("r2Overall", t.FloatType(), True),
        t.StructField("conditionalBeta", t.FloatType(), True),
        t.StructField("conditionalStandardError", t.FloatType(), True),
        t.StructField("rescaledAbsBeta", t.FloatType(), True),
        t.StructField("rescaledStandardError", t.FloatType(), True),
        t.StructField("rescaledDirection", t.ByteType(), True),
        t.StructField("qualityControls", t.ArrayType(t.StringType(), False), False),
    ]
)

SUMSTAT_COLUMNS = {
    "pValueMantissa": t.FloatType(),
    "pValueExponent": t.IntegerType(),
    "beta": t.DoubleType(),
    "standardError": t.DoubleType(),
    "effectAlleleFrequencyFromSource": t.FloatType(),
    "sampleSize": t.IntegerType(),
}


def generate_identifier(columns: tuple[str, ...]) -> Column:
    """The md5 of the columns as one JSON object, as `Dataset.generate_identifier`.

    Args:
        columns (tuple[str, ...]): Columns defining the key, in order.

    Returns:
        Column: The identifier.
    """
    return f.md5(f.to_json(f.struct(*columns), {"ignoreNullFields": "false"}))


def read_summary_statistics(
    spark: SparkSession, template: str, study_ids: list[str]
) -> DataFrame:
    """Read the harmonised summary statistics of the given studies only.

    Args:
        spark (SparkSession): Spark session.
        template (str): Path of one study, with `{studyId}`. A Hive partition such as
            `.../studyId={studyId}` is read with its base path, so `studyId` comes from the path.
        study_ids (list[str]): Studies to read.

    Returns:
        DataFrame: `studyId`, `variantId` and `SUMSTAT_COLUMNS`, one row per study and variant
            that occurs once in the study.
    """
    paths = [template.format(studyId=study_id) for study_id in study_ids]
    reader = spark.read
    if "studyId={studyId}" in template:
        reader = reader.option(
            "basePath", template.split("studyId={studyId}", maxsplit=1)[0]
        )
    sumstats = reader.parquet(*paths)
    columns = [
        (
            f.col(name).cast(dtype)
            if name in sumstats.columns
            else f.lit(None).cast(dtype)
        ).alias(name)
        for name, dtype in SUMSTAT_COLUMNS.items()
    ]
    once = Window.partitionBy("studyId", "variantId")
    return (
        sumstats.select(f.col("studyId").cast("string"), "variantId", *columns)
        .withColumn("_rows", f.count(f.lit(1)).over(once))
        .filter(f.col("_rows") == 1)
        .drop("_rows")
    )


def existing_paths(
    spark: SparkSession, template: str, study_ids: list[str]
) -> list[str]:
    """The studies whose summary statistics path exists.

    Args:
        spark (SparkSession): Spark session.
        template (str): Path of one study, with `{studyId}`.
        study_ids (list[str]): Studies to look for.

    Returns:
        list[str]: Studies with a summary statistics path.
    """
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    found = []
    for study_id in study_ids:
        path = jvm.org.apache.hadoop.fs.Path(template.format(studyId=study_id))
        if path.getFileSystem(conf).exists(path):
            found.append(study_id)
    return found


def convert(
    old: DataFrame, study_index: DataFrame, sumstats: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Convert old SuSiE-inf credible sets into credible sets and their variants.

    Args:
        old (DataFrame): Old `StudyLocus` credible sets.
        study_index (DataFrame): Study index, for `studyType`.
        sumstats (DataFrame): Summary statistics from `read_summary_statistics`.

    Returns:
        tuple[DataFrame, DataFrame]: Credible sets and credible set variants, in the target
            schemas.
    """
    study_types = study_index.select("studyId", "studyType").distinct()
    members = (
        old.join(study_types.withColumnRenamed("studyType", "_studyType"), "studyId")
        .select(
            f.col("studyLocusId").alias("_oldId"),
            "studyId",
            f.coalesce("_studyType", "studyType").alias("studyType"),
            "chromosome",
            "locusStart",
            "locusEnd",
            f.lit(FINE_MAPPING_METHOD).alias("fineMappingMethod"),
            (f.col("credibleSetlog10BF") * math.log(10)).alias(
                "credibleSetLogBayesFactor"
            ),
            "purityMeanR2",
            "purityMinR2",
            f.coalesce("qualityControls", f.array()).alias(
                "credibleSetQualityControls"
            ),
            f.inline("locus"),
        )
        .select(
            "*",
            f.col("logBF").alias("logBayesFactor"),
            f.col("beta").alias("conditionalBeta"),
        )
        .drop(
            "logBF",
            "beta",
            "is95CredibleSet",
            "is99CredibleSet",
            "pValueMantissa",
            "pValueExponent",
            "standardError",
            "r2Overall",
        )
        .join(
            sumstats.withColumn("_inSumstats", f.lit(True)),
            ["studyId", "variantId"],
            "left",
        )
    )
    credible_set = Window.partitionBy("_oldId")
    is_lead = (
        f.row_number().over(
            credible_set.orderBy(f.desc("posteriorProbability"), "variantId")
        )
        == 1
    )
    members = (
        members.withColumn(
            "leadVariantId",
            f.first(f.when(is_lead, f.col("variantId")), ignorenulls=True).over(
                credible_set
            ),
        )
        .withColumn("id", generate_identifier(ID_COLUMNS))
        .withColumns(
            {
                "reportedBeta": f.col("beta"),
                "reportedStandardError": f.col("standardError"),
                "effectAlleleFrequency": f.col("effectAlleleFrequencyFromSource"),
                "qualityControls": f.when(
                    f.col("_inSumstats").isNull(), f.array(f.lit(NOT_IN_LOCUS))
                )
                .when(
                    f.col("effectAlleleFrequency").isNotNull(),
                    f.array(f.lit(EAF_FROM_SOURCE)),
                )
                .otherwise(f.array()),
            }
        )
        .persist()
    )

    def projected(
        df: DataFrame, schema: t.StructType, renamed: dict[str, str]
    ) -> DataFrame:
        """Project the members onto a target schema, casting each column.

        Args:
            df (DataFrame): Members.
            schema (t.StructType): Target schema.
            renamed (dict[str, str]): Source column of a target field, when they differ.

        Returns:
            DataFrame: The target columns, in schema order.
        """
        columns = []
        for field in schema.fields:
            source = renamed.get(field.name, field.name)
            column = f.col(source) if source in df.columns else f.lit(None)
            if field.name == "qualityControls":
                # Spark cannot cast to non-null elements; coalesce makes them non-null.
                column = f.transform(
                    column.cast(t.ArrayType(t.StringType())),
                    lambda flag: f.coalesce(flag, f.lit("")),
                )
            columns.append(column.cast(field.dataType).alias(field.name))
        return df.select(*columns)

    credible_sets = projected(
        members.filter(f.col("variantId") == f.col("leadVariantId")),
        CREDIBLE_SET_SCHEMA,
        {
            "logBayesFactor": "credibleSetLogBayesFactor",
            "qualityControls": "credibleSetQualityControls",
        },
    )
    variants = projected(members, CREDIBLE_SET_VARIANT_SCHEMA, {"credibleSetId": "id"})
    return credible_sets, variants


def check(  # noqa: C901
    old: DataFrame, credible_sets: DataFrame, variants: DataFrame, dropped_studies: int
) -> list[str]:
    """Check the converted datasets against the input and each other.

    Args:
        old (DataFrame): Old credible sets, restricted to studies in the study index.
        credible_sets (DataFrame): Converted credible sets, as written.
        variants (DataFrame): Converted credible set variants, as written.
        dropped_studies (int): Old credible sets dropped for a study missing from the index.

    Returns:
        list[str]: Failed checks; empty when all pass.
    """
    failures = []
    n_old = old.count()
    n_members_old = old.select(f.sum(f.size("locus"))).first()[0] or 0
    n_sets = credible_sets.count()
    n_variants = variants.count()
    stats = {
        "old credible sets kept": n_old,
        "old credible sets dropped (study not in index)": dropped_studies,
        "credible sets": n_sets,
        "credible set variants": n_variants,
        "distinct credible set ids": credible_sets.select("id").distinct().count(),
        "variants without a credible set": variants.join(
            credible_sets, variants.credibleSetId == credible_sets.id, "left_anti"
        ).count(),
        "credible sets without their lead as a variant": credible_sets.join(
            variants,
            (variants.credibleSetId == credible_sets.id)
            & (variants.variantId == credible_sets.leadVariantId),
            "left_anti",
        ).count(),
        "duplicate (credibleSetId, variantId)": n_variants
        - variants.select("credibleSetId", "variantId").distinct().count(),
        "variants NOT_IN_LOCUS": variants.filter(
            f.array_contains("qualityControls", NOT_IN_LOCUS)
        ).count(),
        "leads NOT_IN_LOCUS": credible_sets.join(
            variants.filter(f.array_contains("qualityControls", NOT_IN_LOCUS)),
            (variants.credibleSetId == credible_sets.id)
            & (variants.variantId == credible_sets.leadVariantId),
        ).count(),
    }
    for field in CREDIBLE_SET_SCHEMA.fields:
        if not field.nullable:
            stats[f"credible_set nulls in {field.name}"] = credible_sets.filter(
                f.col(field.name).isNull()
            ).count()
    for field in CREDIBLE_SET_VARIANT_SCHEMA.fields:
        if not field.nullable:
            stats[f"credible_set_variant nulls in {field.name}"] = variants.filter(
                f.col(field.name).isNull()
            ).count()
    for name, value in stats.items():
        print(f"CHECK {name}: {value}")

    if n_sets != n_old:
        failures.append(
            f"{n_sets} credible sets written for {n_old} input credible sets"
        )
    if n_variants != n_members_old:
        failures.append(
            f"{n_variants} variants written for {n_members_old} input members"
        )
    if stats["distinct credible set ids"] != n_sets:
        failures.append("credible set ids are not unique")
    for name in (
        "variants without a credible set",
        "credible sets without their lead as a variant",
        "duplicate (credibleSetId, variantId)",
    ):
        if stats[name]:
            failures.append(name)
    failures += [
        name for name, value in stats.items() if " nulls in " in name and value
    ]
    if list(credible_sets.columns) != CREDIBLE_SET_SCHEMA.fieldNames():
        failures.append("credible_set columns differ from the schema")
    if list(variants.columns) != CREDIBLE_SET_VARIANT_SCHEMA.fieldNames():
        failures.append("credible_set_variant columns differ from the schema")
    return failures


def main() -> int:
    """Run the conversion.

    Returns:
        int: 0 when every check passes, 1 otherwise.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--credible-sets", required=True, help="Old credible sets (QC output)."
    )
    parser.add_argument("--study-index", required=True)
    parser.add_argument(
        "--summary-statistics",
        required=True,
        help="Path of one study's harmonised summary statistics, with {studyId}.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Writes credible_set and credible_set_variant here.",
    )
    parser.add_argument("--partitions", type=int, default=200)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("susie_credible_set_migration")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()
    )
    old_all = spark.read.parquet(args.credible_sets)
    study_index = spark.read.parquet(args.study_index)
    indexed = study_index.select("studyId").distinct()
    old = old_all.join(indexed, "studyId", "left_semi")
    dropped = old_all.count() - old.count()

    study_ids = sorted(r.studyId for r in old.select("studyId").distinct().collect())
    with_sumstats = existing_paths(spark, args.summary_statistics, study_ids)
    print(
        f"INFO studies: {len(study_ids)}, with summary statistics: {len(with_sumstats)}"
    )
    for study_id in sorted(set(study_ids) - set(with_sumstats))[:20]:
        print(f"INFO no summary statistics for {study_id}")
    members = old.select(
        "studyId", f.explode("locus.variantId").alias("variantId")
    ).distinct()
    sumstats = (
        read_summary_statistics(spark, args.summary_statistics, with_sumstats)
        if with_sumstats
        else spark.createDataFrame(
            [],
            t.StructType(
                [
                    t.StructField("studyId", t.StringType()),
                    t.StructField("variantId", t.StringType()),
                ]
                + [t.StructField(n, d) for n, d in SUMSTAT_COLUMNS.items()]
            ),
        )
    ).join(
        f.broadcast(members) if len(study_ids) < 1000 else members,
        ["studyId", "variantId"],
        "left_semi",
    )

    credible_sets, variants = convert(old, study_index, sumstats)
    out_sets = f"{args.output}/credible_set"
    out_variants = f"{args.output}/credible_set_variant"
    credible_sets.repartition(max(1, args.partitions // 4)).write.mode(
        "overwrite"
    ).parquet(out_sets)
    variants.repartition(args.partitions).write.mode("overwrite").parquet(out_variants)

    failures = check(
        old,
        spark.read.parquet(out_sets),
        spark.read.parquet(out_variants),
        dropped,
    )
    for failure in failures:
        print(f"FAILED {failure}")
    print("ALL CHECKS PASSED" if not failures else f"{len(failures)} CHECKS FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
