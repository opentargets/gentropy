"""Verify the output of utils/susie_credible_set_migration.py with gentropy, column by column.

Temporary: delete with the migration scripts. Runs where gentropy is installed, on local copies
of the inputs and outputs:

    python utils/susie_credible_set_migration_check.py \
        --credible-sets <old> --study-index <study index> \
        --summary-statistics '<dir>/{studyId}' [--summary-statistics ...] \
        --output <migration output>

Summary statistics locations are tried in order, as in the migration job.

Both outputs must load as `CredibleSet` and `CredibleSetVariant` (gentropy schema validation),
and every column of both schemas is compared with the value it should have, derived
independently from the inputs. The script fails when a column has no check.
"""

# ruff: noqa: T201  (command-line script: prints are its report)
from __future__ import annotations

import argparse
import math
import sys
from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.dataset.credible_set import (
    CredibleSet,
    CredibleSetVariant,
    CredibleSetVariantQualityCheck,
)

NOT_IN_LOCUS = CredibleSetVariantQualityCheck.NOT_IN_LOCUS.value
EAF_FROM_SOURCE = CredibleSetVariantQualityCheck.EAF_FROM_SOURCE.value
MARGINALS = (
    "pValueMantissa",
    "pValueExponent",
    "beta",
    "standardError",
    "effectAlleleFrequencyFromSource",
    "sampleSize",
)


def close(actual: str, expected: Column, relative: float) -> Column:
    """Whether a numeric column equals its expected value, nulls included.

    Args:
        actual (str): Column of the output.
        expected (Column): Expected value.
        relative (float): Relative tolerance.

    Returns:
        Column: True when both are null or they agree within the tolerance.
    """
    a = f.col(actual)
    return (a.isNull() & expected.isNull()) | f.coalesce(
        f.abs(a - expected) <= relative * f.greatest(f.lit(1.0), f.abs(expected)),
        f.lit(False),
    )


def same(actual: str, expected: Column) -> Column:
    """Whether a column equals its expected value, nulls included.

    Args:
        actual (str): Column of the output.
        expected (Column): Expected value.

    Returns:
        Column: True when both are null or equal.
    """
    return f.col(actual).eqNullSafe(expected)


def expected_marginals(raw: DataFrame) -> DataFrame:
    """One expected row per study and variant whose summary statistics rows share the p-value.

    Of rows with the same p-value, the expected one has the largest sample size, then an allele
    frequency, then the smallest standard error, then the smallest beta.

    Args:
        raw (DataFrame): Summary statistics rows, as read.

    Returns:
        DataFrame: `studyId`, `variantId` and the marginal columns present in the files.
    """
    columns = [c for c in MARGINALS if c in raw.columns]
    rows = raw.select(f.col("studyId").cast("string"), "variantId", *columns)
    eaf = (
        f.col("effectAlleleFrequencyFromSource")
        if "effectAlleleFrequencyFromSource" in columns
        else f.lit(None)
    )
    p_values = (
        rows.groupBy("studyId", "variantId")
        .agg(
            f.countDistinct("pValueMantissa", "pValueExponent").alias("_p"),
            f.count(f.lit(1)).alias("_rows"),
            f.sum(
                (
                    f.col("pValueMantissa").isNull() | f.col("pValueExponent").isNull()
                ).cast("int")
            ).alias("_nullP"),
        )
        # countDistinct skips rows with a null p-value; they still count as a distinct value.
        .withColumn("_p", f.col("_p") + f.when(f.col("_nullP") > 0, 1).otherwise(0))
        .filter(f.col("_p") == 1)
    )
    best = Window.partitionBy("studyId", "variantId").orderBy(
        f.col("sampleSize").desc_nulls_last(),
        eaf.isNull(),
        f.col("standardError").asc_nulls_last(),
        f.col("beta").asc_nulls_last(),
    )
    return (
        rows.join(p_values.select("studyId", "variantId"), ["studyId", "variantId"])
        .withColumn("_rank", f.row_number().over(best))
        .filter(f.col("_rank") == 1)
        .drop("_rank")
    )


def main() -> int:  # noqa: C901
    """Run the checks.

    Returns:
        int: 0 when every check passes, 1 otherwise.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--credible-sets", required=True)
    parser.add_argument("--study-index", required=True)
    parser.add_argument("--summary-statistics", required=True, action="append")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    session = Session(
        extended_spark_conf={
            "spark.sql.sources.partitionColumnTypeInference.enabled": "false"
        }
    )
    spark = session.spark
    sets = CredibleSet.from_parquet(session, f"{args.output}/credible_set").df
    variants = CredibleSetVariant.from_parquet(
        session, f"{args.output}/credible_set_variant"
    ).df
    print(
        "PASS loads as CredibleSet and CredibleSetVariant (gentropy schema validation)"
    )

    failures: list[str] = []
    covered: dict[str, set[str]] = {
        "credible_set": set(),
        "credible_set_variant": set(),
    }

    def expect(dataset: str, columns: list[str], name: str, bad: int) -> None:
        """Report one check, record the columns it covers, and its failure.

        Args:
            dataset (str): `credible_set` or `credible_set_variant`.
            columns (list[str]): Output columns the check verifies.
            name (str): The check.
            bad (int): Rows failing it.
        """
        covered[dataset].update(columns)
        status = "PASS" if bad == 0 else "FAIL"
        print(f"{status} {dataset}.{'/'.join(columns)}: {name} ({bad} mismatches)")
        if bad:
            failures.append(f"{dataset}: {name}")

    old = spark.read.parquet(args.credible_sets)
    study_index = spark.read.parquet(args.study_index).select("studyId", "studyType")

    # Credible sets: pair old and new on study, old lead and bounds, one to one.
    pairs = old.alias("o").join(
        sets.alias("n"),
        (f.col("o.studyId") == f.col("n.studyId"))
        & (f.col("o.variantId") == f.col("n.leadVariantId"))
        & (f.col("o.locusStart") == f.col("n.locusStart"))
        & (f.col("o.locusEnd") == f.col("n.locusEnd")),
        "full",
    )
    expect(
        "credible_set",
        ["studyId", "locusStart", "locusEnd"],
        "one new credible set per old one, same study, lead and bounds",
        pairs.filter(f.col("o.studyLocusId").isNull() | f.col("n.id").isNull()).count(),
    )
    m = pairs.filter(
        f.col("o.studyLocusId").isNotNull() & f.col("n.id").isNotNull()
    ).select(
        "n.*",
        f.col("o.chromosome").alias("_chromosome"),
        f.col("o.credibleSetlog10BF").alias("_log10bf"),
        f.col("o.purityMeanR2").alias("_purityMean"),
        f.col("o.purityMinR2").alias("_purityMin"),
        f.col("o.qualityControls").alias("_qc"),
        f.col("o.locus").alias("_locus"),
    )
    expect(
        "credible_set",
        ["id"],
        "id = md5 of CredibleSet.ID_COLUMNS",
        sets.filter(
            f.col("id") != CredibleSet.generate_identifier(CredibleSet.ID_COLUMNS)
        ).count(),
    )
    expect(
        "credible_set",
        ["chromosome"],
        "chromosome = old",
        m.filter(~same("chromosome", f.col("_chromosome"))).count(),
    )
    expect(
        "credible_set",
        ["studyType"],
        "studyType = study index",
        m.join(
            study_index.withColumnRenamed("studyType", "_studyType"), "studyId", "left"
        )
        .filter(~same("studyType", f.col("_studyType")))
        .count(),
    )
    top = (
        variants.withColumn(
            "_rank",
            f.row_number().over(
                Window.partitionBy("credibleSetId").orderBy(
                    f.desc("posteriorProbability"), "variantId"
                )
            ),
        )
        .filter(f.col("_rank") == 1)
        .select("credibleSetId", f.col("variantId").alias("_top"))
    )
    expect(
        "credible_set",
        ["leadVariantId"],
        "lead = highest posterior probability, ties by variant id",
        sets.join(top, f.col("id") == f.col("credibleSetId"), "left")
        .filter(~same("leadVariantId", f.col("_top")))
        .count(),
    )
    expect(
        "credible_set",
        ["fineMappingMethod"],
        "fineMappingMethod = SuSiE-inf",
        sets.filter(f.col("fineMappingMethod") != "SuSiE-inf").count(),
    )
    expect(
        "credible_set",
        ["logBayesFactor"],
        "logBayesFactor = credibleSetlog10BF · ln 10",
        m.filter(
            ~close("logBayesFactor", f.col("_log10bf") * math.log(10), 1e-9)
        ).count(),
    )
    expect(
        "credible_set",
        ["confidence"],
        "confidence null (set by validation)",
        sets.filter(f.col("confidence").isNotNull()).count(),
    )
    expect(
        "credible_set",
        ["purityMeanR2", "purityMinR2"],
        "purity = old",
        m.filter(
            ~same("purityMeanR2", f.col("_purityMean"))
            | ~same("purityMinR2", f.col("_purityMin"))
        ).count(),
    )
    expect(
        "credible_set",
        ["qualityControls"],
        "qualityControls = old flags",
        m.filter(
            f.array_sort("qualityControls")
            != f.array_sort(f.coalesce("_qc", f.array().cast("array<string>")))
        ).count(),
    )

    # Variants: pair with the old members, one to one.
    old_members = m.select(
        f.col("id").alias("credibleSetId"), "studyId", f.inline("_locus")
    )
    vp = old_members.alias("o").join(
        variants.alias("n"), ["credibleSetId", "variantId"], "full"
    )
    expect(
        "credible_set_variant",
        ["credibleSetId", "variantId"],
        "one variant per old member, in its credible set",
        vp.filter(
            f.col("o.studyId").isNull() | f.col("n.posteriorProbability").isNull()
        ).count(),
    )
    v = vp.filter(
        f.col("o.studyId").isNotNull() & f.col("n.posteriorProbability").isNotNull()
    ).select(
        "credibleSetId",
        "variantId",
        f.col("o.studyId").alias("studyId"),
        *[
            f.col(f"n.{c}").alias(c)
            for c in variants.columns
            if c not in ("credibleSetId", "variantId")
        ],
        f.col("o.posteriorProbability").alias("_alpha"),
        f.col("o.logBF").alias("_lbf"),
        f.col("o.beta").alias("_mu"),
    )
    expect(
        "credible_set_variant",
        ["posteriorProbability"],
        "posteriorProbability = alpha",
        v.filter(~close("posteriorProbability", f.col("_alpha"), 1e-6)).count(),
    )
    expect(
        "credible_set_variant",
        ["logBayesFactor"],
        "logBayesFactor = variant logBF",
        v.filter(~close("logBayesFactor", f.col("_lbf"), 1e-6)).count(),
    )
    expect(
        "credible_set_variant",
        ["conditionalBeta"],
        "conditionalBeta = mu",
        v.filter(~close("conditionalBeta", f.col("_mu"), 1e-6)).count(),
    )
    null_columns = [
        "conditionalStandardError",
        "r2Overall",
        "rescaledAbsBeta",
        "rescaledStandardError",
        "rescaledDirection",
    ]
    expect(
        "credible_set_variant",
        null_columns,
        "null (never stored, or not filled yet)",
        v.filter(f.greatest(*[f.col(c).isNotNull() for c in null_columns])).count(),
    )

    # Marginals: the summary statistics row when all the study's rows for the variant agree.
    study_ids = [r.studyId for r in old.select("studyId").distinct().collect()]
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()

    def exists(path: str) -> bool:
        """Whether a path exists.

        Args:
            path (str): The path.

        Returns:
            bool: True when it exists.
        """
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        return bool(hadoop_path.getFileSystem(conf).exists(hadoop_path))

    location = {}
    for template in args.summary_statistics:
        for study_id in study_ids:
            if study_id not in location and exists(template.format(studyId=study_id)):
                location[study_id] = template
    print(f"INFO studies with summary statistics: {len(location)} of {len(study_ids)}")
    frames = []
    for template in dict.fromkeys(location.values()):
        reader = spark.read
        if "studyId={studyId}" in template:
            reader = reader.option(
                "basePath", template.split("studyId={studyId}", maxsplit=1)[0]
            )
        frame = reader.parquet(
            *[template.format(studyId=s) for s, t in location.items() if t == template]
        )
        frames.append(
            frame.select(
                f.col("studyId").cast("string"),
                "variantId",
                *[c for c in MARGINALS if c in frame.columns],
            )
        )
    member_keys = v.select("studyId", "variantId").distinct()
    expected = expected_marginals(
        reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames).join(
            f.broadcast(member_keys), ["studyId", "variantId"], "left_semi"
        )
    ).withColumn("_found", f.lit(True))
    present = set(expected.columns)

    def e(name: str) -> Column:
        """An expected marginal column, null when the files do not have it.

        Args:
            name (str): Summary statistics column.

        Returns:
            Column: The expected value.
        """
        return f.col(f"e.{name}") if name in present else f.lit(None)

    with_expected = v.alias("v").join(
        expected.alias("e"), ["studyId", "variantId"], "left"
    )
    expect(
        "credible_set_variant",
        [
            "pValueMantissa",
            "pValueExponent",
            "reportedBeta",
            "reportedStandardError",
            "effectAlleleFrequency",
            "sampleSize",
        ],
        "marginals = summary statistics row (null when absent or p-values differ)",
        with_expected.filter(
            ~same("v.pValueMantissa", e("pValueMantissa").cast("float"))
            | ~same("v.pValueExponent", e("pValueExponent").cast("int"))
            | ~same("v.reportedBeta", e("beta").cast("double"))
            | ~same("v.reportedStandardError", e("standardError").cast("double"))
            | ~same(
                "v.effectAlleleFrequency",
                e("effectAlleleFrequencyFromSource").cast("float"),
            )
            | ~same("v.sampleSize", e("sampleSize").cast("int"))
        ).count(),
    )
    expected_flags = (
        f.when(f.col("e._found").isNull(), f.array(f.lit(NOT_IN_LOCUS)))
        .when(
            e("effectAlleleFrequencyFromSource").isNotNull(),
            f.array(f.lit(EAF_FROM_SOURCE)),
        )
        .otherwise(f.array().cast("array<string>"))
    )
    expect(
        "credible_set_variant",
        ["qualityControls"],
        "NOT_IN_LOCUS without statistics, else EAF_FROM_SOURCE with a frequency",
        with_expected.filter(f.col("v.qualityControls") != expected_flags).count(),
    )

    for dataset, columns in (
        ("credible_set", sets.columns),
        ("credible_set_variant", variants.columns),
    ):
        unchecked = [c for c in columns if c not in covered[dataset]]
        print(
            f"{'PASS' if not unchecked else 'FAIL'} {dataset}: "
            f"all {len(columns)} columns checked {unchecked or ''}"
        )
        if unchecked:
            failures.append(f"{dataset}: unchecked columns {unchecked}")

    n_found = with_expected.filter(f.col("e._found").isNotNull()).count()
    n_flagged = variants.filter(
        f.array_contains("qualityControls", NOT_IN_LOCUS)
    ).count()
    print(
        f"INFO credible sets: {sets.count()}, variants: {variants.count()}, "
        f"with summary statistics: {n_found}, NOT_IN_LOCUS: {n_flagged}"
    )
    print("ALL CHECKS PASSED" if not failures else f"{len(failures)} CHECKS FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
