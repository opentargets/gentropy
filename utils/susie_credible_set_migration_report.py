"""Compare migrated credible sets with the original SuSiE-inf credible sets, as a markdown report.

Temporary: delete with the migration scripts. Runs locally with gentropy on downloaded copies:

    python utils/susie_credible_set_migration_report.py --name <source> \
        --old <old credible_set_clean> --old-study-index <study_index> \
        --new <output with credible_set, credible_set_variant, study> >> report.md

The old credible sets are read as plain parquet; the new datasets through the gentropy classes
(schema validation). Old and new credible sets are paired on study, lead and bounds, and their
members on credible set and variant. Lead marginals are checked against the old credible set's
lead z-score, which the fine-mapper computed as beta / standard error from the same summary
statistics.
"""

# ruff: noqa: T201  (command-line script: prints are its report)
from __future__ import annotations

import argparse
import math
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.study_index import StudyIndex


def table(rows: list[tuple[str, object]], header: tuple[str, str]) -> str:
    """A two-column markdown table.

    Args:
        rows (list[tuple[str, object]]): Label and value.
        header (tuple[str, str]): Column titles.

    Returns:
        str: The table.
    """
    lines = [f"| {header[0]} | {header[1]} |", "|---|---|"]
    lines += [f"| {label} | {value} |" for label, value in rows]
    return "\n".join(lines)


def fmt(value: object) -> str:
    """Format a number for the report.

    Args:
        value (object): Number or other value.

    Returns:
        str: Thousands separators for integers, 3 significant digits for floats.
    """
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return "0" if value == 0 else f"{value:.3g}"
    return "null" if value is None else str(value)


def stats(df: DataFrame, column: str) -> str:
    """Median, 99th percentile and maximum of a column.

    Args:
        df (DataFrame): Data.
        column (str): Numeric column.

    Returns:
        str: The summary.
    """
    row = df.select(
        f.percentile_approx(column, [0.5, 0.99], 10000).alias("q"),
        f.max(column).alias("max"),
    ).first()
    if row is None or row["q"] is None:
        return "no rows"
    return (
        f"median {fmt(row['q'][0])}, 99th pct {fmt(row['q'][1])}, max {fmt(row['max'])}"
    )


def main() -> int:  # noqa: C901, PLR0915
    """Write the report section of one source.

    Returns:
        int: 0.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--old", required=True)
    parser.add_argument("--old-study-index", required=True)
    parser.add_argument("--new", required=True)
    args = parser.parse_args()

    session = Session(extended_spark_conf={"spark.driver.memory": "12g"})
    old = session.spark.read.parquet(args.old)
    old_index = session.spark.read.parquet(args.old_study_index)
    sets = CredibleSet.from_parquet(session, f"{args.new}/credible_set").df
    variants = CredibleSetVariant.from_parquet(
        session, f"{args.new}/credible_set_variant"
    ).df
    study = StudyIndex.from_parquet(session, f"{args.new}/study").df
    # The study index is copied as is: compare the stored files, column for column.
    study_raw = session.spark.read.parquet(f"{args.new}/study")
    out: list[str] = [f"### {args.name}", ""]

    # Credible sets.
    pairs = (
        old.alias("o")
        .join(
            sets.alias("n"),
            (f.col("o.studyId") == f.col("n.studyId"))
            & (f.col("o.variantId") == f.col("n.leadVariantId"))
            & (f.col("o.locusStart") == f.col("n.locusStart"))
            & (f.col("o.locusEnd") == f.col("n.locusEnd")),
            "full",
        )
        .persist()
    )
    m = pairs.filter(f.col("o.studyLocusId").isNotNull() & f.col("n.id").isNotNull())
    n_old, n_new = old.count(), sets.count()
    bf_diff = m.select(
        (
            f.abs(
                f.col("n.logBayesFactor") - f.col("o.credibleSetlog10BF") * math.log(10)
            )
            / f.greatest(f.lit(1.0), f.abs(f.col("n.logBayesFactor")))
        ).alias("d")
    )
    type_mismatch = (
        sets.join(
            old_index.select("studyId", f.col("studyType").alias("t")),
            "studyId",
            "left",
        )
        .filter(~f.col("studyType").eqNullSafe(f.col("t")))
        .count()
    )
    out += [
        "**Credible sets**",
        "",
        table(
            [
                ("credible sets, old → new", f"{fmt(n_old)} → {fmt(n_new)}"),
                (
                    "studies, old → new",
                    f"{fmt(old.select('studyId').distinct().count())} → "
                    f"{fmt(sets.select('studyId').distinct().count())}",
                ),
                ("paired one to one on study, lead and bounds", fmt(m.count())),
                (
                    "old without a new credible set",
                    fmt(pairs.filter(f.col("n.id").isNull()).count()),
                ),
                (
                    "new without an old credible set",
                    fmt(pairs.filter(f.col("o.studyLocusId").isNull()).count()),
                ),
                ("distinct ids", fmt(sets.select("id").distinct().count())),
                (
                    "id = md5(CredibleSet.ID_COLUMNS), mismatches",
                    fmt(
                        sets.filter(
                            f.col("id")
                            != CredibleSet.generate_identifier(CredibleSet.ID_COLUMNS)
                        ).count()
                    ),
                ),
                (
                    "logBayesFactor vs log10 BF · ln 10, relative difference",
                    stats(bf_diff, "d"),
                ),
                (
                    "chromosome or purity changed",
                    fmt(
                        m.filter(
                            (f.col("n.chromosome") != f.col("o.chromosome"))
                            | ~f.col("n.purityMinR2").eqNullSafe(f.col("o.purityMinR2"))
                            | ~f.col("n.purityMeanR2").eqNullSafe(
                                f.col("o.purityMeanR2")
                            )
                        ).count()
                    ),
                ),
                (
                    "qualityControls changed",
                    fmt(
                        m.filter(
                            f.array_sort("n.qualityControls")
                            != f.array_sort(
                                f.coalesce(
                                    "o.qualityControls", f.array().cast("array<string>")
                                )
                            )
                        ).count()
                    ),
                ),
                ("studyType differs from the study index", fmt(type_mismatch)),
                (
                    "studyType values",
                    ", ".join(
                        f"{r['studyType']}: {fmt(r['count'])}"
                        for r in sets.groupBy("studyType")
                        .count()
                        .orderBy("studyType")
                        .collect()
                    ),
                ),
                (
                    "fineMappingMethod values",
                    ", ".join(
                        f"{r['fineMappingMethod']}: {fmt(r['count'])}"
                        for r in sets.groupBy("fineMappingMethod").count().collect()
                    ),
                ),
                (
                    "confidence not null",
                    fmt(sets.filter(f.col("confidence").isNotNull()).count()),
                ),
            ],
            ("check", "result"),
        ),
        "",
    ]

    # Credible set variants.
    old_members = m.select(
        f.col("n.id").alias("credibleSetId"),
        f.col("o.studyId").alias("studyId"),
        f.inline("o.locus"),
    )
    vp = (
        old_members.alias("o")
        .join(variants.alias("n"), ["credibleSetId", "variantId"], "full")
        .persist()
    )
    vm = vp.filter(
        f.col("o.studyId").isNotNull() & f.col("n.posteriorProbability").isNotNull()
    )
    n_members = old.select(f.sum(f.size("locus"))).first()[0]
    n_variants = variants.count()

    def max_abs(expr: f.Column) -> str:
        """Largest absolute difference over the paired members.

        Args:
            expr (f.Column): Difference.

        Returns:
            str: Formatted maximum.
        """
        return fmt(vm.select(f.max(f.abs(expr))).first()[0])

    pp_sums = variants.groupBy("credibleSetId").agg(
        f.sum("posteriorProbability").alias("s")
    )
    null_rows = [
        (
            f"`{c}` null",
            f"{fmt(n)} ({n / n_variants:.1%})",
        )
        for c in [
            "pValueMantissa",
            "reportedBeta",
            "reportedStandardError",
            "effectAlleleFrequency",
            "sampleSize",
            "conditionalBeta",
            "conditionalStandardError",
            "r2Overall",
            "rescaledAbsBeta",
        ]
        for n in [variants.filter(f.col(c).isNull()).count()]
    ]
    flags = (
        variants.select(f.explode_outer("qualityControls").alias("flag"))
        .groupBy("flag")
        .count()
        .orderBy("flag")
        .collect()
    )
    out += [
        "**Credible set variants**",
        "",
        table(
            [
                ("old members → new variants", f"{fmt(n_members)} → {fmt(n_variants)}"),
                ("paired one to one on credible set and variant", fmt(vm.count())),
                (
                    "unpaired (old or new side)",
                    fmt(vp.count() - vm.count()),
                ),
                (
                    "duplicate (credibleSetId, variantId)",
                    fmt(
                        n_variants
                        - variants.select("credibleSetId", "variantId")
                        .distinct()
                        .count()
                    ),
                ),
                (
                    "max \\|posteriorProbability − alpha\\|",
                    max_abs(
                        f.col("n.posteriorProbability")
                        - f.col("o.posteriorProbability")
                    ),
                ),
                (
                    "max \\|logBayesFactor − logBF\\| (stored as float)",
                    max_abs(f.col("n.logBayesFactor") - f.col("o.logBF")),
                ),
                (
                    "max \\|conditionalBeta − mu\\| (stored as float)",
                    max_abs(f.col("n.conditionalBeta") - f.col("o.beta")),
                ),
                ("posterior probability sum per credible set", stats(pp_sums, "s")),
                *null_rows,
                (
                    "qualityControls",
                    ", ".join(
                        f"{r['flag'] or 'none'}: {fmt(r['count'])}" for r in flags
                    ),
                ),
            ],
            ("check", "result"),
        ),
        "",
    ]

    # Lead marginals against the old lead z-score (beta / se of the same summary statistics).
    leads = (
        m.select(
            f.col("n.id").alias("credibleSetId"),
            f.col("n.leadVariantId").alias("variantId"),
            f.col("o.zScore").alias("oldZ"),
            (-(f.log10(f.col("o.pValueMantissa")) + f.col("o.pValueExponent"))).alias(
                "oldNeglogp"
            ),
        )
        .join(variants, ["credibleSetId", "variantId"])
        .withColumns(
            {
                "newZ": f.col("reportedBeta") / f.col("reportedStandardError"),
                "newNeglogp": -(
                    f.log10(f.col("pValueMantissa")) + f.col("pValueExponent")
                ),
            }
        )
        .persist()
    )
    with_z = leads.filter(f.col("newZ").isNotNull() & f.col("oldZ").isNotNull())
    z_diff = with_z.select(
        (
            f.abs(f.col("newZ") - f.col("oldZ"))
            / f.greatest(f.lit(1.0), f.abs(f.col("oldZ")))
        ).alias("d")
    )
    p_diff = leads.filter(
        f.col("newNeglogp").isNotNull() & f.col("oldNeglogp").isNotNull()
    ).select(
        (
            f.abs(f.col("newNeglogp") - f.col("oldNeglogp"))
            / f.greatest(f.lit(1.0), f.col("oldNeglogp"))
        ).alias("d")
    )
    out += [
        "**Lead marginals vs the old lead z-score** (both from the same summary statistics)",
        "",
        table(
            [
                (
                    "leads with reportedBeta and reportedStandardError",
                    fmt(with_z.count()),
                ),
                (
                    "relative difference, reportedBeta / reportedStandardError vs old z",
                    stats(z_diff, "d"),
                ),
                (
                    "leads with \\|relative z difference\\| > 1e-3",
                    fmt(z_diff.filter(f.col("d") > 1e-3).count()),
                ),
                (
                    "sign of z changed",
                    fmt(with_z.filter(f.signum("newZ") != f.signum("oldZ")).count()),
                ),
                (
                    "relative difference, reported −log10 p vs old −log10 p (from z)",
                    stats(p_diff, "d"),
                ),
            ],
            ("check", "result"),
        ),
        "",
    ]

    # Study index.
    out += [
        "**Study index**",
        "",
        table(
            [
                (
                    "loads as StudyIndex (gentropy schema validation)",
                    fmt(study.count()) + " rows",
                ),
                (
                    "rows, old → new",
                    f"{fmt(old_index.count())} → {fmt(study_raw.count())}",
                ),
                ("same stored schema", str(old_index.schema == study_raw.schema)),
                (
                    "original columns missing in the new file",
                    ", ".join(
                        c for c in old_index.columns if c not in study_raw.columns
                    )
                    or "none",
                ),
                (
                    "rows missing / extra, on the original columns",
                    f"{fmt(old_index.exceptAll(study_raw.select(old_index.columns)).count())} / "
                    f"{fmt(study_raw.select(old_index.columns).exceptAll(old_index).count())}",
                ),
                (
                    "columns added in the new file (non-null values)",
                    ", ".join(
                        f"{c} ({fmt(study_raw.filter(f.col(c).isNotNull()).count())})"
                        for c in study_raw.columns
                        if c not in old_index.columns
                    )
                    or "none",
                ),
            ],
            ("check", "result"),
        ),
        "",
    ]
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
