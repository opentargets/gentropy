"""Verify the output of utils/susie_credible_set_migration.py with gentropy.

Temporary: delete with the migration scripts. Runs where gentropy is installed, on local copies
of the inputs and outputs:

    python utils/susie_credible_set_migration_check.py \
        --credible-sets <old> --summary-statistics '<dir>/{studyId}' --output <migration output>

Checks:

- both outputs load as `CredibleSet` and `CredibleSetVariant` (gentropy schema validation);
- `id` equals `CredibleSet.generate_identifier(CredibleSet.ID_COLUMNS)`;
- every old credible set has one new credible set, with the same lead, bounds and purity, and
  `logBayesFactor` = `credibleSetlog10BF` · ln 10;
- every old member is a variant of it, with the same posterior probability, `logBayesFactor` =
  `logBF` and `conditionalBeta` = `beta`;
- the marginal statistics equal the summary statistics row of the study and variant.
"""

# ruff: noqa: T201  (command-line script: prints are its report)
from __future__ import annotations

import argparse
import math
import sys

from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant


def main() -> int:
    """Run the checks.

    Returns:
        int: 0 when every check passes, 1 otherwise.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--credible-sets", required=True)
    parser.add_argument("--summary-statistics", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    session = Session(
        extended_spark_conf={
            "spark.sql.sources.partitionColumnTypeInference.enabled": "false"
        }
    )
    spark = session.spark
    sets = CredibleSet.from_parquet(session, f"{args.output}/credible_set")
    variants = CredibleSetVariant.from_parquet(
        session, f"{args.output}/credible_set_variant"
    )
    print("PASS loaded as CredibleSet and CredibleSetVariant")

    failures: list[str] = []

    def expect(name: str, bad: int) -> None:
        """Report one check and record it when it fails.

        Args:
            name (str): The check.
            bad (int): Rows failing it.
        """
        print(f"{'PASS' if bad == 0 else 'FAIL'} {name}: {bad} mismatches")
        if bad:
            failures.append(name)

    expect(
        "id hashes CredibleSet.ID_COLUMNS",
        sets.df.filter(
            f.col("id") != CredibleSet.generate_identifier(CredibleSet.ID_COLUMNS)
        ).count(),
    )

    old = spark.read.parquet(args.credible_sets)
    pairs = old.alias("o").join(
        sets.df.alias("n"),
        (f.col("o.studyId") == f.col("n.studyId"))
        & (f.col("o.variantId") == f.col("n.leadVariantId"))
        & (f.col("o.locusStart") == f.col("n.locusStart"))
        & (f.col("o.locusEnd") == f.col("n.locusEnd")),
        "full",
    )
    expect(
        "old and new credible sets pair one to one on study, lead and bounds",
        pairs.filter(f.col("o.studyLocusId").isNull() | f.col("n.id").isNull()).count(),
    )
    matched = pairs.filter(
        f.col("o.studyLocusId").isNotNull() & f.col("n.id").isNotNull()
    )
    expect(
        "credible set log Bayes factor = log10 BF · ln 10",
        matched.filter(
            f.abs(
                f.col("n.logBayesFactor") - f.col("o.credibleSetlog10BF") * math.log(10)
            )
            > 1e-6 * f.greatest(f.lit(1.0), f.abs(f.col("n.logBayesFactor")))
        ).count(),
    )
    expect(
        "purity and chromosome unchanged",
        matched.filter(
            (f.col("n.purityMinR2") != f.col("o.purityMinR2"))
            | (f.col("n.purityMeanR2") != f.col("o.purityMeanR2"))
            | (f.col("n.chromosome") != f.col("o.chromosome"))
        ).count(),
    )
    expect(
        "fineMappingMethod is SuSiE-inf",
        sets.df.filter(f.col("fineMappingMethod") != "SuSiE-inf").count(),
    )

    old_members = matched.select(
        f.col("n.id").alias("credibleSetId"),
        f.col("o.studyId").alias("studyId"),
        f.inline("o.locus"),
    )
    member_pairs = old_members.alias("o").join(
        variants.df.alias("n"), ["credibleSetId", "variantId"], "full"
    )
    expect(
        "old members and new variants pair one to one",
        member_pairs.filter(
            f.col("o.studyId").isNull() | f.col("n.posteriorProbability").isNull()
        ).count(),
    )
    expect(
        "posterior probability, variant log BF and conditionalBeta = alpha, logBF and mu",
        member_pairs.filter(
            (
                f.abs(f.col("n.posteriorProbability") - f.col("o.posteriorProbability"))
                > 1e-6
            )
            | (
                f.abs(f.col("n.logBayesFactor") - f.col("o.logBF"))
                > 1e-3 * f.greatest(f.lit(1.0), f.abs(f.col("o.logBF")))
            )
            | (
                f.abs(f.col("n.conditionalBeta") - f.col("o.beta"))
                > 1e-6 * f.greatest(f.lit(1.0), f.abs(f.col("o.beta")))
            )
        ).count(),
    )

    study_ids = [r.studyId for r in old.select("studyId").distinct().collect()]
    template = args.summary_statistics
    reader = spark.read
    if "studyId={studyId}" in template:
        reader = reader.option("basePath", template.split("studyId={studyId}")[0])
    sumstats = reader.parquet(*[template.format(studyId=s) for s in study_ids])
    eaf = (
        f.col("s.effectAlleleFrequencyFromSource")
        if "effectAlleleFrequencyFromSource" in sumstats.columns
        else f.lit(None).cast("float")
    )
    marginal = (
        variants.df.alias("n")
        .join(
            sets.df.select("id", "studyId").alias("c"),
            f.col("n.credibleSetId") == f.col("c.id"),
        )
        .join(
            sumstats.alias("s"),
            (f.col("c.studyId") == f.col("s.studyId"))
            & (f.col("n.variantId") == f.col("s.variantId")),
        )
    )
    expect(
        "marginal statistics = summary statistics row",
        marginal.filter(
            ~f.col("n.pValueMantissa").eqNullSafe(
                f.col("s.pValueMantissa").cast("float")
            )
            | ~f.col("n.pValueExponent").eqNullSafe(f.col("s.pValueExponent"))
            | ~f.col("n.reportedBeta").eqNullSafe(f.col("s.beta"))
            | ~f.col("n.reportedStandardError").eqNullSafe(f.col("s.standardError"))
            | ~f.col("n.sampleSize").eqNullSafe(f.col("s.sampleSize"))
            | ~f.col("n.effectAlleleFrequency").eqNullSafe(eaf)
        ).count(),
    )
    found = marginal.select("n.credibleSetId", "n.variantId").distinct().count()
    print(f"INFO variants with summary statistics: {found} of {variants.df.count()}")
    print(f"INFO credible sets: {sets.df.count()}, variants: {variants.df.count()}")
    sets.df.show(truncate=40)
    variants.df.show(5, truncate=30)

    print("ALL CHECKS PASSED" if not failures else f"{len(failures)} CHECKS FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
