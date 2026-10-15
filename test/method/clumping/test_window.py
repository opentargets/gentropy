"""Tests for window-based clumping."""

from __future__ import annotations

import hashlib
import random
from collections import defaultdict
from pathlib import Path

import numpy as np
import pytest
from pyspark.ml import functions as fml
from pyspark.ml.linalg import DenseVector, VectorUDT
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from gentropy.dataset.study_locus import StudyLocus, StudyLocusQualityFlag
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.annotation import annotate_locus
from gentropy.method.clumping.window import WindowClumping
from gentropy.model.core.dataset import Dataset
from gentropy.model.core.schema import implements
from gentropy.model.trait.association import Association as AssociationTrait
from gentropy.model.trait.identifier import HasStudyId
from gentropy.model.trait.locus import VariantLocation


def _id(study: str, chromosome: str, start: int, end: int) -> str:
    """Expected study locus key, computed without Spark.

    Args:
        study (str): Study identifier.
        chromosome (str): Chromosome.
        start (int): Locus start.
        end (int): Locus end.

    Returns:
        str: md5 of the values as a JSON object, as ``generate_identifier`` writes it.
    """
    key = (
        f'{{"studyId":"{study}","chromosome":"{chromosome}",'
        f'"locusStart":{start},"locusEnd":{end}}}'
    )
    return hashlib.md5(key.encode()).hexdigest()


def _prune_peak(position: np.ndarray, window_size: int) -> DenseVector:
    """Reference greedy pruning, kept verbatim from the legacy method.

    Positions come ordered from the most to the least significant. A position
    leads when it is at least ``window_size`` from every earlier lead.

    Args:
        position (np.ndarray): Positions sorted by p-value.
        window_size (int): Distance below which a lead explains a variant.

    Returns:
        DenseVector: 1.0 for a lead and 0.0 for a clumped variant, per position.
    """
    # Initializing the lead list with zeroes:
    is_lead = np.zeros(len(position))

    # List containing indices of leads:
    lead_indices: list[int] = []

    # Looping through all positions:
    for index in range(len(position)):
        # Looping through leads to find out if they are within a window:
        for lead_index in lead_indices:
            # If any of the leads within the window:
            if abs(position[lead_index] - position[index]) < window_size:
                # Skipping further checks:
                break
        else:
            # None of the leads were within the window:
            lead_indices.append(index)
            is_lead[index] = 1

    return DenseVector(is_lead)


Association = tuple[str, str, str, int, float, int]
"""(studyId, variantId, chromosome, position, pValueMantissa, pValueExponent)."""


def _random_associations(seed: int, distance: int) -> list[Association]:
    """Associations with distinct p-values per study, some sharing a position.

    Args:
        seed (int): Random seed.
        distance (int): Clumping distance, used to make clusters dense enough.

    Returns:
        list[Association]: The associations.
    """
    rng = random.Random(seed)
    rows: list[Association] = []
    for study in ("S1", "S2", "S3"):
        pvalues = rng.sample(
            [(m / 10, e) for m in range(10, 100) for e in range(-30, -8)], 240
        )
        for index, (mantissa, exponent) in enumerate(pvalues):
            chromosome = str(1 + index % 2)
            position = rng.randint(1, 40 * distance)
            if index % 10 == 0 and rows:  # a second allele at an earlier position
                position = rows[-1][3] if rows[-1][2] == chromosome else position
            rows.append(
                (
                    study,
                    f"{chromosome}_{position}_{index}",
                    chromosome,
                    position,
                    mantissa,
                    exponent,
                )
            )
    return rows


def _expected_loci(
    rows: list[Association], distance: int, keep_clumped_loci: bool
) -> list[tuple[str, str, int, int, str, list[str]]]:
    """Expected study loci, from the reference pruning over each chromosome.

    Every variant takes the window of the most significant lead within
    ``distance``; a lead takes its own.

    Args:
        rows (list[Association]): The associations.
        distance (int): Clumping distance.
        keep_clumped_loci (bool): Whether clumped variants get rows too.

    Returns:
        list[tuple[str, str, int, int, str, list[str]]]: (id, studyId,
            locusStart, locusEnd, leadVariantId, qualityControls) per variant.
    """
    groups: dict[tuple[str, str], list[Association]] = defaultdict(list)
    for row in rows:
        groups[(row[0], row[2])].append(row)
    loci = []
    for (study, chromosome), variants in groups.items():
        ranked = sorted(variants, key=lambda row: (row[5], row[4]))
        flags = _prune_peak(np.array([row[3] for row in ranked]), distance)
        leads = [row[3] for row, flag in zip(ranked, flags) if flag == 1.0]
        for row, flag in zip(ranked, flags):
            if flag == 0.0 and not keep_clumped_loci:
                continue
            lead = next(p for p in leads if abs(p - row[3]) < distance)
            start, end = max(1, lead - distance), lead + distance
            qc = [] if flag == 1.0 else [StudyLocusQualityFlag.WINDOW_CLUMPED.value]
            loci.append(
                (_id(study, chromosome, start, end), study, start, end, row[1], qc)
            )
    return loci


def _summary_statistics(
    spark: SparkSession, rows: list[Association]
) -> SummaryStatistics:
    """Summary statistics from associations, other statistics as placeholders.

    Args:
        spark (SparkSession): Spark session.
        rows (list[Association]): The associations.

    Returns:
        SummaryStatistics: The dataset.
    """
    return SummaryStatistics(
        spark.createDataFrame(
            [
                (
                    study,
                    variant,
                    chromosome,
                    position,
                    0.1,
                    5000,
                    mantissa,
                    exponent,
                    0.3,
                    0.01,
                )
                for study, variant, chromosome, position, mantissa, exponent in rows
            ],
            SummaryStatistics.schema,
        )
    )


@pytest.mark.parametrize("keep_clumped_loci", [False, True], ids=["leads", "all"])
@pytest.mark.parametrize("seed", [1, 2, 3])
def test_window_clumping_matches_the_reference_pruning(
    spark: SparkSession, seed: int, keep_clumped_loci: bool
) -> None:
    """Leads, flags and the shared window of each clump match the greedy reference."""
    distance = 1_000
    rows = _random_associations(seed, distance)

    loci = WindowClumping(distance=distance, keep_clumped_loci=keep_clumped_loci).clump(
        _summary_statistics(spark, rows)
    )

    assert sorted(
        tuple(row)
        for row in loci.df.select(
            "id",
            "studyId",
            "locusStart",
            "locusEnd",
            "leadVariantId",
            "qualityControls",
        ).collect()
    ) == sorted(_expected_loci(rows, distance, keep_clumped_loci))


def test_every_variant_of_a_clump_shares_the_window_and_id_of_its_lead(
    spark: SparkSession,
) -> None:
    """1000 and 1700 are explained by 1300, so they take its window, not their own.

    1700 is also within 500 of the lead at 2000, but 1300 is more significant.
    """
    rows = [
        ("GCST1", "1_1000_A_T", "1", 1000, 1.0, -10),
        ("GCST1", "1_1300_C_G", "1", 1300, 1.0, -12),
        ("GCST1", "1_1700_G_A", "1", 1700, 1.0, -9),
        ("GCST1", "1_2000_T_C", "1", 2000, 1.0, -11),
    ]

    loci = WindowClumping(distance=500, keep_clumped_loci=True).clump(
        _summary_statistics(spark, rows)
    )

    assert sorted(
        tuple(row)
        for row in loci.df.select(
            "leadVariantId", "id", "locusStart", "locusEnd"
        ).collect()
    ) == [
        ("1_1000_A_T", _id("GCST1", "1", 800, 1800), 800, 1800),
        ("1_1300_C_G", _id("GCST1", "1", 800, 1800), 800, 1800),
        ("1_1700_G_A", _id("GCST1", "1", 800, 1800), 800, 1800),
        ("1_2000_T_C", _id("GCST1", "1", 1500, 2500), 1500, 2500),
    ]


def test_the_significance_filter_only_removes_rows(spark: SparkSession) -> None:
    """Filtering first only removes rows; the remaining rows do not change.

    A variant's result depends only on stronger variants, so filtering before
    clumping equals clumping everything and dropping the weaker rows.
    """
    rows = _random_associations(4, 1_000)
    summary_statistics = _summary_statistics(spark, rows)
    # At most 1e-20, the threshold itself included:
    kept = {row[1] for row in rows if (row[5], row[4]) <= (-20, 1.0)}

    filtered_first = WindowClumping(
        distance=1_000, gwas_significance=1e-20, keep_clumped_loci=True
    ).clump(summary_statistics)
    filtered_after = WindowClumping(distance=1_000, keep_clumped_loci=True).clump(
        summary_statistics
    )

    assert sorted(filtered_first.df.collect()) == sorted(
        row for row in filtered_after.df.collect() if row.leadVariantId in kept
    )


def test_window_clumping_runs_without_python(spark: SparkSession) -> None:
    """The plan has no Python UDF, so rows never leave the JVM."""
    rows = [("GCST1", "1_1000_A_T", "1", 1000, 1.0, -10)]
    loci = WindowClumping(distance=500).clump(_summary_statistics(spark, rows))

    plan = loci.df._jdf.queryExecution().executedPlan().toString()

    assert "BatchEvalPython" not in plan
    assert "ArrowEvalPython" not in plan


def test_window_clumping_returns_one_locus_per_variant_and_flags_the_clumped(
    spark: SparkSession,
) -> None:
    """Kept on request, variants within the window of a stronger one are flagged.

    The clumped variant at 200 takes the window of its lead at 100, and the
    window starts at 1, the first position of the chromosome.
    """
    rows = [
        ("GCST1", "1_100_A_T", "1", 100, 0.2, 5000, 1.0, -10, 0.3, 0.01),
        ("GCST1", "1_200_C_G", "1", 200, 0.1, 5000, 1.0, -9, 0.3, 0.01),
        ("GCST1", "1_2000_G_A", "1", 2000, 0.3, 5000, 1.0, -12, 0.3, 0.01),
    ]
    summary_statistics = SummaryStatistics(
        spark.createDataFrame(rows, SummaryStatistics.schema)
    )

    loci = WindowClumping(
        distance=500, gwas_significance=5e-8, keep_clumped_loci=True
    ).clump(summary_statistics)

    assert isinstance(loci, StudyLocus)
    assert sorted(
        loci.df.select(
            "id", "leadVariantId", "locusStart", "locusEnd", "qualityControls"
        ).collect()
    ) == sorted(
        [
            (_id("GCST1", "1", 1, 600), "1_100_A_T", 1, 600, []),
            (
                _id("GCST1", "1", 1, 600),
                "1_200_C_G",
                1,
                600,
                [StudyLocusQualityFlag.WINDOW_CLUMPED.value],
            ),
            (_id("GCST1", "1", 1500, 2500), "1_2000_G_A", 1500, 2500, []),
        ]
    )


def test_window_clumping_returns_only_the_leads_by_default(
    spark: SparkSession,
) -> None:
    """Clumped variants get no row unless asked for, and leads carry no flag."""
    rows = [
        ("GCST1", "1_100_A_T", "1", 100, 1.0, -10),
        ("GCST1", "1_200_C_G", "1", 200, 1.0, -9),
        ("GCST1", "1_2000_G_A", "1", 2000, 1.0, -12),
    ]

    loci = WindowClumping(distance=500).clump(_summary_statistics(spark, rows))

    assert sorted(
        tuple(row)
        for row in loci.df.select(
            "leadVariantId", "locusStart", "locusEnd", "qualityControls"
        ).collect()
    ) == [("1_100_A_T", 1, 600, []), ("1_2000_G_A", 1500, 2500, [])]


@pytest.mark.parametrize("keep_clumped_loci", [False, True], ids=["leads", "all"])
@pytest.mark.parametrize(
    ("mantissa", "exponent"), [(None, None), (None, -20), (1.0, None)]
)
def test_variants_without_a_p_value_are_neither_leads_nor_clumped(
    spark: SparkSession,
    keep_clumped_loci: bool,
    mantissa: float | None,
    exponent: int | None,
) -> None:
    """A variant without a full p-value cannot be ranked, so it is left out.

    Ranked, a missing part would sort first, and the variant would clump the
    measured signal next to it.
    """
    rows = [
        ("GCST1", "1_100_A_T", "1", 100, mantissa, exponent),
        ("GCST1", "1_200_C_G", "1", 200, 1.0, -10),
    ]

    loci = WindowClumping(distance=500, keep_clumped_loci=keep_clumped_loci).clump(
        _summary_statistics(spark, rows)  # type: ignore[arg-type]
    )

    assert [
        tuple(row)
        for row in loci.df.select("leadVariantId", "qualityControls").collect()
    ] == [("1_200_C_G", [])]


class TopHits(Dataset[HasStudyId | VariantLocation | AssociationTrait]):
    """Associations that are not summary statistics, under their own column names."""

    study = implements(HasStudyId.study_id)
    chrom = implements(VariantLocation.chromosome)
    pos = implements(VariantLocation.position)
    variant = implements(AssociationTrait.variant_id)
    beta = implements(AssociationTrait.beta)
    standard_error = implements(AssociationTrait.standard_error)
    mantissa = implements(AssociationTrait.p_value_mantissa)
    exponent = implements(AssociationTrait.p_value_exponent)
    effect_allele_frequency_from_source = implements(
        AssociationTrait.effect_allele_frequency_from_source
    )
    sample_size = implements(AssociationTrait.sample_size)


def test_window_clumping_accepts_any_dataset_with_the_traits_it_reads(
    spark: SparkSession,
) -> None:
    """Columns are found through the traits, whatever the dataset calls them."""
    rows = [
        ("GCST1", "1", 100, "1_100_A_T", None, None, 1.0, -10, None, None),
        ("GCST1", "1", 200, "1_200_C_G", None, None, 1.0, -9, None, None),
        ("GCST1", "1", 2000, "1_2000_G_A", None, None, 1.0, -12, None, None),
    ]
    top_hits = TopHits(spark.createDataFrame(rows, TopHits.schema))

    loci = WindowClumping(distance=500, gwas_significance=5e-8).clump(top_hits)

    assert sorted(
        tuple(row)
        for row in loci.df.select(
            "studyId", "leadVariantId", "locusStart", "locusEnd"
        ).collect()
    ) == [("GCST1", "1_100_A_T", 1, 600), ("GCST1", "1_2000_G_A", 1500, 2500)]


@pytest.mark.parametrize(
    ("gwas_significance", "leads"),
    [(5e-8, ["1_100_A_T"]), (None, ["1_100_A_T", "1_5000_C_G"])],
    ids=["filtered", "unfiltered"],
)
def test_window_clumping_keeps_only_significant_variants_when_given_a_threshold(
    spark: SparkSession, gwas_significance: float | None, leads: list[str]
) -> None:
    """With a threshold, less significant variants never become loci."""
    rows = [
        ("GCST1", "1_100_A_T", "1", 100, 0.2, 5000, 1.0, -10, 0.3, 0.01),
        ("GCST1", "1_5000_C_G", "1", 5000, 0.1, 5000, 1.0, -6, 0.3, 0.01),
    ]
    summary_statistics = SummaryStatistics(
        spark.createDataFrame(rows, SummaryStatistics.schema)
    )

    loci = WindowClumping(distance=500, gwas_significance=gwas_significance).clump(
        summary_statistics
    )

    assert sorted(row.leadVariantId for row in loci.df.collect()) == leads


# Tests ported from the legacy package (legacy_tests/gentropy/method/
# test_window_based_clumping.py and legacy_tests/gentropy/dataset/test_study_locus.py).

LEGACY_SUMSTATS_SAMPLE = (
    Path(__file__).parents[3] / "legacy_tests/gentropy/data_samples/sumstats_sample"
)
"""GWAS associations on chromosome 18 (GCST005523), shared with the legacy tests."""


@pytest.fixture()
def sample_summary_statistics(spark: SparkSession) -> SummaryStatistics:
    """Sample GWAS associations, read into the new summary statistics dataset.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        SummaryStatistics: The sample associations.
    """
    return SummaryStatistics(spark.read.parquet(str(LEGACY_SUMSTATS_SAMPLE)))


def _significant(summary_statistics: SummaryStatistics) -> SummaryStatistics:
    """Keep GWAS significant associations, as the legacy clumping did itself.

    Args:
        summary_statistics (SummaryStatistics): Associations to filter.

    Returns:
        SummaryStatistics: Associations with p-value below 5e-8.
    """
    p_value = summary_statistics.p_value_mantissa * f.pow(
        f.lit(10.0), summary_statistics.p_value_exponent
    )
    return SummaryStatistics(summary_statistics.df.filter(p_value < 5e-8))


@pytest.mark.legacy
def test_window_based_clump__return_type(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Window clumping returns a study locus dataset."""
    assert isinstance(
        WindowClumping(distance=250_000, gwas_significance=5e-8).clump(
            _significant(sample_summary_statistics)
        ),
        StudyLocus,
    )


@pytest.mark.legacy
def test_window_based_clump__correctness(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """One semi-index is found in the sample.

    Legacy returned only the semi-indices. The new method keeps the clumped
    variants flagged, so the semi-indices are the rows without a flag.
    """
    clumped = WindowClumping(distance=250_000, gwas_significance=5e-8).clump(
        _significant(sample_summary_statistics)
    )
    semi_indices = clumped.df.filter(f.size(clumped.quality_controls) == 0)

    # One semi index was found, and it is the expected variant:
    assert [row.leadVariantId for row in semi_indices.collect()] == ["18_12843138_T_C"]


@pytest.mark.legacy
def test_window_based_clump_with_locus__correctness(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """The semi-index locus collects 218 variants from the summary statistics.

    Legacy collected within 250 kb of the lead; the new locus is collected within
    the lead's window, which is the same 250 kb on each side.
    """
    clumped = WindowClumping(distance=250_000, gwas_significance=5e-8).clump(
        _significant(sample_summary_statistics)
    )
    annotated = annotate_locus(clumped, sample_summary_statistics)

    # Asserting the presence of locus key:
    assert "locus" in annotated.df.columns

    # One semi index was found:
    assert annotated.df.count() == 1

    # Assert the variant found:
    assert (
        annotated.df.filter(annotated.lead_variant_id == "18_12843138_T_C").count() == 1
    )

    # Assert the number of variants in the locus:
    assert annotated.df.select(f.explode_outer(annotated.locus)).count() == 218


@pytest.mark.legacy
def test_prune_peak(spark: SparkSession) -> None:
    """Leads are chosen by p-value order and pruned within the window."""
    data = [
        ("c", 3, 4.0),
        ("c", 4, 2.0),
        ("c", 6, 1.0),
        ("c", 8, 2.5),
        ("c", 9, 3.0),
    ]
    df = (
        spark.createDataFrame(data, ["cluster", "position", "negLogPValue"])
        .withColumn(
            "collected_positions",
            f.collect_list(f.col("position")).over(
                Window.partitionBy("cluster")
                .orderBy(f.col("negLogPValue").desc())
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
        )
        .withColumn(
            "isLeadList",
            f.udf(_prune_peak, VectorUDT())(
                fml.array_to_vector(f.col("collected_positions")), f.lit(2)
            ),
        )
    )

    expected_result = spark.createDataFrame(
        [("c", [1.0, 1.0, 0.0, 0.0, 1.0])], ["cluster", "isLeadList"]
    ).withColumn("isLeadList", fml.array_to_vector(f.col("isLeadList")))

    # Compare the actual DataFrame with the expected result
    assert (
        df.select("cluster", "isLeadList").distinct().collect()
        == expected_result.collect()
    )


@pytest.mark.legacy
class TestStudyLocusWindowClumping:
    """Window clumping on five associations from three studies.

    Legacy clumped a study locus; the new method takes summary statistics, so the
    same associations are summary statistics whose variant ids encode positions.
    """

    TEST_DATASET = [
        ("s1", "c1", 1, -1),
        ("s1", "c1", 2, -2),
        ("s1", "c1", 3, -3),
        ("s2", "c2", 2, -2),
        ("s3", "c2", 2, -2),
    ]

    @pytest.fixture(autouse=True)
    def _setup(self: TestStudyLocusWindowClumping, spark: SparkSession) -> None:
        """Build the summary statistics for the tests.

        Args:
            spark (SparkSession): Spark session.
        """
        self.summary_statistics = SummaryStatistics(
            spark.createDataFrame(
                [
                    (
                        study,
                        f"{chromosome}_{position}_A_T",
                        chromosome,
                        position,
                        None,
                        None,
                        1.0,
                        exponent,
                        None,
                        None,
                    )
                    for study, chromosome, position, exponent in self.TEST_DATASET
                ],
                SummaryStatistics.schema,
            )
        )
        # Legacy kept the clumped associations, so they are asked for here:
        self.clumped = WindowClumping(
            distance=3, gwas_significance=0.5, keep_clumped_loci=True
        ).clump(self.summary_statistics)

    def test_clump_return_type(self: TestStudyLocusWindowClumping) -> None:
        """Testing if the clumping returns the right type."""
        assert isinstance(self.clumped, StudyLocus)

    def test_clump_no_data_loss(self: TestStudyLocusWindowClumping) -> None:
        """Testing if the clumping returns same number of rows."""
        assert self.clumped.df.count() == self.summary_statistics.df.count()

    def test_correct_flag(self: TestStudyLocusWindowClumping) -> None:
        """Testing if the clumping flags are for variants."""
        assert (
            self.clumped.df.filter(
                f.array_contains(
                    self.clumped.quality_controls,
                    StudyLocusQualityFlag.WINDOW_CLUMPED.value,
                )
            ).count()
        ) == 2
