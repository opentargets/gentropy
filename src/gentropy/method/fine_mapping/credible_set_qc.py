"""Credible set quality control: filter, deduplicate and optionally LD-clump credible sets.

Fine-mapping writes the credible sets of every locus on its own; this gathers them and keeps:

1. **A significant lead.** The lead's p-value is the one on its credible set variant row; a lead
   without one, such as an imputed variant, does not pass.
2. **Pure credible sets**, whose `purityMinR2` reaches the threshold; a null purity does not pass.
3. **One credible set per `id`**, the one with the highest `logBayesFactor`. The `id` hashes
   the study, the locus bounds, the lead and the method. Fine-mapped loci of a study do not
   overlap (they come from locus-breaker clumping, and a lead lies inside its locus), so a
   duplicated `id` is the same locus fine-mapped again, which is what the former study locus
   id, over the study, lead and method, caught. Two runs over different bounds give different
   ids and are both kept. The variants of a duplicated `id` cannot be told apart by run, so
   they are deduplicated on `(credibleSetId, variantId)`.
4. **Optionally, leads not in LD with a more significant lead of the study.** Each credible set
   becomes a study locus with its lead's statistics as `locus`, its lead is annotated with the
   LD index (`annotate_ld`), and `LDClumping` drops the leads in LD with a more significant one.

Credible set variants are kept with their credible set.
"""

from __future__ import annotations

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.stats import split_pvalue
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import AnnotatedStudyLocus
from gentropy.method.annotation import annotate_ld
from gentropy.method.clumping.ld import LDClumping


def credible_set_qc(
    credible_sets: CredibleSet,
    credible_set_variants: CredibleSetVariant,
    p_value_threshold: float = 1e-5,
    purity_min_r2: float = 0.01,
    ld_index: LDIndex | None = None,
    study_index: StudyIndex | None = None,
    ld_min_r2: float = 0.8,
) -> tuple[CredibleSet, CredibleSetVariant]:
    """Filter credible sets by lead p-value and purity, deduplicate them, and optionally LD-clump them.

    Args:
        credible_sets (CredibleSet): Credible sets to filter.
        credible_set_variants (CredibleSetVariant): Their variants.
        p_value_threshold (float): Largest lead p-value kept. Defaults to 1e-5.
        purity_min_r2 (float): Smallest `purityMinR2` kept. Defaults to 0.01.
        ld_index (LDIndex | None): LD reference panel; when given, the credible sets are LD-clumped.
        study_index (StudyIndex | None): The studies' ancestries, required with `ld_index`.
        ld_min_r2 (float): Smallest r2 between two leads that clumps the less significant one.
            Defaults to 0.8.

    Returns:
        tuple[CredibleSet, CredibleSetVariant]: The credible sets that pass, and their variants.

    Raises:
        ValueError: When `ld_index` is given without `study_index`.

    Examples:
        >>> def credible_set(id, lead, log10bf, purity):
        ...     return (id, "1", 1, 1000, "S1", "gwas", lead, "SuSiE-inf", log10bf, None,
        ...             None, purity, [])
        >>> def lead(id, lead, exponent):
        ...     return (id, lead, 1.0, exponent, None, None, None, None, 0.9, None, None,
        ...             None, None, None, None, None, [])
        >>> credible_sets = CredibleSet(spark.createDataFrame([
        ...     credible_set("a", "1_10_A_T", 5.0, 0.5),
        ...     credible_set("a", "1_10_A_T", 4.0, 0.5),
        ...     credible_set("b", "1_20_C_G", 5.0, 0.5),
        ...     credible_set("c", "1_30_G_A", 5.0, 0.001),
        ... ], CredibleSet.get_schema()))
        >>> variants = CredibleSetVariant(spark.createDataFrame([
        ...     lead("a", "1_10_A_T", -10),
        ...     lead("a", "1_10_A_T", -10),
        ...     lead("b", "1_20_C_G", -4),
        ...     lead("c", "1_30_G_A", -10),
        ... ], CredibleSetVariant.get_schema()))
        >>> kept, kept_variants = credible_set_qc(credible_sets, variants)
        >>> [(row.id, row.logBayesFactor) for row in kept.df.collect()]
        [('a', 5.0)]
        >>> kept_variants.df.count()
        1
    """
    if ld_index is not None and study_index is None:
        raise ValueError("LD clumping requires the study index.")
    mantissa, exponent = split_pvalue(p_value_threshold)
    leads = credible_set_variants.df.select(
        f.col("credibleSetId").alias("id"),
        f.col("variantId").alias("leadVariantId"),
        f.struct(
            f.col("variantId"),
            f.col("pValueMantissa"),
            f.col("pValueExponent"),
            f.col("effectAlleleFrequency").alias("effectAlleleFrequencyFromSource"),
            f.col("sampleSize"),
            f.col("reportedBeta").alias("beta"),
            f.col("reportedStandardError").alias("standardError"),
        ).alias("lead"),
    ).dropDuplicates(["id", "leadVariantId"])
    lead_p_passes = (f.col("lead.pValueExponent") < exponent) | (
        (f.col("lead.pValueExponent") == exponent)
        & (f.col("lead.pValueMantissa") <= mantissa)
    )
    kept = (
        credible_sets.df.join(leads, on=["id", "leadVariantId"], how="left")
        .filter(lead_p_passes)
        .filter(f.col("purityMinR2") >= purity_min_r2)
        .withColumn(
            "rank",
            f.row_number().over(
                Window.partitionBy("id").orderBy(f.desc_nulls_last("logBayesFactor"))
            ),
        )
        .filter(f.col("rank") == 1)
        .drop("rank")
    )
    if ld_index is not None and study_index is not None:
        loci_schema = AnnotatedStudyLocus.get_schema()
        loci = AnnotatedStudyLocus(
            _df=kept.select(
                "id",
                "studyId",
                "chromosome",
                "locusStart",
                "locusEnd",
                "leadVariantId",
                f.array()
                .cast(loci_schema["qualityControls"].dataType)
                .alias("qualityControls"),
                f.array(f.col("lead"))
                .cast(loci_schema["locus"].dataType)
                .alias("locus"),
                f.lit(None).cast(loci_schema["ldSet"].dataType).alias("ldSet"),
            ),
            _schema=loci_schema,
        )
        unclumped = LDClumping().clump(
            annotate_ld(loci, study_index, ld_index, r2_threshold=ld_min_r2)
        )
        kept = kept.join(unclumped.df.select("id"), on="id", how="left_semi")
    credible_set_schema = CredibleSet.get_schema()
    variant_schema = CredibleSetVariant.get_schema()
    return (
        CredibleSet(
            _df=kept.select(*credible_set_schema.fieldNames()),
            _schema=credible_set_schema,
        ),
        CredibleSetVariant(
            _df=credible_set_variants.df.join(
                kept.select(f.col("id").alias("credibleSetId")),
                on="credibleSetId",
                how="left_semi",
            ).dropDuplicates(["credibleSetId", "variantId"]),
            _schema=variant_schema,
        ),
    )
