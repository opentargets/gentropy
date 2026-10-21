"""Credible sets: the fine-mapping results, at locus and at variant grain.

Fine-mapping a study locus gives zero or more credible sets. Each one is a set of variants
that together most likely contain the causal variant of one signal. Two datasets hold them:

- `CredibleSet`, one row per credible set, keyed by `id`.
- `CredibleSetVariant`, one row per variant of a credible set, keyed by `credibleSetId` and `variantId`.

The credible set key hashes `CredibleSet.ID_COLUMNS`. Its first four columns rebuild the study
locus key, so the parent study locus can be recovered without storing its identifier.

Keys, the lead, the method, the posterior probability and the quality controls are never null.
Every statistic is nullable, as not every method or source gives it: a PICS credible set, for
example, has no Bayes factor, and its tag variants have no conditional effect.

Bayes factors are natural logarithms, as colocalisation reads them, with one definition per
level across all sources:

- `CredibleSetVariant.logBayesFactor` is the variant's log Bayes factor in the SuSiE single
  effect of its credible set.
- `CredibleSet.logBayesFactor` is that single effect's Bayes factor over its whole fine-mapped
  region: the log-sum-exp of the variant log Bayes factors of every variant of the region, with a
  uniform prior over them (`gentropy.common.stats.region_log_bayes_factors`). Credible set Bayes
  factors that a source reports with another definition, such as FinnGen's model comparison
  `cs_log10bf`, are not stored.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, ClassVar

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.genomic_region import GenomicRegion, KnownGenomicRegions
from gentropy.common.processing import extract_position
from gentropy.common.schemas import parse_spark_schema
from gentropy.common.stats import pvalue_at_most
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_index import StudyQualityCheck
from gentropy.dataset.study_locus import FinemappingMethod, StudyLocusQualityCheck

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.target_index import TargetIndex
    from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader


class CredibleSetVariantQualityCheck(Enum):
    """How the inputs of a credible set variant were chosen.

    Attributes:
        EAF_FROM_SOURCE (str): Allele frequency reported by the study
        EAF_FROM_GNOMAD (str): Allele frequency taken from gnomAD, as the study reported none
        RESCALED_FROM_CONDITIONAL (str): Rescaled effect derived from the conditional effect
        RESCALED_FROM_MARGINAL (str): Rescaled effect derived from the marginal effect
        NOT_IN_LOCUS (str): Variant has no statistics in the locus, so its marginal statistics are null
    """

    EAF_FROM_SOURCE = "Allele frequency reported by the study"
    EAF_FROM_GNOMAD = "Allele frequency taken from gnomAD"
    RESCALED_FROM_CONDITIONAL = "Rescaled effect derived from the conditional effect"
    RESCALED_FROM_MARGINAL = "Rescaled effect derived from the marginal effect"
    NOT_IN_LOCUS = "Variant in LD with the lead has no statistics in the locus"


class CredibleSetConfidenceClasses(Enum):
    """Confidence of a credible set, from its fine-mapping method and quality controls: the `CredibleSet.confidence` byte, highest first.

    Attributes:
        FINEMAPPED_IN_SAMPLE_LD (int): 4, SuSiE fine-mapped credible set with in-sample LD
        FINEMAPPED_OUT_OF_SAMPLE_LD (int): 3, SuSiE fine-mapped credible set with out-of-sample LD
        PICSED_SUMMARY_STATS (int): 2, PICS fine-mapped credible set extracted from summary statistics
        PICSED_TOP_HIT (int): 1, PICS fine-mapped credible set based on reported top hit
        UNKNOWN (int): 0, unknown confidence, for credible sets which did not fit any of the above categories
    """

    FINEMAPPED_IN_SAMPLE_LD = 4
    FINEMAPPED_OUT_OF_SAMPLE_LD = 3
    PICSED_SUMMARY_STATS = 2
    PICSED_TOP_HIT = 1
    UNKNOWN = 0


@dataclass
class CredibleSet(Dataset):
    """One fine-mapping result: a credible set of one study, at locus grain.

    `chromosome`, `locusStart`, `locusEnd` and `studyId` are copied from the study locus and
    `studyType` from the study index, so a consumer needs no other dataset. `qualityControls`
    carries the `StudyLocusQualityCheck` flags of the study locus it was fine-mapped from.

    Examples:
        >>> import hashlib
        >>> rows = [
        ...     ("", "1", 100_000, 600_000, "GCST1", "gwas", "1_350000_A_T", "PICS",
        ...      None, None, None, None, ["Variant not found in LD reference"]),
        ... ]
        >>> credible_sets = CredibleSet(
        ...     spark.createDataFrame(rows, CredibleSet.get_schema())
        ...     .withColumn("id", CredibleSet.generate_identifier(CredibleSet.ID_COLUMNS)),
        ...     CredibleSet.get_schema(),
        ... )
        >>> CredibleSet.ID_COLUMNS
        ('studyId', 'chromosome', 'locusStart', 'locusEnd', 'leadVariantId', 'fineMappingMethod')
        >>> credible_sets.df.first().id == hashlib.md5(
        ...     b'{"studyId":"GCST1","chromosome":"1","locusStart":100000,"locusEnd":600000,'
        ...     b'"leadVariantId":"1_350000_A_T","fineMappingMethod":"PICS"}'
        ... ).hexdigest()
        True
    """

    ID_COLUMNS: ClassVar[tuple[str, ...]] = (
        "studyId",
        "chromosome",
        "locusStart",
        "locusEnd",
        "leadVariantId",
        "fineMappingMethod",
    )
    """Columns whose values define the key `id`, hashed by `generate_identifier`."""

    @classmethod
    def get_schema(cls: type[CredibleSet]) -> StructType:
        """Provides the schema for the CredibleSet dataset.

        Returns:
            StructType: schema for the CredibleSet dataset.
        """
        return parse_spark_schema("credible_set.json")

    @classmethod
    def get_QC_column_name(cls: type[CredibleSet]) -> str:
        """Quality control column.

        Returns:
            str: Name of the quality control column.
        """
        return "qualityControls"

    @classmethod
    def get_QC_mappings(cls: type[CredibleSet]) -> dict[str, str]:
        """Quality control flag to QC column category mappings.

        Returns:
            dict[str, str]: Mapping between flag name and QC column category value.
        """
        return {member.name: member.value for member in StudyLocusQualityCheck}

    @classmethod
    def from_members(
        cls: type[CredibleSet],
        members: DataFrame,
        credible_set_keys: list[str],
        lead_pvalue_threshold: float | None = None,
    ) -> tuple[CredibleSet, CredibleSetVariant]:
        """Build credible sets and their variants from one row per credible set member.

        Rows sharing `credible_set_keys` are the members of one credible set. Its lead is the
        member with the highest `posteriorProbability`, ties broken by `variantId`. The credible
        set takes its own columns from the lead's row and its key from `ID_COLUMNS`.

        Where a credible set column and a variant column share a name, the credible set one is
        given in `members` with a `credibleSet` prefix: `credibleSetLogBayesFactor` and
        `credibleSetQualityControls`. A column of either schema missing from `members` is null,
        and a missing quality controls column is empty.

        The members are persisted, as both datasets are projected from them: writing both
        otherwise reads and computes the source twice.

        Args:
            members (DataFrame): One row per member, with `credible_set_keys`, `variantId`,
                `posteriorProbability`, `pValueMantissa`, `pValueExponent` and the credible set
                columns `chromosome`, `locusStart`, `locusEnd`, `studyId`, `studyType` and
                `fineMappingMethod`.
            credible_set_keys (list[str]): Columns identifying the credible set of a member.
            lead_pvalue_threshold (float | None): Drop the credible sets whose lead p-value is above
                it, with their variants. Defaults to None, which keeps every credible set.

        Returns:
            tuple[CredibleSet, CredibleSetVariant]: The credible sets and their variants.

        Examples:
            >>> members = spark.createDataFrame(
            ...     [("r1", "1_10_A_T", 0.7, 1.0, -9, 0.2), ("r1", "1_20_C_G", 0.3, 1.0, -6, 0.1)],
            ...     "region string, variantId string, posteriorProbability double, "
            ...     "pValueMantissa float, pValueExponent int, reportedBeta double",
            ... ).withColumns({
            ...     "chromosome": f.lit("1"), "locusStart": f.lit(1), "locusEnd": f.lit(100),
            ...     "studyId": f.lit("S1"), "studyType": f.lit("gwas"), "fineMappingMethod": f.lit("SuSiE"),
            ... })
            >>> credible_sets, variants = CredibleSet.from_members(members, ["region"])
            >>> credible_sets.df.select("leadVariantId", "logBayesFactor", "qualityControls").show()
            +-------------+--------------+---------------+
            |leadVariantId|logBayesFactor|qualityControls|
            +-------------+--------------+---------------+
            |     1_10_A_T|          NULL|             []|
            +-------------+--------------+---------------+
            <BLANKLINE>
            >>> variants.df.select("variantId", "reportedBeta", "sampleSize").orderBy("variantId").show()
            +---------+------------+----------+
            |variantId|reportedBeta|sampleSize|
            +---------+------------+----------+
            | 1_10_A_T|         0.2|      NULL|
            | 1_20_C_G|         0.1|      NULL|
            +---------+------------+----------+
            <BLANKLINE>
            >>> [d.df.count() for d in CredibleSet.from_members(members, ["region"], 1e-10)]
            [0, 0]
        """
        credible_set = Window.partitionBy(*credible_set_keys)
        is_lead = f.row_number().over(
            credible_set.orderBy(f.desc("posteriorProbability"), "variantId")
        ) == f.lit(1)
        members = members.withColumn(
            "leadVariantId",
            f.first(f.when(is_lead, f.col("variantId")), ignorenulls=True).over(
                credible_set
            ),
        ).withColumn("id", cls.generate_identifier(cls.ID_COLUMNS))
        is_lead_row = f.col("variantId") == f.col("leadVariantId")
        if lead_pvalue_threshold is not None:
            passes = pvalue_at_most(
                f.col("pValueMantissa"), f.col("pValueExponent"), lead_pvalue_threshold
            )
            members = (
                members.withColumn(
                    "leadPasses", f.max(f.when(is_lead_row, passes)).over(credible_set)
                )
                .filter(f.col("leadPasses"))
                .drop("leadPasses")
            )
        # Both datasets are projected from the members: persist them, so the source is read once.
        members = members.persist()

        def projected(
            df: DataFrame, schema: StructType, renamed: dict[str, str]
        ) -> DataFrame:
            columns = []
            for field in schema.fields:
                source = renamed.get(field.name, field.name)
                if source in df.columns:
                    column = f.col(source)
                elif field.name == "qualityControls":
                    column = f.array()
                else:
                    column = f.lit(None)
                columns.append(column.cast(field.dataType).alias(field.name))
            return df.select(*columns)

        credible_set_schema = cls.get_schema()
        variant_schema = CredibleSetVariant.get_schema()
        return (
            cls(
                _df=projected(
                    members.filter(is_lead_row),
                    credible_set_schema,
                    {
                        "logBayesFactor": "credibleSetLogBayesFactor",
                        "qualityControls": "credibleSetQualityControls",
                    },
                ),
                _schema=credible_set_schema,
            ),
            CredibleSetVariant(
                _df=projected(members, variant_schema, {"credibleSetId": "id"}),
                _schema=variant_schema,
            ),
        )

    def to_locus_view(
        self: CredibleSet, credible_set_variants: CredibleSetVariant
    ) -> CredibleSetLocus:
        """Join the credible sets with their variants into the locus view read by locus-to-gene.

        Each credible set keeps its key as `credibleSetId`, its lead as `variantId` and the lead's
        `position`, and gathers its members into `locus`. `isTransQtl` is read from the
        `TRANS_QTL` quality control flag. A credible set without variants gets an empty `locus`.

        Args:
            credible_set_variants (CredibleSetVariant): The variants of the credible sets.

        Returns:
            CredibleSetLocus: One row per credible set, with its members in `locus`.

        Examples:
            >>> credible_sets = CredibleSet(
            ...     spark.createDataFrame(
            ...         [("cs1", "1", 1, 100, "S1", "eqtl", "1_10_A_T", "SuSiE", None, None, None, None,
            ...           ["Trans QTL"])],
            ...         CredibleSet.get_schema(),
            ...     ),
            ...     CredibleSet.get_schema(),
            ... )
            >>> variants = CredibleSetVariant(
            ...     spark.createDataFrame(
            ...         [("cs1", v, None, None, None, None, None, None, pp, None, None, None, None,
            ...           None, None, None, []) for v, pp in [("1_10_A_T", 0.7), ("1_20_C_G", 0.3)]],
            ...         CredibleSetVariant.get_schema(),
            ...     ),
            ...     CredibleSetVariant.get_schema(),
            ... )
            >>> credible_sets.to_locus_view(variants).df.select(
            ...     "credibleSetId", "variantId", "position", "isTransQtl", "locus.variantId"
            ... ).show(truncate=False)
            +-------------+---------+--------+----------+--------------------+
            |credibleSetId|variantId|position|isTransQtl|variantId           |
            +-------------+---------+--------+----------+--------------------+
            |cs1          |1_10_A_T |10      |true      |[1_10_A_T, 1_20_C_G]|
            +-------------+---------+--------+----------+--------------------+
            <BLANKLINE>
        """
        locus = credible_set_variants.df.groupBy(
            f.col("credibleSetId").alias("id")
        ).agg(
            f.sort_array(
                f.collect_list(f.struct("variantId", "posteriorProbability"))
            ).alias("locus")
        )
        schema = CredibleSetLocus.get_schema()
        return CredibleSetLocus(
            _df=self.df.join(locus, on="id", how="left").select(
                f.col("id").alias("credibleSetId"),
                "studyId",
                "studyType",
                "chromosome",
                extract_position(f.col("leadVariantId")).cast("int").alias("position"),
                f.col("leadVariantId").alias("variantId"),
                "fineMappingMethod",
                "confidence",
                "qualityControls",
                f.array_contains(
                    "qualityControls", StudyLocusQualityCheck.TRANS_QTL.value
                ).alias("isTransQtl"),
                f.coalesce("locus", f.array().cast(schema["locus"].dataType)).alias(
                    "locus"
                ),
            ),
            _schema=schema,
        )

    # Validation: the checks of credible set validation (`gentropy.credible_set_validation`), in
    # the order the step runs them. Each returns a new `CredibleSet` with the flag raised.

    VALID_CHROMOSOMES: ClassVar[list[str]] = [str(i) for i in range(1, 23)] + [
        "X",
        "Y",
        "XY",
        "MT",
    ]
    """Chromosome labels a credible set may carry."""

    MEASUREMENT_THERAPEUTIC_AREA: ClassVar[str] = "EFO_0001444"
    """Therapeutic area of the disease index that files a term as a measurement."""

    @classmethod
    def _flagged(
        cls: type[CredibleSet], df: DataFrame, condition: Column, flag: Enum
    ) -> DataFrame:
        """Raise a quality control flag where the condition holds; a null condition is false.

        Args:
            df (DataFrame): Credible sets, possibly with helper columns of a check.
            condition (Column): Boolean column, true where the credible set is flagged.
            flag (Enum): The flag.

        Returns:
            DataFrame: The rows, with the flag raised.
        """
        return df.withColumn(
            "qualityControls",
            cls.update_quality_flag(
                f.col("qualityControls"), f.coalesce(condition, f.lit(False)), flag
            ),
        )

    def _flag(self: CredibleSet, condition: Column, flag: Enum) -> CredibleSet:
        """Raise a quality control flag where the condition holds.

        Args:
            condition (Column): Boolean column of the credible set columns, true where flagged.
            flag (Enum): The flag.

        Returns:
            CredibleSet: The credible sets, with the flag raised.
        """
        return CredibleSet(self._flagged(self.df, condition, flag), self.get_schema())

    def qc_mhc_region(self: CredibleSet) -> CredibleSet:
        """Flag credible sets whose lead variant is in the MHC region.

        Returns:
            CredibleSet: The credible sets, with `IN_MHC` raised.

        Examples:
            >>> cs = CredibleSet(spark.createDataFrame(
            ...     [("a", "6", 1, 2, "S", "gwas", "6_30000000_A_T", "SuSiE", None, None, None, None, []),
            ...      ("b", "6", 1, 2, "S", "gwas", "6_50000000_A_T", "SuSiE", None, None, None, None, [])],
            ...     CredibleSet.get_schema()))
            >>> [(r.id, r.qualityControls) for r in cs.qc_mhc_region().df.orderBy("id").collect()]
            [('a', ['MHC region']), ('b', [])]
        """
        region = GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC)
        position = extract_position(f.col("leadVariantId")).cast("long")
        return self._flag(
            (f.col("chromosome") == region.chromosome)
            & position.between(region.start, region.end),
            StudyLocusQualityCheck.IN_MHC,
        )

    def validate_chromosome_label(self: CredibleSet) -> CredibleSet:
        """Flag credible sets whose chromosome is not 1-22, X, Y, XY or MT.

        Returns:
            CredibleSet: The credible sets, with `INVALID_CHROMOSOME` raised.
        """
        return self._flag(
            ~f.col("chromosome").isin(self.VALID_CHROMOSOMES),
            StudyLocusQualityCheck.INVALID_CHROMOSOME,
        )

    def validate_study(self: CredibleSet, study_index: StudyIndex) -> CredibleSet:
        """Flag credible sets of flagged or missing studies, and curated top hits of studies with summary statistics.

        - `FLAGGED_STUDY`: the study has quality control flags, which are not propagated.
        - `TOP_HIT_AND_SUMMARY_STATS`: a curated top hit, when the study has summary statistics.
        - `MISSING_STUDY`: the study is not in the (validated) study index.

        Args:
            study_index (StudyIndex): The validated study index.

        Returns:
            CredibleSet: The credible sets, with the flags raised.
        """
        study_qc = (
            f.col("qualityControls")
            if "qualityControls" in study_index.df.columns
            else f.lit(None).cast("array<string>")
        )
        studies = study_index.df.select(
            "studyId",
            f.lit(True).alias("_studyFound"),
            study_qc.alias("_studyQualityControls"),
        )
        df = self.df.join(studies, on="studyId", how="left")
        is_top_hit = f.array_contains(
            "qualityControls", StudyLocusQualityCheck.TOP_HIT.value
        )
        df = self._flagged(
            df,
            f.size("_studyQualityControls") > 0,
            StudyLocusQualityCheck.FLAGGED_STUDY,
        )
        df = self._flagged(
            df,
            is_top_hit
            & ~f.array_contains(
                "_studyQualityControls",
                StudyQualityCheck.SUMSTATS_NOT_AVAILABLE.value,
            ),
            StudyLocusQualityCheck.TOP_HIT_AND_SUMMARY_STATS,
        )
        df = self._flagged(
            df, f.col("_studyFound").isNull(), StudyLocusQualityCheck.MISSING_STUDY
        )
        return CredibleSet(
            df.drop("_studyFound", "_studyQualityControls"), self.get_schema()
        )

    def annotate_study_type(self: CredibleSet, study_index: StudyIndex) -> CredibleSet:
        """Take the study type from the study index.

        Args:
            study_index (StudyIndex): The validated study index.

        Returns:
            CredibleSet: The credible sets, with `studyType` from the study index.
        """
        return CredibleSet(
            self.df.drop("studyType").join(
                study_index.study_type_lut(), on="studyId", how="left"
            ),
            self.get_schema(),
        )

    def qc_redundant_top_hits_from_pics(self: CredibleSet) -> CredibleSet:
        """Flag curated top hits of studies that also have PICS credible sets from summary statistics.

        Returns:
            CredibleSet: The credible sets, with `REDUNDANT_PICS_TOP_HIT` raised.
        """
        is_top_hit = f.array_contains(
            "qualityControls", StudyLocusQualityCheck.TOP_HIT.value
        )
        studies_with_pics_sumstats = (
            self.df.filter(
                (f.col("fineMappingMethod") == FinemappingMethod.PICS.value)
                & ~is_top_hit
            )
            .select("studyId", f.lit(True).alias("_hasPicsSumstats"))
            .distinct()
        )
        flagged = self._flagged(
            self.df.join(studies_with_pics_sumstats, on="studyId", how="left"),
            is_top_hit & f.col("_hasPicsSumstats"),
            StudyLocusQualityCheck.REDUNDANT_PICS_TOP_HIT,
        )
        return CredibleSet(flagged.drop("_hasPicsSumstats"), self.get_schema())

    def qc_explained_by_susie(
        self: CredibleSet, variants: CredibleSetVariant
    ) -> CredibleSet:
        """Flag credible sets not fine-mapped with SuSiE that have a variant inside a SuSiE credible set's region of the same study.

        Args:
            variants (CredibleSetVariant): The variants of the credible sets.

        Returns:
            CredibleSet: The credible sets, with `EXPLAINED_BY_SUSIE` raised.
        """
        is_susie = f.col("fineMappingMethod").isin(
            FinemappingMethod.SUSIE.value, FinemappingMethod.SUSIE_INF.value
        )
        susie_regions = (
            self.df.filter(is_susie)
            .select(
                "studyId",
                "chromosome",
                f.col("locusStart").alias("_start"),
                f.col("locusEnd").alias("_end"),
            )
            .distinct()
        )
        explained = (
            self.df.filter(~is_susie)
            .select(f.col("id").alias("credibleSetId"), "studyId", "chromosome")
            .join(
                variants.df.select(
                    "credibleSetId",
                    extract_position(f.col("variantId"))
                    .cast("long")
                    .alias("_position"),
                ),
                on="credibleSetId",
            )
            .join(susie_regions, on=["studyId", "chromosome"])
            .filter(f.col("_position").between(f.col("_start"), f.col("_end")))
            .select(f.col("credibleSetId").alias("id"), f.lit(True).alias("_explained"))
            .distinct()
        )
        flagged = self._flagged(
            self.df.join(explained, on="id", how="left"),
            f.col("_explained") & ~is_susie,
            StudyLocusQualityCheck.EXPLAINED_BY_SUSIE,
        )
        return CredibleSet(flagged.drop("_explained"), self.get_schema())

    def qc_abnormal_pips(
        self: CredibleSet,
        variants: CredibleSetVariant,
        sum_pips_lower_threshold: float = 0.95,
        # Slightly above 1, for floating point error:
        sum_pips_upper_threshold: float = 1.0001,
    ) -> CredibleSet:
        """Flag credible sets whose members' posterior probabilities do not sum to [lower, upper].

        Args:
            variants (CredibleSetVariant): The variants of the credible sets.
            sum_pips_lower_threshold (float): Lowest acceptable sum.
            sum_pips_upper_threshold (float): Highest acceptable sum.

        Returns:
            CredibleSet: The credible sets, with `ABNORMAL_PIPS` raised.
        """
        sums = variants.df.groupBy(f.col("credibleSetId").alias("id")).agg(
            f.sum("posteriorProbability").alias("_sumPip")
        )
        flagged = self._flagged(
            self.df.join(sums, on="id", how="left"),
            (f.col("_sumPip") < sum_pips_lower_threshold)
            | (f.col("_sumPip") > sum_pips_upper_threshold),
            StudyLocusQualityCheck.ABNORMAL_PIPS,
        )
        return CredibleSet(flagged.drop("_sumPip"), self.get_schema())

    def assign_confidence(self: CredibleSet) -> CredibleSet:
        """Assign the confidence class (`CredibleSetConfidenceClasses`) from the fine-mapping method and the quality controls.

        Returns:
            CredibleSet: The credible sets, with `confidence`.

        Examples:
            >>> rows = [
            ...     ("a", "1", 1, 2, "S", "gwas", "1_1_A_T", "SuSiE", None, None, None, None, []),
            ...     ("b", "1", 1, 2, "S", "gwas", "1_1_A_T", "PICS", None, None, None, None,
            ...      ["Study locus from curated top hit"]),
            ... ]
            >>> cs = CredibleSet(spark.createDataFrame(rows, CredibleSet.get_schema()))
            >>> [(r.id, r.confidence) for r in cs.assign_confidence().df.orderBy("id").collect()]
            [('a', 4), ('b', 1)]
        """
        is_susie = f.col("fineMappingMethod").isin(
            FinemappingMethod.SUSIE.value, FinemappingMethod.SUSIE_INF.value
        )
        is_pics = f.col("fineMappingMethod") == FinemappingMethod.PICS.value
        out_of_sample = f.array_contains(
            "qualityControls", StudyLocusQualityCheck.OUT_OF_SAMPLE_LD.value
        )
        top_hit = f.array_contains(
            "qualityControls", StudyLocusQualityCheck.TOP_HIT.value
        )
        confidence = (
            f.when(
                is_susie & ~out_of_sample,
                CredibleSetConfidenceClasses.FINEMAPPED_IN_SAMPLE_LD.value,
            )
            .when(
                is_susie & out_of_sample,
                CredibleSetConfidenceClasses.FINEMAPPED_OUT_OF_SAMPLE_LD.value,
            )
            .when(
                is_pics & ~top_hit,
                CredibleSetConfidenceClasses.PICSED_SUMMARY_STATS.value,
            )
            .when(is_pics & top_hit, CredibleSetConfidenceClasses.PICSED_TOP_HIT.value)
            .otherwise(CredibleSetConfidenceClasses.UNKNOWN.value)
        )
        return CredibleSet(
            self.df.withColumn("confidence", confidence.cast("byte")), self.get_schema()
        )

    def flag_trans_qtls(
        self: CredibleSet,
        variants: CredibleSetVariant,
        study_index: StudyIndex,
        target_index: TargetIndex,
        trans_threshold: int = 5_000_000,
    ) -> CredibleSet:
        """Flag QTL credible sets all of whose variants are in trans to the measured gene.

        A variant is in trans when it is on another chromosome than the gene, or further than
        `trans_threshold` from its transcription start site. GWAS credible sets and QTL studies
        without a measured gene are never flagged.

        Args:
            variants (CredibleSetVariant): The variants of the credible sets.
            study_index (StudyIndex): Study index, for the measured gene of QTL studies.
            target_index (TargetIndex): Target index, for the gene's chromosome and TSS.
            trans_threshold (int): Distance from the TSS above which a variant is in trans.

        Returns:
            CredibleSet: The credible sets, with `TRANS_QTL` raised.
        """
        if "geneId" not in study_index.df.columns:
            return self
        genes = study_index.df.filter(f.col("studyType") != "gwas").select(
            "studyId", "geneId"
        )
        targets = target_index.df.select(
            f.col("id").alias("geneId"),
            "tss",
            f.col("genomicLocation.chromosome").alias("_geneChromosome"),
        )
        variant_chromosome = f.split("variantId", "_")[0]
        variant_position = extract_position(f.col("variantId")).cast("long")
        trans = (
            self.df.select(f.col("id").alias("credibleSetId"), "studyId")
            .join(genes, on="studyId")
            .join(targets, on="geneId", how="left")
            .join(variants.df.select("credibleSetId", "variantId"), on="credibleSetId")
            .withColumn(
                "_isTagTrans",
                f.coalesce(
                    (variant_chromosome != f.col("_geneChromosome"))
                    | (f.abs(f.col("tss") - variant_position) > trans_threshold),
                    f.lit(False),
                ),
            )
            .groupBy(f.col("credibleSetId").alias("id"))
            .agg(f.min(f.col("_isTagTrans").cast("int")).alias("_allTrans"))
        )
        flagged = self._flagged(
            self.df.join(trans, on="id", how="left"),
            f.col("_allTrans") == 1,
            StudyLocusQualityCheck.TRANS_QTL,
        )
        return CredibleSet(flagged.drop("_allTrans"), self.get_schema())

    def validate_unique_id(
        self: CredibleSet, variants: CredibleSetVariant
    ) -> CredibleSet:
        """Flag credible sets whose `id` is not unique, keeping the one with fewest variants.

        Call this on credible sets that passed the other checks, as in validation: the ordering
        only looks at credible set size, so on unfiltered input it can keep a credible set that
        another flag is about to drop.

        Args:
            variants (CredibleSetVariant): The variants of the credible sets.

        Returns:
            CredibleSet: The credible sets, with `DUPLICATED_STUDYLOCUS_ID` raised.
        """
        sizes = variants.df.groupBy(f.col("credibleSetId").alias("id")).agg(
            f.count("*").alias("_size")
        )
        flagged = self._flagged(
            self.df.join(sizes, on="id", how="left"),
            self.flag_duplicates(f.col("id"), [f.col("_size").asc_nulls_last()]),
            StudyLocusQualityCheck.DUPLICATED_STUDYLOCUS_ID,
        )
        return CredibleSet(flagged.drop("_size"), self.get_schema())

    def qc_replication(
        self: CredibleSet, study_index: StudyIndex, disease_index: DataFrame
    ) -> CredibleSet:
        """Flag credible sets whose lead variant is replicated in an independent study.

        A GWAS credible set is replicated when its lead variant is associated with the same
        diseases in at least two independent studies, a molQTL credible set when its lead variant
        is associated with the same gene in at least two studies.

        What GWAS studies have to agree on depends on the kind of terms they are mapped to:

        - only measurements: any one of the measurements is enough, each matched separately;
        - measurements and diseases: the measurements are set aside and the full list of the
          remaining diseases has to match;
        - only diseases: the full list of diseases has to match.

        A term is a measurement when the disease index files it under the measurement therapeutic
        area. GWAS studies with the same cohorts, publication and LD population structure are the
        same evidence twice and are counted once. GWAS studies without diseases and molQTL studies
        without a measured gene are never flagged.

        Run this last, on the credible sets that passed every other check, against a validated
        study index.

        Args:
            study_index (StudyIndex): Study index, for diseases, genes and study annotation.
            disease_index (DataFrame): Disease index, with `id` and `therapeuticAreas`.

        Returns:
            CredibleSet: The credible sets, with `REPLICATED` raised.
        """
        loci = self.df.select("id", "studyId", "leadVariantId")
        measurements = disease_index.filter(
            f.array_contains("therapeuticAreas", self.MEASUREMENT_THERAPEUTIC_AREA)
        ).select(f.col("id").alias("diseaseId"), f.lit(True).alias("isMeasurement"))
        gwas_studies = study_index.df.filter(
            (f.col("studyType") == "gwas")
            & f.col("diseaseIds").isNotNull()
            & (f.size("diseaseIds") > 0)
        )
        # One replication key per measurement for studies mapped to measurements only, otherwise a
        # single key of the sorted diseases:
        replication_keys = (
            gwas_studies.select(
                "studyId", f.explode(f.array_distinct("diseaseIds")).alias("diseaseId")
            )
            .join(f.broadcast(measurements), on="diseaseId", how="left")
            .groupBy("studyId")
            .agg(
                f.collect_set("diseaseId").alias("allIds"),
                f.collect_set(
                    f.when(f.col("isMeasurement").isNull(), f.col("diseaseId"))
                ).alias("diseaseOnlyIds"),
            )
            .select(
                "studyId",
                f.explode(
                    f.when(
                        f.size("diseaseOnlyIds") == 0,
                        f.transform("allIds", lambda x: f.array(x)),
                    ).otherwise(f.array(f.array_sort("diseaseOnlyIds")))
                ).alias("diseaseIdSet"),
            )
        )
        gwas = loci.join(
            f.broadcast(
                gwas_studies.select(
                    "studyId", "cohorts", "pubmedId", "ldPopulationStructure"
                ).join(replication_keys, on="studyId", how="inner")
            ),
            on="studyId",
        )
        replicated_gwas = gwas.join(
            gwas.select(
                "leadVariantId",
                "diseaseIdSet",
                "cohorts",
                "pubmedId",
                "ldPopulationStructure",
            )
            .distinct()
            .groupBy("leadVariantId", "diseaseIdSet")
            .count()
            .filter(f.col("count") >= 2),
            on=["leadVariantId", "diseaseIdSet"],
        ).select("id")
        molqtl = loci.join(
            f.broadcast(
                study_index.df.filter(
                    (f.col("studyType") != "gwas") & f.col("geneId").isNotNull()
                ).select("studyId", "geneId")
            ),
            on="studyId",
        )
        replicated_molqtl = molqtl.join(
            molqtl.groupBy("leadVariantId", "geneId")
            .count()
            .filter(f.col("count") >= 2),
            on=["leadVariantId", "geneId"],
        ).select("id")
        replicated = (
            replicated_gwas.unionByName(replicated_molqtl)
            .distinct()
            .withColumn("_replicated", f.lit(True))
        )
        flagged = self._flagged(
            self.df.join(replicated, on="id", how="left"),
            f.col("_replicated"),
            StudyLocusQualityCheck.REPLICATED,
        )
        return CredibleSet(flagged.drop("_replicated"), self.get_schema())


@dataclass
class CredibleSetVariant(Dataset):
    """One variant of a credible set, with its marginal and fine-mapping statistics.

    The marginal statistics (`pValueMantissa`, `pValueExponent`, `effectAlleleFrequency`,
    `sampleSize`, `reportedBeta`, `reportedStandardError`) are the ones the study reported. The
    conditional effect of the fine-mapping method, when it gives one, is kept apart in
    `conditionalBeta` and `conditionalStandardError`. Bayes factors are natural logarithms.

    Examples:
        >>> rows = [
        ...     ("cs1", "1_350000_A_T", 1.0, -10, 0.3, 5000, 0.2, 0.01, 0.7,
        ...      None, 1.0, None, None, None, None, None, []),
        ...     ("cs1", "1_350100_C_G", None, None, None, None, None, None, 0.3,
        ...      None, 0.8, None, None, None, None, None,
        ...      ["Variant in LD with the lead has no statistics in the locus"]),
        ... ]
        >>> variants = CredibleSetVariant(
        ...     spark.createDataFrame(rows, CredibleSetVariant.get_schema()),
        ...     CredibleSetVariant.get_schema(),
        ... )
        >>> variants.df.select("variantId", "pValueExponent", "posteriorProbability").show()
        +------------+--------------+--------------------+
        |   variantId|pValueExponent|posteriorProbability|
        +------------+--------------+--------------------+
        |1_350000_A_T|           -10|                 0.7|
        |1_350100_C_G|          NULL|                 0.3|
        +------------+--------------+--------------------+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[CredibleSetVariant]) -> StructType:
        """Provides the schema for the CredibleSetVariant dataset.

        Returns:
            StructType: schema for the CredibleSetVariant dataset.
        """
        return parse_spark_schema("credible_set_variant.json")

    @classmethod
    def get_QC_column_name(cls: type[CredibleSetVariant]) -> str:
        """Quality control column.

        Returns:
            str: Name of the quality control column.
        """
        return "qualityControls"

    @classmethod
    def get_QC_mappings(cls: type[CredibleSetVariant]) -> dict[str, str]:
        """Quality control flag to QC column category mappings.

        Returns:
            dict[str, str]: Mapping between flag name and QC column category value.
        """
        return {member.name: member.value for member in CredibleSetVariantQualityCheck}


@dataclass
class CredibleSetLocus(Dataset):
    """Credible sets joined with their variants, the input locus-to-gene annotates.

    Built by `CredibleSet.to_locus_view`, never persisted. It keeps the shape locus-to-gene
    features were written for: the lead as `variantId` with its `position`, the members in `locus`,
    `confidence` and `isTransQtl`.

    `confidence` is the `CredibleSet` byte, assigned by credible set validation.
    `isTransQtl` is true when the credible set carries the `TRANS_QTL` quality control flag.

    Only the key, the lead and the study are mandatory, as in the study locus the features were
    written for: the view is built from validated datasets, and features read a few columns each.
    """

    @classmethod
    def get_schema(cls: type[CredibleSetLocus]) -> StructType:
        """Provides the schema for the CredibleSetLocus dataset.

        Returns:
            StructType: schema for the CredibleSetLocus dataset.
        """
        return parse_spark_schema("credible_set_locus.json")

    def build_feature_matrix(
        self: CredibleSetLocus,
        features_list: list[str],
        features_input_loader: L2GFeatureInputLoader,
        append_null_features: bool = False,
    ) -> L2GFeatureMatrix:
        """Returns the feature matrix for the credible sets.

        Args:
            features_list (list[str]): List of features to include in the feature matrix.
            features_input_loader (L2GFeatureInputLoader): Feature input loader to use.
            append_null_features (bool): If True, appends null features to the feature matrix. Default is False. Usefull with small datasets that may have all null features, that are anyway required by the model.

        Returns:
            L2GFeatureMatrix: Feature matrix for these credible sets.
        """
        from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix

        feature_matrix = L2GFeatureMatrix.from_features_list(
            self,
            features_list,
            features_input_loader,
        )
        if append_null_features:
            feature_matrix = feature_matrix.append_null_features(features_list)
        return feature_matrix.fill_na()
