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
  uniform prior over them (`gentropy.common.stats.region_log_bayes_factor`). Credible set Bayes
  factors that a source reports with another definition, such as FinnGen's model comparison
  `cs_log10bf`, are not stored.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, ClassVar

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.processing import extract_position
from gentropy.common.schemas import parse_spark_schema
from gentropy.common.stats import split_pvalue
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_locus import StudyLocusQualityCheck

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
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
            mantissa, exponent = split_pvalue(lead_pvalue_threshold)
            passes = (f.col("pValueExponent") < exponent) | (
                (f.col("pValueExponent") == exponent)
                & (f.col("pValueMantissa") <= mantissa)
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

    `confidence` is the `CredibleSet` byte, assigned by the platform transformation stage.
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
