"""Study locus dataset."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, ClassVar, Self

import numpy as np
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, FloatType, StringType

from gentropy.common.genomic_region import GenomicRegion
from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark import (
    create_empty_column_if_not_exists,
    get_struct_field_schema,
    order_array_of_structs_by_field,
)
from gentropy.common.stats import get_logsum, neglogpval_from_pvalue, split_pvalue
from gentropy.dataset.dataset import Dataset, qc_test
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
    from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader


class CredibleSetConfidenceClasses(Enum):
    """Confidence assignments for credible sets, based on finemapping method and quality checks.

    List of confidence classes, from the highest to the lowest confidence level.

    Attributes:
        FINEMAPPED_IN_SAMPLE_LD (str): SuSiE fine-mapped credible set with in-sample LD
        FINEMAPPED_OUT_OF_SAMPLE_LD (str): SuSiE fine-mapped credible set with out-of-sample LD
        PICSED_SUMMARY_STATS (str): PICS fine-mapped credible set extracted from summary statistics
        PICSED_TOP_HIT (str): PICS fine-mapped credible set based on reported top hit
        UNKNOWN (str): Unknown confidence, for credible sets which did not fit any of the above categories
    """

    FINEMAPPED_IN_SAMPLE_LD = "SuSiE fine-mapped credible set with in-sample LD"
    FINEMAPPED_OUT_OF_SAMPLE_LD = "SuSiE fine-mapped credible set with out-of-sample LD"
    PICSED_SUMMARY_STATS = (
        "PICS fine-mapped credible set extracted from summary statistics"
    )
    PICSED_TOP_HIT = "PICS fine-mapped credible set based on reported top hit"
    UNKNOWN = "Unknown confidence"


class StudyLocusQualityCheck(Enum):
    """Study locus quality control flags, raised by clumping and LD annotation.

    `CredibleSet.qualityControls` carries the flags of the study locus it was fine-mapped from.

    Attributes:
        WINDOW_CLUMPED (str): Explained by a more significant variant in the same window
        LD_CLUMPED (str): Explained by a more significant variant in high LD
        NO_POPULATION (str): Study does not have population annotation to resolve LD
        UNRESOLVED_LD (str): Variant not found in LD reference
    """

    WINDOW_CLUMPED = "Explained by a more significant variant in the same window"
    LD_CLUMPED = "Explained by a more significant variant in high LD"
    NO_POPULATION = "Study does not have population annotation to resolve LD"
    UNRESOLVED_LD = "Variant not found in LD reference"


class CredibleInterval(Enum):
    """Credible interval enum.

    Interval within which an unobserved parameter value falls with a particular probability.

    Attributes:
        IS95 (str): 95% credible interval
        IS99 (str): 99% credible interval
    """

    IS95 = "is95CredibleSet"
    IS99 = "is99CredibleSet"


class FinemappingMethod(Enum):
    """Finemapping method enum.

    Attributes:
        PICS (str): PICS
        SUSIE (str): SuSiE method
        SUSIE_INF (str): SuSiE-inf method implemented in `gentropy`
    """

    PICS = "PICS"
    SUSIE = "SuSiE"
    SUSIE_INF = "SuSiE-inf"

    @classmethod
    def methods_with_lbf(cls) -> list[str]:
        """List of finemapping methods that report log Bayes factors.

        Returns:
            list[str]: List of finemapping methods that report log Bayes factors.
        """
        return [cls.SUSIE.value, cls.SUSIE_INF.value]


@dataclass
class StudyLocus(Dataset):
    """Study locus: a region of one study that clumping found worth fine-mapping.

    Produced by window, locus-breaker and LD clumping and consumed by fine-mapping. Its key
    `id` hashes `ID_COLUMNS`, and its lead `leadVariantId` is the variant clumping used to
    define the locus. `ldSet` holds the variants correlated with the lead, once the locus
    went through LD annotation.

    A study locus comes in three states, each a subclass of the previous one:

    - `StudyLocus`, as clumping produces it.
    - `AnnotatedStudyLocus`, which adds `locus`: the marginal statistics of every variant the study measured inside the region.
    - `LDAnnotatedStudyLocus`, whose `ldSet` is mandatory.

    Examples:
        >>> import hashlib
        >>> ld_set = [("1_100501_G_A", 0.5)]
        >>> rows = [
        ...     ("", "GCST1", "1", 100_000, 500_000, "1_100500_C_T", [], ld_set),
        ...     ("", "GCST1", "2", 100_000, 500_000, "2_100500_C_T", [], None),
        ... ]
        >>> loci = StudyLocus(
        ...     spark.createDataFrame(rows, StudyLocus.get_schema())
        ...     .withColumn("id", StudyLocus.generate_identifier(StudyLocus.ID_COLUMNS)),
        ...     StudyLocus.get_schema(),
        ... )
        >>> loci.df.select("chromosome", "leadVariantId", "ldSet").show(truncate=False)
        +----------+-------------+---------------------+
        |chromosome|leadVariantId|ldSet                |
        +----------+-------------+---------------------+
        |1         |1_100500_C_T |[{1_100501_G_A, 0.5}]|
        |2         |2_100500_C_T |NULL                 |
        +----------+-------------+---------------------+
        <BLANKLINE>
        >>> StudyLocus.ID_COLUMNS
        ('studyId', 'chromosome', 'locusStart', 'locusEnd')
        >>> loci.df.filter("chromosome = '1'").first().id == hashlib.md5(
        ...     b'{"studyId":"GCST1","chromosome":"1","locusStart":100000,"locusEnd":500000}'
        ... ).hexdigest()
        True
    """

    ID_COLUMNS: ClassVar[tuple[str, ...]] = (
        "studyId",
        "chromosome",
        "locusStart",
        "locusEnd",
    )
    """Columns whose values define the key `id`, hashed by `generate_identifier`."""

    @qc_test
    def validate_variant_identifiers(
        self: StudyLocus, variant_index: VariantIndex
    ) -> StudyLocus:
        """Flagging study loci, where tagging variant identifiers are not found in variant index.

        Args:
            variant_index (VariantIndex): Variant index to resolve variant identifiers.

        Returns:
            StudyLocus: Updated study locus with quality control flags.
        """
        # QC column might not be present in the variant index schema, so we have to be ready to handle it:
        qc_select_expression = (
            f.col("qualityControls")
            if "qualityControls" in self.df.columns
            else f.lit(None).cast(ArrayType(StringType()))
        )

        # Find out which study loci have variants not in the variant index:
        flag = (
            self.df
            # Exploding locus:
            .select("studyLocusId", f.explode("locus").alias("locus"))
            .select("studyLocusId", "locus.variantId")
            # Join with variant index variants:
            .join(
                variant_index.df.select(
                    "variantId", f.lit(True).alias("inVariantIndex")
                ),
                on="variantId",
                how="left",
            )
            # Flagging variants not in the variant index:
            .withColumn("inVariantIndex", f.col("inVariantIndex").isNotNull())
            # Flagging study loci with ANY variants not in the variant index:
            .groupBy("studyLocusId")
            .agg(f.collect_set("inVariantIndex").alias("inVariantIndex"))
            .select(
                "studyLocusId",
                f.array_contains("inVariantIndex", False).alias("toFlag"),
            )
        )

        return StudyLocus(
            _df=(
                self.df.join(flag, on="studyLocusId", how="left")
                .withColumn(
                    "qualityControls",
                    self.update_quality_flag(
                        qc_select_expression,
                        f.col("toFlag"),
                        StudyLocusQualityCheck.INVALID_VARIANT_IDENTIFIER,
                    ),
                )
                .drop("toFlag")
            ),
            _schema=self.get_schema(),
        )

    @qc_test
    def validate_lead_pvalue(self: StudyLocus, pvalue_cutoff: float) -> StudyLocus:
        """Flag associations below significant threshold.

        Args:
            pvalue_cutoff (float): association p-value cut-off

        Returns:
            StudyLocus: Updated study locus with quality control flags.
        """
        df = self.df
        qc_colname = StudyLocus.get_QC_column_name()
        if qc_colname not in self.df.columns:
            df = self.df.withColumn(
                qc_colname,
                create_empty_column_if_not_exists(
                    qc_colname,
                    get_struct_field_schema(StudyLocus.get_schema(), qc_colname),
                ),
            )
        return StudyLocus(
            _df=(
                df.withColumn(
                    qc_colname,
                    # Because this QC might already run on the dataset, the unique set of flags is generated:
                    f.array_distinct(
                        self._qc_subsignificant_associations(
                            f.col("qualityControls"),
                            f.col("pValueMantissa"),
                            f.col("pValueExponent"),
                            pvalue_cutoff,
                        )
                    ),
                )
            ),
            _schema=self.get_schema(),
        )

    @staticmethod
    def _qc_subsignificant_associations(
        quality_controls_column: Column,
        p_value_mantissa: Column,
        p_value_exponent: Column,
        pvalue_cutoff: float,
    ) -> Column:
        """Flag associations below significant threshold.

        Args:
            quality_controls_column (Column): QC column
            p_value_mantissa (Column): P-value mantissa column
            p_value_exponent (Column): P-value exponent column
            pvalue_cutoff (float): association p-value cut-off

        Returns:
            Column: Updated QC column with flag.

        Examples:
            >>> import pyspark.sql.types as t
            >>> d = [{'qc': None, 'p_value_mantissa': 1, 'p_value_exponent': -7}, {'qc': None, 'p_value_mantissa': 1, 'p_value_exponent': -8}, {'qc': None, 'p_value_mantissa': 5, 'p_value_exponent': -8}, {'qc': None, 'p_value_mantissa': 1, 'p_value_exponent': -9}]
            >>> df = spark.createDataFrame(d, t.StructType([t.StructField('qc', t.ArrayType(t.StringType()), True), t.StructField('p_value_mantissa', t.IntegerType()), t.StructField('p_value_exponent', t.IntegerType())]))
            >>> df.withColumn('qc', StudyLocus._qc_subsignificant_associations(f.col("qc"), f.col("p_value_mantissa"), f.col("p_value_exponent"), 5e-8)).show(truncate = False)
            +------------------------+----------------+----------------+
            |qc                      |p_value_mantissa|p_value_exponent|
            +------------------------+----------------+----------------+
            |[Subsignificant p-value]|1               |-7              |
            |[]                      |1               |-8              |
            |[]                      |5               |-8              |
            |[]                      |1               |-9              |
            +------------------------+----------------+----------------+
            <BLANKLINE>

        """
        return StudyLocus.update_quality_flag(
            quality_controls_column,
            neglogpval_from_pvalue(p_value_mantissa, p_value_exponent)
            < f.lit(-np.log10(pvalue_cutoff)),
            StudyLocusQualityCheck.SUBSIGNIFICANT_FLAG,
        )

    @staticmethod
    def _overlapping_peaks(
        credset_to_overlap: DataFrame,
        restrict_right_studies: list[str] | None = None,
        gwas_v_qtl_overlap_only: bool = False,
    ) -> DataFrame:
        """Calculate overlapping signals (study-locus) between GWAS-GWAS and GWAS-Molecular trait.

        Args:
            credset_to_overlap (DataFrame): DataFrame containing at least `studyLocusId`, `studyType`, `chromosome` and `tagVariantId` columns.
            restrict_right_studies (list[str] | None): List of studyIds to restrict finding overlaps on the right side. Default is None.
            gwas_v_qtl_overlap_only (bool): When True, finds overlaps with just molecular-QTLs on the right-hand side. Default is False.

        Returns:
            DataFrame: containing `leftStudyLocusId`, `rightStudyLocusId` and `chromosome` columns.
        """
        # Reduce columns to the minimum to reduce the size of the dataframe
        credset_to_overlap = credset_to_overlap.select(
            "studyLocusId",
            "studyId",
            "studyType",
            "chromosome",
            "region",
            "tagVariantId",
        )
        # Define join condition
        # If gwas_v_qtl_overlap_only is True, finds only gwas vs molQTL overlaps.
        if gwas_v_qtl_overlap_only:
            join_condition = [
                f.col("left.chromosome") == f.col("right.chromosome"),
                f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                f.col("left.studyType") == f.lit("gwas"),
                f.col("right.studyType") != f.lit("gwas"),
            ]
        # If restrict_right_studies is not empty, restrict finding overlaps to those studies
        elif restrict_right_studies is not None:
            join_condition = [
                f.col("left.chromosome") == f.col("right.chromosome"),
                f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                f.col("left.studyType") == f.lit("gwas"),
                (f.col("right.studyId").isin(restrict_right_studies))
                & (~f.col("left.studyId").isin(restrict_right_studies))
                & (f.col("left.studyLocusId") != f.col("right.studyLocusId")),
            ]
        else:
            join_condition = [
                f.col("left.chromosome") == f.col("right.chromosome"),
                f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                (f.col("right.studyType") != "gwas")
                | (f.col("left.studyLocusId") > f.col("right.studyLocusId")),
                f.col("left.studyType") == f.lit("gwas"),
            ]

        return (
            credset_to_overlap.alias("left")
            # Self join with complex condition.
            .join(
                credset_to_overlap.alias("right"),
                on=join_condition,
                how="inner",
            )
            .select(
                f.col("left.studyLocusId").alias("leftStudyLocusId"),
                f.col("right.studyLocusId").alias("rightStudyLocusId"),
                f.col("right.studyType").alias("rightStudyType"),
                f.col("left.chromosome").alias("chromosome"),
            )
            .distinct()
            # NOTE: avoid `.repartition("chromosome")` here: with ~25 distinct
            # chromosomes it collapses the result into a handful of severely
            # skewed partitions (chr6/HLA dominates), capping downstream
            # parallelism and risking spill/OOM. `.distinct()` already produces
            # balanced `spark.sql.shuffle.partitions` partitions.
            .persist()
        )

    @staticmethod
    def _align_overlapping_tags(
        loci_to_overlap: DataFrame, peak_overlaps: DataFrame
    ) -> StudyLocusOverlap:
        """Align overlapping tags in pairs of overlapping study-locus, keeping all tags in both loci.

        Args:
            loci_to_overlap (DataFrame): containing `studyLocusId`, `studyType`, `chromosome`, `tagVariantId`, `logBF` and `posteriorProbability` columns.
            peak_overlaps (DataFrame): containing `leftStudyLocusId`, `rightStudyLocusId` and `chromosome` columns.

        Returns:
            StudyLocusOverlap: Pairs of overlapping study-locus with aligned tags.
        """
        # Complete information about all tags in the left study-locus of the overlap
        stats_cols = [
            "logBF",
            "posteriorProbability",
            "beta",
            "pValueMantissa",
            "pValueExponent",
        ]
        overlapping_left = loci_to_overlap.select(
            f.col("chromosome"),
            f.col("tagVariantId"),
            f.col("studyLocusId").alias("leftStudyLocusId"),
            *[f.col(col).alias(f"left_{col}") for col in stats_cols],
        ).join(peak_overlaps, on=["chromosome", "leftStudyLocusId"], how="inner")

        # Complete information about all tags in the right study-locus of the overlap
        overlapping_right = loci_to_overlap.select(
            f.col("chromosome"),
            f.col("tagVariantId"),
            f.col("studyLocusId").alias("rightStudyLocusId"),
            *[f.col(col).alias(f"right_{col}") for col in stats_cols],
        ).join(peak_overlaps, on=["chromosome", "rightStudyLocusId"], how="inner")

        # Include information about all tag variants in both study-locus aligned by tag variant id
        overlaps = overlapping_left.join(
            overlapping_right,
            on=[
                "chromosome",
                "rightStudyLocusId",
                "leftStudyLocusId",
                "tagVariantId",
                "rightStudyType",
            ],
            how="outer",
        ).select(
            "leftStudyLocusId",
            "rightStudyLocusId",
            "rightStudyType",
            "chromosome",
            "tagVariantId",
            f.struct(
                *[f"left_{e}" for e in stats_cols] + [f"right_{e}" for e in stats_cols]
            ).alias("statistics"),
        )
        return StudyLocusOverlap(
            _df=overlaps,
            _schema=StudyLocusOverlap.get_schema(),
        )

    @classmethod
    def calculate_credible_set_log10bf(
        cls: type[StudyLocus], logbfs: Column, num_variants_region: int = 500
    ) -> Column:
        """Calculate Bayes factor for the entire credible set. The Bayes factor is calculated as the logsumexp of the logBF values of the variants in the locus.

        Args:
            logbfs (Column): Array column with the logBF values of the variants in the locus.
            num_variants_region (int): Number of variants in the region for calculation of priors. Default: 500.

        Returns:
            Column: log10 Bayes factor for the entire credible set.

        Examples:
            >>> spark.createDataFrame([([1.0, 0.5, 0.25, 0.0],)]).toDF("logBF").select(f.round(StudyLocus.calculate_credible_set_log10bf(f.col("logBF"), 4), 7).alias("credibleSetlog10BF")).show()
            +------------------+
            |credibleSetlog10BF|
            +------------------+
            |         0.2208288|
            +------------------+
            <BLANKLINE>
        """
        # log10=log/log(10)=log*0.43429448190325176
        logsumexp_udf = f.udf(
            lambda x: (
                get_logsum(x + np.log(1 / num_variants_region)) * 0.43429448190325176
            ),
            FloatType(),
        )
        return logsumexp_udf(logbfs).cast("double").alias("credibleSetlog10BF")

    @classmethod
    def get_schema(cls: type[StudyLocus]) -> StructType:
        """Provides the schema for the StudyLocus dataset.

        Returns:
            StructType: schema for the StudyLocus dataset.
        """
        return parse_spark_schema("study_locus.json")

    @classmethod
    def get_QC_column_name(cls: type[StudyLocus]) -> str:
        """Quality control column.

        Returns:
            str: Name of the quality control column.
        """
        return "qualityControls"

    @classmethod
    def get_QC_mappings(cls: type[StudyLocus]) -> dict[str, str]:
        """Quality control flag to QC column category mappings.

        Returns:
            dict[str, str]: Mapping between flag name and QC column category value.
        """
        return {member.name: member.value for member in StudyLocusQualityCheck}

    def filter_credible_set(
        self: StudyLocus,
        credible_interval: CredibleInterval,
    ) -> StudyLocus:
        """Annotate and filter study-locus tag variants based on given credible interval.

        Args:
            credible_interval (CredibleInterval): Credible interval to filter for.

        Returns:
            StudyLocus: Filtered study-locus dataset.
        """
        return StudyLocus(
            _df=self.annotate_credible_sets().df.withColumn(
                "locus",
                f.filter(
                    f.col("locus"),
                    lambda tag: tag[credible_interval.value],
                ),
            ),
            _schema=self._schema,
        )

    def find_overlaps(
        self: StudyLocus,
        restrict_right_studies: list[str] | None = None,
        gwas_v_qtl_overlap_only: bool = False,
    ) -> StudyLocusOverlap:
        """Calculate overlapping study-locus.

        Find overlapping study-locus that share at least one tagging variant. By default all GWAS-GWAS and all GWAS-Molecular traits are computed with the Molecular traits always
        appearing on the right side of the overlap. The user can restrict computation of overlaps to a list of studyIds on the right-hand side or
        can restrict the overlaps to just GWAS vs Molecular traits.

        Args:
            restrict_right_studies (list[str] | None): List of studyIds to restrict finding overlaps to on the right-side. Default is None.
            gwas_v_qtl_overlap_only (bool): If True, finds overlaps between all GWAS loci vs molecular-QTLs. Default is False.

        Returns:
            StudyLocusOverlap: Pairs of overlapping study-locus with aligned tags.
        """
        loci_to_overlap = (
            self.df.filter(f.col("studyType").isNotNull())
            .withColumn("locus", f.explode("locus"))
            .select(
                "studyLocusId",
                "studyId",
                "studyType",
                "chromosome",
                "region",
                f.col("locus.variantId").alias("tagVariantId"),
                f.col("locus.logBF").alias("logBF"),
                f.col("locus.posteriorProbability").alias("posteriorProbability"),
                f.col("locus.pValueMantissa").alias("pValueMantissa"),
                f.col("locus.pValueExponent").alias("pValueExponent"),
                f.col("locus.beta").alias("beta"),
            )
            .persist()
        )

        # overlapping study-locus
        peak_overlaps = self._overlapping_peaks(
            loci_to_overlap,
            restrict_right_studies=restrict_right_studies,
            gwas_v_qtl_overlap_only=gwas_v_qtl_overlap_only,
        )

        # study-locus overlap by aligning overlapping variants
        return self._align_overlapping_tags(loci_to_overlap, peak_overlaps)

    def unique_variants_in_locus(self: StudyLocus) -> DataFrame:
        """All unique variants collected in a `StudyLocus` dataframe.

        Returns:
            DataFrame: A dataframe containing `variantId` and `chromosome` columns.
        """
        return (
            self.df.withColumn(
                "variantId",
                # Joint array of variants in that studylocus. Locus can be null
                f.explode(
                    f.array_union(
                        f.array(f.col("variantId")),
                        f.coalesce(f.col("locus.variantId"), f.array()),
                    )
                ),
            )
            .select(
                "variantId", f.split(f.col("variantId"), "_")[0].alias("chromosome")
            )
            .distinct()
        )

    def neglog_pvalue(self: StudyLocus) -> Column:
        """Returns the negative log p-value.

        Returns:
            Column: Negative log p-value
        """
        return neglogpval_from_pvalue(
            self.df.pValueMantissa,
            self.df.pValueExponent,
        )

    def build_feature_matrix(
        self: StudyLocus,
        features_list: list[str],
        features_input_loader: L2GFeatureInputLoader,
        append_null_features: bool = False,
    ) -> L2GFeatureMatrix:
        """Returns the feature matrix for a StudyLocus.

        Args:
            features_list (list[str]): List of features to include in the feature matrix.
            features_input_loader (L2GFeatureInputLoader): Feature input loader to use.
            append_null_features (bool): If True, appends null features to the feature matrix. Default is False. Usefull with small datasets that may have all null features, that are anyway required by the model.

        Returns:
            L2GFeatureMatrix: Feature matrix for this study-locus.
        """
        from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix

        if append_null_features:
            feature_matrix = (
                L2GFeatureMatrix.from_features_list(
                    self,
                    features_list,
                    features_input_loader,
                )
                .append_null_features(features_list)
                .fill_na()
            )
        else:
            feature_matrix = L2GFeatureMatrix.from_features_list(
                self,
                features_list,
                features_input_loader,
            ).fill_na()

        return feature_matrix

    def annotate_credible_sets(self: StudyLocus) -> StudyLocus:
        """Annotate study-locus dataset with credible set flags.

        Sorts the array in the `locus` column elements by their `posteriorProbability` values in descending order and adds
        `is95CredibleSet` and `is99CredibleSet` fields to the elements, indicating which are the tagging variants whose cumulative sum
        of their `posteriorProbability` values is below 0.95 and 0.99, respectively.

        Returns:
            StudyLocus: including annotation on `is95CredibleSet` and `is99CredibleSet`.

        Raises:
            ValueError: If `locus` column is not available.
        """
        if "locus" not in self.df.columns:
            raise ValueError("Locus column not available.")

        self.df = self.df.withColumn(
            # Sort credible set by posterior probability in descending order
            "locus",
            f.when(
                f.col("locus").isNotNull() & (f.size(f.col("locus")) > 0),
                order_array_of_structs_by_field("locus", "posteriorProbability"),
            ),
        ).withColumn(
            # Calculate array of cumulative sums of posterior probabilities to determine which variants are in the 95% and 99% credible sets
            # and zip the cumulative sums array with the credible set array to add the flags
            "locus",
            f.when(
                f.col("locus").isNotNull() & (f.size(f.col("locus")) > 0),
                f.zip_with(
                    f.col("locus"),
                    f.transform(
                        f.sequence(f.lit(1), f.size(f.col("locus"))),
                        lambda index: f.aggregate(
                            f.slice(
                                # By using `index - 1` we introduce a value of `0.0` in the cumulative sums array. to ensure that the last variant
                                # that exceeds the 0.95 threshold is included in the cumulative sum, as its probability is necessary to satisfy the threshold.
                                f.col("locus.posteriorProbability"),
                                1,
                                index - 1,
                            ),
                            f.lit(0.0),
                            lambda acc, el: acc + el,
                        ),
                    ),
                    lambda struct_e, acc: struct_e.withField(
                        CredibleInterval.IS95.value, (acc < 0.95) & acc.isNotNull()
                    ).withField(
                        CredibleInterval.IS99.value, (acc < 0.99) & acc.isNotNull()
                    ),
                ),
            ),
        )
        return self

    def exclude_region(
        self: StudyLocus, region: GenomicRegion, exclude_overlap: bool = False
    ) -> StudyLocus:
        """Exclude a region from the StudyLocus dataset.

        Args:
            region (GenomicRegion): genomic region object.
            exclude_overlap (bool): If True, excludes StudyLocus windows with any overlap with the region.

        Returns:
            StudyLocus: filtered StudyLocus object.
        """
        if exclude_overlap:
            filter_condition = ~(
                (f.col("chromosome") == region.chromosome)
                & (
                    (f.col("locusStart") <= region.end)
                    & (f.col("locusEnd") >= region.start)
                )
            )
        else:
            filter_condition = ~(
                (f.col("chromosome") == region.chromosome)
                & (
                    (f.col("position") >= region.start)
                    & (f.col("position") <= region.end)
                )
            )

        return StudyLocus(
            _df=self.df.filter(filter_condition),
            _schema=StudyLocus.get_schema(),
        )

    def assign_confidence(self: StudyLocus) -> StudyLocus:
        """Assign confidence to study locus.

        Returns:
            StudyLocus: Study locus with confidence assigned.
        """
        # Return self if the required columns are not in the dataframe:
        if (
            "qualityControls" not in self.df.columns
            or "finemappingMethod" not in self.df.columns
        ):
            return self

        # Assign confidence based on the presence of quality controls
        df = self.df.withColumn(
            "confidence",
            f.when(
                (
                    f.col("finemappingMethod").isin(
                        FinemappingMethod.SUSIE.value,
                        FinemappingMethod.SUSIE_INF.value,
                    )
                )
                & (
                    ~f.array_contains(
                        f.col("qualityControls"),
                        StudyLocusQualityCheck.OUT_OF_SAMPLE_LD.value,
                    )
                ),
                CredibleSetConfidenceClasses.FINEMAPPED_IN_SAMPLE_LD.value,
            )
            .when(
                (
                    f.col("finemappingMethod").isin(
                        FinemappingMethod.SUSIE.value,
                        FinemappingMethod.SUSIE_INF.value,
                    )
                )
                & (
                    f.array_contains(
                        f.col("qualityControls"),
                        StudyLocusQualityCheck.OUT_OF_SAMPLE_LD.value,
                    )
                ),
                CredibleSetConfidenceClasses.FINEMAPPED_OUT_OF_SAMPLE_LD.value,
            )
            .when(
                (f.col("finemappingMethod") == FinemappingMethod.PICS.value)
                & (
                    ~f.array_contains(
                        f.col("qualityControls"), StudyLocusQualityCheck.TOP_HIT.value
                    )
                ),
                CredibleSetConfidenceClasses.PICSED_SUMMARY_STATS.value,
            )
            .when(
                (f.col("finemappingMethod") == FinemappingMethod.PICS.value)
                & (
                    f.array_contains(
                        f.col("qualityControls"), StudyLocusQualityCheck.TOP_HIT.value
                    )
                ),
                CredibleSetConfidenceClasses.PICSED_TOP_HIT.value,
            )
            .otherwise(CredibleSetConfidenceClasses.UNKNOWN.value),
        )

        return StudyLocus(
            _df=df,
            _schema=self.get_schema(),
        )


@dataclass
class AnnotatedStudyLocus(StudyLocus):
    """Study locus whose `locus` holds the marginal statistics of every variant inside its region.

    `locus` is collected from the summary statistics of the same study and chromosome, between
    `locusStart` and `locusEnd` (both included), sorted by position. It is empty, not null,
    when the study measured no variant inside the region.

    Examples:
        >>> locus = [("1_100500_C_T", 1.0, -10, 0.3, 5000, 0.2, 0.01)]
        >>> rows = [("sl1", "GCST1", "1", 100_000, 500_000, "1_100500_C_T", [], locus, None)]
        >>> loci = AnnotatedStudyLocus(
        ...     spark.createDataFrame(rows, AnnotatedStudyLocus.get_schema()),
        ...     AnnotatedStudyLocus.get_schema(),
        ... )
        >>> loci.df.select("id", f.explode("locus.beta").alias("beta")).show()
        +---+----+
        | id|beta|
        +---+----+
        |sl1| 0.2|
        +---+----+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[AnnotatedStudyLocus]) -> StructType:
        """Provides the schema for the AnnotatedStudyLocus dataset.

        Returns:
            StructType: schema for the AnnotatedStudyLocus dataset.
        """
        return parse_spark_schema("annotated_study_locus.json")

    def lead_pvalue_filter(self: Self, pvalue: float) -> Self:
        """Keep the loci whose lead is at least as significant as `pvalue`.

        The lead's p-value is its own entry in `locus`. A locus whose lead has no
        entry, or no full p-value, is dropped.

        Args:
            pvalue (float): The largest lead p-value to keep, between 0 and 1.

        Returns:
            Self: The loci that pass, as the same dataset type.

        Examples:
            >>> def locus(lead, exponent):
            ...     return [(lead, 5.0, exponent, 0.3, 1000, 0.1, 0.01)]
            >>> rows = [
            ...     ("L1", "S1", "1", 1, 1000, "A", [], locus("A", -8), None),
            ...     ("L2", "S1", "1", 1, 1000, "B", [], locus("B", -7), None),
            ...     ("L3", "S1", "1", 1, 1000, "C", [], [], None),
            ... ]
            >>> loci = AnnotatedStudyLocus(
            ...     spark.createDataFrame(rows, AnnotatedStudyLocus.get_schema()),
            ...     AnnotatedStudyLocus.get_schema(),
            ... )
            >>> [row.id for row in loci.lead_pvalue_filter(5e-8).df.collect()]
            ['L1']
        """
        mantissa, exponent = split_pvalue(pvalue)
        lead = f.get(
            f.filter("locus", lambda e: e["variantId"] == f.col("leadVariantId")), 0
        )
        return self.filter(
            (lead["pValueExponent"] < exponent)
            | (
                (lead["pValueExponent"] == exponent)
                & (lead["pValueMantissa"] <= mantissa)
            )
        )


@dataclass
class LDAnnotatedStudyLocus(AnnotatedStudyLocus):
    """Annotated study locus whose `ldSet` holds the variants in LD with the lead.

    `r2Overall` is the r² with the lead in the major population of the study. The lead is
    always part of its own `ldSet`, with an r² of 1.

    Examples:
        >>> ld_set = [("1_100500_C_T", 1.0), ("1_100501_G_A", 0.5)]
        >>> rows = [("sl1", "GCST1", "1", 100_000, 500_000, "1_100500_C_T", [], [], ld_set)]
        >>> loci = LDAnnotatedStudyLocus(
        ...     spark.createDataFrame(rows, LDAnnotatedStudyLocus.get_schema()),
        ...     LDAnnotatedStudyLocus.get_schema(),
        ... )
        >>> loci.df.select("id", f.size("ldSet").alias("tags")).show()
        +---+----+
        | id|tags|
        +---+----+
        |sl1|   2|
        +---+----+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[LDAnnotatedStudyLocus]) -> StructType:
        """Provides the schema for the LDAnnotatedStudyLocus dataset.

        Returns:
            StructType: schema for the LDAnnotatedStudyLocus dataset.
        """
        return parse_spark_schema("ld_annotated_study_locus.json")
