"""Variant index dataset."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from dataclasses import dataclass
from enum import Enum
from itertools import chain
from typing import TYPE_CHECKING

import numpy as np
import pyspark.sql.functions as f
from pyspark.sql.types import DoubleType, IntegerType, LongType
from pyspark.sql.window import Window

from otg.assets import data
from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import (
    calculate_neglog_pvalue,
    get_record_with_maximum_value,
    order_array_of_structs_by_field,
    pvalue_to_zscore,
)
from otg.common.utils import parse_efos
from otg.dataset.dataset import Dataset
from otg.dataset.study_locus_overlap import StudyLocusOverlap
from otg.method.clump import LDclumping
from otg.method.ld import LDAnnotator

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType

    from otg.dataset.ld_index import LDIndex
    from otg.dataset.study_index import StudyIndex, StudyIndexGWASCatalog
    from otg.dataset.variant_annotation import VariantAnnotation


class StudyLocusQualityCheck(Enum):
    """Study-Locus quality control options listing concerns on the quality of the association.

    Attributes:
        SUBSIGNIFICANT_FLAG (str): p-value below significance threshold
        NO_GENOMIC_LOCATION_FLAG (str): Incomplete genomic mapping
        COMPOSITE_FLAG (str): Composite association due to variant x variant interactions
        VARIANT_INCONSISTENCY_FLAG (str): Inconsistencies in the reported variants
        NON_MAPPED_VARIANT_FLAG (str): Variant not mapped to GnomAd
        PALINDROMIC_ALLELE_FLAG (str): Alleles are palindromic - cannot harmonize
        AMBIGUOUS_STUDY (str): Association with ambiguous study
        UNRESOLVED_LD (str): Variant not found in LD reference
        LD_CLUMPED (str): Explained by a more significant variant in high LD (clumped)
    """

    SUBSIGNIFICANT_FLAG = "Subsignificant p-value"
    NO_GENOMIC_LOCATION_FLAG = "Incomplete genomic mapping"
    COMPOSITE_FLAG = "Composite association"
    INCONSISTENCY_FLAG = "Variant inconsistency"
    NON_MAPPED_VARIANT_FLAG = "No mapping in GnomAd"
    PALINDROMIC_ALLELE_FLAG = "Palindrome alleles - cannot harmonize"
    AMBIGUOUS_STUDY = "Association with ambiguous study"
    UNRESOLVED_LD = "Variant not found in LD reference"
    LD_CLUMPED = "Explained by a more significant variant in high LD (clumped)"


class CredibleInterval(Enum):
    """Credible interval enum.

    Interval within which an unobserved parameter value falls with a particular probability.

    Attributes:
        IS95 (str): 95% credible interval
        IS99 (str): 99% credible interval
    """

    IS95 = "is95CredibleSet"
    IS99 = "is99CredibleSet"


@dataclass
class StudyLocus(Dataset):
    """Study-Locus dataset.

    This dataset captures associations between study/traits and a genetic loci as provided by finemapping methods.
    """

    @staticmethod
    def _overlapping_peaks(credset_to_overlap: DataFrame) -> DataFrame:
        """Calculate overlapping signals (study-locus) between GWAS-GWAS and GWAS-Molecular trait.

        Args:
            credset_to_overlap (DataFrame): DataFrame containing at least `studyLocusId`, `studyType`, `chromosome` and `tagVariantId` columns.

        Returns:
            DataFrame: containing `left_studyLocusId`, `right_studyLocusId` and `chromosome` columns.
        """
        # Reduce columns to the minimum to reduce the size of the dataframe
        credset_to_overlap = credset_to_overlap.select(
            "studyLocusId", "studyType", "chromosome", "tagVariantId"
        )
        return (
            credset_to_overlap.alias("left")
            .filter(f.col("studyType") == "gwas")
            # Self join with complex condition. Left it's all gwas and right can be gwas or molecular trait
            .join(
                credset_to_overlap.alias("right"),
                on=[
                    f.col("left.chromosome") == f.col("right.chromosome"),
                    f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                    (f.col("right.studyType") != "gwas")
                    | (f.col("left.studyLocusId") > f.col("right.studyLocusId")),
                ],
                how="inner",
            )
            .select(
                f.col("left.studyLocusId").alias("left_studyLocusId"),
                f.col("right.studyLocusId").alias("right_studyLocusId"),
                f.col("left.chromosome").alias("chromosome"),
            )
            .distinct()
            .repartition("chromosome")
            .persist()
        )

    @staticmethod
    def _align_overlapping_tags(
        credset_to_overlap: DataFrame, peak_overlaps: DataFrame
    ) -> StudyLocusOverlap:
        """Align overlapping tags in pairs of overlapping study-locus, keeping all tags in both loci.

        Args:
            credset_to_overlap (DataFrame): containing `studyLocusId`, `studyType`, `chromosome`, `tagVariantId`, `logABF` and `posteriorProbability` columns.
            peak_overlaps (DataFrame): containing `left_studyLocusId`, `right_studyLocusId` and `chromosome` columns.

        Returns:
            StudyLocusOverlap: Pairs of overlapping study-locus with aligned tags.
        """
        # Complete information about all tags in the left study-locus of the overlap
        stats_cols = ["logABF", "posteriorProbability", "tagPValue", "tagBeta"]
        overlapping_left = credset_to_overlap.select(
            f.col("chromosome"),
            f.col("tagVariantId"),
            f.col("studyLocusId").alias("left_studyLocusId"),
            *[f.col(col).alias(f"left_{col}") for col in stats_cols],
        ).join(peak_overlaps, on=["chromosome", "left_studyLocusId"], how="inner")

        # Complete information about all tags in the right study-locus of the overlap
        overlapping_right = credset_to_overlap.select(
            f.col("chromosome"),
            f.col("tagVariantId"),
            f.col("studyLocusId").alias("right_studyLocusId"),
            *[f.col(col).alias(f"right_{col}") for col in stats_cols],
        ).join(peak_overlaps, on=["chromosome", "right_studyLocusId"], how="inner")

        # Include information about all tag variants in both study-locus aligned by tag variant id
        overlaps = overlapping_left.join(
            overlapping_right,
            on=[
                "chromosome",
                "right_studyLocusId",
                "left_studyLocusId",
                "tagVariantId",
            ],
            how="outer",
        ).select(
            "left_studyLocusId",
            "right_studyLocusId",
            "chromosome",
            "tagVariantId",
            f.struct(
                *[f"left_{e}" for e in stats_cols] + [f"right_{e}" for e in stats_cols]
            ).alias("statistics"),
        )
        return StudyLocusOverlap(
            _df=overlaps,
            _schema=StudyLocusOverlap._get_schema(),
        )

    @staticmethod
    def _update_quality_flag(
        qc: Column, flag_condition: Column, flag_text: StudyLocusQualityCheck
    ) -> Column:
        """Update the provided quality control list with a new flag if condition is met.

        Args:
            qc (Column): Array column with the current list of qc flags.
            flag_condition (Column): This is a column of booleans, signing which row should be flagged
            flag_text (StudyLocusQualityCheck): Text for the new quality control flag

        Returns:
            Column: Array column with the updated list of qc flags.
        """
        qc = f.when(qc.isNull(), f.array()).otherwise(qc)
        return f.when(
            flag_condition,
            f.array_union(qc, f.array(f.lit(flag_text.value))),
        ).otherwise(qc)

    @staticmethod
    def _filter_credible_set(credible_set: Column) -> Column:
        """Filter credible set to only contain variants that belong to the 95% credible set.

        Args:
            credible_set (Column): Credible set column containing all variants in the LD set.

        Returns:
            Column: Filtered credible set column.

        Example:
            >>> df = spark.createDataFrame([([{"tagVariantId": "varA", "is95CredibleSet": True}, {"tagVariantId": "varB", "is95CredibleSet": False}],)], "credibleSet: array<struct<tagVariantId: string, is95CredibleSet: boolean>>")
            >>> df.select(StudyLocus._filter_credible_set(f.col("credibleSet")).alias("filtered")).show(truncate=False)
            +--------------+
            |filtered      |
            +--------------+
            |[{varA, true}]|
            +--------------+
            <BLANKLINE>
        """
        return f.filter(credible_set, lambda x: x["is95CredibleSet"])

    @staticmethod
    def assign_study_locus_id(study_id_col: Column, variant_id_col: Column) -> Column:
        """Hashes a column with a variant ID and a study ID to extract a consistent studyLocusId.

        Args:
            study_id_col (Column): column name with a study ID
            variant_id_col (Column): column name with a variant ID

        Returns:
            Column: column with a study locus ID

        Examples:
            >>> df = spark.createDataFrame([("GCST000001", "1_1000_A_C"), ("GCST000002", "1_1000_A_C")]).toDF("studyId", "variantId")
            >>> df.withColumn("study_locus_id", StudyLocus.assign_study_locus_id(*[f.col("variantId"), f.col("studyId")])).show()
            +----------+----------+--------------------+
            |   studyId| variantId|      study_locus_id|
            +----------+----------+--------------------+
            |GCST000001|1_1000_A_C| 7437284926964690765|
            |GCST000002|1_1000_A_C|-7653912547667845377|
            +----------+----------+--------------------+
            <BLANKLINE>
        """
        return f.xxhash64(*[study_id_col, variant_id_col]).alias("studyLocusId")

    @classmethod
    def _get_schema(cls: type[StudyLocus]) -> StructType:
        """Provides the schema for the StudyLocus dataset."""
        return parse_spark_schema("study_locus.json")

    def credible_set(
        self: StudyLocus,
        credible_interval: CredibleInterval,
    ) -> StudyLocus:
        """Filter study-locus tag variants based on given credible interval.

        Args:
            credible_interval (CredibleInterval): Credible interval to filter for.

        Returns:
            StudyLocus: Filtered study-locus dataset.
        """
        self.df = self._df.withColumn(
            "credibleSet",
            f.expr(f"filter(credibleSet, tag -> (tag.{credible_interval.value}))"),
        )
        return self

    def find_overlaps(self: StudyLocus, study_index: StudyIndex) -> StudyLocusOverlap:
        """Calculate overlapping study-locus.

        Find overlapping study-locus that share at least one tagging variant. All GWAS-GWAS and all GWAS-Molecular traits are computed with the Molecular traits always
        appearing on the right side.

        Args:
            study_index (StudyIndex): Study index to resolve study types.

        Returns:
            StudyLocusOverlap: Pairs of overlapping study-locus with aligned tags.
        """
        credset_to_overlap = (
            self.df.join(study_index.study_type_lut(), on="studyId", how="inner")
            .withColumn("credibleSet", f.explode("credibleSet"))
            .select(
                "studyLocusId",
                "studyType",
                "chromosome",
                f.col("credibleSet.tagVariantId").alias("tagVariantId"),
                f.col("credibleSet.logABF").alias("logABF"),
                f.col("credibleSet.posteriorProbability").alias("posteriorProbability"),
                f.col("credibleSet.tagPValue").alias("tagPValue"),
                f.col("credibleSet.tagBeta").alias("tagBeta"),
            )
            .persist()
        )

        # overlapping study-locus
        peak_overlaps = self._overlapping_peaks(credset_to_overlap)

        # study-locus overlap by aligning overlapping variants
        return self._align_overlapping_tags(credset_to_overlap, peak_overlaps)

    def unique_lead_tag_variants(self: StudyLocus) -> DataFrame:
        """All unique lead and tag variants contained in the `StudyLocus` dataframe.

        Returns:
            DataFrame: A dataframe containing `variantId` and `chromosome` columns.
        """
        lead_tags = (
            self.df.select(
                f.col("variantId"),
                f.col("chromosome"),
                f.explode("ldSet.tagVariantId").alias("tagVariantId"),
            )
            .repartition("chromosome")
            .persist()
        )
        return (
            lead_tags.select("variantId", "chromosome")
            .union(
                lead_tags.select(f.col("tagVariantId").alias("variantId"), "chromosome")
            )
            .distinct()
        )

    def neglog_pvalue(self: StudyLocus) -> Column:
        """Returns the negative log p-value.

        Returns:
            Column: Negative log p-value
        """
        return calculate_neglog_pvalue(
            self.df.pValueMantissa,
            self.df.pValueExponent,
        )

    def annotate_credible_sets(self: StudyLocus) -> StudyLocus:
        """Annotate study-locus dataset with credible set flags.

        Sorts the array in the `credibleSet` column elements by their `posteriorProbability` values in descending order and adds
        `is95CredibleSet` and `is99CredibleSet` fields to the elements, indicating which are the tagging variants whose cumulative sum
        of their `posteriorProbability` values is below 0.95 and 0.99, respectively.

        Returns:
            StudyLocus: including annotation on `is95CredibleSet` and `is99CredibleSet`.
        """
        self.df = (
            self.df.withColumn(
                # Sort credible set by posterior probability in descending order
                "credibleSet",
                f.when(
                    f.size(f.col("credibleSet")) > 0,
                    order_array_of_structs_by_field(
                        "credibleSet", "posteriorProbability"
                    ),
                ).when(f.size(f.col("credibleSet")) == 0, f.col("credibleSet")),
            )
            .withColumn(
                # Calculate array of cumulative sums of posterior probabilities to determine which variants are in the 95% and 99% credible sets
                # and zip the cumulative sums array with the credible set array to add the flags
                "credibleSet",
                f.when(
                    f.size(f.col("credibleSet")) > 0,
                    f.zip_with(
                        f.col("credibleSet"),
                        f.transform(
                            f.sequence(f.lit(1), f.size(f.col("credibleSet"))),
                            lambda index: f.aggregate(
                                f.slice(
                                    # By using `index - 1` we introduce a value of `0.0` in the cumulative sums array. to ensure that the last variant
                                    # that exceeds the 0.95 threshold is included in the cumulative sum, as its probability is necessary to satisfy the threshold.
                                    f.col("credibleSet.posteriorProbability"),
                                    1,
                                    index - 1,
                                ),
                                f.lit(0.0),
                                lambda acc, el: acc + el,
                            ),
                        ),
                        lambda struct_e, acc: struct_e.withField(
                            CredibleInterval.IS95.value, acc < 0.95
                        ).withField(CredibleInterval.IS99.value, acc < 0.99),
                    ),
                ).when(f.size(f.col("credibleSet")) == 0, f.col("credibleSet")),
            )
            .withColumn(
                "credibleSet", StudyLocus._filter_credible_set(f.col("credibleSet"))
            )
        )
        return self

    def clump(self: StudyLocus) -> StudyLocus:
        """Perform LD clumping of the studyLocus.

        Evaluates whether a lead variant is linked to a tag (with lowest p-value) in the same studyLocus dataset.

        Returns:
            StudyLocus: with empty credible sets for linked variants and QC flag.
        """
        self.df = (
            self.df.withColumn(
                "is_lead_linked",
                LDclumping._is_lead_linked(
                    self.df.studyId,
                    self.df.variantId,
                    self.df.pValueExponent,
                    self.df.pValueMantissa,
                    self.df.ldSet,
                ),
            )
            .withColumn(
                "ldSet",
                f.when(f.col("is_lead_linked"), f.array()).otherwise(f.col("ldSet")),
            )
            .withColumn(
                "qualityControls",
                StudyLocus._update_quality_flag(
                    f.col("qualityControls"),
                    f.col("is_lead_linked"),
                    StudyLocusQualityCheck.LD_CLUMPED,
                ),
            )
            .drop("is_lead_linked")
        )
        return self


class StudyLocusGWASCatalog(StudyLocus):
    """Study-locus dataset derived from GWAS Catalog."""

    @staticmethod
    def _parse_pvalue(pvalue: Column) -> tuple[Column, Column]:
        """Parse p-value column.

        Args:
            pvalue (Column): p-value [string]

        Returns:
            tuple[Column, Column]: p-value mantissa and exponent

        Example:
            >>> import pyspark.sql.types as t
            >>> d = [("1.0"), ("0.5"), ("1E-20"), ("3E-3"), ("1E-1000")]
            >>> df = spark.createDataFrame(d, t.StringType())
            >>> df.select('value',*StudyLocusGWASCatalog._parse_pvalue(f.col('value'))).show()
            +-------+--------------+--------------+
            |  value|pValueMantissa|pValueExponent|
            +-------+--------------+--------------+
            |    1.0|           1.0|             1|
            |    0.5|           0.5|             1|
            |  1E-20|           1.0|           -20|
            |   3E-3|           3.0|            -3|
            |1E-1000|           1.0|         -1000|
            +-------+--------------+--------------+
            <BLANKLINE>

        """
        split = f.split(pvalue, "E")
        return split.getItem(0).cast("float").alias("pValueMantissa"), f.coalesce(
            split.getItem(1).cast("integer"), f.lit(1)
        ).alias("pValueExponent")

    @staticmethod
    def _normalise_pvaluetext(p_value_text: Column) -> Column:
        """Normalised p-value text column to a standardised format.

        For cases where there is no mapping, the value is set to null.

        Args:
            p_value_text (Column): `pValueText` column from GWASCatalog

        Returns:
            Column: Array column after using GWAS Catalog mappings. There might be multiple mappings for a single p-value text.

        Example:
            >>> import pyspark.sql.types as t
            >>> d = [("European Ancestry"), ("African ancestry"), ("Alzheimer’s Disease"), ("(progression)"), (""), (None)]
            >>> df = spark.createDataFrame(d, t.StringType())
            >>> df.withColumn('normalised', StudyLocusGWASCatalog._normalise_pvaluetext(f.col('value'))).show()
            +-------------------+----------+
            |              value|normalised|
            +-------------------+----------+
            |  European Ancestry|      [EA]|
            |   African ancestry|      [AA]|
            |Alzheimer’s Disease|      [AD]|
            |      (progression)|      null|
            |                   |      null|
            |               null|      null|
            +-------------------+----------+
            <BLANKLINE>

        """
        # GWAS Catalog to p-value mapping
        json_dict = json.loads(
            pkg_resources.read_text(data, "gwas_pValueText_map.json", encoding="utf-8")
        )
        map_expr = f.create_map(*[f.lit(x) for x in chain(*json_dict.items())])

        splitted_col = f.split(f.regexp_replace(p_value_text, r"[\(\)]", ""), ",")
        mapped_col = f.transform(splitted_col, lambda x: map_expr[x])
        return f.when(f.forall(mapped_col, lambda x: x.isNull()), None).otherwise(
            mapped_col
        )

    @staticmethod
    def _normalise_risk_allele(risk_allele: Column) -> Column:
        """Normalised risk allele column to a standardised format.

        If multiple risk alleles are present, the first one is returned.

        Args:
            risk_allele (Column): `riskAllele` column from GWASCatalog

        Returns:
            Column: mapped using GWAS Catalog mapping

        Example:
            >>> import pyspark.sql.types as t
            >>> d = [("rs1234-A-G"), ("rs1234-A"), ("rs1234-A; rs1235-G")]
            >>> df = spark.createDataFrame(d, t.StringType())
            >>> df.withColumn('normalised', StudyLocusGWASCatalog._normalise_risk_allele(f.col('value'))).show()
            +------------------+----------+
            |             value|normalised|
            +------------------+----------+
            |        rs1234-A-G|         A|
            |          rs1234-A|         A|
            |rs1234-A; rs1235-G|         A|
            +------------------+----------+
            <BLANKLINE>

        """
        # GWAS Catalog to risk allele mapping
        return f.split(f.split(risk_allele, "; ").getItem(0), "-").getItem(1)

    @staticmethod
    def _collect_rsids(
        snp_id: Column, snp_id_current: Column, risk_allele: Column
    ) -> Column:
        """It takes three columns, and returns an array of distinct values from those columns.

        Args:
            snp_id (Column): The original snp id from the GWAS catalog.
            snp_id_current (Column): The current snp id field is just a number at the moment (stored as a string). Adding 'rs' prefix if looks good.
            risk_allele (Column): The risk allele for the SNP.

        Returns:
            An array of distinct values.
        """
        # The current snp id field is just a number at the moment (stored as a string). Adding 'rs' prefix if looks good.
        snp_id_current = f.when(
            snp_id_current.rlike("^[0-9]*$"),
            f.format_string("rs%s", snp_id_current),
        )
        # Cleaning risk allele:
        risk_allele = f.split(risk_allele, "-").getItem(0)

        # Collecting all values:
        return f.array_distinct(f.array(snp_id, snp_id_current, risk_allele))

    @staticmethod
    def _map_to_variant_annotation_variants(
        gwas_associations: DataFrame, variant_annotation: VariantAnnotation
    ) -> DataFrame:
        """Add variant metadata in associations.

        Args:
            gwas_associations (DataFrame): raw GWAS Catalog associations
            variant_annotation (VariantAnnotation): variant annotation dataset

        Returns:
            DataFrame: GWAS Catalog associations data including `variantId`, `referenceAllele`,
            `alternateAllele`, `chromosome`, `position` with variant metadata
        """
        # Subset of GWAS Catalog associations required for resolving variant IDs:
        gwas_associations_subset = gwas_associations.select(
            "studyLocusId",
            f.col("CHR_ID").alias("chromosome"),
            f.col("CHR_POS").cast(IntegerType()).alias("position"),
            # List of all SNPs associated with the variant
            StudyLocusGWASCatalog._collect_rsids(
                f.split(f.col("SNPS"), "; ").getItem(0),
                f.col("SNP_ID_CURRENT"),
                f.split(f.col("STRONGEST SNP-RISK ALLELE"), "; ").getItem(0),
            ).alias("rsIdsGwasCatalog"),
            StudyLocusGWASCatalog._normalise_risk_allele(
                f.col("STRONGEST SNP-RISK ALLELE")
            ).alias("riskAllele"),
        )

        # Subset of variant annotation required for GWAS Catalog annotations:
        va_subset = variant_annotation.df.select(
            "variantId",
            "chromosome",
            "position",
            f.col("rsIds").alias("rsIdsGnomad"),
            "referenceAllele",
            "alternateAllele",
            "alleleFrequencies",
            variant_annotation.max_maf().alias("maxMaf"),
        ).join(
            f.broadcast(
                gwas_associations_subset.select("chromosome", "position").distinct()
            ),
            on=["chromosome", "position"],
            how="inner",
        )

        # Semi-resolved ids (still contains duplicates when conclusion was not possible to make
        # based on rsIds or allele concordance)
        filtered_associations = (
            gwas_associations_subset.join(
                f.broadcast(va_subset),
                on=["chromosome", "position"],
                how="left",
            )
            .withColumn(
                "rsIdFilter",
                StudyLocusGWASCatalog._flag_mappings_to_retain(
                    f.col("studyLocusId"),
                    StudyLocusGWASCatalog._compare_rsids(
                        f.col("rsIdsGnomad"), f.col("rsIdsGwasCatalog")
                    ),
                ),
            )
            .withColumn(
                "concordanceFilter",
                StudyLocusGWASCatalog._flag_mappings_to_retain(
                    f.col("studyLocusId"),
                    StudyLocusGWASCatalog._check_concordance(
                        f.col("riskAllele"),
                        f.col("referenceAllele"),
                        f.col("alternateAllele"),
                    ),
                ),
            )
            .filter(
                # Filter out rows where GWAS Catalog rsId does not match with GnomAD rsId,
                # but there is corresponding variant for the same association
                f.col("rsIdFilter")
                # or filter out rows where GWAS Catalog alleles are not concordant with GnomAD alleles,
                # but there is corresponding variant for the same association
                | f.col("concordanceFilter")
            )
        )

        # Keep only highest maxMaf variant per studyLocusId
        fully_mapped_associations = get_record_with_maximum_value(
            filtered_associations, grouping_col="studyLocusId", sorting_col="maxMaf"
        ).select(
            "studyLocusId",
            "variantId",
            "referenceAllele",
            "alternateAllele",
            "chromosome",
            "position",
        )

        return gwas_associations.join(
            fully_mapped_associations, on="studyLocusId", how="left"
        )

    @staticmethod
    def _compare_rsids(gnomad: Column, gwas: Column) -> Column:
        """If the intersection of the two arrays is greater than 0, return True, otherwise return False.

        Args:
            gnomad (Column): rsids from gnomad
            gwas (Column): rsids from the GWAS Catalog

        Returns:
            A boolean column that is true if the GnomAD rsIDs can be found in the GWAS rsIDs.

        Examples:
            >>> d = [
            ...    (1, ["rs123", "rs523"], ["rs123"]),
            ...    (2, [], ["rs123"]),
            ...    (3, ["rs123", "rs523"], []),
            ...    (4, [], []),
            ... ]
            >>> df = spark.createDataFrame(d, ['associationId', 'gnomad', 'gwas'])
            >>> df.withColumn("rsid_matches", StudyLocusGWASCatalog._compare_rsids(f.col("gnomad"),f.col('gwas'))).show()
            +-------------+--------------+-------+------------+
            |associationId|        gnomad|   gwas|rsid_matches|
            +-------------+--------------+-------+------------+
            |            1|[rs123, rs523]|[rs123]|        true|
            |            2|            []|[rs123]|       false|
            |            3|[rs123, rs523]|     []|       false|
            |            4|            []|     []|       false|
            +-------------+--------------+-------+------------+
            <BLANKLINE>

        """
        return f.when(f.size(f.array_intersect(gnomad, gwas)) > 0, True).otherwise(
            False
        )

    @staticmethod
    def _flag_mappings_to_retain(
        association_id: Column, filter_column: Column
    ) -> Column:
        """Flagging mappings to drop for each association.

        Some associations have multiple mappings. Some has matching rsId others don't. We only
        want to drop the non-matching mappings, when a matching is available for the given association.
        This logic can be generalised for other measures eg. allele concordance.

        Args:
            association_id (Column): association identifier column
            filter_column (Column): boolean col indicating to keep a mapping

        Returns:
            A column with a boolean value.

        Examples:
        >>> d = [
        ...    (1, False),
        ...    (1, False),
        ...    (2, False),
        ...    (2, True),
        ...    (3, True),
        ...    (3, True),
        ... ]
        >>> df = spark.createDataFrame(d, ['associationId', 'filter'])
        >>> df.withColumn("isConcordant", StudyLocusGWASCatalog._flag_mappings_to_retain(f.col("associationId"),f.col('filter'))).show()
        +-------------+------+------------+
        |associationId|filter|isConcordant|
        +-------------+------+------------+
        |            1| false|        true|
        |            1| false|        true|
        |            2| false|       false|
        |            2|  true|        true|
        |            3|  true|        true|
        |            3|  true|        true|
        +-------------+------+------------+
        <BLANKLINE>

        """
        w = Window.partitionBy(association_id)

        # Generating a boolean column informing if the filter column contains true anywhere for the association:
        aggregated_filter = f.when(
            f.array_contains(f.collect_set(filter_column).over(w), True), True
        ).otherwise(False)

        # Generate a filter column:
        return f.when(aggregated_filter & (~filter_column), False).otherwise(True)

    @staticmethod
    def _check_concordance(
        risk_allele: Column, reference_allele: Column, alternate_allele: Column
    ) -> Column:
        """A function to check if the risk allele is concordant with the alt or ref allele.

        If the risk allele is the same as the reference or alternate allele, or if the reverse complement of
        the risk allele is the same as the reference or alternate allele, then the allele is concordant.
        If no mapping is available (ref/alt is null), the function returns True.

        Args:
            risk_allele (Column): The allele that is associated with the risk of the disease.
            reference_allele (Column): The reference allele from the GWAS catalog
            alternate_allele (Column): The alternate allele of the variant.

        Returns:
            A boolean column that is True if the risk allele is the same as the reference or alternate allele,
            or if the reverse complement of the risk allele is the same as the reference or alternate allele.

        Examples:
            >>> d = [
            ...     ('A', 'A', 'G'),
            ...     ('A', 'T', 'G'),
            ...     ('A', 'C', 'G'),
            ...     ('A', 'A', '?'),
            ...     (None, None, 'A'),
            ... ]
            >>> df = spark.createDataFrame(d, ['riskAllele', 'referenceAllele', 'alternateAllele'])
            >>> df.withColumn("isConcordant", StudyLocusGWASCatalog._check_concordance(f.col("riskAllele"),f.col('referenceAllele'), f.col('alternateAllele'))).show()
            +----------+---------------+---------------+------------+
            |riskAllele|referenceAllele|alternateAllele|isConcordant|
            +----------+---------------+---------------+------------+
            |         A|              A|              G|        true|
            |         A|              T|              G|        true|
            |         A|              C|              G|       false|
            |         A|              A|              ?|        true|
            |      null|           null|              A|        true|
            +----------+---------------+---------------+------------+
            <BLANKLINE>

        """
        # Calculating the reverse complement of the risk allele:
        risk_allele_reverse_complement = f.when(
            risk_allele.rlike(r"^[ACTG]+$"),
            f.reverse(f.translate(risk_allele, "ACTG", "TGAC")),
        ).otherwise(risk_allele)

        # OK, is the risk allele or the reverse complent is the same as the mapped alleles:
        return (
            f.when(
                (risk_allele == reference_allele) | (risk_allele == alternate_allele),
                True,
            )
            # If risk allele is found on the negative strand:
            .when(
                (risk_allele_reverse_complement == reference_allele)
                | (risk_allele_reverse_complement == alternate_allele),
                True,
            )
            # If risk allele is ambiguous, still accepted: < This condition could be reconsidered
            .when(risk_allele == "?", True)
            # If the association could not be mapped we keep it:
            .when(reference_allele.isNull(), True)
            # Allele is discordant:
            .otherwise(False)
        )

    @staticmethod
    def _get_reverse_complement(allele_col: Column) -> Column:
        """A function to return the reverse complement of an allele column.

        It takes a string and returns the reverse complement of that string if it's a DNA sequence,
        otherwise it returns the original string. Assumes alleles in upper case.

        Args:
            allele_col (Column): The column containing the allele to reverse complement.

        Returns:
            A column that is the reverse complement of the allele column.

        Examples:
            >>> d = [{"allele": 'A'}, {"allele": 'T'},{"allele": 'G'}, {"allele": 'C'},{"allele": 'AC'}, {"allele": 'GTaatc'},{"allele": '?'}, {"allele": None}]
            >>> df = spark.createDataFrame(d)
            >>> df.withColumn("revcom_allele", StudyLocusGWASCatalog._get_reverse_complement(f.col("allele"))).show()
            +------+-------------+
            |allele|revcom_allele|
            +------+-------------+
            |     A|            T|
            |     T|            A|
            |     G|            C|
            |     C|            G|
            |    AC|           GT|
            |GTaatc|       GATTAC|
            |     ?|            ?|
            |  null|         null|
            +------+-------------+
            <BLANKLINE>

        """
        allele_col = f.upper(allele_col)
        return f.when(
            allele_col.rlike("[ACTG]+"),
            f.reverse(f.translate(allele_col, "ACTG", "TGAC")),
        ).otherwise(allele_col)

    @staticmethod
    def _effect_needs_harmonisation(
        risk_allele: Column, reference_allele: Column
    ) -> Column:
        """A function to check if the effect allele needs to be harmonised.

        Args:
            risk_allele (Column): Risk allele column
            reference_allele (Column): Effect allele column

        Returns:
            A boolean column indicating if the effect allele needs to be harmonised.

        Examples:
            >>> d = [{"risk": 'A', "reference": 'A'}, {"risk": 'A', "reference": 'T'}, {"risk": 'AT', "reference": 'TA'}, {"risk": 'AT', "reference": 'AT'}]
            >>> df = spark.createDataFrame(d)
            >>> df.withColumn("needs_harmonisation", StudyLocusGWASCatalog._effect_needs_harmonisation(f.col("risk"), f.col("reference"))).show()
            +---------+----+-------------------+
            |reference|risk|needs_harmonisation|
            +---------+----+-------------------+
            |        A|   A|               true|
            |        T|   A|               true|
            |       TA|  AT|              false|
            |       AT|  AT|               true|
            +---------+----+-------------------+
            <BLANKLINE>

        """
        return (risk_allele == reference_allele) | (
            risk_allele
            == StudyLocusGWASCatalog._get_reverse_complement(reference_allele)
        )

    @staticmethod
    def _are_alleles_palindromic(
        reference_allele: Column, alternate_allele: Column
    ) -> Column:
        """A function to check if the alleles are palindromic.

        Args:
            reference_allele (Column): Reference allele column
            alternate_allele (Column): Alternate allele column

        Returns:
            A boolean column indicating if the alleles are palindromic.

        Examples:
            >>> d = [{"reference": 'A', "alternate": 'T'}, {"reference": 'AT', "alternate": 'AG'}, {"reference": 'AT', "alternate": 'AT'}, {"reference": 'CATATG', "alternate": 'CATATG'}, {"reference": '-', "alternate": None}]
            >>> df = spark.createDataFrame(d)
            >>> df.withColumn("is_palindromic", StudyLocusGWASCatalog._are_alleles_palindromic(f.col("reference"), f.col("alternate"))).show()
            +---------+---------+--------------+
            |alternate|reference|is_palindromic|
            +---------+---------+--------------+
            |        T|        A|          true|
            |       AG|       AT|         false|
            |       AT|       AT|          true|
            |   CATATG|   CATATG|          true|
            |     null|        -|         false|
            +---------+---------+--------------+
            <BLANKLINE>

        """
        revcomp = StudyLocusGWASCatalog._get_reverse_complement(alternate_allele)
        return (
            f.when(reference_allele == revcomp, True)
            .when(revcomp.isNull(), False)
            .otherwise(False)
        )

    @staticmethod
    def _harmonise_beta(
        risk_allele: Column,
        reference_allele: Column,
        alternate_allele: Column,
        effect_size: Column,
        confidence_interval: Column,
    ) -> Column:
        """A function to extract the beta value from the effect size and confidence interval.

        If the confidence interval contains the word "increase" or "decrease" it indicates, we are dealing with betas.
        If it's "increase" and the effect size needs to be harmonized, then multiply the effect size by -1

        Args:
            risk_allele (Column): Risk allele column
            reference_allele (Column): Reference allele column
            alternate_allele (Column): Alternate allele column
            effect_size (Column): GWAS Catalog effect size column
            confidence_interval (Column): GWAS Catalog confidence interval column

        Returns:
            A column containing the beta value.
        """
        return (
            f.when(
                StudyLocusGWASCatalog._are_alleles_palindromic(
                    reference_allele, alternate_allele
                ),
                None,
            )
            .when(
                (
                    StudyLocusGWASCatalog._effect_needs_harmonisation(
                        risk_allele, reference_allele
                    )
                    & confidence_interval.contains("increase")
                )
                | (
                    ~StudyLocusGWASCatalog._effect_needs_harmonisation(
                        risk_allele, reference_allele
                    )
                    & confidence_interval.contains("decrease")
                ),
                -effect_size,
            )
            .otherwise(effect_size)
            .cast(DoubleType())
        )

    @staticmethod
    def _harmonise_beta_ci(
        risk_allele: Column,
        reference_allele: Column,
        alternate_allele: Column,
        effect_size: Column,
        confidence_interval: Column,
        p_value: Column,
        direction: str,
    ) -> Column:
        """Calculating confidence intervals for beta values.

        Args:
            risk_allele (Column): Risk allele column
            reference_allele (Column): Reference allele column
            alternate_allele (Column): Alternate allele column
            effect_size (Column): GWAS Catalog effect size column
            confidence_interval (Column): GWAS Catalog confidence interval column
            p_value (Column): GWAS Catalog p-value column
            direction (str): This is the direction of the confidence interval. It can be either "upper" or "lower".

        Returns:
            The upper and lower bounds of the confidence interval for the beta coefficient.
        """
        zscore_95 = f.lit(1.96)
        beta = StudyLocusGWASCatalog._harmonise_beta(
            risk_allele,
            reference_allele,
            alternate_allele,
            effect_size,
            confidence_interval,
        )
        zscore = pvalue_to_zscore(p_value)
        return (
            f.when(f.lit(direction) == "upper", beta + f.abs(zscore_95 * beta) / zscore)
            .when(f.lit(direction) == "lower", beta - f.abs(zscore_95 * beta) / zscore)
            .otherwise(None)
        )

    @staticmethod
    def _harmonise_odds_ratio(
        risk_allele: Column,
        reference_allele: Column,
        alternate_allele: Column,
        effect_size: Column,
        confidence_interval: Column,
    ) -> Column:
        """Harmonizing odds ratio.

        Args:
            risk_allele (Column): Risk allele column
            reference_allele (Column): Reference allele column
            alternate_allele (Column): Alternate allele column
            effect_size (Column): GWAS Catalog effect size column
            confidence_interval (Column): GWAS Catalog confidence interval column

        Returns:
            A column with the odds ratio, or 1/odds_ratio if harmonization required.
        """
        return (
            f.when(
                StudyLocusGWASCatalog._are_alleles_palindromic(
                    reference_allele, alternate_allele
                ),
                None,
            )
            .when(
                (
                    StudyLocusGWASCatalog._effect_needs_harmonisation(
                        risk_allele, reference_allele
                    )
                    & ~confidence_interval.rlike("|".join(["decrease", "increase"]))
                ),
                1 / effect_size,
            )
            .otherwise(effect_size)
            .cast(DoubleType())
        )

    @staticmethod
    def _harmonise_odds_ratio_ci(
        risk_allele: Column,
        reference_allele: Column,
        alternate_allele: Column,
        effect_size: Column,
        confidence_interval: Column,
        p_value: Column,
        direction: str,
    ) -> Column:
        """Calculating confidence intervals for beta values.

        Args:
            risk_allele (Column): Risk allele column
            reference_allele (Column): Reference allele column
            alternate_allele (Column): Alternate allele column
            effect_size (Column): GWAS Catalog effect size column
            confidence_interval (Column): GWAS Catalog confidence interval column
            p_value (Column): GWAS Catalog p-value column
            direction (str): This is the direction of the confidence interval. It can be either "upper" or "lower".

        Returns:
            The upper and lower bounds of the 95% confidence interval for the odds ratio.
        """
        zscore_95 = f.lit(1.96)
        odds_ratio = StudyLocusGWASCatalog._harmonise_odds_ratio(
            risk_allele,
            reference_allele,
            alternate_allele,
            effect_size,
            confidence_interval,
        )
        odds_ratio_estimate = f.log(odds_ratio)
        zscore = pvalue_to_zscore(p_value)
        odds_ratio_se = odds_ratio_estimate / zscore
        return f.when(
            f.lit(direction) == "upper",
            f.exp(odds_ratio_estimate + f.abs(zscore_95 * odds_ratio_se)),
        ).when(
            f.lit(direction) == "lower",
            f.exp(odds_ratio_estimate - f.abs(zscore_95 * odds_ratio_se)),
        )

    @staticmethod
    def _concatenate_substudy_description(
        association_trait: Column, pvalue_text: Column, mapped_trait_uri: Column
    ) -> Column:
        """Substudy description parsing. Complex string containing metadata about the substudy (e.g. QTL, specific EFO, etc.).

        Args:
            association_trait (Column): GWAS Catalog association trait column
            pvalue_text (Column): GWAS Catalog p-value text column
            mapped_trait_uri (Column): GWAS Catalog mapped trait URI column

        Returns:
            A column with the substudy description in the shape trait|pvaluetext1_pvaluetext2|EFO1_EFO2.

        Examples:
        >>> df = spark.createDataFrame([
        ...    ("Height", "http://www.ebi.ac.uk/efo/EFO_0000001,http://www.ebi.ac.uk/efo/EFO_0000002", "European Ancestry"),
        ...    ("Schizophrenia", "http://www.ebi.ac.uk/efo/MONDO_0005090", None)],
        ...    ["association_trait", "mapped_trait_uri", "pvalue_text"]
        ... )
        >>> df.withColumn('substudy_description', StudyLocusGWASCatalog._concatenate_substudy_description(df.association_trait, df.pvalue_text, df.mapped_trait_uri)).show(truncate=False)
        +-----------------+-------------------------------------------------------------------------+-----------------+------------------------------------------+
        |association_trait|mapped_trait_uri                                                         |pvalue_text      |substudy_description                      |
        +-----------------+-------------------------------------------------------------------------+-----------------+------------------------------------------+
        |Height           |http://www.ebi.ac.uk/efo/EFO_0000001,http://www.ebi.ac.uk/efo/EFO_0000002|European Ancestry|Height|EA|EFO_0000001/EFO_0000002         |
        |Schizophrenia    |http://www.ebi.ac.uk/efo/MONDO_0005090                                   |null             |Schizophrenia|no_pvalue_text|MONDO_0005090|
        +-----------------+-------------------------------------------------------------------------+-----------------+------------------------------------------+
        <BLANKLINE>
        """
        p_value_text = f.coalesce(
            StudyLocusGWASCatalog._normalise_pvaluetext(pvalue_text),
            f.array(f.lit("no_pvalue_text")),
        )
        return f.concat_ws(
            "|",
            association_trait,
            f.concat_ws(
                "/",
                p_value_text,
            ),
            f.concat_ws(
                "/",
                parse_efos(mapped_trait_uri),
            ),
        )

    @staticmethod
    def _qc_all(
        qc: Column,
        chromosome: Column,
        position: Column,
        reference_allele: Column,
        alternate_allele: Column,
        strongest_snp_risk_allele: Column,
        p_value_mantissa: Column,
        p_value_exponent: Column,
        p_value_cutoff: float,
    ) -> Column:
        """Flag associations that fail any QC.

        Args:
            qc (Column): QC column
            chromosome (Column): Chromosome column
            position (Column): Position column
            reference_allele (Column): Reference allele column
            alternate_allele (Column): Alternate allele column
            strongest_snp_risk_allele (Column): Strongest SNP risk allele column
            p_value_mantissa (Column): P-value mantissa column
            p_value_exponent (Column): P-value exponent column
            p_value_cutoff (float): P-value cutoff

        Returns:
            Column: Updated QC column with flag.
        """
        qc = StudyLocusGWASCatalog._qc_variant_interactions(
            qc, strongest_snp_risk_allele
        )
        qc = StudyLocusGWASCatalog._qc_subsignificant_associations(
            qc, p_value_mantissa, p_value_exponent, p_value_cutoff
        )
        qc = StudyLocusGWASCatalog._qc_genomic_location(qc, chromosome, position)
        qc = StudyLocusGWASCatalog._qc_variant_inconsistencies(
            qc, chromosome, position, strongest_snp_risk_allele
        )
        qc = StudyLocusGWASCatalog._qc_unmapped_variants(qc, alternate_allele)
        qc = StudyLocusGWASCatalog._qc_palindromic_alleles(
            qc, reference_allele, alternate_allele
        )
        return qc

    @staticmethod
    def _qc_variant_interactions(
        qc: Column, strongest_snp_risk_allele: Column
    ) -> Column:
        """Flag associations based on variant x variant interactions.

        Args:
            qc (Column): QC column
            strongest_snp_risk_allele (Column): Column with the strongest SNP risk allele

        Returns:
            Column: Updated QC column with flag.
        """
        return StudyLocusGWASCatalog._update_quality_flag(
            qc,
            strongest_snp_risk_allele.contains(";"),
            StudyLocusQualityCheck.COMPOSITE_FLAG,
        )

    @staticmethod
    def _qc_subsignificant_associations(
        qc: Column,
        p_value_mantissa: Column,
        p_value_exponent: Column,
        pvalue_cutoff: float,
    ) -> Column:
        """Flag associations below significant threshold.

        Args:
            qc (Column): QC column
            p_value_mantissa (Column): P-value mantissa column
            p_value_exponent (Column): P-value exponent column
            pvalue_cutoff (float): association p-value cut-off

        Returns:
            Column: Updated QC column with flag.

        Examples:
            >>> import pyspark.sql.types as t
            >>> d = [{'qc': None, 'p_value_mantissa': 1, 'p_value_exponent': -7}, {'qc': None, 'p_value_mantissa': 1, 'p_value_exponent': -8}, {'qc': None, 'p_value_mantissa': 5, 'p_value_exponent': -8}, {'qc': None, 'p_value_mantissa': 1, 'p_value_exponent': -9}]
            >>> df = spark.createDataFrame(d, t.StructType([t.StructField('qc', t.ArrayType(t.StringType()), True), t.StructField('p_value_mantissa', t.IntegerType()), t.StructField('p_value_exponent', t.IntegerType())]))
            >>> df.withColumn('qc', StudyLocusGWASCatalog._qc_subsignificant_associations(f.col("qc"), f.col("p_value_mantissa"), f.col("p_value_exponent"), 5e-8)).show(truncate = False)
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
        return StudyLocus._update_quality_flag(
            qc,
            calculate_neglog_pvalue(p_value_mantissa, p_value_exponent)
            < f.lit(-np.log10(pvalue_cutoff)),
            StudyLocusQualityCheck.SUBSIGNIFICANT_FLAG,
        )

    @staticmethod
    def _qc_genomic_location(
        qc: Column, chromosome: Column, position: Column
    ) -> Column:
        """Flag associations without genomic location in GWAS Catalog.

        Args:
            qc (Column): QC column
            chromosome (Column): Chromosome column in GWAS Catalog
            position (Column): Position column in GWAS Catalog

        Returns:
            Column: Updated QC column with flag.

        Examples:
            >>> import pyspark.sql.types as t
            >>> d = [{'qc': None, 'chromosome': None, 'position': None}, {'qc': None, 'chromosome': '1', 'position': None}, {'qc': None, 'chromosome': None, 'position': 1}, {'qc': None, 'chromosome': '1', 'position': 1}]
            >>> df = spark.createDataFrame(d, schema=t.StructType([t.StructField('qc', t.ArrayType(t.StringType()), True), t.StructField('chromosome', t.StringType()), t.StructField('position', t.IntegerType())]))
            >>> df.withColumn('qc', StudyLocusGWASCatalog._qc_genomic_location(df.qc, df.chromosome, df.position)).show(truncate=False)
            +----------------------------+----------+--------+
            |qc                          |chromosome|position|
            +----------------------------+----------+--------+
            |[Incomplete genomic mapping]|null      |null    |
            |[Incomplete genomic mapping]|1         |null    |
            |[Incomplete genomic mapping]|null      |1       |
            |[]                          |1         |1       |
            +----------------------------+----------+--------+
            <BLANKLINE>

        """
        return StudyLocus._update_quality_flag(
            qc,
            position.isNull() | chromosome.isNull(),
            StudyLocusQualityCheck.NO_GENOMIC_LOCATION_FLAG,
        )

    @staticmethod
    def _qc_variant_inconsistencies(
        qc: Column,
        chromosome: Column,
        position: Column,
        strongest_snp_risk_allele: Column,
    ) -> Column:
        """Flag associations with inconsistencies in the variant annotation.

        Args:
            qc (Column): QC column
            chromosome (Column): Chromosome column in GWAS Catalog
            position (Column): Position column in GWAS Catalog
            strongest_snp_risk_allele (Column): Strongest SNP risk allele column in GWAS Catalog

        Returns:
            Column: Updated QC column with flag.
        """
        return StudyLocusGWASCatalog._update_quality_flag(
            qc,
            # Number of chromosomes does not correspond to the number of positions:
            (f.size(f.split(chromosome, ";")) != f.size(f.split(position, ";")))
            # Number of chromosome values different from riskAllele values:
            | (
                f.size(f.split(chromosome, ";"))
                != f.size(f.split(strongest_snp_risk_allele, ";"))
            ),
            StudyLocusQualityCheck.INCONSISTENCY_FLAG,
        )

    @staticmethod
    def _qc_unmapped_variants(qc: Column, alternate_allele: Column) -> Column:
        """Flag associations with variants not mapped to variantAnnotation.

        Args:
            qc (Column): QC column
            alternate_allele (Column): alternate allele

        Returns:
            Column: Updated QC column with flag.

        Example:
            >>> import pyspark.sql.types as t
            >>> d = [{'alternate_allele': 'A', 'qc': None}, {'alternate_allele': None, 'qc': None}]
            >>> schema = t.StructType([t.StructField('alternate_allele', t.StringType(), True), t.StructField('qc', t.ArrayType(t.StringType()), True)])
            >>> df = spark.createDataFrame(data=d, schema=schema)
            >>> df.withColumn("new_qc", StudyLocusGWASCatalog._qc_unmapped_variants(f.col("qc"), f.col("alternate_allele"))).show()
            +----------------+----+--------------------+
            |alternate_allele|  qc|              new_qc|
            +----------------+----+--------------------+
            |               A|null|                  []|
            |            null|null|[No mapping in Gn...|
            +----------------+----+--------------------+
            <BLANKLINE>

        """
        return StudyLocus._update_quality_flag(
            qc,
            alternate_allele.isNull(),
            StudyLocusQualityCheck.NON_MAPPED_VARIANT_FLAG,
        )

    @staticmethod
    def _qc_palindromic_alleles(
        qc: Column, reference_allele: Column, alternate_allele: Column
    ) -> Column:
        """Flag associations with palindromic variants which effects can not be harmonised.

        Args:
            qc (Column): QC column
            reference_allele (Column): reference allele
            alternate_allele (Column): alternate allele

        Returns:
            Column: Updated QC column with flag.

        Example:
            >>> import pyspark.sql.types as t
            >>> schema = t.StructType([t.StructField('reference_allele', t.StringType(), True), t.StructField('alternate_allele', t.StringType(), True), t.StructField('qc', t.ArrayType(t.StringType()), True)])
            >>> d = [{'reference_allele': 'A', 'alternate_allele': 'T', 'qc': None}, {'reference_allele': 'AT', 'alternate_allele': 'TA', 'qc': None}, {'reference_allele': 'AT', 'alternate_allele': 'AT', 'qc': None}]
            >>> df = spark.createDataFrame(data=d, schema=schema)
            >>> df.withColumn("qc", StudyLocusGWASCatalog._qc_palindromic_alleles(f.col("qc"), f.col("reference_allele"), f.col("alternate_allele"))).show(truncate=False)
            +----------------+----------------+---------------------------------------+
            |reference_allele|alternate_allele|qc                                     |
            +----------------+----------------+---------------------------------------+
            |A               |T               |[Palindrome alleles - cannot harmonize]|
            |AT              |TA              |[]                                     |
            |AT              |AT              |[Palindrome alleles - cannot harmonize]|
            +----------------+----------------+---------------------------------------+
            <BLANKLINE>

        """
        return StudyLocus._update_quality_flag(
            qc,
            StudyLocusGWASCatalog._are_alleles_palindromic(
                reference_allele, alternate_allele
            ),
            StudyLocusQualityCheck.PALINDROMIC_ALLELE_FLAG,
        )

    @classmethod
    def from_source(
        cls: type[StudyLocusGWASCatalog],
        gwas_associations: DataFrame,
        variant_annotation: VariantAnnotation,
        pvalue_threshold: float = 5e-8,
    ) -> StudyLocusGWASCatalog:
        """Read GWASCatalog associations.

        It reads the GWAS Catalog association dataset, selects and renames columns, casts columns, and
        applies some pre-defined filters on the data:

        Args:
            gwas_associations (DataFrame): GWAS Catalog raw associations dataset
            variant_annotation (VariantAnnotation): Variant annotation dataset
            pvalue_threshold (float): P-value threshold for flagging associations

        Returns:
            StudyLocusGWASCatalog: StudyLocusGWASCatalog dataset
        """
        return cls(
            _df=gwas_associations.withColumn(
                "studyLocusId", f.monotonically_increasing_id().cast(LongType())
            )
            .transform(
                # Map/harmonise variants to variant annotation dataset:
                # This function adds columns: variantId, referenceAllele, alternateAllele, chromosome, position
                lambda df: StudyLocusGWASCatalog._map_to_variant_annotation_variants(
                    df, variant_annotation
                )
            )
            .withColumn(
                # Perform all quality control checks:
                "qualityControls",
                StudyLocusGWASCatalog._qc_all(
                    f.array().alias("qualityControls"),
                    f.col("CHR_ID"),
                    f.col("CHR_POS").cast(IntegerType()),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
                    f.col("STRONGEST SNP-RISK ALLELE"),
                    *StudyLocusGWASCatalog._parse_pvalue(f.col("P-VALUE")),
                    pvalue_threshold,
                ),
            )
            .select(
                # INSIDE STUDY-LOCUS SCHEMA:
                "studyLocusId",
                "variantId",
                # Mapped genomic location of the variant (; separated list)
                "chromosome",
                "position",
                f.col("STUDY ACCESSION").alias("studyId"),
                # beta value of the association
                StudyLocusGWASCatalog._harmonise_beta(
                    StudyLocusGWASCatalog._normalise_risk_allele(
                        f.col("STRONGEST SNP-RISK ALLELE")
                    ),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
                    f.col("OR or BETA"),
                    f.col("95% CI (TEXT)"),
                ).alias("beta"),
                # odds ratio of the association
                StudyLocusGWASCatalog._harmonise_odds_ratio(
                    StudyLocusGWASCatalog._normalise_risk_allele(
                        f.col("STRONGEST SNP-RISK ALLELE")
                    ),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
                    f.col("OR or BETA"),
                    f.col("95% CI (TEXT)"),
                ).alias("oddsRatio"),
                # CI lower of the beta value
                StudyLocusGWASCatalog._harmonise_beta_ci(
                    StudyLocusGWASCatalog._normalise_risk_allele(
                        f.col("STRONGEST SNP-RISK ALLELE")
                    ),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
                    f.col("OR or BETA"),
                    f.col("95% CI (TEXT)"),
                    f.col("P-VALUE"),
                    "lower",
                ).alias("betaConfidenceIntervalLower"),
                # CI upper for the beta value
                StudyLocusGWASCatalog._harmonise_beta_ci(
                    StudyLocusGWASCatalog._normalise_risk_allele(
                        f.col("STRONGEST SNP-RISK ALLELE")
                    ),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
                    f.col("OR or BETA"),
                    f.col("95% CI (TEXT)"),
                    f.col("P-VALUE"),
                    "upper",
                ).alias("betaConfidenceIntervalUpper"),
                # CI lower of the odds ratio value
                StudyLocusGWASCatalog._harmonise_odds_ratio_ci(
                    StudyLocusGWASCatalog._normalise_risk_allele(
                        f.col("STRONGEST SNP-RISK ALLELE")
                    ),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
                    f.col("OR or BETA"),
                    f.col("95% CI (TEXT)"),
                    f.col("P-VALUE"),
                    "lower",
                ).alias("oddsRatioConfidenceIntervalLower"),
                # CI upper of the odds ratio value
                StudyLocusGWASCatalog._harmonise_odds_ratio_ci(
                    StudyLocusGWASCatalog._normalise_risk_allele(
                        f.col("STRONGEST SNP-RISK ALLELE")
                    ),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
                    f.col("OR or BETA"),
                    f.col("95% CI (TEXT)"),
                    f.col("P-VALUE"),
                    "upper",
                ).alias("oddsRatioConfidenceIntervalUpper"),
                # p-value of the association, string: split into exponent and mantissa.
                *StudyLocusGWASCatalog._parse_pvalue(f.col("P-VALUE")),
                # Capturing phenotype granularity at the association level
                StudyLocusGWASCatalog._concatenate_substudy_description(
                    f.col("DISEASE/TRAIT"),
                    f.col("P-VALUE (TEXT)"),
                    f.col("MAPPED_TRAIT_URI"),
                ).alias("subStudyDescription"),
                # Quality controls (array of strings)
                "qualityControls",
            ),
            _schema=cls._get_schema(),
        )

    def update_study_id(
        self: StudyLocusGWASCatalog, study_annotation: DataFrame
    ) -> StudyLocusGWASCatalog:
        """Update final studyId and studyLocusId with a dataframe containing study annotation.

        Args:
            study_annotation (DataFrame): Dataframe containing `updatedStudyId` and key columns `studyId` and `subStudyDescription`.

        Returns:
            StudyLocusGWASCatalog: Updated study locus with new `studyId` and `studyLocusId`.
        """
        self.df = (
            self._df.join(
                study_annotation, on=["studyId", "subStudyDescription"], how="left"
            )
            .withColumn("studyId", f.coalesce("updatedStudyId", "studyId"))
            .drop("subStudyDescription", "updatedStudyId")
        ).withColumn(
            "studyLocusId",
            StudyLocus.assign_study_locus_id(f.col("studyId"), f.col("variantId")),
        )
        return self

    def annotate_ld(
        self: StudyLocusGWASCatalog, studies: StudyIndexGWASCatalog, ld_index: LDIndex
    ) -> StudyLocus:
        """Annotate LD set for every studyLocus using gnomAD.

        Args:
            studies (StudyIndexGWASCatalog): Study index containing ancestry information
            ld_index (LDIndex): LD index

        Returns:
            StudyLocus: Study-locus with an annotated credible set.
        """
        associations_df = self.df.join(
            studies.get_gnomad_population_structure(), on="studyId", how="left"
        )

        self.df = LDAnnotator.annotate_associations_with_ld(associations_df, ld_index)
        return self._qc_unresolved_ld()

    def _qc_ambiguous_study(self: StudyLocusGWASCatalog) -> StudyLocusGWASCatalog:
        """Flag associations with variants that can not be unambiguously associated with one study.

        Returns:
            StudyLocusGWASCatalog: Updated study locus.
        """
        assoc_ambiguity_window = Window.partitionBy(
            f.col("studyId"), f.col("variantId")
        )

        self._df.withColumn(
            "qualityControls",
            StudyLocus._update_quality_flag(
                f.col("qualityControls"),
                f.count(f.col("variantId")).over(assoc_ambiguity_window) > 1,
                StudyLocusQualityCheck.AMBIGUOUS_STUDY,
            ),
        )
        return self

    def _qc_unresolved_ld(
        self: StudyLocus | StudyLocusGWASCatalog,
    ) -> StudyLocus | StudyLocusGWASCatalog:
        """Flag associations with variants that are not found in the LD reference.

        Returns:
            StudyLocusGWASCatalog | StudyLocus: Updated study locus.
        """
        self.df = self.df.withColumn(
            "qualityControls",
            self._update_quality_flag(
                f.col("qualityControls"),
                f.col("ldSet").isNull(),
                StudyLocusQualityCheck.UNRESOLVED_LD,
            ),
        )
        return self
