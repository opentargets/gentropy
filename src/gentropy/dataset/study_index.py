"""Study index dataset."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from itertools import chain
from typing import TYPE_CHECKING, cast

from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window

from gentropy.assets import data
from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark import convert_from_wide_to_long, filter_array_struct
from gentropy.dataset.dataset import Dataset, qc_test

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC
    from gentropy.dataset.target_index import TargetIndex


class StudyAnalysisFlag(Enum):
    """Enum type hosting the expected analysis flags derived from the study curation."""

    MULTIVARIATE_ANALYSIS = "Multivariate analysis"
    EXWAS = "ExWAS"
    NON_ADDITIVE = "Non-additive model"
    WGS_WAS = "wgsGWAS"
    METABOLITE = "Metabolite"
    GXG = "GxG"
    GxE = "GxE"
    CASE_CASE_STUDY = "Case-case study"


class StudyQualityCheck(Enum):
    """Study quality control options listing concerns on the quality of the study.

    Attributes:
        UNRESOLVED_TARGET (str): Target/gene identifier could not match to reference - Labelling failing target.
        UNRESOLVED_DISEASE (str): Disease identifier could not match to referece or retired identifier - labelling failing disease
        UNKNOWN_STUDY_TYPE (str): Indicating the provided type of study is not supported.
        UNKNOWN_BIOSAMPLE (str): Flagging if a biosample identifier is not found in the reference.
        DUPLICATED_STUDY (str): Flagging if a study identifier is not unique.
        SUMSTATS_NOT_AVAILABLE (str): Flagging if harmonized summary statistics are not available or empty.
        NO_OT_CURATION (str): Flagging if a study has not been curated by Open Targets.
        FAILED_MEAN_BETA_CHECK (str): Flagging if the mean beta QC check value is not within the expected range.
        FAILED_PZ_CHECK (str): Flagging if the PZ QC check values are not within the expected range.
        FAILED_GC_LAMBDA_CHECK (str): Flagging if the GC lambda value is not within the expected range.
        SMALL_NUMBER_OF_SNPS (str): Flagging if the number of SNPs in the study is below the expected threshold.
        CASE_CASE_STUDY_DESIGN (str): Flagging if the study design is case-case.
        DEPRECATED_PROJECT (str): Flagging if the projectId is deprecated.
        INVALID_SAMPLE_SIZE (str): Flagging if the sample size is null or equal to zero.
        CASE_CONTROL_STUDY_DESIGN (str): Flagging if the study has non empty or zero nCases, nControls & nSamples with matching sum.
        CASE_CONTROL_SUM_NEQ_SAMPLE_SIZE (str): Flagging if the study has non empty or zero nCases, nControls & nSamples with non matching sum.
        ONE_ONLY_CASE_OR_CONTROL (str): Flagging if the study has only cases or only controls.
        MEASUREMENT_STUDY_DESIGN (str): Flagging if the study has non empty or zero nSamples with empty nCases and nControls.

    """

    UNRESOLVED_TARGET = "Target/gene identifier could not match to reference"
    UNRESOLVED_DISEASE = "No valid disease identifier found"
    UNKNOWN_STUDY_TYPE = "This type of study is not supported"
    UNKNOWN_BIOSAMPLE = "Biosample identifier was not found in the reference"
    DUPLICATED_STUDY = "The identifier of this study is not unique"
    SUMSTATS_NOT_AVAILABLE = "Harmonized summary statistics are not available or empty"
    NO_OT_CURATION = "GWAS Catalog study has not been curated by Open Targets"
    FAILED_MEAN_BETA_CHECK = (
        "The mean beta QC check value is not within the expected range"
    )
    FAILED_PZ_CHECK = "The PZ QC check values are not within the expected range"
    FAILED_GC_LAMBDA_CHECK = "The GC lambda value is not within the expected range"
    SMALL_NUMBER_OF_SNPS = (
        "The number of SNPs in the study is below the expected threshold"
    )
    CASE_CASE_STUDY_DESIGN = "Case-case study design"
    DEPRECATED_PROJECT = "Deprecated project"
    INVALID_SAMPLE_SIZE = "Sample size is null or equal to zero"
    CASE_CONTROL_STUDY_DESIGN = (
        "Study has non empty or zero nCases, nControls & nSamples with matching sum"
    )
    CASE_CONTROL_SUM_NEQ_SAMPLE_SIZE = (
        "Study has non empty or zero nCases, nControls & nSamples with non matching sum"
    )
    ONE_ONLY_CASE_OR_CONTROL = "Study has only cases or only controls"
    MEASUREMENT_STUDY_DESIGN = (
        "Study has non empty or zero nSamples with empty nCases and nControls"
    )


class StudyType(Enum):
    """Enum type hosting the expected study types."""

    GWAS = "gwas"
    EQTL = "eqtl"
    PQTL = "pqtl"
    SQTL = "sqtl"
    TUQTL = "tuqtl"
    SCEQTL = "sceqtl"
    SCPQTL = "scpqtl"
    SCSQTL = "scsqtl"
    SCTUQTL = "sctuqtl"


@dataclass
class StudyIndex(Dataset):
    """Study index dataset.

    A study index dataset captures all the metadata for all studies including GWAS and Molecular QTL.
    """

    VALID_TYPES = [s.value for s in StudyType]

    @staticmethod
    def _aggregate_samples_by_ancestry(merged: Column, ancestry: Column) -> Column:
        """Aggregate sample counts by ancestry in a list of struct colmns.

        Args:
            merged (Column): A column representing merged data (list of structs).
            ancestry (Column): The `ancestry` parameter is a column that represents the ancestry of each
                sample. (a struct)

        Returns:
            Column: the modified "merged" column after aggregating the samples by ancestry.
        """
        # Iterating over the list of ancestries and adding the sample size if label matches:
        return f.transform(
            merged,
            lambda a: f.when(
                a.ancestry == ancestry.ancestry,
                f.struct(
                    a.ancestry.alias("ancestry"),
                    (a.sampleSize + ancestry.sampleSize).alias("sampleSize"),
                ),
            ).otherwise(a),
        )

    @staticmethod
    def _map_ancestries_to_ld_population(gwas_ancestry_label: Column) -> Column:
        """Normalise ancestry column from GWAS studies into reference LD panel based on a pre-defined map.

        This function assumes all possible ancestry categories have a corresponding
        LD panel in the LD index. It is very important to have the ancestry labels
        moved to the LD panel map.

        Args:
            gwas_ancestry_label (Column): A struct column with ancestry label like Finnish,
                European, African etc. and the corresponding sample size.

        Returns:
            Column: Struct column with the mapped LD population label and the sample size.
        """
        # Loading ancestry label to LD population label:

        pkg = pkg_resources.files(data).joinpath("gwas_population_2_LD_panel_map.json")
        with pkg.open(encoding="utf-8") as file:
            json_dict = json.load(file)
            json_dict = cast(dict[str, str], json_dict)

        map_expr = f.create_map(*[f.lit(x) for x in chain(*json_dict.items())])

        return f.struct(
            map_expr[gwas_ancestry_label.ancestry].alias("ancestry"),
            gwas_ancestry_label.sampleSize.alias("sampleSize"),
        )

    @classmethod
    def get_schema(cls: type[StudyIndex]) -> StructType:
        """Provide the schema for the StudyIndex dataset.

        Returns:
            StructType: The schema of the StudyIndex dataset.
        """
        return parse_spark_schema("study_index.json")

    @classmethod
    def get_QC_column_name(cls: type[StudyIndex]) -> str:
        """Return the name of the quality control column.

        Returns:
            str: The name of the quality control column.
        """
        return "qualityControls"

    @classmethod
    def get_QC_mappings(cls: type[StudyIndex]) -> dict[str, str]:
        """Quality control flag to QC column category mappings.

        Returns:
            dict[str, str]: Mapping between flag name and QC column category value.
        """
        return {member.name: member.value for member in StudyQualityCheck}

    @classmethod
    def aggregate_and_map_ancestries(
        cls: type[StudyIndex], discovery_samples: Column
    ) -> Column:
        """Map ancestries to populations in the LD reference and calculate relative sample size.

        Args:
            discovery_samples (Column): A list of struct column. Has an `ancestry` column and a `sampleSize` columns

        Returns:
            Column: A list of struct with mapped LD population and their relative sample size.
        """
        # Map ancestry categories to population labels of the LD index:
        mapped_ancestries = f.transform(
            discovery_samples, cls._map_ancestries_to_ld_population
        )

        # Aggregate sample sizes belonging to the same LD population:
        aggregated_counts = f.aggregate(
            mapped_ancestries,
            f.array_distinct(
                f.transform(
                    mapped_ancestries,
                    lambda x: f.struct(
                        x.ancestry.alias("ancestry"), f.lit(0.0).alias("sampleSize")
                    ),
                )
            ),
            cls._aggregate_samples_by_ancestry,
        )
        # Getting total sample count:
        total_sample_count = f.aggregate(
            aggregated_counts, f.lit(0.0), lambda total, pop: total + pop.sampleSize
        ).alias("sampleSize")
        # Calculating relative sample size for each LD population:
        return f.transform(
            aggregated_counts,
            lambda ld_population: f.struct(
                ld_population.ancestry.alias("ldPopulation"),
                (ld_population.sampleSize / total_sample_count).alias(
                    "relativeSampleSize"
                ),
            ),
        )

    def study_type_lut(self: StudyIndex) -> DataFrame:
        """Return a lookup table of study type.

        Returns:
            DataFrame: A dataframe containing `studyId` and `studyType` columns.
        """
        return self.df.select("studyId", "studyType")

    def is_qtl(self: StudyIndex) -> Column:
        """Return a boolean column with true values for QTL studies.

        Returns:
            Column: True if the study is a QTL study.
        """
        return self.df.studyType.endswith("qtl")

    def is_gwas(self: StudyIndex) -> Column:
        """Return a boolean column with true values for GWAS studies.

        Returns:
            Column: True if the study is a GWAS study.
        """
        return self.df.studyType == "gwas"

    def has_mapped_trait(self: StudyIndex) -> Column:
        """Return a boolean column indicating if a study has mapped disease.

        Returns:
            Column: True if the study has mapped disease.
        """
        return f.size(self.df.traitFromSourceMappedIds) > 0

    def is_quality_flagged(self: StudyIndex) -> Column:
        """Return a boolean column indicating if a study is flagged due to quality issues.

        Returns:
            Column: True if the study is flagged.
        """
        # Testing for the presence of the qualityControls column:
        if "qualityControls" not in self.df.columns:
            return f.lit(False)
        else:
            return f.size(self.df["qualityControls"]) != 0

    def has_summarystats(self: StudyIndex) -> Column:
        """Return a boolean column indicating if a study has harmonized summary statistics.

        Returns:
            Column: True if the study has harmonized summary statistics.
        """
        return self.df.hasSumstats

    @qc_test
    def validate_unique_study_id(self: StudyIndex) -> StudyIndex:
        """Validating the uniqueness of study identifiers and flagging duplicated studies.

        Returns:
            StudyIndex: with flagged duplicated studies.
        """
        return StudyIndex(
            _df=self.df.withColumn(
                "qualityControls",
                self.update_quality_flag(
                    f.col("qualityControls"),
                    self.flag_duplicates(f.col("studyId")),
                    StudyQualityCheck.DUPLICATED_STUDY,
                ),
            ),
            _schema=StudyIndex.get_schema(),
        )

    @qc_test
    def validate_ccs(self: StudyIndex) -> StudyIndex:
        """Validate nCases, nControls & nSamples.

        Returns:
            StudyIndex: where studies are flagged if nCases, nControls & nSamples are not consistent with each other.
        """
        cases_above_zero = (f.col("nCases") > 0) & (f.col("nCases").isNotNull())
        cases_null_or_zero = (f.col("nCases").isNull()) | (f.col("nCases") == 0)
        ctrls_above_zero = (f.col("nControls") > 0) & (f.col("nControls").isNotNull())
        ctrls_null_or_zero = (f.col("nControls").isNull()) | (f.col("nControls") == 0)
        samples_above_zero = (f.col("nSamples") > 0) & (f.col("nSamples").isNotNull())
        samples_null_or_zero = (f.col("nSamples").isNull()) | (f.col("nSamples") == 0)
        sum_match = f.col("nCases") + f.col("nControls") == f.col("nSamples")

        # Complex conditions
        cc_eq_n = cases_above_zero & ctrls_above_zero & samples_above_zero & sum_match
        cc_neq_n = cases_above_zero & ctrls_above_zero & samples_above_zero & ~sum_match
        empty_cc_and_n = cases_null_or_zero & ctrls_null_or_zero & samples_above_zero
        non_empty_cases = cases_above_zero & ctrls_null_or_zero & samples_above_zero
        non_empty_ctrls = cases_null_or_zero & ctrls_above_zero & samples_above_zero

        validated_df = (
            self.df.withColumn(
                "qualityControls",
                StudyIndex.update_quality_flag(
                    f.col("qualityControls"),
                    cc_eq_n,
                    StudyQualityCheck.CASE_CONTROL_STUDY_DESIGN,
                ),
            )
            .withColumn(
                "qualityControls",
                StudyIndex.update_quality_flag(
                    f.col("qualityControls"),
                    empty_cc_and_n,
                    StudyQualityCheck.MEASUREMENT_STUDY_DESIGN,
                ),
            )
            .withColumn(
                "qualityControls",
                StudyIndex.update_quality_flag(
                    f.col("qualityControls"),
                    samples_null_or_zero,
                    StudyQualityCheck.INVALID_SAMPLE_SIZE,
                ),
            )
            .withColumn(
                "qualityControls",
                StudyIndex.update_quality_flag(
                    f.col("qualityControls"),
                    cc_neq_n,
                    StudyQualityCheck.CASE_CONTROL_SUM_NEQ_SAMPLE_SIZE,
                ),
            )
            .withColumn(
                "qualityControls",
                StudyIndex.update_quality_flag(
                    f.col("qualityControls"),
                    non_empty_cases | non_empty_ctrls,
                    StudyQualityCheck.ONE_ONLY_CASE_OR_CONTROL,
                ),
            )
        )

        return StudyIndex(_df=validated_df, _schema=StudyIndex.get_schema())

    @qc_test
    def annotate_sumstats_qc(
        self: StudyIndex,
        sumstats_qc: SummaryStatisticsQC,
        threshold_mean_beta: float = 0.05,
        threshold_mean_diff_pz: float = 0.05,
        threshold_se_diff_pz: float = 0.05,
        threshold_min_gc_lambda: float = 0.7,
        threshold_max_gc_lambda: float = 2.5,
        threshold_min_n_variants: int = 2_000_000,
        seq_threshold_mean_beta: float = 1.0,
        seq_threshold_min_gc_lambda: float = 0.1,
        seq_threshold_max_gc_lambda: float = 2.5,
        seq_threshold_min_n_variants: int = 1_000_000,
    ) -> StudyIndex:
        """Annotate summary stats QC information.

        For studies whose ``analysisFlags`` contain ``ExWAS`` or ``wgsGWAS``, relaxed
        thresholds are applied because the genetic data source is exome or whole genome
        sequencing. The PZ check (mean/se diff PZ) is skipped for these studies.

        Args:
            sumstats_qc (SummaryStatisticsQC): Dataset containing summary statistics-based quality controls.
            threshold_mean_beta (float): Threshold for mean beta check. Defaults to 0.05.
            threshold_mean_diff_pz (float): Threshold for mean diff PZ check. Defaults to 0.05.
            threshold_se_diff_pz (float): Threshold for SE diff PZ check. Defaults to 0.05.
            threshold_min_gc_lambda (float): Minimum threshold for GC lambda check. Defaults to 0.7.
            threshold_max_gc_lambda (float): Maximum threshold for GC lambda check. Defaults to 2.5.
            threshold_min_n_variants (int): Minimum number of variants for SuSiE check. Defaults to 2_000_000.
            seq_threshold_mean_beta (float): Mean beta threshold for exome/wgs-gwas studies. Defaults to 1.0.
            seq_threshold_min_gc_lambda (float): Minimum GC lambda threshold for exome/wgs-gwas studies. Defaults to 0.1.
            seq_threshold_max_gc_lambda (float): Maximum GC lambda threshold for exome/wgs-gwas studies. Defaults to 2.5.
            seq_threshold_min_n_variants (int): Minimum number of variants for exome/wgs-gwas studies. Defaults to 1_000_000.

        Returns:
            StudyIndex: Updated study index with QC information
        """
        # convert all columns in sumstats_qc dataframe in array of structs grouped by studyId
        qc_check_cols = [c for c in sumstats_qc.df.columns if c != "studyId"]

        studies = self.df

        # Prepare the QC flags format for the study index:
        melted_qc = convert_from_wide_to_long(
            sumstats_qc.df,
            id_vars=["studyId"],
            value_vars=qc_check_cols,
            var_name="QCCheckName",
            value_name="QCCheckValue",
        )
        qc_struct = f.struct(f.col("QCCheckName"), f.col("QCCheckValue"))
        qc_df = (
            melted_qc.groupBy("studyId")
            .agg(f.collect_list(qc_struct).alias("sumstatQCValues"))
            .select("studyId", "sumstatQCValues")
            .withColumn("hasSumstats", f.lit(True))
        )
        extract_qc_value: Callable[[str], Column] = lambda x: filter_array_struct(
            "sumstatQCValues", "QCCheckName", x, "QCCheckValue"
        )

        # Studies sourced from exome or whole genome sequencing get relaxed thresholds:
        if "analysisFlags" in studies.columns:
            is_seq = f.coalesce(
                f.array_contains("analysisFlags", StudyAnalysisFlag.EXWAS.value)
                | f.array_contains("analysisFlags", StudyAnalysisFlag.WGS_WAS.value),
                f.lit(False),
            )
        else:
            is_seq = f.lit(False)
        seq_or: Callable[[float, float], Column] = lambda seq_val, default_val: f.when(
            is_seq, f.lit(seq_val)
        ).otherwise(f.lit(default_val))

        df = (
            studies.drop("sumstatQCValues", "hasSumstats")
            .join(qc_df, how="left", on="studyId")
            .withColumn("hasSumstats", f.coalesce(f.col("hasSumstats"), f.lit(False)))
            .withColumn(
                "qualityControls",
                StudyIndex.update_quality_flag(
                    f.col("qualityControls"),
                    ~f.col("hasSumstats"),
                    StudyQualityCheck.SUMSTATS_NOT_AVAILABLE,
                ),
            )
            .withColumn(
                "qualityControls",
                StudyIndex.update_quality_flag(
                    f.col("qualityControls"),
                    ~(
                        f.abs(extract_qc_value("mean_beta"))
                        <= seq_or(seq_threshold_mean_beta, threshold_mean_beta)
                    ),
                    StudyQualityCheck.FAILED_MEAN_BETA_CHECK,
                ),
            )
            .withColumn(
                "qualityControls",
                StudyIndex.update_quality_flag(
                    f.col("qualityControls"),
                    # PZ check is skipped for exome/wgs-gwas studies:
                    ~is_seq
                    & ~(
                        (
                            f.abs(extract_qc_value("mean_diff_pz"))
                            <= threshold_mean_diff_pz
                        )
                        & (extract_qc_value("se_diff_pz") <= threshold_se_diff_pz)
                    ),
                    StudyQualityCheck.FAILED_PZ_CHECK,
                ),
            )
            .withColumn(
                "qualityControls",
                StudyIndex.update_quality_flag(
                    f.col("qualityControls"),
                    ~(
                        (
                            extract_qc_value("gc_lambda")
                            <= seq_or(
                                seq_threshold_max_gc_lambda, threshold_max_gc_lambda
                            )
                        )
                        & (
                            extract_qc_value("gc_lambda")
                            >= seq_or(
                                seq_threshold_min_gc_lambda, threshold_min_gc_lambda
                            )
                        )
                    ),
                    StudyQualityCheck.FAILED_GC_LAMBDA_CHECK,
                ),
            )
            .withColumn(
                "qualityControls",
                StudyIndex.update_quality_flag(
                    f.col("qualityControls"),
                    extract_qc_value("n_variants")
                    < seq_or(seq_threshold_min_n_variants, threshold_min_n_variants),
                    StudyQualityCheck.SMALL_NUMBER_OF_SNPS,
                ),
            )
        )

        # Annotate study index with QC information:
        return self.__class__(
            _df=df,
            _schema=self.__class__.get_schema(),
        )

    def get_summary_statistics_paths(self: StudyIndex) -> list[str]:
        """Get summary statistics paths from study index.

        Returns:
            list[str]: List of summary statistics file paths.
        """
        paths_df = self.df.filter(self.has_summarystats()).select(
            f.col("summarystatsLocation")
        )
        paths = [row["summarystatsLocation"] for row in paths_df.collect()]
        return paths


class ProteinQuantitativeTraitLocusStudyIndex(StudyIndex):
    """Protein quantitative trait locus (pQTL) study index dataset.

    A pQTL study index dataset captures all the metadata for pQTL studies.
    """

    @classmethod
    def get_schema(cls: type[ProteinQuantitativeTraitLocusStudyIndex]) -> StructType:
        """Provide the schema for the ProteinQuantitativeTraitLocusStudyIndex dataset.

        Returns:
            StructType: The schema of the ProteinQuantitativeTraitLocusStudyIndex dataset.
        """
        study_schema = parse_spark_schema("study_index.json")
        return study_schema.add(
            t.StructField(
                "targetsFromSource",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("proteinId", t.StringType(), nullable=True),
                            t.StructField("proteinName", t.StringType(), nullable=True),
                            t.StructField("geneId", t.StringType(), nullable=True),
                            t.StructField("geneSymbol", t.StringType(), nullable=True),
                        ]
                    ),
                    containsNull=True,
                ),
                nullable=True,
            ),
        ).add(
            t.StructField(
                "molecularComplexIds",
                t.ArrayType(t.StringType(), containsNull=True),
                nullable=True,
            )
        )

    def to_study(
        self: ProteinQuantitativeTraitLocusStudyIndex, target: TargetIndex
    ) -> StudyIndex:
        """Convert ProteinQuantitativeTraitLocusStudyIndex to StudyIndex.

        This method maps the pQTL-specific fields to the corresponding fields in the StudyIndex dataset and returns a StudyIndex instance.
        Map proteinIds to geneIds and coalesce geneIds and then explode the list of targets to have one row per target.
        Targets that the target index does not resolve are dropped.

        Args:
            target (TargetIndex): Target index containing the reference gene identifiers (Ensembl gene identifiers)

        Returns:
            StudyIndex: A StudyIndex instance with mapped fields from ProteinQuantitativeTraitLocusStudyIndex.
        """
        symbol_id_lut = target.symbols_lut().persist()

        _symbol_annot_si = (
            self.df.withColumn("targetFromSource", f.explode("targetsFromSource"))
            .select(
                *[n for n in StudyIndex.get_schema().fieldNames() if n != "geneId"],
                f.col("targetFromSource.geneId").alias("geneIdFromSource"),
                f.col("targetFromSource.geneSymbol").alias("geneSymbol"),
                f.col("targetFromSource.proteinId").alias("proteinId"),
            )
            .join(symbol_id_lut, on="geneSymbol", how="left")
            .withColumn("geneId", f.coalesce("geneIdFromSource", "geneId"))
            .withColumn(
                "ambiguousGeneIdMapping",
                f.size(f.collect_set("geneId").over(Window.partitionBy("geneSymbol")))
                > 1,
            )
            .drop("geneIdFromSource", "geneSymbol")
            .persist()
        )
        symbol_id_lut.unpersist()
        protein_id_lut = target.protein_id_lut().persist()
        non_ambiguous_df = (
            _symbol_annot_si.filter(~f.col("ambiguousGeneIdMapping"))
            .drop("ambiguousGeneIdMapping")
            .select(StudyIndex.get_schema().fieldNames())
        )
        # The symbol join emitted one row per candidate gene, differing in the lookup
        # columns geneId, chromosome and tss. Project to the output columns plus
        # proteinId and collapse, so re-resolving on proteinId yields one row per
        # target.
        ambiguous_columns = [
            *(c for c in StudyIndex.get_schema().fieldNames() if c != "geneId"),
            "proteinId",
        ]
        ambiguous_df = (
            _symbol_annot_si.filter(f.col("ambiguousGeneIdMapping"))
            .select(ambiguous_columns)
            .distinct()
            .join(protein_id_lut, on="proteinId", how="left")
            .select(StudyIndex.get_schema().fieldNames())
        )

        # A target the reference does not resolve carries no gene mapping, and several
        # such targets in one study produce identical rows that validate_unique_study_id
        # would flag, discarding the targets that did resolve. Keep the resolved rows.
        return StudyIndex(
            _df=non_ambiguous_df.unionByName(ambiguous_df).filter(
                f.col("geneId").isNotNull()
            ),
            _schema=StudyIndex.get_schema(),
        )


class MetaAnalysisStudyIndex(StudyIndex):
    """Meta-analysis study index dataset.

    A meta-analysis study index dataset captures all the metadata for meta-analysis studies.
    This studyIndex defines additional fields capturing nCases and nSamples per individual cohorts.
    """

    @classmethod
    def get_schema(cls) -> t.StructType:
        """Get the Spark schema for the manifest DataFrame.

        Returns:
            t.StructType: Spark schema for the manifest DataFrame.
        """
        return (
            super()
            .get_schema()
            .add(
                t.StructField(
                    "nSamplesPerCohort",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField("cohort", t.StringType(), nullable=False),
                                t.StructField(
                                    "nSamples", t.IntegerType(), nullable=False
                                ),
                            ]
                        )
                    ),
                    nullable=False,
                )
            )
            .add(
                t.StructField(
                    "nCasesPerCohort",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField("cohort", t.StringType(), nullable=False),
                                t.StructField(
                                    "nCases", t.IntegerType(), nullable=False
                                ),
                            ]
                        )
                    ),
                    # Only relevant for binary traits
                    nullable=True,
                )
            )
        )

    def to_study(self: MetaAnalysisStudyIndex) -> StudyIndex:
        """Convert MetaAnalysisStudyIndex to StudyIndex.

        This method maps the meta-analysis-specific fields to the corresponding fields in the StudyIndex dataset and returns a StudyIndex instance.

        Returns:
            StudyIndex: A StudyIndex instance with mapped fields from MetaAnalysisStudyIndex.
        """
        return StudyIndex(
            _df=self.df.select(StudyIndex.get_schema().fieldNames()),
            _schema=StudyIndex.get_schema(),
        )
