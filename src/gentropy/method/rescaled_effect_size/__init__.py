"""Rescaled statistics for trait analysis."""

from __future__ import annotations

from enum import Enum, StrEnum

from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql import types as t


class StudyType(StrEnum):
    """Enum for study types."""

    GWAS = "gwas"
    EQTL = "eqtl"
    SCEQTL = "sceqtl"
    PQTL = "pqtl"
    SQTL = "sqtl"
    TUQTL = "tuqtl"
    TRANS_PQTL = "trans-pqtl"
    CIS_PQTL = "cis-pqtl"
    GWAS_MEASUREMENT = "gwas-measurement"
    GWAS_DISEASE = "gwas-disease"


class CaseControlDiscrepancy(Enum):
    """Case control discrepancy types."""

    EMPTY_CASES_NON_EMPTY_CONTROLS = 0
    EMPTY_CONTROLS_NON_EMPTY_CASES = 1
    SUM_CASES_CONTROLS_NEQUAL_SAMPLES = 2
    EMPTY_CASES_EMPTY_CONTROLS = 3


class StudyStatistics:
    """Study class to define a study statistics."""

    name = "studyStatistics"
    """Name of the study statistics."""
    schema = "struct<nCases: INT, nControls: INT, nSamples: INT, trait: STRING, studyType: STRING, traitClass: STRING>"

    non_gwas_study_types = [
        StudyType.EQTL.value,
        StudyType.SCEQTL.value,
        StudyType.PQTL.value,
        StudyType.SQTL.value,
        StudyType.TUQTL.value,
    ]

    def __init__(self, col: Column | None = None):
        """Initialize Cohort with an optional column.

        Args:
            col (Column, optional): Optional column to initialize the cohort.


        """
        self.col = col.alias(self.name) if col is not None else f.col(self.name)

    @property
    def n_cases(self) -> Column:
        """Get the number of cases in the cohort."""
        return self.col.getField("nCases").alias("nCases")

    @property
    def n_controls(self) -> Column:
        """Get the number of controls in the cohort."""
        return self.col.getField("nControls").alias("nControls")

    @property
    def n_samples(self) -> Column:
        """Get the total number of samples in the cohort."""
        return self.col.getField("nSamples").alias("nSamples")

    @property
    def trait(self) -> Column:
        """Get the trait associated with the cohort."""
        return self.col.getField("trait").alias("trait")

    @property
    def study_type(self) -> Column:
        """Get the study type associated with the cohort."""
        return self.col.getField("studyType").alias("studyType")

    @property
    def trait_class(self) -> Column:
        """Get the trait class associated with the cohort."""
        return self.col.getField("traitClass").alias("traitClass")

    @property
    def molecular_trait(self) -> Column:
        """Get the gene ID associated with the cohort."""
        return self.col.getField("molecularTrait").alias("molecularTrait")

    @classmethod
    def classify_trait(
        cls, n_cases: Column, n_controls: Column, study_type: Column
    ) -> Column:
        """Classify the trait as continuous or binary."""
        expr = (
            f.when(
                study_type.isin(cls.non_gwas_study_types),
                f.lit(TraitClassName.QUANTITATIVE),
            )
            .when((n_cases > 0) & (n_controls > 0), f.lit(TraitClassName.BINARY))
            .when((n_cases == 0), f.lit(TraitClassName.QUANTITATIVE))
            .when((n_cases.isNull()), f.lit(TraitClassName.QUANTITATIVE))
            .when((n_controls == 0), f.lit(TraitClassName.QUANTITATIVE))
            .when((n_controls.isNull()), f.lit(TraitClassName.QUANTITATIVE))
            .otherwise(f.lit(TraitClassName.UNKNOWN))
        )

        return expr.alias("traitClass")

    def transform_study_type(self, value: StudyType) -> StudyStatistics:
        """Transform the study type to a string."""
        return StudyStatistics(self.col.withField("studyType", f.lit(value)))

    @classmethod
    def split_pqtl(cls, study_type: Column, is_trans_pqtl: Column) -> Column:
        """Transform the study type to a string."""
        expr = (
            f.when(
                (study_type == f.lit(StudyType.PQTL)) & is_trans_pqtl,
                f.lit(StudyType.TRANS_PQTL),
            )
            .when(
                (study_type == f.lit(StudyType.PQTL)) & ~is_trans_pqtl,
                f.lit(StudyType.CIS_PQTL),
            )
            .otherwise(study_type)
        )

        return expr.alias("studyType")

    @classmethod
    def merge_gwas_and_molecular_traits(cls, gene_id: Column, trait: Column) -> Column:
        """Merge GWAS and molecular traits into a single trait column."""
        return f.coalesce(trait, gene_id).alias("trait")

    def validate_trait_class(self) -> Column:
        """Validate the trait class."""
        expr = (
            f.when(
                (self.n_cases > 0) & (self.n_controls == 0),
                f.lit(CaseControlDiscrepancy.EMPTY_CONTROLS_NON_EMPTY_CASES),
            )
            .when(
                (self.n_cases == 0) & (self.n_controls > 0),
                f.lit(CaseControlDiscrepancy.EMPTY_CASES_NON_EMPTY_CONTROLS),
            )
            .when(
                (self.n_cases + self.n_controls) != self.n_samples,
                f.lit(CaseControlDiscrepancy.SUM_CASES_CONTROLS_NEQUAL_SAMPLES),
            )
        )

        return expr.alias("caseControlDiscrepancy")

    @classmethod
    def compute(
        cls,
        n_cases: Column,
        n_controls: Column,
        n_samples: Column,
        trait: Column,
        study_type: Column,
        is_trans_pqtl: Column,
        gene_id: Column,
    ) -> StudyStatistics:
        """Compute the cohort statistics from the number of cases, controls, and trait.

        The cardinality of this table is 1:1 with credible sets.

        Args:
            n_cases (Column): Number of cases in the cohort.
            n_controls (Column): Number of controls in the cohort.
            n_samples (Column): Total number of samples in the cohort.
            trait (Column): Trait associated with the cohort.
            study_type (Column): Type of study (e.g., gwas, eqtl, etc.).
            is_trans_pqtl (Column): Boolean indicating if the credible set refers to cis or trans qtl.
            gene_id (Column): Gene ID associated with the molecular trait.

        Returns:
            studyStatistics: A studyStatistics object containing the computed cohort statistics.

        Examples:
        --------
        >>> r1 = (100, 50, 150, "EFO_0000508", "gwas", False, None)
        >>> r2 = (0, 0, 210, "EFO_0000408", "pqtl", True, "ENSG00000139618")
        >>> r3 = (None, None, 300, "EFO_0000608", "gwas", False, None)
        >>> data = [r1, r2, r3]
        >>> schema = "nCases INT, nControls INT, nSamples INT, trait STRING, studyType STRING, isTransPqtl BOOLEAN, geneId STRING"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +------+---------+--------+-----------+---------+-----------+---------------+
        |nCases|nControls|nSamples|      trait|studyType|isTransPqtl|         geneId|
        +------+---------+--------+-----------+---------+-----------+---------------+
        |   100|       50|     150|EFO_0000508|     gwas|      false|           NULL|
        |     0|        0|     210|EFO_0000408|     pqtl|       true|ENSG00000139618|
        |  NULL|     NULL|     300|EFO_0000608|     gwas|      false|           NULL|
        +------+---------+--------+-----------+---------+-----------+---------------+
        <BLANKLINE>
        >>> study_stats = StudyStatistics.compute(
        ... n_cases=f.col("nCases"),
        ... n_controls=f.col("nControls"),
        ... n_samples=f.col("nSamples"),
        ... trait=f.col("trait"),
        ... study_type=f.col("studyType"),
        ... is_trans_pqtl=f.col("isTransPqtl"),
        ... gene_id=f.col("geneId"),
        ... )
        >>> df = df.select(study_stats.col)
        >>> df.select("studyStatistics.*").show()
        +------+---------+--------+-----------+----------+------------+---------------+
        |nCases|nControls|nSamples|      trait| studyType|  traitClass| molecularTrait|
        +------+---------+--------+-----------+----------+------------+---------------+
        |   100|       50|     150|EFO_0000508|      gwas|      binary|           NULL|
        |     0|        0|     210|EFO_0000408|trans-pqtl|quantitative|ENSG00000139618|
        |  NULL|     NULL|     300|EFO_0000608|      gwas|quantitative|           NULL|
        +------+---------+--------+-----------+----------+------------+---------------+
        <BLANKLINE>

        """
        return cls(
            f.struct(
                n_cases.alias("nCases"),
                n_controls.alias("nControls"),
                n_samples.alias("nSamples"),
                cls.merge_gwas_and_molecular_traits(gene_id, trait).alias("trait"),
                cls.split_pqtl(study_type, is_trans_pqtl),
                cls.classify_trait(n_cases, n_controls, study_type),
                f.col("geneId").alias("molecularTrait"),
            )
        )


class TraitClassName(StrEnum):
    """Enum for trait class names."""

    QUANTITATIVE = "quantitative"
    BINARY = "binary"
    UNKNOWN = "unknown"


class RescaledStatistics:
    """Class for rescaling beta values based on the trait class."""

    name = "rescaledStatistics"
    """Name of the rescaled statistics."""
    schema = "struct<directionOfEffect: SHORT, absZScore: FLOAT, absEstimatedBeta: FLOAT, estimatedSE: FLOAT, varG: FLOAT, prev: FLOAT, minorAlleleEstimatedBeta: FLOAT>"

    def __init__(self, col: Column | None = None):
        """Initialize RescaledBeta with an optional column.

        Args:
            col (f.Column, optional): Optional column to initialize the rescaled beta.

        """
        self.col = col.alias(self.name) if col is not None else f.col(self.name)

    @staticmethod
    def direction_of_effect(beta: Column) -> Column:
        """Determine the direction of effect based on beta value.

        The value is:
        *  -1 if beta < 0
        *  1 if beta > 0
        *  Null if beta is 0 or NULL
        """
        return (
            f.when((beta.isNull()) | (beta == 0), f.lit(None).cast(t.ShortType()))
            .when(beta < 0, f.lit(-1).cast(t.ShortType()))
            .when(beta > 0, f.lit(1).cast(t.ShortType()))
            .alias("directionOfEffect")
        )

    @classmethod
    def abs_z_score(cls, chi2_stat: Column) -> Column:
        """Calculate the z-score from the chi-squared statistic.

        Note z-score sign is not determined here.
        """
        return f.sqrt(chi2_stat).alias("zScore")

    @classmethod
    def effective_sample_size(cls, prev: Column, n_samples: Column) -> Column:
        """Calculate the effective sample size based on trait class."""
        return (prev * (1 - prev) * n_samples).alias("effectiveSampleSize")

    @classmethod
    def var_g(cls, maf: Column) -> Column:
        """Calculate the variance explained by the additive genotype."""
        return (2 * maf * (1 - maf)).alias("varG")

    @classmethod
    def prevalence(cls, n_cases: Column, n_samples: Column) -> Column:
        """Calculate the prevalence of the trait."""
        return (n_cases / n_samples).alias("prev")

    @classmethod
    def compute_se(
        cls,
        var_g: Column,
        n_samples: Column,
        trait_class: Column,
        prev: Column,
        var_phen: Column | None = None,
    ) -> Column:
        """Calculate the standard error based on trait class.

        If `var_phen` is not provided, the method assumes that the phenotype was scaled to have a variance of 1 for quantitative traits.

        The definition of the standard errors is derived from the
        https://www.mv.helsinki.fi/home/mjxpirin/GWAS_course/material/GWAS3.pdf
        """
        var_phen = var_phen if isinstance(var_phen, Column) else f.lit(1.0)
        effective_n_samples = cls.effective_sample_size(prev, n_samples)
        linear_se = f.sqrt(var_phen / (var_g * n_samples))
        logit_se = f.sqrt(1 / (var_g * effective_n_samples))
        return (
            f.when(trait_class == f.lit(TraitClassName.QUANTITATIVE), linear_se)
            .when(trait_class == f.lit(TraitClassName.BINARY), logit_se)
            .alias("se")
        )

    @classmethod
    def compute_minor_allele_rescaled_beta(
        cls, major_ancestry_af: Column, rescaled_beta: Column
    ) -> Column:
        """Compute the minor allele rescaled beta based on the major ancestry allele frequency.

        Note: The function expects `rescaled_beta` to already have the correct sign based on the effect direction.
        """
        return (
            f.when(major_ancestry_af <= 0.5, rescaled_beta)
            .otherwise(-rescaled_beta)
            .alias("minorAlleleRescaledBeta")
        )

    @classmethod
    def compute(
        cls,
        chi2_stat: Column,
        trait_class: Column,
        beta: Column,
        maf: Column,
        af: Column,
        n_samples: Column,
        n_cases: Column,
    ) -> RescaledStatistics:
        """Compute rescaled statistics for trait analysis."""
        beta_sign = cls.direction_of_effect(beta)
        abs_z_score = cls.abs_z_score(chi2_stat)
        var_g = cls.var_g(maf)
        prev = cls.prevalence(n_cases, n_samples)
        se = cls.compute_se(var_g, n_samples, trait_class, prev)
        abs_rescaled_beta = f.abs(abs_z_score * se)
        minor_allele_rescaled_beta = cls.compute_minor_allele_rescaled_beta(
            af, abs_rescaled_beta * beta_sign
        )

        return cls(
            f.struct(
                beta_sign.alias("directionOfEffect"),
                abs_z_score.alias("absZScore"),
                var_g.alias("varG"),
                prev.alias("prevalence"),
                se.alias("estimatedSE"),
                abs_rescaled_beta.alias("absEstimatedBeta"),
                minor_allele_rescaled_beta.alias("minorAlleleEstimatedBeta"),
            )
        )
