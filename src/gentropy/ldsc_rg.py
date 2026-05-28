"""Step to calculate genetic correlation using LDSC from two sets of summary statistics."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.common.session import Session
from gentropy.method.ldsc import run_ldsc_rg_from_arrays


class GeneticCorrelationStep:
    """Step to calculate genetic correlation using LDSC from two sets of summary statistics."""

    def __init__(
        self,
        session: Session,
        summary_statistics_input_path_1: str,
        summary_statistics_input_path_2: str,
        study_index_input_path: str,
        ldscore_base_path: str,
        rg_output_path: str,
        ldscore_template: str = "gnomad_r2.1.1_{ancestry}_hg38.csv.gz",
        twostep: float = 30.0,
        n_blocks: int = 200,
        intercept: float | None = None,
        max_rows_for_collection: int = 20_000_000,
        min_samples: int = 10_000,
        m_ldsc_override: float | None = None,
    ) -> None:
        """Initialise the LDSC genetic correlation step.

        Args:
            session (Session): Gentropy session object.
            summary_statistics_input_path_1 (str): Path to summary stats parquet for trait 1.
            summary_statistics_input_path_2 (str): Path to summary stats parquet for trait 2.
            study_index_input_path (str): Path to study index parquet.
            ldscore_base_path (str): Base directory for LD score files.
            rg_output_path (str): Path to write genetic correlation results.
            ldscore_template (str): LD score file template with "{ancestry}" placeholder.
            twostep (float): LDSC two-step chi-square cut-off.
            n_blocks (int): Number of jackknife blocks.
            intercept (float | None): Optional fixed cross-trait intercept.
            max_rows_for_collection (int): Maximum joined SNP row count before skipping.
            min_samples (int): Minimum sample size for both studies.
            m_ldsc_override (float | None): Override M_ldsc SNP count.
        """
        self.session = session
        self.summary_statistics_input_path_1 = summary_statistics_input_path_1
        self.summary_statistics_input_path_2 = summary_statistics_input_path_2
        self.study_index_input_path = study_index_input_path
        self.ldscore_base_path = ldscore_base_path
        self.rg_output_path = rg_output_path
        self.ldscore_template = ldscore_template
        self.twostep = twostep
        self.n_blocks = n_blocks
        self.intercept = intercept
        self.max_rows_for_collection = max_rows_for_collection
        self.min_samples = min_samples
        self.m_ldsc_override = m_ldsc_override
        self.results: dict[str, Any] | None = None

        self._run()

    def _run(self) -> None:
        """Execute the LDSC genetic correlation pipeline."""
        sumstats_df_1 = self._read_sumstats(self.summary_statistics_input_path_1)
        sumstats_df_2 = self._read_sumstats(self.summary_statistics_input_path_2)
        study_id_1 = self._extract_single_study_id(sumstats_df_1)
        study_id_2 = self._extract_single_study_id(sumstats_df_2)

        study_index_df = self._read_study_index()

        validation_1 = self._validate_study(study_index_df, study_id_1, self.min_samples)
        validation_2 = self._validate_study(study_index_df, study_id_2, self.min_samples)

        skip_reasons: list[str] = list(validation_1["skip_reasons"]) + [
            f"Trait 2: {r}" for r in validation_2["skip_reasons"]
        ]

        ancestry_1 = validation_1["ancestry"]
        ancestry_2 = validation_2["ancestry"]
        if ancestry_1 is not None and ancestry_2 is not None and ancestry_1 != ancestry_2:
            skip_reasons.append(
                f"Ancestry mismatch: trait 1 is {ancestry_1}, trait 2 is {ancestry_2}"
            )

        if skip_reasons:
            self._write_result_row(
                study_id_1=study_id_1,
                study_id_2=study_id_2,
                rg_output_path=self.rg_output_path,
                run_status="skipped",
                skip_reasons=skip_reasons,
                analysis_flags_1=validation_1["analysis_flags"],
                analysis_flags_2=validation_2["analysis_flags"],
                ld_ancestry=ancestry_1,
                result=None,
                m_ldsc=None,
                n_snps_used=None,
            )
            return

        ancestry = ancestry_1
        ldscore_input_path = self._build_ldscore_input_path(ancestry)

        prepared_1 = self._prepare_sumstats(sumstats_df_1, study_index_df)
        prepared_2 = self._prepare_sumstats(sumstats_df_2, study_index_df)
        ld_df = self._read_and_prepare_ld_scores(ldscore_input_path)

        merged_df, m_ldsc = self._merge_all(prepared_1, prepared_2, ld_df)

        n_rows = merged_df.count()

        if n_rows == 0:
            self._write_result_row(
                study_id_1=study_id_1,
                study_id_2=study_id_2,
                rg_output_path=self.rg_output_path,
                run_status="skipped",
                skip_reasons=["No overlapping SNPs between both summary statistics and LD scores"],
                analysis_flags_1=validation_1["analysis_flags"],
                analysis_flags_2=validation_2["analysis_flags"],
                ld_ancestry=ancestry,
                result=None,
                m_ldsc=m_ldsc,
                n_snps_used=0,
            )
            return

        if n_rows > self.max_rows_for_collection:
            self._write_result_row(
                study_id_1=study_id_1,
                study_id_2=study_id_2,
                rg_output_path=self.rg_output_path,
                run_status="skipped",
                skip_reasons=["Too many joined SNPs for collection"],
                analysis_flags_1=validation_1["analysis_flags"],
                analysis_flags_2=validation_2["analysis_flags"],
                ld_ancestry=ancestry,
                result=None,
                m_ldsc=m_ldsc,
                n_snps_used=n_rows,
            )
            return

        arrays = self._collect_rg_arrays(merged_df)
        w_raw = 1.0 / np.maximum(arrays["ld"], 1.0)
        w_ld = w_raw / np.mean(w_raw)

        result = run_ldsc_rg_from_arrays(
            beta1=arrays["beta1"],
            se1=arrays["se1"],
            N1=arrays["n1"],
            beta2=arrays["beta2"],
            se2=arrays["se2"],
            N2=arrays["n2"],
            ld=arrays["ld"],
            w_ld=w_ld,
            M_ldsc_scalar=m_ldsc,
            intercept=self.intercept,
            twostep=self.twostep,
            n_blocks=self.n_blocks,
        )

        self.results = result

        self._write_result_row(
            study_id_1=study_id_1,
            study_id_2=study_id_2,
            rg_output_path=self.rg_output_path,
            run_status="success",
            skip_reasons=[],
            analysis_flags_1=validation_1["analysis_flags"],
            analysis_flags_2=validation_2["analysis_flags"],
            ld_ancestry=ancestry,
            result=result,
            m_ldsc=m_ldsc,
            n_snps_used=len(arrays["beta1"]),
        )

    def _read_sumstats(self, path: str) -> DataFrame:
        """Read summary statistics parquet, selecting only the columns needed for LDSC.

        Args:
            path (str): Path to the summary statistics parquet file.

        Returns:
            DataFrame: Summary statistics dataframe with the columns required for LDSC.
        """
        return self.session.spark.read.parquet(path).select(
            "studyId", "variantId", "chromosome", "position",
            "beta", "standardError", "sampleSize",
        )

    def _extract_single_study_id(self, sumstats_df: DataFrame) -> str:
        """Extract and validate that exactly one studyId is present in the dataframe.

        Args:
            sumstats_df (DataFrame): Summary statistics dataframe.

        Returns:
            str: The single study identifier present in the dataframe.

        Raises:
            ValueError: If the dataframe contains zero or multiple study identifiers.
        """
        study_ids = [
            row["studyId"] for row in sumstats_df.select("studyId").distinct().collect()
        ]
        if len(study_ids) != 1:
            raise ValueError(
                f"Expected one study in summary statistics, got {len(study_ids)}: {study_ids}"
            )
        return study_ids[0]

    def _read_study_index(self) -> DataFrame:
        """Read the study index parquet with the columns required for validation.

        Returns:
            DataFrame: Study index dataframe with metadata required for validation.
        """
        return self.session.spark.read.parquet(self.study_index_input_path).select(
            "studyId", "nCases", "nControls", "nSamples",
            "ldPopulationStructure", "analysisFlags",
        )

    def _build_ldscore_input_path(self, ancestry: str) -> str:
        """Build the LD-score file path for a given ancestry.

        Args:
            ancestry (str): Canonical ancestry label used in the LD-score filename.

        Returns:
            str: Fully resolved path to the LD-score file.
        """
        return (
            f"{self.ldscore_base_path.rstrip('/')}/"
            f"{self.ldscore_template.format(ancestry=ancestry)}"
        )

    def _prepare_sumstats(
        self, sumstats_df: DataFrame, study_index_df: DataFrame
    ) -> DataFrame:
        """Prepare summary statistics by filling missing N, extracting alleles, and filtering.

        Args:
            sumstats_df (DataFrame): Raw summary statistics dataframe.
            study_index_df (DataFrame): Study index dataframe containing fallback sample sizes.

        Returns:
            DataFrame: Filtered and deduplicated summary statistics ready for joining.
        """
        n_df = study_index_df.select("studyId", "nSamples", "nCases", "nControls")
        neff_expr = (
            4.0
            * F.col("nCases").cast("double")
            * F.col("nControls").cast("double")
            / (F.col("nCases").cast("double") + F.col("nControls").cast("double"))
        )
        fallback_n_expr = F.when(
            F.col("nCases").isNotNull()
            & F.col("nControls").isNotNull()
            & (F.col("nCases") > 0)
            & (F.col("nControls") > 0),
            neff_expr,
        ).otherwise(F.col("nSamples").cast("double"))

        return (
            sumstats_df.join(n_df, on="studyId", how="left")
            .withColumn(
                "sampleSize",
                F.when(F.col("sampleSize").isNull(), fallback_n_expr).otherwise(
                    F.col("sampleSize").cast("double")
                ),
            )
            .withColumn("variant_parts", F.split(F.col("variantId"), "_"))
            .withColumn("ref", F.col("variant_parts").getItem(2))
            .withColumn("alt", F.col("variant_parts").getItem(3))
            .drop("variant_parts", "nSamples", "nCases", "nControls")
            .filter(F.col("beta").isNotNull())
            .filter(F.col("standardError").isNotNull())
            .filter(F.col("sampleSize").isNotNull())
            .filter(F.col("standardError") > 0)
            .filter(F.col("chromosome").isNotNull())
            .filter(F.col("position").isNotNull())
            .filter(F.col("ref").isNotNull())
            .filter(F.col("alt").isNotNull())
            .dropDuplicates(["studyId", "chromosome", "position", "ref", "alt"])
        )

    def _read_and_prepare_ld_scores(self, ldscore_input_path: str) -> DataFrame:
        """Read and standardise LD-score input data.

        Args:
            ldscore_input_path (str): Path to the LD-score file.

        Returns:
            DataFrame: Standardised LD-score dataframe containing chromosome, position,
                ref, alt, and L2 columns.

        Raises:
            ValueError: If required LD-score columns are missing.
        """
        ld_df = self.session.spark.read.csv(ldscore_input_path, header=True, sep="\t")
        if "BP_hg38" in ld_df.columns:
            ld_df = ld_df.withColumnRenamed("BP_hg38", "position")
        else:
            raise ValueError("LD score file must contain 'BP_hg38' column.")
        if "CHR" in ld_df.columns:
            ld_df = ld_df.withColumnRenamed("CHR", "chromosome")
        elif "chromosome" not in ld_df.columns:
            raise ValueError("LD score file must contain 'CHR' or 'chromosome' column.")
        required = {"chromosome", "position", "ref", "alt", "L2"}
        missing = required - set(ld_df.columns)
        if missing:
            raise ValueError(f"LD score file is missing required columns: {sorted(missing)}")
        return (
            ld_df.select("chromosome", "position", "ref", "alt", "L2")
            .withColumn("position", F.col("position").cast("int"))
            .withColumn("chromosome", F.col("chromosome").cast("string"))
            .withColumn("L2", F.col("L2").cast("double"))
            .filter(F.col("L2").isNotNull())
            .dropDuplicates(["chromosome", "position", "ref", "alt"])
        )

    def _merge_all(
        self,
        prepared_1: DataFrame,
        prepared_2: DataFrame,
        ld_df: DataFrame,
    ) -> tuple[DataFrame, float]:
        """Join trait 1, trait 2, and LD scores with allele-flip handling.

        Args:
            prepared_1 (DataFrame): Prepared summary statistics for trait 1.
            prepared_2 (DataFrame): Prepared summary statistics for trait 2.
            ld_df (DataFrame): Prepared LD-score dataframe.

        Returns:
            tuple[DataFrame, float]: Merged dataframe and total LD-score SNP count (M_ldsc).
        """
        join_cols = ["chromosome", "position", "ref", "alt"]

        m_ldsc = (
            self.m_ldsc_override
            if self.m_ldsc_override is not None
            else float(ld_df.count())
        )

        # Rename columns to avoid ambiguity
        t1 = prepared_1.select(
            F.col("chromosome"), F.col("position"),
            F.col("ref"), F.col("alt"),
            F.col("beta").alias("beta1"),
            F.col("standardError").alias("se1"),
            F.col("sampleSize").alias("n1"),
        )
        t2 = prepared_2.select(
            F.col("chromosome"), F.col("position"),
            F.col("ref"), F.col("alt"),
            F.col("beta").alias("beta2"),
            F.col("standardError").alias("se2"),
            F.col("sampleSize").alias("n2"),
        )

        # Direct join (same ref/alt)
        t2_direct = t2.withColumn("flipped", F.lit(False))

        # Flipped join: swap ref/alt in trait 2, negate beta2 later
        t2_flipped = t2.select(
            F.col("chromosome"),
            F.col("position"),
            F.col("alt").alias("ref"),    # swap
            F.col("ref").alias("alt"),    # swap
            F.col("beta2"),
            F.col("se2"),
            F.col("n2"),
        ).withColumn("flipped", F.lit(True))

        t2_combined = t2_direct.unionByName(t2_flipped).dropDuplicates(join_cols)

        # Negate beta2 where alleles were flipped
        t2_combined = t2_combined.withColumn(
            "beta2",
            F.when(F.col("flipped"), -F.col("beta2")).otherwise(F.col("beta2")),
        ).drop("flipped")

        merged = (
            t1
            .join(t2_combined, on=join_cols, how="inner")
            .join(ld_df, on=join_cols, how="inner")
            .dropDuplicates(join_cols)
        )

        return merged, m_ldsc

    def _collect_rg_arrays(
        self, merged_df: DataFrame
    ) -> dict[str, np.ndarray]:
        """Collect the merged SNP data into numpy arrays for LDSC.

        Args:
            merged_df (DataFrame): Three-way joined dataframe of both sumstats and LD scores.

        Returns:
            dict[str, np.ndarray]: Dict with keys beta1, se1, n1, beta2, se2, n2, ld.
        """
        rows = merged_df.select(
            "beta1", "se1", "n1", "beta2", "se2", "n2", "L2"
        ).collect()

        beta1_list, se1_list, n1_list = [], [], []
        beta2_list, se2_list, n2_list = [], [], []
        ld_list = []

        for row in rows:
            beta1_list.append(row["beta1"])
            se1_list.append(row["se1"])
            n1_list.append(row["n1"])
            beta2_list.append(row["beta2"])
            se2_list.append(row["se2"])
            n2_list.append(row["n2"])
            ld_list.append(row["L2"])

        return {
            "beta1": np.array(beta1_list, dtype=float),
            "se1": np.array(se1_list, dtype=float),
            "n1": np.array(n1_list, dtype=float),
            "beta2": np.array(beta2_list, dtype=float),
            "se2": np.array(se2_list, dtype=float),
            "n2": np.array(n2_list, dtype=float),
            "ld": np.array(ld_list, dtype=float),
        }

    def _write_result_row(
        self,
        study_id_1: str,
        study_id_2: str,
        rg_output_path: str,
        run_status: str,
        skip_reasons: list[str],
        analysis_flags_1: list[str],
        analysis_flags_2: list[str],
        ld_ancestry: str | None,
        result: dict[str, Any] | None,
        m_ldsc: float | None,
        n_snps_used: int | None,
    ) -> None:
        """Write a single output row for either a successful or skipped run.

        Args:
            study_id_1 (str): Study identifier for trait 1.
            study_id_2 (str): Study identifier for trait 2.
            rg_output_path (str): Output parquet path.
            run_status (str): Run status, e.g. "success" or "skipped".
            skip_reasons (list[str]): Reasons for skipping, if any.
            analysis_flags_1 (list[str]): Normalised analysis flags for trait 1.
            analysis_flags_2 (list[str]): Normalised analysis flags for trait 2.
            ld_ancestry (str | None): Inferred ancestry used for LD scores.
            result (dict[str, Any] | None): LDSC result dictionary, if available.
            m_ldsc (float | None): Number of SNPs in the LD-score universe.
            n_snps_used (int | None): Number of SNPs used in regression.
        """
        def _r(key: str) -> Any:
            return result[key] if result else None

        intercept_se_val = None
        if result and result.get("intercept_se") not in (None, "NA"):
            try:
                intercept_se_val = float(result["intercept_se"])
            except (TypeError, ValueError):
                intercept_se_val = None

        out_dict: dict[str, list[Any]] = {
            "studyId1": [study_id_1],
            "studyId2": [study_id_2],
            "runStatus": [run_status],
            "skipReasons": [skip_reasons],
            "analysisFlags1": [analysis_flags_1],
            "analysisFlags2": [analysis_flags_2],
            "ld_ancestry": [ld_ancestry],
            "M_ldsc": [m_ldsc],
            "n_snps_used": [n_snps_used],
            "rg": [_r("rg")],
            "rg_se": [_r("rg_se")],
            "rg_clipped": [_r("rg_clipped")],
            "gcov": [_r("gcov")],
            "gcov_se": [_r("gcov_se")],
            "h2_1": [_r("h2_1")],
            "h2_1_se": [_r("h2_1_se")],
            "h2_2": [_r("h2_2")],
            "h2_2_se": [_r("h2_2_se")],
            "intercept": [_r("intercept")],
            "intercept_se": [intercept_se_val],
        }

        schema = StructType([
            StructField("studyId1", StringType(), True),
            StructField("studyId2", StringType(), True),
            StructField("runStatus", StringType(), True),
            StructField("skipReasons", ArrayType(StringType()), True),
            StructField("analysisFlags1", ArrayType(StringType()), True),
            StructField("analysisFlags2", ArrayType(StringType()), True),
            StructField("ld_ancestry", StringType(), True),
            StructField("M_ldsc", DoubleType(), True),
            StructField("n_snps_used", LongType(), True),
            StructField("rg", DoubleType(), True),
            StructField("rg_se", DoubleType(), True),
            StructField("rg_clipped", BooleanType(), True),
            StructField("gcov", DoubleType(), True),
            StructField("gcov_se", DoubleType(), True),
            StructField("h2_1", DoubleType(), True),
            StructField("h2_1_se", DoubleType(), True),
            StructField("h2_2", DoubleType(), True),
            StructField("h2_2_se", DoubleType(), True),
            StructField("intercept", DoubleType(), True),
            StructField("intercept_se", DoubleType(), True),
        ])

        out_pdf = pd.DataFrame(out_dict)
        out_sdf = self.session.spark.createDataFrame(out_pdf, schema=schema)
        out_sdf.write.mode("overwrite").parquet(rg_output_path)

    def _validate_study(
        self,
        study_index_df: DataFrame,
        study_id: str,
        min_samples: int,
    ) -> dict[str, Any]:
        """Validate study metadata and decide whether LDSC should run.

        Args:
            study_index_df (DataFrame): Study index dataframe.
            study_id (str): Study identifier.
            min_samples (int): Minimum allowed sample size.

        Returns:
            dict[str, Any]: Validation result with run_status, skip_reasons,
                analysis_flags, and ancestry.

        Raises:
            ValueError: If the study ID is not found in the study index.
        """
        row = (
            study_index_df.filter(F.col("studyId") == study_id)
            .select("ldPopulationStructure", "analysisFlags", "nSamples")
            .first()
        )
        if row is None:
            raise ValueError(f"studyId {study_id} not found in study index")

        analysis_flags = self._normalise_analysis_flags(row["analysisFlags"])
        skip_reasons: list[str] = []

        if not self._is_allowed_by_analysis_flags(row["analysisFlags"]):
            skip_reasons.append("Invalid study design")

        n_samples = row["nSamples"]
        if n_samples is None:
            skip_reasons.append("Sample size missing")
        elif float(n_samples) < float(min_samples):
            skip_reasons.append("Sample size too small")

        ancestry: str | None
        try:
            ancestry = self._infer_ld_ancestry(row["ldPopulationStructure"])
        except Exception:
            ancestry = None
            skip_reasons.append("Could not infer ancestry")

        return {
            "run_status": "skipped" if skip_reasons else "success",
            "skip_reasons": skip_reasons,
            "analysis_flags": analysis_flags,
            "ancestry": ancestry,
        }

    @staticmethod
    def _extract_population_and_weight(entry: Any) -> tuple[str | None, float | None]:
        """Extract population label and weight from a population structure entry.

        Args:
            entry (Any): Population structure entry.

        Returns:
            tuple[str | None, float | None]: Population label and weight.
        """
        pop = None
        weight = None

        if hasattr(entry, "population"):
            pop = entry.population
        elif hasattr(entry, "ldPopulation"):
            pop = entry.ldPopulation

        if hasattr(entry, "proportion"):
            weight = entry.proportion
        elif hasattr(entry, "relativeSampleSize"):
            weight = entry.relativeSampleSize

        if isinstance(entry, dict):
            pop = pop or entry.get("population") or entry.get("ldPopulation")
            weight = (
                weight
                or entry.get("proportion")
                or entry.get("relativeSampleSize")
                or entry.get("weight")
            )

        try:
            weight = float(weight) if weight is not None else None
        except Exception:
            weight = None

        return (str(pop).strip().lower() if pop else None, weight)

    @staticmethod
    def _infer_ld_ancestry(ld_pop_struct: Any) -> str:
        """Infer canonical LD ancestry from ldPopulationStructure.

        In the event of a tie, prefer ``nfe`` if it is one of the tied ancestries.

        Args:
            ld_pop_struct (Any): Iterable population structure.

        Returns:
            str: Canonical ancestry label.
        """
        if ld_pop_struct is None:
            raise ValueError("ldPopulationStructure is None, cannot infer ancestry")

        if not isinstance(ld_pop_struct, Iterable):
            raise TypeError(
                f"ldPopulationStructure has unexpected type {type(ld_pop_struct)}; "
                "expected iterable of population structures."
            )

        pop_map = {
            "afr": "afr",
            "amr": "amr",
            "eas": "eas",
            "fin": "fin",
            "nfe": "nfe",
        }

        agg: dict[str, float] = {}

        for entry in ld_pop_struct:
            pop, weight = GeneticCorrelationStep._extract_population_and_weight(entry)
            if pop is None or weight is None:
                continue

            canonical = pop_map.get(pop)
            if canonical is None:
                continue

            agg[canonical] = agg.get(canonical, 0.0) + weight

        if not agg:
            raise ValueError(
                f"Could not map any populations from ldPopulationStructure: {ld_pop_struct}"
            )

        max_weight = max(agg.values())
        tied = [pop for pop, weight in agg.items() if weight == max_weight]

        if "nfe" in tied:
            return "nfe"

        return sorted(tied)[0]

    @staticmethod
    def _normalise_analysis_flags(analysis_flags: Any) -> list[str]:
        """Normalise analysisFlags into a lowercase string list.

        Args:
            analysis_flags (Any): Raw analysis flags.

        Returns:
            list[str]: Normalised analysis flags.
        """
        if analysis_flags is None:
            return []

        normalised: list[str] = []
        for flag in analysis_flags:
            if flag is None:
                continue
            normalised.append(str(flag).strip().lower())

        return normalised

    @staticmethod
    def _is_allowed_by_analysis_flags(analysis_flags: Any) -> bool:
        """Check whether analysisFlags allow LDSC estimation.

        Allowed cases:
            - no flags
            - flags are a subset of the allowed set (exwas, wgsgwas, metabolite)

        Args:
            analysis_flags (Any): Raw analysis flags.

        Returns:
            bool: Whether the study is allowed.
        """
        allowed_flags = {"exwas", "wgsgwas", "metabolite"}
        flags = set(GeneticCorrelationStep._normalise_analysis_flags(analysis_flags))
        return flags.issubset(allowed_flags)
