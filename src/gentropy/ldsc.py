"""Step to calculate SNP-heritability using LDSC from summary statistics."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

from gentropy.common.session import Session
from gentropy.method.ldsc_h2 import run_ldsc_h2_from_arrays


class HeritabilityEstimateStep:
    """Step to calculate SNP-heritability using LDSC from summary statistics."""

    def __init__(
        self,
        session: Session,
        summary_statistics_input_path: str,
        study_index_input_path: str,
        ldscore_base_path: str,
        heritability_output_path: str,
        ldscore_template: str = "gnomad_r2.1.1_{ancestry}_hg38.csv.gz",
        twostep: float = 30.0,
        n_blocks: int = 200,
        intercept: float | None = None,
        max_rows_for_collection: int = 1_500_000,
        min_samples: int = 10_000,
    ) -> None:
        """Run LDSC heritability estimation as a pipeline step.

        Args:
            session (Session): Gentropy session object.
            summary_statistics_input_path (str): Path to summary statistics parquet.
            study_index_input_path (str): Path to study index parquet.
            ldscore_base_path (str): Base directory for LD score files.
            heritability_output_path (str): Path to write heritability results.
            ldscore_template (str): LD score file template with "{ancestry}" placeholder.
            twostep (float): LDSC twostep parameter.
            n_blocks (int): Number of jackknife blocks.
            intercept (float | None): Optional fixed intercept.
            max_rows_for_collection (int): Maximum allowed joined SNP row count before collection.
            min_samples (int): Minimum allowed sample size for running LDSC.
        """
        self.session = session
        self.results: dict[str, Any] | None = None

        spark = session.spark

        beta_col = "beta"
        se_col = "standardError"
        n_col = "sampleSize"
        chrom_col = "chromosome"
        pos_col = "position"
        study_col = "studyId"

        sumstats_df = (
            spark.read.parquet(summary_statistics_input_path)
            .select(
                study_col,
                "variantId",
                chrom_col,
                pos_col,
                beta_col,
                se_col,
                n_col,
                "effectAlleleFrequencyFromSource",
            )
        )

        study_ids = [r[study_col] for r in sumstats_df.select(study_col).distinct().collect()]
        if len(study_ids) != 1:
            raise ValueError(
                f"Expected one study in summary statistics, got {len(study_ids)}: {study_ids}"
            )

        study_id = study_ids[0]

        study_index_df = spark.read.parquet(study_index_input_path).select(
            "studyId",
            "nSamples",
            "ldPopulationStructure",
            "analysisFlags",
        )

        validation = self._validate_study(
            study_index_df=study_index_df,
            study_id=study_id,
            min_samples=min_samples,
        )

        if validation["run_status"] == "skipped":
            self._write_result_row(
                study_id=study_id,
                heritability_output_path=heritability_output_path,
                run_status="skipped",
                skip_reasons=validation["skip_reasons"],
                analysis_flags=validation["analysis_flags"],
                ld_ancestry=validation["ancestry"],
                result=None,
                m_ldsc=None,
                n_snps_used=None,
            )
            return

        ancestry = validation["ancestry"]
        ldscore_input_path = (
            f"{ldscore_base_path.rstrip('/')}/{ldscore_template.format(ancestry=ancestry)}"
        )

        n_df = study_index_df.select("studyId", "nSamples")

        sumstats = (
            sumstats_df.join(n_df, on=study_col, how="left")
            .withColumn(
                n_col,
                F.when(F.col(n_col).isNull(), F.col("nSamples").cast("double")).otherwise(
                    F.col(n_col).cast("double")
                ),
            )
            .withColumn("variant_parts", F.split(F.col("variantId"), "_"))
            .withColumn("ref", F.col("variant_parts").getItem(2))
            .withColumn("alt", F.col("variant_parts").getItem(3))
            .drop("variant_parts", "nSamples")
            .filter(F.col("effectAlleleFrequencyFromSource") > 0.01)
            .filter(F.col(beta_col).isNotNull())
            .filter(F.col(se_col).isNotNull())
            .filter(F.col(n_col).isNotNull())
            .filter(F.col(se_col) > 0)
            .filter(F.col(chrom_col).isNotNull())
            .filter(F.col(pos_col).isNotNull())
            .filter(F.col("ref").isNotNull())
            .filter(F.col("alt").isNotNull())
            .dropDuplicates([study_col, chrom_col, pos_col, "ref", "alt"])
        )

        ld_df = spark.read.csv(ldscore_input_path, header=True, sep="\t")

        if "BP_hg38" in ld_df.columns:
            ld_df = ld_df.withColumnRenamed("BP_hg38", pos_col)
        else:
            raise ValueError("LD score file must contain 'BP_hg38' column.")

        if "CHR" in ld_df.columns:
            ld_df = ld_df.withColumnRenamed("CHR", chrom_col)
        elif chrom_col not in ld_df.columns:
            raise ValueError("LD score file must contain 'CHR' or 'chromosome' column.")

        required_ld_cols = {chrom_col, pos_col, "ref", "alt", "L2"}
        missing_ld_cols = required_ld_cols - set(ld_df.columns)
        if missing_ld_cols:
            raise ValueError(
                f"LD score file is missing required columns: {sorted(missing_ld_cols)}"
            )

        ld_df = (
            ld_df.select(chrom_col, pos_col, "ref", "alt", "L2")
            .withColumn("L2", F.col("L2").cast("double"))
            .filter(F.col("L2").isNotNull())
            .dropDuplicates([chrom_col, pos_col, "ref", "alt"])
        )

        m_ldsc = float(ld_df.count())

        merged_ld = sumstats.join(
            ld_df,
            on=[chrom_col, pos_col, "ref", "alt"],
            how="inner",
        )

        dedup = merged_ld.dropDuplicates([chrom_col, pos_col, "ref", "alt"])

        n_rows = dedup.count()
        if n_rows == 0:
            self._write_result_row(
                study_id=study_id,
                heritability_output_path=heritability_output_path,
                run_status="skipped",
                skip_reasons=["No overlapping SNPs between summary statistics and LD scores"],
                analysis_flags=validation["analysis_flags"],
                ld_ancestry=ancestry,
                result=None,
                m_ldsc=m_ldsc,
                n_snps_used=0,
            )
            return

        if n_rows > max_rows_for_collection:
            self._write_result_row(
                study_id=study_id,
                heritability_output_path=heritability_output_path,
                run_status="skipped",
                skip_reasons=[
                    f"Too many joined SNPs for collection: {n_rows} > {max_rows_for_collection}"
                ],
                analysis_flags=validation["analysis_flags"],
                ld_ancestry=ancestry,
                result=None,
                m_ldsc=m_ldsc,
                n_snps_used=n_rows,
            )
            return

        rows = dedup.select(beta_col, se_col, n_col, "L2", study_col).collect()

        study_ids_rows = {row[study_col] for row in rows}
        if len(study_ids_rows) != 1:
            raise ValueError(
                f"Expected one study in merged data, got {len(study_ids_rows)}: {study_ids_rows}"
            )

        beta = np.array([row[beta_col] for row in rows], dtype=float)
        se = np.array([row[se_col] for row in rows], dtype=float)
        n_array = np.array([row[n_col] for row in rows], dtype=float)
        ld = np.array([row["L2"] for row in rows], dtype=float)

        w_raw = 1.0 / np.maximum(ld, 1.0)
        w_ld = w_raw / np.mean(w_raw)

        res = run_ldsc_h2_from_arrays(
            beta=beta,
            se=se,
            N=n_array,
            ld=ld,
            w_ld=w_ld,
            M_ldsc_scalar=m_ldsc,
            intercept=intercept,
            twostep=twostep,
            n_blocks=n_blocks,
        )

        self.results = res

        self._write_result_row(
            study_id=study_id,
            heritability_output_path=heritability_output_path,
            run_status="success",
            skip_reasons=[],
            analysis_flags=validation["analysis_flags"],
            ld_ancestry=ancestry,
            result=res,
            m_ldsc=m_ldsc,
            n_snps_used=len(beta),
        )

    def _write_result_row(
        self,
        study_id: str,
        heritability_output_path: str,
        run_status: str,
        skip_reasons: list[str],
        analysis_flags: list[str],
        ld_ancestry: str | None,
        result: dict[str, Any] | None,
        m_ldsc: float | None,
        n_snps_used: int | None,
    ) -> None:
        """Write a single output row for either success or skipped studies.

        Args:
            study_id (str): Study identifier.
            heritability_output_path (str): Output parquet path.
            run_status (str): Run status, for example "success" or "skipped".
            skip_reasons (list[str]): Reasons for skipping.
            analysis_flags (list[str]): Normalised analysis flags.
            ld_ancestry (str | None): Inferred ancestry.
            result (dict[str, Any] | None): LDSC result dictionary, if available.
            m_ldsc (float | None): Number of SNPs in the LD-score universe.
            n_snps_used (int | None): Number of SNPs used in regression.
        """
        out_dict: dict[str, list[Any]] = {
            "studyId": [study_id],
            "runStatus": [run_status],
            "skipReasons": [skip_reasons],
            "analysisFlags": [analysis_flags],
            "ld_ancestry": [ld_ancestry],
            "M_ldsc": [m_ldsc],
            "n_snps_used": [n_snps_used],
            "h2": [result["h2"] if result else None],
            "intercept": [result["intercept"] if result else None],
            "h2_se": [result["h2_se"] if result and "h2_se" in result else None],
            "intercept_se": [
                result["intercept_se"] if result and "intercept_se" in result else None
            ],
            "mean_chisq": [result["mean_chisq"] if result and "mean_chisq" in result else None],
            "lambda_gc": [result["lambda_gc"] if result and "lambda_gc" in result else None],
        }
        schema = StructType([
            StructField("studyId", StringType(), True),
            StructField("runStatus", StringType(), True),
            StructField("skipReasons", ArrayType(StringType()), True),
            StructField("analysisFlags", ArrayType(StringType()), True),
            StructField("ld_ancestry", StringType(), True),
            StructField("M_ldsc", DoubleType(), True),
            StructField("n_snps_used", DoubleType(), True),
            StructField("h2", DoubleType(), True),
            StructField("intercept", DoubleType(), True),
            StructField("h2_se", DoubleType(), True),
            StructField("intercept_se", DoubleType(), True),
            StructField("mean_chisq", DoubleType(), True),
            StructField("lambda_gc", DoubleType(), True),
        ])
        out_pdf = pd.DataFrame(out_dict)
        out_sdf = self.session.spark.createDataFrame(out_pdf, schema=schema)
        out_sdf.write.mode("overwrite").parquet(heritability_output_path)

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
            dict[str, Any]: Validation result with status, reasons, flags, and ancestry.
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
            skip_reasons.append(f"Sample size too small: {n_samples} < {min_samples}")

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
            pop, weight = HeritabilityEstimateStep._extract_population_and_weight(entry)
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
            - only 'metabolite'

        Args:
            analysis_flags (Any): Raw analysis flags.

        Returns:
            bool: Whether the study is allowed.
        """
        flags = HeritabilityEstimateStep._normalise_analysis_flags(analysis_flags)
        if not flags:
            return True

        return set(flags) == {"metabolite"}
