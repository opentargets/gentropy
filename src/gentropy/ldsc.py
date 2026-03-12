"""Step to calculate SNP-heritability using LDSC from summary statistics."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

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
        ldscore_template: str = (
            "gnomad_r2.1.1_{ancestry}_hg38.csv.gz"
        ),
        twostep: float = 30.0,
        n_blocks: int = 200,
        intercept: float | None = None,
    ) -> None:
        """Run LDSC heritability estimation as a pipeline step.

        Assumes:
          - summary statistics parquet has columns:
              studyId, chrom, pos, beta, se, sampleSize
          - study index parquet has columns:
              studyId, nSamples, ldPopulationStructure
            where ldPopulationStructure is an array of structs like:
              [{population = "nfe", proportion = 1.0}, ...]
          - LD scores are already:
              * in hg38
              * (optionally) HapMap3-filtered
            and stored at:
              ldscore_base_path / ldscore_template.format(ancestry=...)
            with columns:
              CHR, BP or BP_hg38, L2, ...

        Args:
            session (Session): gentropy Session object.
            summary_statistics_input_path (str): Path to the summary statistics
                parquet.
            study_index_input_path (str): Path to study index parquet containing
                nSamples and ldPopulationStructure.
            ldscore_base_path (str): Base directory for LD score files, for
                example "gs://.../inputs/gnomad/ldscores".
            heritability_output_path (str): Path to write a 1 row parquet with
                LDSC results.
            ldscore_template (str): File name template containing an
                "{ancestry}" placeholder.
            twostep (float): LDSC twostep parameter passed to the heritability
                regression.
            n_blocks (int): Number of jackknife blocks used in LDSC.
            intercept (float | None): Optional fixed intercept for constrained
                LDSC. If None, the intercept is estimated.
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

        sumstats_df = spark.read.parquet(summary_statistics_input_path)
        study_ids = [
            r[study_col] for r in sumstats_df.select(study_col).distinct().collect()
        ]

        if len(study_ids) != 1:
            raise ValueError(
                f"Expected one study in summary statistics, got {len(study_ids)}: {study_ids}"
            )

        study_id = study_ids[0]
        study_index_df = spark.read.parquet(study_index_input_path)

        row = (
            study_index_df.filter(F.col("studyId") == study_id)
            .select("ldPopulationStructure", "analysisFlags")
            .first()
        )

        if row is None:
            raise ValueError(f"studyId {study_id} not found in study index")

        ancestry = self._get_study_ancestry(study_index_df, study_id)

        # Build LDscore path for this ancestry
        # e.g. "gs://.../gnomad.genomes.r2.1.1.nfe.adj.ld_scores.ldscore_hg38_hm3.csv.gz"
        ldscore_input_path = f"{ldscore_base_path.rstrip('/')}/{ldscore_template.format(ancestry=ancestry)}"

        # Join nSamples in, then define sampleSize as existing sampleSize or nSamples
        n_df = study_index_df.select("studyId", "nSamples")

        sumstats = sumstats_df.join(n_df, on="studyId", how="left").withColumn(
            n_col,
            F.when(F.col(n_col).isNull(), F.col("nSamples").cast("double")).otherwise(
                F.col(n_col).cast("double")
            ),
        ).withColumn("variant_parts", F.split(F.col("variantId"), "_")
        ).withColumn("ref", F.col("variant_parts").getItem(2)
        ).withColumn("alt", F.col("variant_parts").getItem(3)
        ).drop("variant_parts").filter(F.col("effectAlleleFrequencyFromSource") > 0.01)

        ld_df = spark.read.csv(ldscore_input_path, header=True, sep="\t")

        if "BP_hg38" in ld_df.columns:
            ld_df = ld_df.withColumnRenamed("BP_hg38", pos_col)
        else:
            raise ValueError("LD score file must contain 'BP_hg38' column.")

        # Harmonise chromosome column
        if "CHR" in ld_df.columns:
            ld_df = ld_df.withColumnRenamed("CHR", chrom_col)
        elif chrom_col not in ld_df.columns:
            raise ValueError("LD score file must contain 'CHR' or 'chromosome' column.")

        ld_df = ld_df.withColumn("L2", F.col("L2").cast("double"))

        merged_ld = sumstats.join(
            ld_df.select(chrom_col, pos_col, "ref", "alt", "L2"),
            on=[chrom_col, pos_col, "ref", "alt"],
            how="inner",
        )

        if "rsID" in merged_ld.columns:
            dedup = merged_ld.dropDuplicates(["rsID"])
        else:
            dedup = merged_ld.dropDuplicates([chrom_col, pos_col])

        pdf = dedup.select(
            beta_col,
            se_col,
            n_col,
            "L2",
            study_col,
        ).toPandas()

        study_ids_pdf = pdf[study_col].unique()
        if len(study_ids_pdf) != 1:
            raise ValueError(
                f"Expected one study in merged data, got {len(study_ids_pdf)}: {study_ids_pdf}"
            )

        beta = pdf[beta_col].values
        se = pdf[se_col].values
        N = pdf[n_col].values
        ld = pdf["L2"].values

        # Heuristic regression weights based on LD scores
        w_raw = 1.0 / np.maximum(ld, 1.0)
        w_ld = w_raw / np.mean(w_raw)

        # Define M_ldsc as the number of SNPs actually used in the regression
        M_ldsc = float(len(beta))

        res = run_ldsc_h2_from_arrays(
            beta=beta,
            se=se,
            N=N,
            ld=ld,
            w_ld=w_ld,
            M_ldsc_scalar=M_ldsc,
            intercept=intercept,
            twostep=twostep,
            n_blocks=n_blocks,
        )

        self.results = res

        out_dict: dict[str, list[Any]] = {
            "studyId": [study_id],
            "ld_ancestry": [ancestry],
            "M_ldsc": [M_ldsc],
            "n_snps_used": [len(beta)],
            "h2": [res["h2"]],
            "intercept": [res["intercept"]],
        }

        for key in ("h2_se", "intercept_se", "mean_chisq", "lambda_gc"):
            if key in res:
                out_dict[key] = [res[key]]

        out_pdf = pd.DataFrame(out_dict)
        out_sdf = spark.createDataFrame(out_pdf)

        out_sdf.write.mode("overwrite").parquet(heritability_output_path)

    # Helper to infer LD ancestry from ldPopulationStructure

    @staticmethod
    def _extract_population_and_weight(entry: Any) -> tuple[str | None, float | None]:
        """Extract population label and weight from a population structure entry.

        Args:
            entry (Any): A Spark Row or dictionary describing a population.

        Returns:
            tuple[str | None, float | None]: Population label and weight if
            available, otherwise (None, None).
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
            weight = weight or entry.get("proportion") or entry.get(
                "relativeSampleSize"
            ) or entry.get("weight")

        try:
            weight = float(weight) if weight is not None else None
        except Exception:
            weight = None

        return (str(pop).strip().lower() if pop else None, weight)

    @staticmethod
    def _infer_ld_ancestry(ld_pop_struct: Any) -> str:
        """Infer canonical LD ancestry from ldPopulationStructure.

        Args:
            ld_pop_struct (Any): Iterable describing LD population structure.

        Returns:
            str: Canonical ancestry label such as "afr", "amr", "eas", "fin" or "nfe".
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
            pop, weight = HeritabilityEstimateStep._extract_population_and_weight(
                entry
            )

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

        return max(agg.items(), key=lambda kv: kv[1])[0]

    def _get_study_ancestry(self, study_index_df: Any, study_id: str) -> str:
        """Get study ancestry after validating study metadata.

        Args:
            study_index_df (Any): Study index DataFrame.
            study_id (str): Study ID to validate and inspect.

        Returns:
            str: Canonical LD ancestry label inferred from ldPopulationStructure.
        """
        row = (
            study_index_df.filter(F.col("studyId") == study_id)
            .select("ldPopulationStructure", "analysisFlags")
            .first()
        )

        if row is None:
            raise ValueError(f"studyId {study_id} not found in study index")

        analysis_flags = row["analysisFlags"]
        if analysis_flags is not None and len(analysis_flags) > 0:
            raise ValueError(
                f"studyId {study_id} excluded from LDSC heritability estimation "
                f"because analysisFlags={analysis_flags}"
            )

        return self._infer_ld_ancestry(row["ldPopulationStructure"])
