"""Step to prioritise cell types for one GWAS using stratified LD score regression.

This is the lightweight, per-GWAS half of the gentropy re-implementation of the
CELLECT / LDSC ``--h2-cts`` workflow. It consumes the annotation-stratified LD
scores produced once per specificity matrix by
:class:`~gentropy.ldsc_cts.SpecificityLDScoreStep` and, for a single GWAS, runs
a stratified LD score regression per cell type to obtain the focal coefficient,
its standard error and a one-sided enrichment p-value. It is designed to be run
across thousands of GWAS.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.common.session import Session
from gentropy.ldsc import HeritabilityEstimateStep
from gentropy.method.ldsc import run_ldsc_cts_from_arrays
from gentropy.method.ldsc.cell_type_annotation import CONTROL_ANNOTATION

# Column names treated as coordinate keys or metadata (never annotations).
_KEY_AND_META = {
    "variantId",
    "chromosome",
    "position",
    "ref",
    "alt",
    "referenceAllele",
    "alternateAllele",
    "SNP",
    "CM",
    "BP",
    "BP_hg38",
    "CHR",
    "MAF",
}


class CellTypeHeritabilityStep:
    """Prioritise cell types for a single GWAS via stratified LD score regression."""

    def __init__(
        self,
        session: Session,
        summary_statistics_input_path: str,
        study_index_input_path: str,
        annotation_ld_scores_path: str,
        annotation_m_annot_path: str,
        prioritisation_output_path: str,
        ldscore_base_path: str = "",
        ldscore_template: str = "gnomad_r2.1.1_{ancestry}_hg38.csv.gz",
        baseline_ld_scores_path: str | None = None,
        baseline_format: str = "parquet",
        baseline_sep: str = "\t",
        baseline_annotation_columns: list[str] | None = None,
        baseline_base_column: str | None = None,
        baseline_m_path: str | None = None,
        weights_ld_scores_path: str | None = None,
        weights_format: str = "parquet",
        weights_sep: str = "\t",
        weights_column: str = "L2",
        control_annotation: str = CONTROL_ANNOTATION,
        n_blocks: int = 200,
        intercept: float | None = None,
        min_samples: int = 10_000,
        max_rows_for_collection: int = 20_000_000,
    ) -> None:
        """Initialise the cell-type heritability prioritisation step.

        The regression model matches the CELLECT ``--h2-cts`` design of
        ``[baseline..., all_genes_control, cell_type]`` with the cell-type
        annotation as the focal (last) coefficient. The baseline can be either a
        single univariate LD-score column (the default gnomAD file) or a
        CELLECT-style multi-annotation baseline model.

        Args:
            session (Session): Gentropy session object.
            summary_statistics_input_path (str): Path to the summary statistics
                parquet for a single study.
            study_index_input_path (str): Path to the study index parquet.
            annotation_ld_scores_path (str): Path to the annotation LD scores
                produced by :class:`~gentropy.ldsc_cts.SpecificityLDScoreStep`
                (``{output_path}/ld_scores``).
            annotation_m_annot_path (str): Path to the per-annotation ``M`` totals
                (``{output_path}/m_annot``).
            prioritisation_output_path (str): Output parquet path for the
                per-cell-type prioritisation results.
            ldscore_base_path (str): Base directory containing the single-column
                baseline LD score files (the univariate genome-wide LD scores).
                Used only when ``baseline_ld_scores_path`` is not provided.
            ldscore_template (str): Single-column baseline LD score filename
                template with an ``{ancestry}`` placeholder.
            baseline_ld_scores_path (str | None): Optional path to a multi-annotation
                baseline LD-score table (CELLECT-style baseline model). When
                provided it overrides the single-column baseline. The table must be
                keyed by ``variantId`` or by ``chromosome``/``position``/``ref``/``alt``
                and contain one column per baseline annotation.
            baseline_format (str): Format of ``baseline_ld_scores_path``, either
                ``"parquet"`` or ``"csv"``. Defaults to ``"parquet"``.
            baseline_sep (str): Field separator when reading a CSV baseline table.
                Defaults to a tab.
            baseline_annotation_columns (list[str] | None): Explicit list of baseline
                annotation LD-score columns. If ``None`` they are inferred as all
                non-key columns of the baseline table.
            baseline_base_column (str | None): Name of the baseline "base" (all-SNPs
                total LD score) column, used as the regression weight when no
                separate weights file is given. If ``None`` a column named ``base``
                or ``baseL2`` is auto-detected.
            baseline_m_path (str | None): Optional path to a parquet table with
                columns ``annotation`` and ``M`` giving the per-annotation ``M`` for
                the baseline model. If ``None`` each baseline annotation defaults to
                the baseline SNP count.
            weights_ld_scores_path (str | None): Optional path to a separate
                regression-weights LD-score table (the CELLECT ``--w-ld`` weights),
                keyed by ``variantId`` or ``chromosome``/``position``/``ref``/``alt``.
                When provided, SNPs without a weight are dropped.
            weights_format (str): Format of ``weights_ld_scores_path``, either
                ``"parquet"`` or ``"csv"``. Defaults to ``"parquet"``.
            weights_sep (str): Field separator when reading a CSV weights table.
                Defaults to a tab.
            weights_column (str): Name of the LD-score column in the weights table.
                Defaults to ``"L2"``.
            control_annotation (str): Name of the "all genes" control annotation.
                Defaults to :data:`~gentropy.method.ldsc.cell_type_annotation.CONTROL_ANNOTATION`.
            n_blocks (int): Number of block-jackknife blocks. Defaults to ``200``.
            intercept (float | None): Optional fixed LDSC intercept.
            min_samples (int): Minimum allowed sample size to run the regression.
                Defaults to ``10_000``.
            max_rows_for_collection (int): Maximum number of joined SNPs to collect
                to the driver. Defaults to ``20_000_000``.
        """
        self.session = session
        self.summary_statistics_input_path = summary_statistics_input_path
        self.study_index_input_path = study_index_input_path
        self.annotation_ld_scores_path = annotation_ld_scores_path
        self.annotation_m_annot_path = annotation_m_annot_path
        self.prioritisation_output_path = prioritisation_output_path
        self.ldscore_base_path = ldscore_base_path
        self.ldscore_template = ldscore_template
        self.baseline_ld_scores_path = baseline_ld_scores_path
        self.baseline_format = baseline_format
        self.baseline_sep = baseline_sep
        self.baseline_annotation_columns = baseline_annotation_columns
        self.baseline_base_column = baseline_base_column
        self.baseline_m_path = baseline_m_path
        self.weights_ld_scores_path = weights_ld_scores_path
        self.weights_format = weights_format
        self.weights_sep = weights_sep
        self.weights_column = weights_column
        self.control_annotation = control_annotation
        self.n_blocks = n_blocks
        self.intercept = intercept
        self.min_samples = min_samples
        self.max_rows_for_collection = max_rows_for_collection
        self.results: list[dict[str, Any]] | None = None

        self._run()

    def _run(self) -> None:
        """Execute the cell-type prioritisation pipeline."""
        sumstats_df = self._read_sumstats()
        study_id = self._extract_single_study_id(sumstats_df)

        study_index_df = self._read_study_index()
        validation = self._validate_study(
            study_index_df=study_index_df,
            study_id=study_id,
            min_samples=self.min_samples,
        )

        specificity_id, m_annot_map = self._read_m_annot()

        if validation["run_status"] == "skipped":
            self._write_results(
                study_id=study_id,
                specificity_id=specificity_id,
                ld_ancestry=validation["ancestry"],
                analysis_flags=validation["analysis_flags"],
                run_status="skipped",
                skip_reasons=validation["skip_reasons"],
                rows=[],
            )
            return

        ancestry = validation["ancestry"]

        prepared_sumstats = self._prepare_sumstats(sumstats_df, study_index_df)
        (
            baseline_df,
            baseline_key,
            baseline_columns,
            baseline_m,
        ) = self._read_baseline(ancestry)
        weights_df, weights_key = self._read_weights()
        annotation_wide, cell_types = self._read_annotation_wide()

        joined = prepared_sumstats.join(baseline_df, on=baseline_key, how="inner")
        if weights_df is not None and weights_key is not None:
            joined = joined.join(weights_df, on=weights_key, how="inner")
        joined = joined.join(annotation_wide, on="variantId", how="left").dropDuplicates(
            ["chromosome", "position", "ref", "alt"]
        )

        annotation_columns = [self.control_annotation, *cell_types]
        for annotation in annotation_columns:
            joined = joined.withColumn(
                annotation,
                F.coalesce(F.col(annotation), F.lit(0.0)),
            )

        n_rows = joined.count()
        if n_rows == 0 or n_rows > self.max_rows_for_collection:
            skip_reason = (
                "No overlapping SNPs between summary statistics and LD scores"
                if n_rows == 0
                else "Too many joined SNPs for collection"
            )
            self._write_results(
                study_id=study_id,
                specificity_id=specificity_id,
                ld_ancestry=ancestry,
                analysis_flags=validation["analysis_flags"],
                run_status="skipped",
                skip_reasons=[skip_reason],
                rows=[],
            )
            return

        select_columns = [
            "beta",
            "standardError",
            "sampleSize",
            *baseline_columns,
            *annotation_columns,
        ]
        if weights_df is not None:
            select_columns.append("weightLd")
        pdf = joined.select(*select_columns).toPandas()

        rows = self._regress_cell_types(
            pdf=pdf,
            baseline_columns=baseline_columns,
            baseline_m=baseline_m,
            cell_types=cell_types,
            m_annot_map=m_annot_map,
            has_weights=weights_df is not None,
        )
        self.results = rows

        self._write_results(
            study_id=study_id,
            specificity_id=specificity_id,
            ld_ancestry=ancestry,
            analysis_flags=validation["analysis_flags"],
            run_status="success",
            skip_reasons=[],
            rows=rows,
        )

    def _read_sumstats(self) -> DataFrame:
        """Read the input summary statistics parquet.

        Returns:
            DataFrame: Summary statistics with the columns required for regression.
        """
        return self.session.spark.read.parquet(
            self.summary_statistics_input_path
        ).select(
            "studyId",
            "variantId",
            "chromosome",
            "position",
            "beta",
            "standardError",
            "sampleSize",
        )

    def _extract_single_study_id(self, sumstats_df: DataFrame) -> str:
        """Extract and validate that exactly one studyId is present.

        Args:
            sumstats_df (DataFrame): Summary statistics dataframe.

        Returns:
            str: The single study identifier present in the dataframe.

        Raises:
            ValueError: If the dataframe contains zero or multiple study ids.
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
        """Read the study index parquet.

        Returns:
            DataFrame: Study index dataframe with metadata required for validation.
        """
        return self.session.spark.read.parquet(self.study_index_input_path).select(
            "studyId",
            "nCases",
            "nControls",
            "nSamples",
            "ldPopulationStructure",
            "analysisFlags",
        )

    def _validate_study(
        self,
        study_index_df: DataFrame,
        study_id: str,
        min_samples: int,
    ) -> dict[str, Any]:
        """Validate study metadata and decide whether the regression should run.

        Args:
            study_index_df (DataFrame): Study index dataframe.
            study_id (str): Study identifier.
            min_samples (int): Minimum allowed sample size.

        Returns:
            dict[str, Any]: Validation result with status, reasons, flags and
            ancestry.

        Raises:
            ValueError: If ``study_id`` is not present in the study index.
        """
        row = (
            study_index_df.filter(F.col("studyId") == study_id)
            .select("ldPopulationStructure", "analysisFlags", "nSamples")
            .first()
        )
        if row is None:
            raise ValueError(f"studyId {study_id} not found in study index")

        analysis_flags = HeritabilityEstimateStep._normalise_analysis_flags(
            row["analysisFlags"]
        )
        skip_reasons: list[str] = []

        if not HeritabilityEstimateStep._is_allowed_by_analysis_flags(
            row["analysisFlags"]
        ):
            skip_reasons.append("Invalid study design")

        n_samples = row["nSamples"]
        if n_samples is None:
            skip_reasons.append("Sample size missing")
        elif float(n_samples) < float(min_samples):
            skip_reasons.append("Sample size too small")

        ancestry: str | None
        try:
            ancestry = HeritabilityEstimateStep._infer_ld_ancestry(
                row["ldPopulationStructure"]
            )
        except Exception:  # noqa: BLE001 - ancestry inference failure is recoverable
            ancestry = None
            skip_reasons.append("Could not infer ancestry")

        return {
            "run_status": "skipped" if skip_reasons else "success",
            "skip_reasons": skip_reasons,
            "analysis_flags": analysis_flags,
            "ancestry": ancestry,
        }

    def _build_ldscore_input_path(self, ancestry: str) -> str:
        """Build the baseline LD-score file path for a given ancestry.

        Args:
            ancestry (str): Canonical ancestry label used in the LD-score filename.

        Returns:
            str: Fully resolved path to the baseline LD-score file.
        """
        return (
            f"{self.ldscore_base_path.rstrip('/')}/"
            f"{self.ldscore_template.format(ancestry=ancestry)}"
        )

    def _prepare_sumstats(
        self, sumstats_df: DataFrame, study_index_df: DataFrame
    ) -> DataFrame:
        """Prepare summary statistics for regression.

        Fills missing sample sizes from the study index (effective sample size for
        case/control studies), extracts ref/alt alleles from ``variantId``, filters
        invalid rows, and deduplicates variants.

        Args:
            sumstats_df (DataFrame): Raw summary statistics dataframe.
            study_index_df (DataFrame): Study index dataframe with fallback sizes.

        Returns:
            DataFrame: Filtered, deduplicated summary statistics with ``ref`` and
            ``alt`` columns ready for joining with LD scores.
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
                F.when(
                    F.col("sampleSize").isNull(), fallback_n_expr
                ).otherwise(F.col("sampleSize").cast("double")),
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

    def _read_baseline(
        self, ancestry: str
    ) -> tuple[DataFrame, list[str], list[str], dict[str, float]]:
        """Read the baseline LD-score model.

        Dispatches to the multi-annotation baseline reader when
        ``baseline_ld_scores_path`` is set, otherwise to the single-column
        (univariate) gnomAD baseline reader.

        Args:
            ancestry (str): Canonical ancestry label, used only for the
                single-column baseline filename.

        Returns:
            tuple[DataFrame, list[str], list[str], dict[str, float]]: The baseline
            dataframe, the join-key columns, the baseline annotation column names,
            and a mapping from baseline annotation to its ``M`` value.
        """
        if self.baseline_ld_scores_path:
            return self._read_multi_annotation_baseline()
        return self._read_single_column_baseline(ancestry)

    def _read_single_column_baseline(
        self, ancestry: str
    ) -> tuple[DataFrame, list[str], list[str], dict[str, float]]:
        """Read and standardise the single-column (univariate) baseline LD scores.

        Args:
            ancestry (str): Canonical ancestry label used in the LD-score filename.

        Returns:
            tuple[DataFrame, list[str], list[str], dict[str, float]]: The baseline
            dataframe with a single ``baseline`` column, the ``chromosome``/
            ``position``/``ref``/``alt`` join key, the ``["baseline"]`` column list,
            and the baseline ``M`` (the reference SNP count).

        Raises:
            ValueError: If ``ldscore_base_path`` is unset or required columns are
                missing.
        """
        if not self.ldscore_base_path:
            raise ValueError(
                "Provide either 'baseline_ld_scores_path' (multi-annotation baseline) "
                "or 'ldscore_base_path' (single-column baseline)."
            )
        ldscore_input_path = self._build_ldscore_input_path(ancestry)
        ld_df = self.session.spark.read.csv(ldscore_input_path, header=True, sep="\t")

        if "BP_hg38" in ld_df.columns:
            ld_df = ld_df.withColumnRenamed("BP_hg38", "position")
        else:
            raise ValueError("LD score file must contain 'BP_hg38' column.")

        if "CHR" in ld_df.columns:
            ld_df = ld_df.withColumnRenamed("CHR", "chromosome")
        elif "chromosome" not in ld_df.columns:
            raise ValueError("LD score file must contain 'CHR' or 'chromosome' column.")

        required_ld_cols = {"chromosome", "position", "ref", "alt", "L2"}
        missing_ld_cols = required_ld_cols - set(ld_df.columns)
        if missing_ld_cols:
            raise ValueError(
                f"LD score file is missing required columns: {sorted(missing_ld_cols)}"
            )

        baseline_df = (
            ld_df.select("chromosome", "position", "ref", "alt", "L2")
            .withColumn("position", F.col("position").cast("long"))
            .withColumn("chromosome", F.col("chromosome").cast("string"))
            .withColumn("baseline", F.col("L2").cast("double"))
            .drop("L2")
            .filter(F.col("baseline").isNotNull())
            .dropDuplicates(["chromosome", "position", "ref", "alt"])
        )
        m_baseline = float(baseline_df.count())
        key = ["chromosome", "position", "ref", "alt"]
        return baseline_df, key, ["baseline"], {"baseline": m_baseline}

    def _read_multi_annotation_baseline(
        self,
    ) -> tuple[DataFrame, list[str], list[str], dict[str, float]]:
        """Read a CELLECT-style multi-annotation baseline LD-score table.

        Returns:
            tuple[DataFrame, list[str], list[str], dict[str, float]]: The baseline
            dataframe, its join-key columns, the baseline annotation column names,
            and a mapping from baseline annotation to its ``M`` value.

        Raises:
            ValueError: If the baseline table has no usable join key or no
                annotation columns.
        """
        raw = self._read_table(
            self.baseline_ld_scores_path,  # type: ignore[arg-type]
            self.baseline_format,
            self.baseline_sep,
        )
        key = self._detect_join_key(raw)
        raw = self._standardise_key(raw, key)

        if self.baseline_annotation_columns is not None:
            baseline_columns = list(self.baseline_annotation_columns)
        else:
            baseline_columns = [c for c in raw.columns if c not in _KEY_AND_META]
        if not baseline_columns:
            raise ValueError(
                "No baseline annotation columns found; provide "
                "'baseline_annotation_columns' explicitly."
            )

        selected = raw.select(
            *key,
            *[F.col(c).cast("double").alias(c) for c in baseline_columns],
        ).dropDuplicates(key)

        default_m = float(selected.count())
        baseline_m = self._read_baseline_m(baseline_columns, default_m)
        return selected, key, baseline_columns, baseline_m

    def _read_baseline_m(
        self, baseline_columns: list[str], default_m: float
    ) -> dict[str, float]:
        """Read per-annotation ``M`` values for the baseline model.

        Args:
            baseline_columns (list[str]): Baseline annotation column names.
            default_m (float): Fallback ``M`` when a value is not supplied.

        Returns:
            dict[str, float]: Mapping from baseline annotation to its ``M`` value.
        """
        if not self.baseline_m_path:
            return dict.fromkeys(baseline_columns, default_m)
        m_df = self.session.spark.read.parquet(self.baseline_m_path)
        mapping = {row["annotation"]: float(row["M"]) for row in m_df.collect()}
        return {c: mapping.get(c, default_m) for c in baseline_columns}

    def _read_weights(self) -> tuple[DataFrame | None, list[str] | None]:
        """Read the optional separate regression-weights LD-score table.

        Returns:
            tuple[DataFrame | None, list[str] | None]: The weights dataframe with a
            ``weightLd`` column and its join key, or ``(None, None)`` if no weights
            file is configured.

        Raises:
            ValueError: If the weights table is missing the configured LD-score
                column.
        """
        if not self.weights_ld_scores_path:
            return None, None
        raw = self._read_table(
            self.weights_ld_scores_path,
            self.weights_format,
            self.weights_sep,
        )
        if self.weights_column not in raw.columns:
            raise ValueError(
                f"Weights file is missing the '{self.weights_column}' column."
            )
        key = self._detect_join_key(raw)
        raw = self._standardise_key(raw, key)
        weights_df = (
            raw.select(*key, F.col(self.weights_column).cast("double").alias("weightLd"))
            .filter(F.col("weightLd").isNotNull())
            .dropDuplicates(key)
        )
        return weights_df, key

    def _read_table(self, path: str, fmt: str, sep: str) -> DataFrame:
        """Read a parquet or CSV table.

        Args:
            path (str): Path to the table.
            fmt (str): Either ``"parquet"`` or ``"csv"``.
            sep (str): Field separator for CSV input.

        Returns:
            DataFrame: The table as read from disk.

        Raises:
            ValueError: If ``fmt`` is not ``"parquet"`` or ``"csv"``.
        """
        fmt_lower = fmt.lower()
        if fmt_lower == "parquet":
            return self.session.spark.read.parquet(path)
        if fmt_lower == "csv":
            return self.session.spark.read.csv(
                path, header=True, sep=sep, inferSchema=True
            )
        raise ValueError(f"Unsupported format '{fmt}'; expected 'parquet' or 'csv'.")

    @staticmethod
    def _detect_join_key(df: DataFrame) -> list[str]:
        """Detect the join key of an LD-score table.

        Args:
            df (DataFrame): The table to inspect.

        Returns:
            list[str]: ``["variantId"]`` if present, otherwise the
            ``chromosome``/``position``/``ref``/``alt`` key.

        Raises:
            ValueError: If neither key is available.
        """
        columns = set(df.columns)
        if "variantId" in columns:
            return ["variantId"]
        if {"chromosome", "position", "ref", "alt"}.issubset(columns):
            return ["chromosome", "position", "ref", "alt"]
        raise ValueError(
            "LD-score table must contain 'variantId' or "
            "'chromosome'/'position'/'ref'/'alt' columns."
        )

    @staticmethod
    def _standardise_key(df: DataFrame, key: list[str]) -> DataFrame:
        """Cast the coordinate join key to consistent types.

        Args:
            df (DataFrame): The table to standardise.
            key (list[str]): The join-key columns.

        Returns:
            DataFrame: The table with ``chromosome`` cast to string and
            ``position`` cast to long when the coordinate key is used.
        """
        if key == ["chromosome", "position", "ref", "alt"]:
            return df.withColumn(
                "chromosome", F.col("chromosome").cast("string")
            ).withColumn("position", F.col("position").cast("long"))
        return df

    def _resolve_base_column(self, baseline_columns: list[str]) -> str | None:
        """Resolve the baseline "base" (total-LD) column used as a weight fallback.

        Args:
            baseline_columns (list[str]): Baseline annotation column names.

        Returns:
            str | None: The configured or auto-detected base column, or ``None`` if
            none is available.
        """
        if self.baseline_base_column:
            return self.baseline_base_column
        for column in baseline_columns:
            if column.lower() in {"base", "basel2"}:
                return column
        return None

    def _read_m_annot(self) -> tuple[str | None, dict[str, float]]:
        """Read the per-annotation ``M`` totals.

        Returns:
            tuple[str | None, dict[str, float]]: The specificity identifier (or
            ``None`` if not uniquely defined) and a mapping from annotation name to
            its ``M`` value.
        """
        m_annot_df = self.session.spark.read.parquet(self.annotation_m_annot_path)
        rows = m_annot_df.collect()
        m_annot_map = {row["annotation"]: float(row["M"]) for row in rows}

        specificity_ids = (
            {row["specificityId"] for row in rows}
            if "specificityId" in m_annot_df.columns
            else set()
        )
        specificity_id = specificity_ids.pop() if len(specificity_ids) == 1 else None
        return specificity_id, m_annot_map

    def _read_annotation_wide(self) -> tuple[DataFrame, list[str]]:
        """Read the annotation LD scores and pivot them to wide format.

        Returns:
            tuple[DataFrame, list[str]]: A wide dataframe keyed by ``variantId`` with
            one LD-score column per annotation (including the control annotation),
            and the sorted list of cell-type annotation names (excluding control).
        """
        ld_scores_long = self.session.spark.read.parquet(
            self.annotation_ld_scores_path
        ).select("variantId", "annotation", "ldScore")

        annotations = sorted(
            row["annotation"]
            for row in ld_scores_long.select("annotation").distinct().collect()
        )
        cell_types = [a for a in annotations if a != self.control_annotation]

        annotation_wide = (
            ld_scores_long.groupBy("variantId")
            .pivot("annotation", annotations)
            .agg(F.first("ldScore"))
        )
        return annotation_wide, cell_types

    def _regress_cell_types(
        self,
        pdf: pd.DataFrame,
        baseline_columns: list[str],
        baseline_m: dict[str, float],
        cell_types: list[str],
        m_annot_map: dict[str, float],
        has_weights: bool,
    ) -> list[dict[str, Any]]:
        """Run one stratified LD score regression per cell type.

        The design matrix is ``[baseline..., control, cell_type]`` with the
        cell-type annotation as the focal (last) coefficient, matching the CELLECT
        ``--h2-cts`` model.

        Args:
            pdf (pd.DataFrame): Collected per-SNP data with columns ``beta``,
                ``standardError``, ``sampleSize``, the baseline annotation columns,
                the control annotation, one column per cell type, and optionally
                ``weightLd``.
            baseline_columns (list[str]): Baseline annotation column names.
            baseline_m (dict[str, float]): Mapping from baseline annotation to ``M``.
            cell_types (list[str]): Cell-type annotation names.
            m_annot_map (dict[str, float]): Mapping from annotation name to ``M``.
            has_weights (bool): Whether a separate ``weightLd`` column is present.

        Returns:
            list[dict[str, Any]]: One result dictionary per cell type.
        """
        beta = pdf["beta"].to_numpy(dtype=float)
        se = pdf["standardError"].to_numpy(dtype=float)
        n_array = pdf["sampleSize"].to_numpy(dtype=float)
        baseline_matrix = pdf[baseline_columns].to_numpy(dtype=float)
        control = pdf[self.control_annotation].to_numpy(dtype=float)
        w_ld = self._resolve_weights(pdf, baseline_columns, has_weights)

        m_control = m_annot_map.get(self.control_annotation, float(len(control)))
        baseline_m_values = [baseline_m[column] for column in baseline_columns]

        rows: list[dict[str, Any]] = []
        for cell_type in cell_types:
            focal = pdf[cell_type].to_numpy(dtype=float)
            ref_ld = np.column_stack([baseline_matrix, control, focal])
            m_annot = np.array(
                [
                    *baseline_m_values,
                    m_control,
                    m_annot_map.get(cell_type, float("nan")),
                ],
                dtype=float,
            )
            try:
                result = run_ldsc_cts_from_arrays(
                    beta=beta,
                    se=se,
                    N=n_array,
                    ref_ld=ref_ld,
                    w_ld=w_ld,
                    M_annot=m_annot,
                    focal_index=-1,
                    intercept=self.intercept,
                    n_blocks=self.n_blocks,
                )
            except Exception as exc:  # noqa: BLE001 - record failure per cell type
                rows.append(
                    {
                        "cellType": cell_type,
                        "coefficient": None,
                        "coefficient_se": None,
                        "coefficient_z": None,
                        "pvalue": None,
                        "h2": None,
                        "intercept": None,
                        "mean_chisq": None,
                        "lambda_gc": None,
                        "n_snps_used": int(len(beta)),
                        "cellTypeStatus": f"failed: {exc}",
                    }
                )
                continue
            rows.append(
                {
                    "cellType": cell_type,
                    "coefficient": result["coefficient"],
                    "coefficient_se": result["coefficient_se"],
                    "coefficient_z": result["coefficient_z"],
                    "pvalue": result["coefficient_p_value"],
                    "h2": result["h2"],
                    "intercept": result["intercept"],
                    "mean_chisq": result["mean_chisq"],
                    "lambda_gc": result["lambda_gc"],
                    "n_snps_used": int(result["n_snps"]),
                    "cellTypeStatus": "success",
                }
            )
        return rows

    def _resolve_weights(
        self,
        pdf: pd.DataFrame,
        baseline_columns: list[str],
        has_weights: bool,
    ) -> np.ndarray:
        """Resolve the per-SNP regression-weight LD score.

        Priority: an explicit ``weightLd`` column, then the baseline "base"
        (total-LD) column, then the sum of the baseline annotation columns.

        Args:
            pdf (pd.DataFrame): Collected per-SNP data.
            baseline_columns (list[str]): Baseline annotation column names.
            has_weights (bool): Whether a separate ``weightLd`` column is present.

        Returns:
            np.ndarray: Per-SNP regression-weight LD scores.
        """
        if has_weights:
            return pdf["weightLd"].to_numpy(dtype=float)
        base_column = self._resolve_base_column(baseline_columns)
        if base_column is not None and base_column in pdf.columns:
            return pdf[base_column].to_numpy(dtype=float)
        return pdf[baseline_columns].sum(axis=1).to_numpy(dtype=float)

    def _write_results(
        self,
        study_id: str,
        specificity_id: str | None,
        ld_ancestry: str | None,
        analysis_flags: list[str],
        run_status: str,
        skip_reasons: list[str],
        rows: list[dict[str, Any]],
    ) -> None:
        """Write the per-cell-type prioritisation results to parquet.

        When ``rows`` is empty (skipped study) a single summary row is written with
        null coefficients so that skipped studies remain traceable in the output.

        Args:
            study_id (str): Study identifier.
            specificity_id (str | None): Specificity matrix identifier.
            ld_ancestry (str | None): Inferred LD ancestry.
            analysis_flags (list[str]): Normalised analysis flags.
            run_status (str): Overall run status, ``"success"`` or ``"skipped"``.
            skip_reasons (list[str]): Reasons for skipping, if any.
            rows (list[dict[str, Any]]): Per-cell-type result dictionaries.
        """
        schema = StructType(
            [
                StructField("studyId", StringType(), True),
                StructField("specificityId", StringType(), True),
                StructField("cellType", StringType(), True),
                StructField("runStatus", StringType(), True),
                StructField("cellTypeStatus", StringType(), True),
                StructField("skipReasons", ArrayType(StringType()), True),
                StructField("analysisFlags", ArrayType(StringType()), True),
                StructField("ld_ancestry", StringType(), True),
                StructField("coefficient", DoubleType(), True),
                StructField("coefficient_se", DoubleType(), True),
                StructField("coefficient_z", DoubleType(), True),
                StructField("pvalue", DoubleType(), True),
                StructField("h2", DoubleType(), True),
                StructField("intercept", DoubleType(), True),
                StructField("mean_chisq", DoubleType(), True),
                StructField("lambda_gc", DoubleType(), True),
                StructField("n_snps_used", LongType(), True),
            ]
        )

        if rows:
            records = [
                {
                    "studyId": study_id,
                    "specificityId": specificity_id,
                    "cellType": row["cellType"],
                    "runStatus": run_status,
                    "cellTypeStatus": row["cellTypeStatus"],
                    "skipReasons": skip_reasons,
                    "analysisFlags": analysis_flags,
                    "ld_ancestry": ld_ancestry,
                    "coefficient": _to_float_or_none(row["coefficient"]),
                    "coefficient_se": _to_float_or_none(row["coefficient_se"]),
                    "coefficient_z": _to_float_or_none(row["coefficient_z"]),
                    "pvalue": _to_float_or_none(row["pvalue"]),
                    "h2": _to_float_or_none(row["h2"]),
                    "intercept": _to_float_or_none(row["intercept"]),
                    "mean_chisq": _to_float_or_none(row["mean_chisq"]),
                    "lambda_gc": _to_float_or_none(row["lambda_gc"]),
                    "n_snps_used": row["n_snps_used"],
                }
                for row in rows
            ]
        else:
            records = [
                {
                    "studyId": study_id,
                    "specificityId": specificity_id,
                    "cellType": None,
                    "runStatus": run_status,
                    "cellTypeStatus": None,
                    "skipReasons": skip_reasons,
                    "analysisFlags": analysis_flags,
                    "ld_ancestry": ld_ancestry,
                    "coefficient": None,
                    "coefficient_se": None,
                    "coefficient_z": None,
                    "pvalue": None,
                    "h2": None,
                    "intercept": None,
                    "mean_chisq": None,
                    "lambda_gc": None,
                    "n_snps_used": None,
                }
            ]

        out_pdf = pd.DataFrame(records)
        out_sdf = self.session.spark.createDataFrame(out_pdf, schema=schema)
        out_sdf.write.mode("overwrite").parquet(self.prioritisation_output_path)


def _to_float_or_none(value: Any) -> float | None:
    """Convert a value to float, mapping NaN and None to ``None``.

    Args:
        value (Any): Value to convert.

    Returns:
        float | None: The float value, or ``None`` if the value is missing or NaN.
    """
    if value is None:
        return None
    float_value = float(value)
    if np.isnan(float_value):
        return None
    return float_value
