"""Munge step: pre-join each study's sumstats with LD scores once.

Produces a compact partitioned parquet at::

    {munged_output_path}/ancestry=<pop>/studyId=<id>/part-*.parquet

Columns: variantKey (chr_pos_ref_alt), beta, se, n, L2

Running this once before the manifest step makes each pairwise regression cheap:
executors only need to read two small pre-munged files and inner-join them on
variantKey rather than re-reading full sumstats and LD scores for every pair.
"""

from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gentropy.common.session import Session

logger = logging.getLogger(__name__)


class LdscMungeStep:
    """Pre-join each study's sumstats with LD scores, saving compact partitioned parquets.

    The output is written once and reused for all downstream pairwise rg runs.
    Already-processed (ancestry, studyId) partitions are skipped so the step is
    safe to re-run after partial failures.
    """

    def __init__(
        self,
        session: Session,
        manifest_path: str,
        study_index_path: str,
        ldscore_base_path: str,
        munged_output_path: str,
        ldscore_template: str = "gnomad_r2.1.1_{ancestry}_hg38.csv.gz",
        batch_size: int = 50,
    ) -> None:
        """Initialise and run the munge step.

        Args:
            session (Session): Gentropy session object.
            manifest_path (str): Path to the pairs manifest CSV (produced by the
                representative-study selection step).  Must have columns
                ``studyId_1``, ``studyId_2``, ``ancestry``.
            study_index_path (str): Path to the study index parquet.
            ldscore_base_path (str): Base directory for LD score files.
            munged_output_path (str): GCS (or local) path for the munged parquets.
            ldscore_template (str): LD score filename template with ``{ancestry}``
                placeholder.
            batch_size (int): Number of studies to process per Spark job. Each
                batch commits independently so restarts only lose the in-progress
                batch. Defaults to 50.
        """
        self.session = session
        self.manifest_path = manifest_path
        self.study_index_path = study_index_path
        self.ldscore_base_path = ldscore_base_path
        self.munged_output_path = munged_output_path.rstrip("/")
        self.ldscore_template = ldscore_template
        self.batch_size = batch_size

        self.session.spark.conf.set(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )
        self._run()

    # ── public entry point ────────────────────────────────────────────────────

    def _run(self) -> None:
        """Drive the munge pipeline."""
        manifest = pd.read_csv(self.manifest_path)
        study_ancestry = self._unique_studies(manifest)
        already_done = self._already_munged()

        to_process = study_ancestry[
            ~study_ancestry.apply(
                lambda r: (r["studyId"], r["ancestry"]) in already_done, axis=1
            )
        ]

        if to_process.empty:
            logger.info("All studies already munged — nothing to do.")
            return

        logger.info(
            "Munging %d studies (%d already done).",
            len(to_process),
            len(study_ancestry) - len(to_process),
        )

        study_index_df = self._load_study_index()

        for ancestry, group in to_process.groupby("ancestry"):
            study_ids = group["studyId"].tolist()
            sumstats_paths = group["sumstats_path"].tolist()
            n_batches = max(1, len(study_ids) // self.batch_size)
            logger.info("  %s: %d studies in %d batches …", ancestry, len(study_ids), n_batches)

            ld_df = self._load_ld_scores(str(ancestry))
            for i in range(0, len(study_ids), self.batch_size):
                batch_ids = study_ids[i : i + self.batch_size]
                batch_paths = sumstats_paths[i : i + self.batch_size]
                self._munge_batch(batch_ids, batch_paths, str(ancestry), study_index_df, ld_df)
                logger.info(
                    "  %s: batch %d/%d done (%d studies committed).",
                    ancestry, i // self.batch_size + 1, n_batches, len(batch_ids),
                )

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _unique_studies(manifest: pd.DataFrame) -> pd.DataFrame:
        """Return one row per unique (studyId, ancestry, sumstats_path) from the manifest.

        Args:
            manifest (pd.DataFrame): Pairs manifest with studyId_1, studyId_2,
                ancestry, sumstats_path_1, sumstats_path_2 columns.

        Returns:
            pd.DataFrame: Deduplicated table with studyId, ancestry, sumstats_path columns.
        """
        t1 = manifest[["studyId_1", "ancestry", "sumstats_path_1"]].rename(
            columns={"studyId_1": "studyId", "sumstats_path_1": "sumstats_path"}
        )
        t2 = manifest[["studyId_2", "ancestry", "sumstats_path_2"]].rename(
            columns={"studyId_2": "studyId", "sumstats_path_2": "sumstats_path"}
        )
        return pd.concat([t1, t2]).drop_duplicates(["studyId", "ancestry"])

    def _already_munged(self) -> set[tuple[str, str]]:
        """Return (studyId, ancestry) pairs that already have munged data.

        Returns:
            set[tuple[str, str]]: Already-processed (studyId, ancestry) pairs,
                or an empty set if the output path does not yet exist.
        """
        try:
            existing = self.session.spark.read.parquet(self.munged_output_path)
            return {
                (row["studyId"], row["ancestry"])
                for row in existing.select("studyId", "ancestry").distinct().collect()
            }
        except Exception:
            return set()

    def _load_study_index(self) -> DataFrame:
        """Load sample-size columns from the study index parquet.

        Returns:
            DataFrame: Spark DataFrame with studyId, nSamples, nCases, nControls.
        """
        return self.session.spark.read.parquet(self.study_index_path).select(
            "studyId", "nCases", "nControls", "nSamples"
        )

    def _load_ld_scores(self, ancestry: str) -> DataFrame:
        """Load and normalise an LD score file for the given ancestry.

        Args:
            ancestry (str): LD ancestry label (e.g. ``"nfe"``).

        Returns:
            DataFrame: Spark DataFrame with chromosome, position, ref, alt, L2 columns.
        """
        path = (
            f"{self.ldscore_base_path.rstrip('/')}/"
            f"{self.ldscore_template.format(ancestry=ancestry)}"
        )
        ld_df = self.session.spark.read.csv(path, header=True, sep="\t")
        if "BP_hg38" in ld_df.columns:
            ld_df = ld_df.withColumnRenamed("BP_hg38", "position")
        if "CHR" in ld_df.columns:
            ld_df = ld_df.withColumnRenamed("CHR", "chromosome")
        return (
            ld_df.select("chromosome", "position", "ref", "alt", "L2")
            .withColumn("position", F.col("position").cast("int"))
            .withColumn("chromosome", F.col("chromosome").cast("string"))
            .withColumn("L2", F.col("L2").cast("double"))
            .filter(F.col("L2").isNotNull())
            .dropDuplicates(["chromosome", "position", "ref", "alt"])
        )

    def _munge_batch(
        self,
        study_ids: list[str],
        sumstats_paths: list[str],
        ancestry: str,
        study_index_df: DataFrame,
        ld_df: DataFrame,
    ) -> None:
        """Read all sumstats for one ancestry group, join with LD scores, write parquet.

        Args:
            study_ids (list[str]): Study identifiers to process in this batch.
            sumstats_paths (list[str]): GCS paths to harmonised sumstat parquets,
                parallel to ``study_ids``.
            ancestry (str): LD ancestry label written as the partition value.
            study_index_df (DataFrame): Study index with sample-size columns.
            ld_df (DataFrame): LD scores for this ancestry.
        """
        sumstats = self.session.spark.read.parquet(*sumstats_paths).select(
            "studyId", "variantId", "chromosome", "position",
            "beta", "standardError", "sampleSize",
        )

        n_df = study_index_df.select("studyId", "nSamples", "nCases", "nControls")
        neff_expr = (
            4.0
            * F.col("nCases").cast("double")
            * F.col("nControls").cast("double")
            / (F.col("nCases").cast("double") + F.col("nControls").cast("double"))
        )
        fallback_n = F.when(
            F.col("nCases").isNotNull()
            & F.col("nControls").isNotNull()
            & (F.col("nCases") > 0)
            & (F.col("nControls") > 0),
            neff_expr,
        ).otherwise(F.col("nSamples").cast("double"))

        prepared = (
            sumstats.join(n_df, on="studyId", how="left")
            .withColumn(
                "sampleSize",
                F.when(F.col("sampleSize").isNull(), fallback_n).otherwise(
                    F.col("sampleSize").cast("double")
                ),
            )
            .withColumn("_p", F.split(F.col("variantId"), "_"))
            .withColumn("ref", F.col("_p").getItem(2))
            .withColumn("alt", F.col("_p").getItem(3))
            .drop("_p", "variantId", "nSamples", "nCases", "nControls")
            .filter(F.col("beta").isNotNull())
            .filter(F.col("standardError").isNotNull())
            .filter(F.col("sampleSize").isNotNull())
            .filter(F.col("standardError") > 0)
            .filter(F.col("ref").isNotNull())
            .filter(F.col("alt").isNotNull())
            .dropDuplicates(["studyId", "chromosome", "position", "ref", "alt"])
        )

        merged = (
            prepared.join(ld_df, on=["chromosome", "position", "ref", "alt"], how="inner")
            .withColumn(
                "variantKey",
                F.concat_ws(
                    "_",
                    F.col("chromosome"),
                    F.col("position").cast("string"),
                    F.col("ref"),
                    F.col("alt"),
                ),
            )
            .withColumn("ancestry", F.lit(ancestry))
            .select(
                "ancestry",
                "studyId",
                "variantKey",
                F.col("beta"),
                F.col("standardError").alias("se"),
                F.col("sampleSize").alias("n"),
                F.col("L2"),
            )
        )

        # dynamic partition overwrite — only touches partitions being written
        (
            merged.write.mode("overwrite")
            .partitionBy("ancestry", "studyId")
            .parquet(self.munged_output_path)
        )
