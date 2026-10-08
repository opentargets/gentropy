"""Step to compute the fine-mapping-based polygenic priority score (fmPops) of every gene."""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator

import numpy as np
import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.dataset.fm_pops_score import FmPopsScore
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.external.gcs import copy_from_gcs
from gentropy.method.fm_pops import FmPops

logger = logging.getLogger(__name__)


class FmPopsStep:
    """Compute the fmPops gene-level prior from the credible sets of a release.

    fmPops keeps the model of the polygenic priority score (PoPS; Weeks et al. 2023, Nat Genet
    55:1267-1276) and replaces its MAGMA target, which needs full summary statistics, with one
    computed from credible sets: for every protein-coding gene, the log of one plus the number of
    distinct diseases it is the nearest gene for across all GWAS credible sets. The target is
    regressed on the PoPS gene features with a leave-one-chromosome-out kernel ridge regression,
    see [`FmPops`][gentropy.method.fm_pops.FmPops]. No label from the L2G gold standard enters
    the score.

    Before fitting, an intercept, the 84 PoPS control features and two covariates from the
    target index (log gene length and the log number of protein-coding genes within 500 kb) are
    projected out of the target within each fold. The control features do not enter the kernel.
    Genes in the major histocompatibility complex are scored but kept out of every training
    fold, as PoPS does, because the many immune-mediated diseases mapped there inflate their
    counts.

    Inputs. The PoPS gene features are the `pops_features_full` directory of the FLAMES
    annotation data (Schipper, Zenodo 10.5281/zenodo.12635505, CC BY 4.0): `control.features`,
    `munged_features/pops_features.rows.txt` and the `pops_features.cols.{i}.txt` and
    `pops_features.mat.{i}.npy` chunks. Only genes that are protein coding in the target index
    and present in the feature rows are scored. A `gs://` directory is copied to local disk
    first (about 9 GB).

    Licence. Cite Weeks et al. 2023 and the Zenodo record when using the features. The
    features derive from sources with their own terms: the pathway features include KEGG, which
    restricts commercial use, and the protein-protein interaction features come from InWeb_IM,
    whose commercial terms need checking. The pathway columns are anonymous, so KEGG cannot be
    filtered out. A licence review is needed before fmPops is used in a release.

    Resources. The numerical part runs on the Spark driver: the kernel over about 18,000 genes
    takes 2.7 GB, and each fold holds a copy of its training block and its eigenvectors on top,
    so peak memory is about 10 GB. Give the driver at least 32 GB; 64 GB is safer. One fold per
    chromosome, 22 in all, takes tens of minutes in total.
    """

    MHC_REGION: tuple[str, int, int] = ("6", 28_510_120, 33_480_577)
    """Major histocompatibility complex on GRCh38, as defined by the Genome Reference Consortium."""

    def __init__(
        self,
        session: Session,
        credible_set_path: str,
        study_index_path: str,
        variant_index_path: str,
        target_index_path: str,
        pops_feature_dir: str,
        output_path: str,
        lambda_grid: list[float] | None = None,
    ) -> None:
        """Run the fmPops step.

        Args:
            session (Session): Session object.
            credible_set_path (str): Path to the credible sets.
            study_index_path (str): Path to the study index.
            variant_index_path (str): Path to the variant index, the source of the distances to
                genes.
            target_index_path (str): Path to the target index.
            pops_feature_dir (str): Directory of the extracted PoPS gene features, local or on
                GCS.
            output_path (str): Output path of the `FmPopsScore` dataset.
            lambda_grid (list[float] | None): Ridge penalties searched by generalised
                cross-validation. Defaults to `10^-2` to `10^10` in steps of `10^0.5`.
        """
        study_locus = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        )
        study_index = StudyIndex.from_parquet(
            session, study_index_path, recursiveFileLookup=True
        )
        variant_index = VariantIndex.from_parquet(session, variant_index_path)
        target_index = TargetIndex.from_parquet(
            session, target_index_path, recursiveFileLookup=True
        )

        counts = FmPopsScore.disease_counts_per_nearest_gene(
            study_locus, study_index, variant_index, target_index
        )
        genes = (
            FmPopsScore.gene_covariates(target_index)
            .join(counts, "geneId", "left")
            .withColumn("targetCount", f.coalesce(f.col("targetCount"), f.lit(0)))
            .toPandas()
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            feature_dir = pops_feature_dir
            if pops_feature_dir.startswith("gs://"):
                copy_from_gcs(pops_feature_dir, tmp_dir)
                feature_dir = tmp_dir

            feature_genes = set(FmPops.read_feature_genes(feature_dir))
            genes = (
                genes[genes["geneId"].isin(feature_genes)]
                .sort_values("geneId")
                .reset_index(drop=True)
            )
            gene_ids = genes["geneId"].tolist()
            control_names = FmPops.read_control_feature_names(feature_dir)

            control_columns: list[np.ndarray] = []

            def feature_chunks() -> Iterator[np.ndarray]:
                for columns, chunk in FmPops.read_feature_chunks(feature_dir, gene_ids):
                    is_control = np.array([c in control_names for c in columns])
                    if is_control.any():
                        control_columns.append(chunk[:, is_control])
                    yield chunk[:, ~is_control]

            kernel, n_features = FmPops.accumulate_kernel(feature_chunks())

        controls = (
            FmPops.standardise(np.column_stack(control_columns))
            if control_columns
            else np.empty((len(gene_ids), 0))
        )
        covariates = np.column_stack(
            [controls, genes[["logGeneLength", "logNeighbouringGenes"]].to_numpy()]
        )
        chromosome, start, end = self.MHC_REGION
        in_mhc = (
            (genes["chromosome"] == chromosome) & genes["tss"].between(start, end)
        ).to_numpy()

        fit = FmPops.loco_kernel_ridge(
            kernel,
            np.log1p(genes["targetCount"].to_numpy(dtype=np.float64)),
            genes["chromosome"].to_numpy(dtype=str),
            covariates=covariates,
            fit_mask=~in_mhc,
            lambdas=lambda_grid or FmPops.DEFAULT_LAMBDA_GRID,
        )
        logger.info(
            "fmPops: %d genes, %d features, %d control features, %d genes nearest to at "
            "least one disease, %d MHC genes kept out of the fit.",
            len(gene_ids),
            n_features,
            controls.shape[1],
            int((genes["targetCount"] > 0).sum()),
            int(in_mhc.sum()),
        )
        for held_out, penalty in fit.lambdas.items():
            logger.info("fmPops: chromosome %s, lambda %.3g", held_out, penalty)

        genes["fmPops"] = fit.scores
        scores = FmPopsScore(
            _df=session.spark.createDataFrame(
                genes[["geneId", "chromosome", "targetCount", "fmPops"]].astype(
                    {"targetCount": "int32", "fmPops": "float64"}
                ),
                schema=FmPopsScore.get_schema(),
            ),
            _schema=FmPopsScore.get_schema(),
        )
        scores.df.coalesce(1).write.mode(session.write_mode).parquet(output_path)
