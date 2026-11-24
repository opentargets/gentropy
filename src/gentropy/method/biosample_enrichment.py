"""Enrichment of disease genes among the genes specifically expressed in each biosample."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import scipy.sparse
from scipy.stats import norm

from gentropy.common.stats import benjamini_hochberg

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from gentropy.dataset.target_index import TargetIndex


@dataclass(frozen=True)
class SpecificityPanel:
    """One datasource of the baseline expression table at one biosample resolution.

    Attributes:
        datasource_id (str): `datasourceId` of the rows
        biosample_type (Literal["tissue", "celltype", "tissue_celltype"]): Rows with only a
            tissue, only a cell type, or both (a cell type profiled within a tissue)
    """

    datasource_id: str
    biosample_type: Literal["tissue", "celltype", "tissue_celltype"]

    def rows(self: SpecificityPanel) -> Column:
        """Condition selecting the panel's rows of the baseline expression table.

        Returns:
            Column: Boolean condition
        """
        tissue = f.col("tissueBiosampleId").isNotNull()
        celltype = f.col("celltypeBiosampleId").isNotNull()
        resolution = {
            "tissue": tissue & ~celltype,
            "celltype": celltype & ~tissue,
            "tissue_celltype": tissue & celltype,
        }[self.biosample_type]
        return (f.col("datasourceId") == self.datasource_id) & resolution


# The panels of the PTS biosample enrichment step (opentargets/pipeline#225), the CELLEX
# specificity panels of the LDSC cell type analysis, without its cell types within a tissue:
# those are reported by cell type alone, so a cell type enriched through its tissue's signal
# (ambient hepatocyte RNA in the liver, for instance) would admit that cell type's QTLs from
# every tissue. The pan-tissue cell type panel covers the same cell types.
DEFAULT_PANELS = (
    SpecificityPanel("gtex", "tissue"),
    SpecificityPanel("tabula_sapiens", "tissue"),
    SpecificityPanel("tabula_sapiens", "celltype"),
)

_ENRICHMENT_COLUMNS = [
    "diseaseId",
    "heldOutChromosome",
    "datasourceId",
    "tissueBiosampleId",
    "biosampleId",
    "beta",
    "betaError",
    "pValue",
    "pValueAdjusted",
]
_ENRICHMENT_SCHEMA = (
    "diseaseId string, heldOutChromosome string, datasourceId string, "
    "tissueBiosampleId string, biosampleId string, beta double, betaError double, "
    "pValue double, pValueAdjusted double"
)


@dataclass
class _PanelDesign:
    """What a panel's regressions need that does not depend on the disease or held-out chromosome."""

    datasource_id: str
    chromosomes: np.ndarray
    biosamples: pd.DataFrame
    predictors: np.ndarray
    outcomes: scipy.sparse.csr_matrix
    covariates: np.ndarray
    regions: np.ndarray


@dataclass
class ExpressionSpecificity:
    """Expression specificity of every gene in every biosample, from an Open Targets platform release.

    Wraps the release's `baseline_expression` table: one row per gene and biosample of a
    datasource, with the gene's `specificity_score` there. A biosample is a tissue
    (`tissueBiosampleId`, UBERON), a cell type (`celltypeBiosampleId`, CL), or a cell type
    within a tissue, where both are set. Only the rows of `panels` with a specificity score are
    used.

    Attributes:
        baseline_expression (DataFrame): The release's `baseline_expression` table
        panels (tuple[SpecificityPanel, ...]): Datasources and resolutions to test, by default
            GTEx tissues, and Tabula Sapiens tissues and cell types, as in the PTS biosample
            enrichment step without its cell types within a tissue
    """

    baseline_expression: DataFrame
    panels: tuple[SpecificityPanel, ...] = DEFAULT_PANELS

    def specificity(
        self: ExpressionSpecificity, panel: SpecificityPanel
    ) -> pd.DataFrame:
        """Specificity score of every gene in every biosample of one panel.

        `biosampleId` is the cell type for a cell type within a tissue, with the tissue in
        `tissueBiosampleId`, as in the PTS step. When several source ids map to one gene and
        biosample, the highest score is kept.

        Args:
            panel (SpecificityPanel): The panel to read

        Returns:
            pd.DataFrame: `tissueBiosampleId`, `biosampleId`, `geneId` and `specificityScore`
        """
        return (
            self.baseline_expression.filter(
                panel.rows() & f.col("specificity_score").isNotNull()
            )
            .select(
                f.when(
                    f.col("celltypeBiosampleId").isNotNull(),
                    f.col("tissueBiosampleId"),
                ).alias("tissueBiosampleId"),
                f.coalesce("celltypeBiosampleId", "tissueBiosampleId").alias(
                    "biosampleId"
                ),
                f.col("targetId").alias("geneId"),
                f.col("specificity_score").cast("double").alias("specificityScore"),
            )
            .groupBy("tissueBiosampleId", "biosampleId", "geneId")
            .agg(f.max("specificityScore").alias("specificityScore"))
            .toPandas()
        )

    def _design(
        self: ExpressionSpecificity,
        panel: SpecificityPanel,
        genes: pd.DataFrame,
        lists: pd.DataFrame,
        disease_ids: np.ndarray,
        region_window: int,
    ) -> _PanelDesign | None:
        """Genes, biosamples, specificity matrix, outcomes and covariates of one panel.

        Args:
            panel (SpecificityPanel): The panel
            genes (pd.DataFrame): Protein-coding genes with their covariates, from
                `_gene_covariates`
            lists (pd.DataFrame): `diseaseId` and `geneId`, the genes of each disease
            disease_ids (np.ndarray): Sorted disease ids, the columns of the outcomes
            region_window (int): Size, in base pairs, of the windows standard errors cluster on

        Returns:
            _PanelDesign | None: The design, or None when the panel has no scores
        """
        scores = self.specificity(panel)
        universe = genes[genes["geneId"].isin(scores["geneId"])].reset_index(drop=True)
        if universe.empty:
            return None
        gene_index = pd.Series(np.arange(len(universe)), index=universe["geneId"])
        biosamples = (
            scores[["tissueBiosampleId", "biosampleId"]]
            .drop_duplicates()
            .sort_values(["biosampleId", "tissueBiosampleId"], na_position="first")
            .reset_index(drop=True)
        )
        scores = scores[scores["geneId"].isin(gene_index.index)].merge(
            biosamples.reset_index(names="biosampleIndex"),
            on=["tissueBiosampleId", "biosampleId"],
        )
        # A gene without a score in a biosample of the panel counts as 0, as in the PTS step.
        predictors = np.zeros((len(universe), len(biosamples)))
        predictors[
            gene_index[scores["geneId"]].to_numpy(),
            scores["biosampleIndex"].to_numpy(),
        ] = scores["specificityScore"].to_numpy()
        listed = lists[lists["geneId"].isin(gene_index.index)]
        return _PanelDesign(
            datasource_id=panel.datasource_id,
            chromosomes=universe["chromosome"].to_numpy(),
            biosamples=biosamples,
            predictors=predictors,
            outcomes=scipy.sparse.csr_matrix(
                (
                    np.ones(len(listed)),
                    (
                        gene_index[listed["geneId"]].to_numpy(),
                        np.searchsorted(disease_ids, listed["diseaseId"].to_numpy()),
                    ),
                ),
                shape=(len(universe), len(disease_ids)),
            ),
            covariates=np.column_stack(
                [np.ones(len(universe)), universe["logLength"], universe["density"]]
            ),
            regions=(
                universe["chromosome"].astype(str)
                + ":"
                + (universe["tss"] // region_window).astype(str)
            ).to_numpy(),
        )

    def enrichment(
        self: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
        p_value_adjusted_threshold: float = 0.05,
        min_genes: int = 25,
        held_out_chromosomes: list[str] | None = None,
        region_window: int = 1_000_000,
        density_window: int = 1_000_000,
    ) -> DataFrame:
        """Test every biosample for disease genes being more specifically expressed there, leaving one chromosome out at a time.

        The model is the one of the PTS biosample enrichment step (opentargets/pipeline#225),
        with the disease's gene list as the outcome. For each disease and each biosample of a
        panel, a linear regression over the protein-coding genes with a specificity score in
        the panel:

        `y_g = b0 + beta * x_g + c1 * logLength_g + c2 * density_g`

        - `y_g` is 1 if the gene is on the disease's list, else 0.
        - `x_g` is the gene's specificity score in the biosample, 0 when it has none there.
        - `logLength_g` is `log10 |end - start|` and `density_g` the number of TSSs of any
          biotype within `density_window` of the gene's TSS, the gene's own included. Long
          genes in gene-poor regions are more often both the nearest gene of a locus and
          tissue specific, so that alone should not look like enrichment.

        Standard errors are cluster-robust over fixed `region_window` windows of the TSS, so
        that genes picked up by one association count once. The p-value is one-sided,
        `P(Z >= beta / SE)` under a standard normal.

        Each disease is tested once per held-out chromosome, with the genes of that chromosome
        removed from the regression, so that a locus can be scored without its own genes or
        those of any other credible set nearby. Diseases with fewer than `min_genes` listed
        genes among the remaining genes of a panel are not tested in it.

        P-values are adjusted with Benjamini-Hochberg within each disease and held-out
        chromosome over every biosample of every panel the disease was tested in, leaving out
        biosamples with no variance left after the covariates. The regressions run on the
        driver, see `_clustered_regression`.

        Args:
            gene_lists (DataFrame): `diseaseId`, `chromosome` and `geneId`, the genes of each
                disease with the chromosome they are on
            target_index (TargetIndex): Target index, the source of gene positions and lengths
            p_value_adjusted_threshold (float): Only biosamples with an adjusted p-value below
                this are returned
            min_genes (int): Smallest number of listed genes a disease needs to be tested in a
                panel
            held_out_chromosomes (list[str] | None): Chromosomes to hold out; all chromosomes of
                `gene_lists` when None
            region_window (int): Size, in base pairs, of the windows standard errors cluster on
            density_window (int): Distance, in base pairs, from a TSS within which TSSs count
                towards its density

        Returns:
            DataFrame: `diseaseId`, `heldOutChromosome`, `datasourceId`, `tissueBiosampleId`,
                `biosampleId`, `beta`, `betaError`, `pValue` and `pValueAdjusted`, for the
                enriched biosamples only
        """
        spark = gene_lists.sparkSession
        genes = _gene_covariates(target_index, density_window)
        lists = (
            gene_lists.select("diseaseId", "geneId", "chromosome").distinct().toPandas()
        )
        if held_out_chromosomes is None:
            held_out_chromosomes = sorted(lists["chromosome"].dropna().unique())
        lists = lists[["diseaseId", "geneId"]].drop_duplicates()
        disease_ids = np.sort(lists["diseaseId"].unique())
        designs = [
            design
            for panel in self.panels
            if (design := self._design(panel, genes, lists, disease_ids, region_window))
            is not None
        ]
        if not designs:
            return spark.createDataFrame([], _ENRICHMENT_SCHEMA)
        biosamples = pd.concat(
            [d.biosamples.assign(datasourceId=d.datasource_id) for d in designs],
            ignore_index=True,
        )

        results = []
        for chromosome in held_out_chromosomes:
            # diseases x biosamples of every panel, NaN where a disease is not tested.
            fits = []
            for design in designs:
                kept = design.chromosomes != chromosome
                y = design.outcomes[kept]
                tested = np.asarray(y.sum(axis=0)).ravel() >= min_genes
                beta = np.full((len(disease_ids), len(design.biosamples)), np.nan)
                standard_error = np.full_like(beta, np.nan)
                if tested.any():
                    beta[tested], standard_error[tested] = _clustered_regression(
                        design.covariates[kept],
                        design.predictors[kept],
                        y[:, tested].tocsc(),
                        np.unique(design.regions[kept], return_inverse=True)[1].ravel(),
                    )
                fits.append((beta, standard_error))
            beta = np.hstack([fit[0] for fit in fits])
            standard_error = np.hstack([fit[1] for fit in fits])
            with np.errstate(divide="ignore", invalid="ignore"):
                z = beta / standard_error
            p_value = np.where(np.isfinite(z), norm.sf(z), np.nan)
            adjusted = benjamini_hochberg(p_value)
            diseases, columns = np.nonzero(adjusted < p_value_adjusted_threshold)
            results.append(
                biosamples.iloc[columns]
                .reset_index(drop=True)
                .assign(
                    diseaseId=disease_ids[diseases],
                    heldOutChromosome=chromosome,
                    beta=beta[diseases, columns],
                    betaError=standard_error[diseases, columns],
                    pValue=p_value[diseases, columns],
                    pValueAdjusted=adjusted[diseases, columns],
                )
            )
        rows = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
        if rows.empty:
            return spark.createDataFrame([], _ENRICHMENT_SCHEMA)
        rows = rows[_ENRICHMENT_COLUMNS].astype(object)
        return spark.createDataFrame(rows.where(rows.notna(), None), _ENRICHMENT_SCHEMA)


def _gene_covariates(target_index: TargetIndex, density_window: int) -> pd.DataFrame:
    """Position, log length and TSS density of every protein-coding gene with a usable location.

    Args:
        target_index (TargetIndex): Target index
        density_window (int): Distance, in base pairs, from a TSS within which TSSs count

    Returns:
        pd.DataFrame: `geneId`, `chromosome`, `tss`, `logLength` and `density`, one row per
            protein-coding gene, sorted by `geneId`. `density` counts the TSSs of genes of any
            biotype, the gene's own included.
    """
    genes = (
        target_index.df.select(
            f.col("id").alias("geneId"),
            "biotype",
            "tss",
            f.col("genomicLocation.chromosome").alias("chromosome"),
            f.abs(f.col("genomicLocation.end") - f.col("genomicLocation.start")).alias(
                "length"
            ),
        )
        .filter(f.col("tss").isNotNull() & f.col("chromosome").isNotNull())
        .toPandas()
    )
    tss = genes["tss"].to_numpy(dtype=np.int64)
    density = np.empty(len(genes), dtype=np.int64)
    for rows in genes.groupby("chromosome").indices.values():
        ordered = np.sort(tss[rows])
        density[rows] = np.searchsorted(
            ordered, tss[rows] + density_window, side="right"
        ) - np.searchsorted(ordered, tss[rows] - density_window, side="left")
    genes["density"] = density
    coding = genes[(genes["biotype"] == "protein_coding") & (genes["length"] > 0)]
    return (
        coding.assign(
            tss=lambda df: df["tss"].astype(np.int64),
            logLength=lambda df: np.log10(df["length"]),
        )
        .sort_values("geneId")
        .reset_index(drop=True)[["geneId", "chromosome", "tss", "logLength", "density"]]
    )


# Memory for one block of predictors: the largest genes x predictors or (disease, region) x
# predictors array of a block stays below it.
_BLOCK_BYTES = 2**28


def _clustered_regression(
    covariates: np.ndarray,
    membership: scipy.sparse.csc_matrix | np.ndarray,
    outcomes: scipy.sparse.csc_matrix,
    regions: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Slope of every outcome on every predictor, with standard errors clustered by region.

    A predictor is a biosample's specificity scores. The covariates are projected out of each
    predictor once, which gives the slope of the full model (Frisch-Waugh-Lovell). The
    cluster-robust variance of a slope is `sum_c (sum_{g in c} xr_g e_g)^2 / sxx^2`, where `xr` is the projected predictor, `e` the
    residual of the full model and `sxx = sum_g xr_g^2`. The per-region sum is
    `Y_c - D_c - beta * B_c` with `Y_c = sum_{g in c} y_g xr_g`, `D_c = sum_{g in c} xr_g
    (Q Q^T y)_g` and `B_c = sum_{g in c} xr_g^2`. Squaring and summing it over the regions
    splits into terms that depend on the outcome only through `Q^T y`, computed for all
    outcomes at once, and terms in `Y_c`, which is zero outside the regions holding an
    outcome's genes and so is only computed there.

    Args:
        covariates (np.ndarray): genes x covariates, the intercept included
        membership (scipy.sparse.csc_matrix | np.ndarray): genes x predictors
        outcomes (scipy.sparse.csc_matrix): genes x diseases, 1 for listed genes
        regions (np.ndarray): Region index of each gene, from 0

    Returns:
        tuple[np.ndarray, np.ndarray]: Slopes and standard errors, diseases x predictors, NaN
            for predictors with no variance left after the covariates
    """
    n_genes, n_pathways = membership.shape
    n_units = outcomes.shape[1]
    basis = _orthonormal_basis(covariates)
    projected_outcomes = np.asarray((outcomes.T @ basis).T)
    order = np.argsort(regions, kind="stable")
    starts = np.flatnonzero(np.r_[True, np.diff(regions[order]) != 0])
    n_regions = len(starts)

    # One row per disease and region holding at least one of the disease's genes.
    hits = outcomes.tocoo()
    pair_keys, pair_rows = np.unique(
        hits.col.astype(np.int64) * n_regions + regions[hits.row], return_inverse=True
    )
    pair_units, pair_regions = pair_keys // n_regions, pair_keys % n_regions
    pair_genes = scipy.sparse.csr_matrix(
        (hits.data, (pair_rows.ravel(), hits.row)), shape=(len(pair_keys), n_genes)
    )
    unit_pairs = scipy.sparse.csr_matrix(
        (np.ones(len(pair_keys)), (pair_units, np.arange(len(pair_keys)))),
        shape=(n_units, len(pair_keys)),
    )

    beta = np.full((n_units, n_pathways), np.nan)
    standard_error = np.full((n_units, n_pathways), np.nan)
    block = max(1, _BLOCK_BYTES // (8 * max(n_genes, len(pair_keys), n_regions)))
    for start in range(0, n_pathways, block):
        stop = min(start + block, n_pathways)
        x = membership[:, start:stop]
        x = x.toarray() if scipy.sparse.issparse(x) else np.asarray(x, dtype=float)
        xr = x - basis @ (basis.T @ x)
        sxx = (xr**2).sum(axis=0)
        # A pathway the covariates explain leaves rounding noise, not zero: treat it as none.
        valid = sxx > 1e-10 * (x**2).sum(axis=0)
        safe_sxx = np.where(valid, sxx, 1.0)

        region_xx = np.add.reduceat((xr**2)[order], starts, axis=0)
        region_xq = np.stack(
            [
                np.add.reduceat((xr * basis[:, [i]])[order], starts, axis=0)
                for i in range(basis.shape[1])
            ],
            axis=2,
        )
        slope = np.asarray(outcomes.T @ xr) / safe_sxx
        # Terms that depend on the outcome only through its projection on the covariates.
        d_squared = np.einsum(
            "iu,pij,ju->up",
            projected_outcomes,
            np.einsum("cpi,cpj->pij", region_xq, region_xq),
            projected_outcomes,
            optimize=True,
        )
        d_b = projected_outcomes.T @ np.einsum("cpi,cp->ip", region_xq, region_xx)
        b_squared = (region_xx**2).sum(axis=0)
        # Terms in Y_c, over the regions that hold the disease's genes only.
        y_c = np.asarray(pair_genes @ xr)
        d_c = np.zeros_like(y_c)
        for i in range(basis.shape[1]):
            d_c += (
                projected_outcomes[i, pair_units][:, None]
                * region_xq[pair_regions, :, i]
            )
        y_squared = np.asarray(unit_pairs @ y_c**2)
        y_d = np.asarray(unit_pairs @ (y_c * d_c))
        y_b = np.asarray(unit_pairs @ (y_c * region_xx[pair_regions]))

        noise = (
            y_squared
            + d_squared
            + slope**2 * b_squared
            - 2 * y_d
            - 2 * slope * y_b
            + 2 * slope * d_b
        )
        beta[:, start:stop] = np.where(valid, slope, np.nan)
        standard_error[:, start:stop] = np.where(
            valid, np.sqrt(np.maximum(noise, 0.0)) / safe_sxx, np.nan
        )
    return beta, standard_error


def _orthonormal_basis(covariates: np.ndarray) -> np.ndarray:
    """Orthonormal basis of the column space of the covariates.

    A covariate that is constant, or a combination of the others, adds no direction, which a
    plain QR decomposition would not notice.

    Args:
        covariates (np.ndarray): genes x covariates

    Returns:
        np.ndarray: genes x rank
    """
    left, singular_values, _ = np.linalg.svd(covariates, full_matrices=False)
    return left[:, singular_values > singular_values[0] * 1e-10]
