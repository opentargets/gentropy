"""Enrichment of disease genes among the genes specifically expressed in each biosample."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import scipy.sparse
from scipy.stats import norm

from gentropy.method.pathway_enrichment import (
    benjamini_hochberg,
    clustered_regression,
    gene_covariates,
)

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


# The panels of the PTS biosample enrichment step (opentargets/pipeline#225).
DEFAULT_PANELS = (
    SpecificityPanel("gtex", "tissue"),
    SpecificityPanel("tabula_sapiens", "tissue"),
    SpecificityPanel("tabula_sapiens", "celltype"),
    SpecificityPanel("DICE", "celltype"),
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
            those of the PTS biosample enrichment step: GTEx tissues, Tabula Sapiens tissues and
            cell types, and DICE cell types
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
                `gene_covariates`
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
        driver, see `gentropy.method.pathway_enrichment.clustered_regression`.

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
        genes = gene_covariates(target_index, density_window)
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
            if (
                design := self._design(
                    panel, genes, lists, disease_ids, region_window
                )
            )
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
                    beta[tested], standard_error[tested] = clustered_regression(
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
