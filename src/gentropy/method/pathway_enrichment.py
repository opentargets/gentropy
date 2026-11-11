"""Pathway gene sets from a platform release and their enrichment among disease genes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import scipy.sparse
from pyspark.sql import Window
from scipy.stats import norm

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from gentropy.dataset.target_index import TargetIndex


@dataclass
class PathwayLibrary:
    """GO biological process and Reactome gene sets built from an Open Targets platform release.

    The target index only carries the terms a gene is annotated to directly, so a gene sits in
    almost none of the broader processes and pathways above them. Both ontologies are therefore
    propagated up their hierarchies (the true-path rule): a gene annotated to a term belongs to
    every ancestor of the term as well.

    - GO: biological process annotations (`aspect` "P") from `target.go`, propagated over the
      `isA` and `partOf` relations of the release's `go` table. The `ancestors` column of that
      table also follows the `regulates` relations and is not used. Annotations with an evidence
      code in `excluded_go_evidence`, by default only the electronic ones (IEA), are left out.
      Obsolete terms are dropped.
    - Reactome: `target.pathways`, propagated with the `ancestors` of the release's `reactome`
      table.

    Against the GMT library the previous pathway features were built on (Enrichr GO Biological
    Process and Reactome, 26.09 release), this reproduces the GO sets with a median Jaccard
    index of 0.92 and the Reactome sets with a median of 1.00. MSigDB Hallmark sets are not
    part of the platform and have no equivalent here.

    Attributes:
        go (DataFrame): The release's `go` table
        reactome (DataFrame): The release's `reactome` table
        excluded_go_evidence (tuple[str, ...]): GO evidence codes whose annotations are left out
    """

    go: DataFrame
    reactome: DataFrame
    excluded_go_evidence: tuple[str, ...] = ("IEA",)

    def go_term_ancestors(self: PathwayLibrary) -> DataFrame:
        """Map every biological process term to itself and each of its ancestors.

        The closure is computed on the driver: there are about 30,000 non-obsolete biological
        process terms. Alternative identifiers of a term map to the same ancestors as the term.

        Returns:
            DataFrame: `termId` and `pathwayId`, one row per term and ancestor, the term
                included
        """
        terms = (
            self.go.filter(
                (f.col("namespace") == "biological_process")
                & ~f.coalesce(f.col("isObsolete"), f.lit(False))
            )
            .select("id", "altIds", "isA", "partOf")
            .collect()
        )
        parents = {term.id: {*(term.isA or []), *(term.partOf or [])} for term in terms}
        ancestors: dict[str, set[str]] = {}

        def resolve(term_id: str) -> set[str]:
            if term_id not in ancestors:
                ancestors[term_id] = {term_id}.union(
                    *(
                        resolve(parent)
                        for parent in parents[term_id]
                        if parent in parents
                    )
                )
            return ancestors[term_id]

        rows = [
            (alias, ancestor)
            for term in terms
            for alias in (term.id, *(term.altIds or []))
            for ancestor in resolve(term.id)
        ]
        return self.go.sparkSession.createDataFrame(
            rows, "termId string, pathwayId string"
        ).distinct()

    def gene_sets(
        self: PathwayLibrary,
        target_index: TargetIndex,
        min_size: int = 5,
        max_size: int = 4000,
    ) -> DataFrame:
        """Protein-coding members of every pathway whose size falls within the limits.

        Args:
            target_index (TargetIndex): Target index, the source of the annotations
            min_size (int): Smallest number of member genes a pathway may have
            max_size (int): Largest number of member genes a pathway may have

        Returns:
            DataFrame: `pathwayId` and `geneId`, one row per pathway and member gene
        """
        genes = target_index.df.filter(f.col("biotype") == "protein_coding").select(
            f.col("id").alias("geneId"), "go", "pathways"
        )
        go_sets = (
            genes.select("geneId", f.explode("go").alias("annotation"))
            .filter(
                (f.col("annotation.aspect") == "P")
                & ~f.coalesce(
                    f.col("annotation.evidence").isin(list(self.excluded_go_evidence)),
                    f.lit(False),
                )
            )
            .select("geneId", f.col("annotation.id").alias("termId"))
            .join(self.go_term_ancestors(), "termId", "inner")
        )
        reactome_sets = genes.select(
            "geneId", f.explode("pathways.pathwayId").alias("termId")
        ).join(
            self.reactome.select(
                f.col("id").alias("termId"),
                f.explode(
                    f.array_union(
                        f.array(f.col("id")),
                        f.coalesce(f.col("ancestors"), f.array().cast("array<string>")),
                    )
                ).alias("pathwayId"),
            ),
            "termId",
            "inner",
        )
        pathway_size = f.count("geneId").over(Window.partitionBy("pathwayId"))
        return (
            go_sets.select("pathwayId", "geneId")
            .unionByName(reactome_sets.select("pathwayId", "geneId"))
            .distinct()
            .withColumn("pathwaySize", pathway_size)
            .filter(f.col("pathwaySize").between(min_size, max_size))
            .drop("pathwaySize")
        )

    @staticmethod
    def regression(
        gene_lists: DataFrame,
        gene_sets: DataFrame,
        target_index: TargetIndex,
        p_value_adjusted_threshold: float = 0.05,
        min_genes: int = 25,
        held_out_chromosomes: list[str] | None = None,
        region_window: int = 1_000_000,
        density_window: int = 1_000_000,
    ) -> DataFrame:
        """Test every pathway for enrichment among the genes of each disease, leaving one chromosome out at a time.

        For each disease and pathway, a linear regression over protein-coding genes:

        `y_g = b0 + beta * x_g + c1 * logLength_g + c2 * density_g + c3 * log(1 + nPathways_g)`

        - `y_g` is 1 if the gene is on the disease's list, else 0.
        - `x_g` is 1 if the gene is a member of the pathway, else 0.
        - `logLength_g` is `log10 |end - start|`, `density_g` the number of TSSs of any biotype
          within `density_window` of the gene's TSS, the gene's own included, and `nPathways_g`
          the number of pathways of `gene_sets` the gene belongs to. Long genes in gene-poor
          regions are more often the nearest gene of a locus, and genes in many pathways are
          better studied, so neither alone should look like enrichment.

        Standard errors are cluster-robust over fixed `region_window` windows of the TSS, so
        that genes picked up by one association count once. The p-value is one-sided,
        `P(Z >= beta / SE)` under a standard normal, and is adjusted with Benjamini-Hochberg
        within each disease and held-out chromosome over every pathway with variance left after
        the covariates.

        Each disease is tested once per held-out chromosome, with the genes of that chromosome
        removed from the regression. A locus is then scored with the results that left its own
        chromosome out, so neither its genes nor those of any other credible set nearby take
        part in the test. Diseases with fewer than `min_genes` listed genes among the remaining
        genes are not tested.

        The regressions run on the driver. Covariates are projected out once per held-out
        chromosome and reused for every disease and pathway (Frisch-Waugh-Lovell), and the
        cluster-robust variance is expanded so that only the clusters holding a disease's genes
        are visited per disease.

        Args:
            gene_lists (DataFrame): `diseaseId`, `chromosome` and `geneId`, the genes of each
                disease with the chromosome they are on
            gene_sets (DataFrame): `pathwayId` and `geneId`, as returned by `gene_sets`
            target_index (TargetIndex): Target index, the source of gene positions and lengths
            p_value_adjusted_threshold (float): Only pathways with an adjusted p-value below this
                are returned
            min_genes (int): Smallest number of listed genes a disease needs to be tested
            held_out_chromosomes (list[str] | None): Chromosomes to hold out; all chromosomes of
                `gene_lists` when None
            region_window (int): Size, in base pairs, of the windows standard errors cluster on
            density_window (int): Distance, in base pairs, from a TSS within which TSSs count
                towards its density

        Returns:
            DataFrame: `diseaseId`, `heldOutChromosome`, `pathwayId`, `beta`, `betaError`,
                `pValue` and `pValueAdjusted`, for the enriched pathways only
        """
        spark = gene_lists.sparkSession
        genes = gene_covariates(target_index, density_window)
        gene_index = pd.Series(np.arange(len(genes)), index=genes["geneId"])

        sets = gene_sets.select("pathwayId", "geneId").distinct().toPandas()
        sets = sets[sets["geneId"].isin(gene_index.index)]
        pathway_ids = np.sort(sets["pathwayId"].unique())
        membership = scipy.sparse.csc_matrix(
            (
                np.ones(len(sets)),
                (
                    gene_index[sets["geneId"]].to_numpy(),
                    np.searchsorted(pathway_ids, sets["pathwayId"].to_numpy()),
                ),
            ),
            shape=(len(genes), len(pathway_ids)),
        )
        covariates = np.column_stack(
            [
                np.ones(len(genes)),
                genes["logLength"],
                genes["density"],
                np.log1p(np.asarray(membership.sum(axis=1)).ravel()),
            ]
        )
        regions = (
            genes["chromosome"].astype(str)
            + ":"
            + (genes["tss"] // region_window).astype(str)
        ).to_numpy()

        lists = (
            gene_lists.select("diseaseId", "geneId", "chromosome").distinct().toPandas()
        )
        if held_out_chromosomes is None:
            held_out_chromosomes = sorted(lists["chromosome"].dropna().unique())
        lists = lists[lists["geneId"].isin(gene_index.index)][["diseaseId", "geneId"]]
        lists = lists.drop_duplicates()
        disease_ids = np.sort(lists["diseaseId"].unique())
        outcomes = scipy.sparse.csr_matrix(
            (
                np.ones(len(lists)),
                (
                    gene_index[lists["geneId"]].to_numpy(),
                    np.searchsorted(disease_ids, lists["diseaseId"].to_numpy()),
                ),
            ),
            shape=(len(genes), len(disease_ids)),
        )

        results = []
        for chromosome in held_out_chromosomes:
            kept = (genes["chromosome"] != chromosome).to_numpy()
            y = outcomes[kept]
            tested = np.asarray(y.sum(axis=0)).ravel() >= min_genes
            if not tested.any():
                continue
            beta, standard_error = clustered_regression(
                covariates[kept],
                membership[kept],
                y[:, tested].tocsc(),
                np.unique(regions[kept], return_inverse=True)[1].ravel(),
            )
            with np.errstate(divide="ignore", invalid="ignore"):
                z = beta / standard_error
            p_value = np.where(np.isfinite(z), norm.sf(z), np.nan)
            adjusted = benjamini_hochberg(p_value)
            units, pathways = np.nonzero(adjusted < p_value_adjusted_threshold)
            results.append(
                pd.DataFrame(
                    {
                        "diseaseId": disease_ids[tested][units],
                        "heldOutChromosome": chromosome,
                        "pathwayId": pathway_ids[pathways],
                        "beta": beta[units, pathways],
                        "betaError": standard_error[units, pathways],
                        "pValue": p_value[units, pathways],
                        "pValueAdjusted": adjusted[units, pathways],
                    }
                )
            )
        schema = (
            "diseaseId string, heldOutChromosome string, pathwayId string, beta double, "
            "betaError double, pValue double, pValueAdjusted double"
        )
        rows = pd.concat(results) if results else pd.DataFrame()
        if rows.empty:
            return spark.createDataFrame([], schema)
        return spark.createDataFrame(rows, schema)


def gene_covariates(target_index: TargetIndex, density_window: int) -> pd.DataFrame:
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


# Memory for one block of pathways: the largest genes x pathways or (disease, region) x
# pathways array of a block stays below it.
_BLOCK_BYTES = 2**28


def clustered_regression(
    covariates: np.ndarray,
    membership: scipy.sparse.csc_matrix | np.ndarray,
    outcomes: scipy.sparse.csc_matrix,
    regions: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Slope of every outcome on every predictor, with standard errors clustered by region.

    A predictor is a pathway's membership here, and a biosample's specificity scores in
    `ExpressionSpecificity.enrichment`. The covariates are projected out of each predictor
    once, which gives the slope of the full model (Frisch-Waugh-Lovell). The cluster-robust
    variance of a slope is
    `sum_c (sum_{g in c} xr_g e_g)^2 / sxx^2`, where `xr` is the projected predictor, `e` the
    residual of the full model and `sxx = sum_g xr_g^2`. The per-region sum is
    `Y_c - D_c - beta * B_c` with `Y_c = sum_{g in c} y_g xr_g`, `D_c = sum_{g in c} xr_g
    (Q Q^T y)_g` and `B_c = sum_{g in c} xr_g^2`. Squaring and summing it over the regions
    splits into terms that depend on the outcome only through `Q^T y`, computed for all
    outcomes at once, and terms in `Y_c`, which is zero outside the regions holding an
    outcome's genes and so is only computed there.

    Args:
        covariates (np.ndarray): genes x covariates, the intercept included
        membership (scipy.sparse.csc_matrix | np.ndarray): genes x predictors, such as 1 for
            the members of each pathway
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


def benjamini_hochberg(p_value: np.ndarray) -> np.ndarray:
    """Benjamini-Hochberg adjusted p-values along each row, ignoring NaN.

    Args:
        p_value (np.ndarray): units x tests

    Returns:
        np.ndarray: Adjusted p-values of the same shape, NaN where `p_value` is NaN
    """
    finite = np.isfinite(p_value)
    tests = finite.sum(axis=1, keepdims=True)
    order = np.argsort(np.where(finite, p_value, np.inf), axis=1)
    ranked = np.take_along_axis(p_value, order, axis=1)
    rank = np.arange(1, p_value.shape[1] + 1)[None, :]
    with np.errstate(invalid="ignore"):
        step = np.where(np.isfinite(ranked), ranked * tests / rank, np.inf)
    step = np.minimum(np.minimum.accumulate(step[:, ::-1], axis=1)[:, ::-1], 1.0)
    adjusted = np.empty_like(step)
    np.put_along_axis(adjusted, order, step, axis=1)
    return np.where(finite, adjusted, np.nan)
