"""Pathway gene sets from a platform release and their enrichment among disease genes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import scipy.sparse
from scipy.special import expit
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

        Pathways with exactly the same members are collapsed into one, keeping the lowest
        `pathwayId`, across GO and Reactome alike. Propagation makes such copies common: a
        parent term whose only annotated child is one term gets exactly that child's genes.
        Kept, every copy would count again in a gene's score and be tested again.

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
        # Deduplicated straight after each propagation: a gene's terms share most ancestors.
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
            .select("pathwayId", "geneId")
            .distinct()
        )
        reactome_sets = (
            genes.select("geneId", f.explode("pathways.pathwayId").alias("termId"))
            .join(
                self.reactome.select(
                    f.col("id").alias("termId"),
                    f.explode(
                        f.array_union(
                            f.array(f.col("id")),
                            f.coalesce(
                                f.col("ancestors"), f.array().cast("array<string>")
                            ),
                        )
                    ).alias("pathwayId"),
                ),
                "termId",
                "inner",
            )
            .select("pathwayId", "geneId")
            .distinct()
        )
        # GO and Reactome identifiers never coincide, so the union needs no deduplication.
        members = go_sets.unionByName(reactome_sets)
        kept_pathways = (
            members.groupBy("pathwayId")
            .agg(f.array_sort(f.collect_list("geneId")).alias("members"))
            .filter(f.size("members").between(min_size, max_size))
            .groupBy("members")
            .agg(f.min("pathwayId").alias("pathwayId"))
            .select("pathwayId")
        )
        return members.join(kept_pathways, "pathwayId", "semi")

    @staticmethod
    def score_test(
        gene_lists: DataFrame,
        gene_sets: DataFrame,
        target_index: TargetIndex,
        p_value_adjusted_threshold: float = 0.05,
        min_genes: int = 25,
        held_out_chromosomes: list[str] | None = None,
        density_window: int = 1_000_000,
        saddlepoint_z: float = 2.0,
    ) -> DataFrame:
        """Test every pathway for enrichment among the genes of each disease, leaving one chromosome out at a time.

        For each disease, a logistic model of whether a protein-coding gene is on the disease's
        list is fitted on covariates only:

        `logit P(y_g = 1) = b0 + c1 * logLength_g + c2 * logDensity_g + c3 * log(1 + nPathways_g)`

        - `logLength_g` is `log10 |end - start|`, `logDensity_g` the log of one plus the number of
          protein-coding TSSs within `density_window` of the gene's TSS, the gene's own included,
          and `nPathways_g` the number of pathways of `gene_sets` the gene belongs to. Long genes
          in gene-poor regions are more often the nearest gene of a locus, and genes in many
          pathways are better studied, so neither alone should look like enrichment.

        Each pathway is then tested by the score test for adding its membership to that model,
        which needs no refit per pathway: the score is `sum_g x_g (y_g - mu_g)` and its variance
        the efficient information `x'Wx - x'WZ (Z'WZ)^-1 Z'Wx`. The p-value is one-sided, from the
        normal approximation, or from the saddlepoint approximation (Lugannani-Rice) of the exact
        distribution of the score where `|z| > saddlepoint_z`, which stays accurate when only a
        few genes of a small pathway are listed. Genes are treated as independent, as SAIGE and
        regenie do for rare binary traits. P-values are adjusted with Benjamini-Hochberg within
        each disease and held-out chromosome over every pathway with information left after the
        covariates.

        Each disease is tested once per held-out chromosome, with the genes of that chromosome
        removed. A locus is then scored with the results that left its own chromosome out, so
        neither its genes nor those of any other credible set nearby take part in the test.
        Diseases with fewer than `min_genes` listed genes, counted over all chromosomes, are not
        tested, so a disease is tested on every held-out chromosome or on none.

        The tests run on the driver.

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
            density_window (int): Distance, in base pairs, from a TSS within which protein-coding
                TSSs count towards its density
            saddlepoint_z (float): Absolute z-score above which the saddlepoint p-value is used

        Returns:
            DataFrame: `diseaseId`, `heldOutChromosome`, `pathwayId`, `zScore`, `pValue` and
                `pValueAdjusted`, for the enriched pathways only
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
                genes["logDensity"],
                np.log1p(np.asarray(membership.sum(axis=1)).ravel()),
            ]
        )

        lists = (
            gene_lists.select("diseaseId", "geneId", "chromosome").distinct().toPandas()
        )
        if held_out_chromosomes is None:
            held_out_chromosomes = sorted(lists["chromosome"].dropna().unique())
        lists = lists[lists["geneId"].isin(gene_index.index)][["diseaseId", "geneId"]]
        lists = lists.drop_duplicates()
        disease_ids = np.sort(lists["diseaseId"].unique())
        outcomes = scipy.sparse.csc_matrix(
            (
                np.ones(len(lists)),
                (
                    gene_index[lists["geneId"]].to_numpy(),
                    np.searchsorted(disease_ids, lists["diseaseId"].to_numpy()),
                ),
            ),
            shape=(len(genes), len(disease_ids)),
        )
        tested = np.flatnonzero(np.asarray(outcomes.sum(axis=0)).ravel() >= min_genes)

        results = []
        for chromosome in held_out_chromosomes if len(tested) else []:
            kept = (genes["chromosome"] != chromosome).to_numpy()
            if not kept.any():
                continue
            z_kept, x_kept = covariates[kept], membership[kept]
            y_kept = outcomes[kept]
            for disease in tested:
                y = y_kept[:, disease].toarray().ravel()
                if not 0 < y.sum() < len(y):
                    continue
                z_score, p_value = _score_test(z_kept, x_kept, y, saddlepoint_z)
                adjusted = benjamini_hochberg(p_value[None, :])[0]
                (enriched,) = np.nonzero(adjusted < p_value_adjusted_threshold)
                results.append(
                    pd.DataFrame(
                        {
                            "diseaseId": disease_ids[disease],
                            "heldOutChromosome": chromosome,
                            "pathwayId": pathway_ids[enriched],
                            "zScore": z_score[enriched],
                            "pValue": p_value[enriched],
                            "pValueAdjusted": adjusted[enriched],
                        }
                    )
                )
        schema = (
            "diseaseId string, heldOutChromosome string, pathwayId string, zScore double, "
            "pValue double, pValueAdjusted double"
        )
        rows = pd.concat(results) if results else pd.DataFrame()
        if rows.empty:
            return spark.createDataFrame([], schema)
        return spark.createDataFrame(rows, schema)


def gene_covariates(target_index: TargetIndex, density_window: int) -> pd.DataFrame:
    """Position, log length and log protein-coding TSS density of every protein-coding gene.

    Args:
        target_index (TargetIndex): Target index
        density_window (int): Distance, in base pairs, from a TSS within which TSSs count

    Returns:
        pd.DataFrame: `geneId`, `chromosome`, `tss`, `logLength` and `logDensity`, one row per
            protein-coding gene with a usable location, sorted by `geneId`. `logDensity` is the
            log of one plus the number of protein-coding TSSs within `density_window`, the gene's
            own included: only protein-coding genes compete to be a locus's nearest gene.
    """
    genes = (
        target_index.df.filter(f.col("biotype") == "protein_coding")
        .select(
            f.col("id").alias("geneId"),
            "tss",
            f.col("genomicLocation.chromosome").alias("chromosome"),
            f.abs(f.col("genomicLocation.end") - f.col("genomicLocation.start")).alias(
                "length"
            ),
        )
        .filter(
            f.col("tss").isNotNull()
            & f.col("chromosome").isNotNull()
            & (f.col("length") > 0)
        )
        .toPandas()
    )
    tss = genes["tss"].to_numpy(dtype=np.int64)
    density = np.empty(len(genes), dtype=np.int64)
    for rows in genes.groupby("chromosome").indices.values():
        ordered = np.sort(tss[rows])
        density[rows] = np.searchsorted(
            ordered, tss[rows] + density_window, side="right"
        ) - np.searchsorted(ordered, tss[rows] - density_window, side="left")
    return (
        genes.assign(
            tss=tss,
            logLength=np.log10(genes["length"]),
            logDensity=np.log1p(density),
        )
        .sort_values("geneId")
        .reset_index(drop=True)[["geneId", "chromosome", "tss", "logLength", "logDensity"]]
    )


# Pathways whose saddlepoint is solved together: bounds the genes x pathways block in memory.
_SADDLEPOINT_BLOCK = 400


def _logistic_fit(
    covariates: np.ndarray, outcome: np.ndarray, iterations: int = 25
) -> np.ndarray:
    """Fitted probabilities of a logistic regression, by iteratively reweighted least squares.

    Args:
        covariates (np.ndarray): genes x covariates, the intercept included
        outcome (np.ndarray): 0/1 per gene, with both values present
        iterations (int): Largest number of Newton steps

    Returns:
        np.ndarray: Fitted probability per gene
    """
    coefficients = np.zeros(covariates.shape[1])
    rate = outcome.mean()
    coefficients[0] = np.log(rate / (1 - rate))
    for _ in range(iterations):
        fitted = expit(covariates @ coefficients)
        weight = fitted * (1 - fitted)
        step = np.linalg.lstsq(
            covariates.T @ (covariates * weight[:, None]),
            covariates.T @ (outcome - fitted),
            rcond=None,
        )[0]
        coefficients += step
        if np.abs(step).max() < 1e-8:
            break
    return expit(covariates @ coefficients)


def _score_test(
    covariates: np.ndarray,
    membership: scipy.sparse.csc_matrix,
    outcome: np.ndarray,
    saddlepoint_z: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Score test of every pathway's membership added to a covariates-only logistic model.

    With `Z` the covariates, `mu` the fitted probabilities and `W = diag(mu (1 - mu))`, the
    membership `x` is adjusted to `x~ = x - Z (Z'WZ)^-1 Z'W x`. The score `x~'(y - mu)` equals
    `x'(y - mu)`, since `Z'(y - mu)` is zero at the fit, and its variance is `x~'W x~`.

    Args:
        covariates (np.ndarray): genes x covariates, the intercept included
        membership (scipy.sparse.csc_matrix): genes x pathways, 1 for members
        outcome (np.ndarray): 0/1 per gene
        saddlepoint_z (float): Absolute z-score above which the saddlepoint p-value is used

    Returns:
        tuple[np.ndarray, np.ndarray]: z-score and one-sided p-value per pathway, NaN for
            pathways with no information left after the covariates
    """
    fitted = _logistic_fit(covariates, outcome)
    weight = fitted * (1 - fitted)
    weighted_covariates = covariates * weight[:, None]
    cross = np.asarray((membership.T @ weighted_covariates).T)
    projection = np.linalg.pinv(covariates.T @ weighted_covariates) @ cross
    score = np.asarray(membership.T @ (outcome - fitted)).ravel()
    information = np.asarray(membership.T @ weight).ravel()
    variance = information - np.einsum("kp,kp->p", cross, projection)
    valid = variance > 1e-10 * np.maximum(information, 1e-300)
    z_score = np.where(valid, score / np.sqrt(np.where(valid, variance, 1.0)), np.nan)
    p_value = norm.sf(z_score)

    tail = np.flatnonzero(valid & (np.abs(z_score) > saddlepoint_z))
    for block in np.array_split(tail, max(1, -(-len(tail) // _SADDLEPOINT_BLOCK))):
        if len(block) == 0:
            continue
        members = membership[:, block]
        column = np.repeat(np.arange(len(block)), np.diff(members.indptr))
        covariate_part = covariates @ projection[:, block]
        member_part = covariate_part[members.indices, column]
        # Non-members contribute -covariate_part each: many small terms, taken as normal.
        rest_variance = np.maximum(
            np.einsum("gp,g,gp->p", covariate_part, weight, covariate_part)
            - np.bincount(column, member_part**2 * weight[members.indices], len(block)),
            0.0,
        )
        p_saddle = _saddlepoint_upper(
            score[block],
            1.0 - member_part,
            fitted[members.indices],
            column,
            rest_variance,
        )
        p_value[block] = np.where(
            np.isfinite(p_saddle), np.clip(p_saddle, 0.0, 1.0), p_value[block]
        )
    return z_score, np.where(valid, p_value, np.nan)


def _saddlepoint_upper(
    score: np.ndarray,
    coefficient: np.ndarray,
    probability: np.ndarray,
    column: np.ndarray,
    rest_variance: np.ndarray,
) -> np.ndarray:
    """Upper-tail probability of `S = sum_g a_g (y_g - mu_g)` by the Lugannani-Rice saddlepoint.

    The member genes' terms, `y_g ~ Bernoulli(mu_g)`, enter the cumulant generating function
    exactly; the other genes' terms are folded into a normal with variance `rest_variance`.
    Solved for all pathways together with Newton steps.

    Args:
        score (np.ndarray): Observed score per pathway
        coefficient (np.ndarray): `a_g` of each member gene, pathway by pathway
        probability (np.ndarray): `mu_g` of each member gene, aligned with `coefficient`
        column (np.ndarray): Pathway of each member gene, aligned with `coefficient`
        rest_variance (np.ndarray): Variance of the non-member part per pathway

    Returns:
        np.ndarray: `P(S >= score)` per pathway
    """
    n = len(score)

    def cumulants(t: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        exponent = t[column] * coefficient
        tilted = probability * np.exp(exponent)
        denominator = 1 - probability + tilted
        shifted = tilted / denominator
        k0 = (
            np.bincount(column, np.log(denominator) - exponent * probability, n)
            + 0.5 * rest_variance * t**2
        )
        k1 = (
            np.bincount(column, coefficient * (shifted - probability), n)
            + rest_variance * t
        )
        k2 = (
            np.bincount(column, coefficient**2 * shifted * (1 - shifted), n)
            + rest_variance
        )
        return k0, k1, k2

    t = score / np.maximum(cumulants(np.zeros(n))[2], 1e-300)
    for _ in range(50):
        _, k1, k2 = cumulants(t)
        step = np.clip((k1 - score) / np.maximum(k2, 1e-300), -5.0, 5.0)
        t -= step
        if np.nanmax(np.abs(step)) < 1e-10:
            break
    k0, _, k2 = cumulants(t)
    w = np.sign(t) * np.sqrt(np.maximum(2 * (t * score - k0), 0.0))
    v = t * np.sqrt(k2)
    with np.errstate(divide="ignore", invalid="ignore"):
        return norm.sf(w) + norm.pdf(w) * (1 / v - 1 / w)


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
