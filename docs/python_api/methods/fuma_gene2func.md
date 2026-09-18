---
title: FUMA Gene2Func
---

**FUMA Gene2Func Overview:**

FumaGene2Func implements the [FUMA gene2func](https://fuma.ctglab.nl/tutorial#gene2func) hypergeometric gene-set enrichment approach for GWAS-prioritised genes. Given a scored gene DataFrame (e.g. L2G predictions or Open Targets association scores) and a long-format gene-sets DataFrame (e.g. GTEx DEGs or MSigDB gene sets), it tests whether prioritised genes are over-represented in each gene set per group.

The method resolves identifiers automatically (`studyLocusId` → `studyId` via a credible set DataFrame; optionally `studyId` → `diseaseId` via a study index) and infers group columns dynamically. For each group × gene set combination it computes:

- **fold enrichment** — observed vs expected overlap under the null
- **p-value** — one-sided hypergeometric survival P(X ≥ k)
- **p_bonferroni** — Bonferroni correction over the per-group family size
- **p_fdr_bh** — Benjamini–Hochberg FDR with proper step-down monotonicity, computed per group

:::gentropy.method.fuma_gene2func.FumaGene2Func
