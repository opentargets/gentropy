---
title: Biosample enrichment
---

Expression specificity of genes across tissues and cell types from the `baseline_expression` table of an Open Targets platform release, and a linear regression testing each disease's genes for being more specifically expressed in each biosample, adjusted for gene length, protein-coding TSS density and the gene's specificity breadth across the panel, with standard errors clustered by 1 Mb region. The model is the Open Targets platform biosample enrichment step's, with protein-coding density on a log scale and the breadth term added. Used by the enriched biosample colocalisation L2G features.

:::gentropy.method.biosample_enrichment.ExpressionSpecificity

:::gentropy.method.biosample_enrichment.SpecificityPanel
