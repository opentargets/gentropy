---
Title: Run step in CLI
---

# Run step in CLI

To run a step in the command line interface (CLI), you need to know the step's name. To list what steps are avaiable in your current environment, simply run `gentropy` with no arguments. This will list all the steps:

```
You must specify 'step', e.g, step=<OPTION>
Available options:
        clump
        colocalisation
        eqtl_catalogue
        finngen_studies
        finngen_sumstat_preprocess
        gene_index
        gwas_catalog_ingestion
        gwas_catalog_sumstat_preprocess
        ld_index
        locus_to_gene
        overlaps
        pics
        ukbiobank
        variant_annotation
        variant_index
        variant_to_gene

Set the environment variable HYDRA_FULL_ERROR=1 for a complete stack trace.
```

As indicated, you can run a step by specifying the step's name with the `step` argument. For example, to run the `gene_index` step, you can run:

```bash
gentropy step=gene_index
```

In most occassions, some mandatory values will be required to run the step. For example, the `gene_index` step requires the `step.target_path` and `step.gene_index_path` argument to be specified. You can complete the necessary arguments by adding them to the command line:

```bash
gentropy step=gene_index step.target_path=/path/to/target step.gene_index_path=/path/to/gene_index
```

You can find more about the available steps in the [documentation](../../python_api/steps/_steps.md).
