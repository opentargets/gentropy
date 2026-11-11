---
Title: Run step in CLI
---

## Run step in CLI

To run a step in the command line interface (CLI), you need to know the step's name. To list the steps available in your current environment, run `gentropy` with no arguments. It prints every registered step, for example:

```
You must specify 'step', e.g, step=<OPTION>
Available options:
        bigbrain_finemapping_ingestion
        colocalisation
        credible_set_qc
        ...
        susie_finemapping
        training_set
        window_based_clumping

Set the environment variable HYDRA_FULL_ERROR=1 for a complete stack trace.
```

As indicated, you can run a step by specifying the step's name with the `step` argument. For example, to run the `gwas_catalog_sumstat_preprocess` step, you can run:

```bash
gentropy step=gwas_catalog_sumstat_preprocess
```

In most cases, some mandatory values will be required to run the step. For example, the `gwas_catalog_sumstat_preprocess` step requires the `step.raw_sumstats_path` and `step.out_sumstats_path` argument to be specified. You can complete the necessary arguments by adding them to the command line:

```bash
gentropy step=gwas_catalog_sumstat_preprocess step.raw_sumstats_path=/path/to/raw_sumstats step.out_sumstats_path=/path/to/out_sumstats
```

You can find more about the available steps in the [documentation](../../python_api/steps/_steps.md).
