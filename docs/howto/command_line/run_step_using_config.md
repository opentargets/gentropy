---
Title: Run step using config
---

## Run step using YAML config

It's possible to parametrise the functionality of a step using a YAML configuration file. This is useful when you want to run a step multiple times with different parameters or simply to avoid having to specify the same parameters every time you run a step.

!!! info Configuration files using Hydra

    The package uses [Hydra](https://hydra.cc) to handle configuration files. For more information, please visit the [Hydra documentation](https://hydra.cc/docs/intro/).

To run a step using a configuration file, you need to create a configuration file in YAML format.

```{ .sh .no-copy }
config/
├─ step/
│  └─ my_gwas_catalog_sumstat_preprocess.md
└─ my_config.yml
```

The configuration file should contain the parameters you want to use to run the step. For example, to run the `gwas_catalog_sumstat_preprocess` step, you need to specify the `step.raw_sumstats_path` and `step.out_sumstats_path` parameters. The configuration file should look like this:

=== "my_config.yaml"

    ``` yaml
    defaults:
        - config
        - _self_
    ```

    This config file will specify that your configuration file will inherit the default configuration (`config`) and everything provided (`_self_`) will overwrite the default configuration.

=== "step/my_gwas_catalog_sumstat_preprocess.md"

    ``` yaml
    defaults:
        - gwas_catalog_sumstat_preprocess

    raw_sumstats_path: /path/to/raw_sumstats
    out_sumstats_path: /path/to/out_sumstats
    ```

    This config file will inherit the default configuration for the `gwas_catalog_sumstat_preprocess` step and overwrite the `raw_sumstats_path` and `out_sumstats_path` parameters.

Once you have created the configuration file, you can run your own new `my_gwas_catalog_sumstat_preprocess`:

```bash
gentropy step=my_gwas_catalog_sumstat_preprocess --config-dir=config --config-name=my_config
```
