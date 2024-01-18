---
Title: Run step using config
---

# Run step using YAML config

It's possible to parametrise the functionality of a step using a YAML configuration file. This is useful when you want to run a step multiple times with different parameters or simply to avoid having to specify the same parameters every time you run a step.

!!! info Configuration files using Hydra

    The package uses [Hydra](https://hydra.cc) to handle configuration files. For more information, please visit the [Hydra documentation](https://hydra.cc/docs/intro/).

To run a step using a configuration file, you need to create a configuration file in YAML format.

```{ .sh .no-copy }
config/
├─ step/
│  └─ my_gene_index.md
└─ my_config.yml
```

The configuration file should contain the parameters you want to use to run the step. For example, to run the `gene_index` step, you need to specify the `step.target_path` and `step.gene_index_path` parameters. The configuration file should look like this:

=== "my_config.yaml"

    ``` yaml
    defaults:
        - config
        - _self_
    ```

    This config file will specify that your configuration file will inherit the default configuration (`config`) and everything provided (`_self_`) will overwrite the default configuration.

=== "step/my_gene_index.md"

    ``` yaml
    defaults:
        - gene_index

    target_path: /path/to/target
    gene_index_path: /path/to/gene_index
    ```

    This config file will inherit the default configuration for the `gene_index` step and overwrite the `target_path` and `gene_index_path` parameters.

Once you have created the configuration file, you can run your own new `my_gene_index`:

```bash
gentropy step=my_gene_index --config-dir=config --config-name=my_config
```
