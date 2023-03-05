"""CLI for OTG."""
from __future__ import annotations

import hydra
from hydra.utils import instantiate
from omegaconf import DictConfig, OmegaConf

from otg.config import Config, LocusToGeneConfig

# register_configs()


@hydra.main(version_base="1.1", config_path=None, config_name="config")
def main(cfg: Config) -> None:
    """OTG ETL CLI.

    Args:
        cfg (Config): hydra configuration object
    """
    # Print config
    print(cfg.step)
    print(OmegaConf.to_yaml(cfg))
    # Instantiate ETL session
    session = instantiate(cfg.session)
    # Initialise and run step
    step = instantiate(cfg.step, session=session)
    step.run()


@hydra.main(config_path="config/step", version_base=None, config_name="locus_to_gene")
def run_l2g(cfg: DictConfig) -> None:
    """OTG L2G step.

    Args:
        cfg (DictConfig): hydra configuration object
    """
    print(OmegaConf.to_yaml(cfg))
    print(cfg)

    struct_cfg = LocusToGeneConfig(
        run_mode=cfg.l2g.run_mode,
        study_locus_path=cfg.l2g.study_locus_path,
        study_locus_overlap_path=cfg.l2g.study_locus_overlap_path,
        variant_gene_path=cfg.l2g.variant_gene_path,
        study_index_path=cfg.l2g.study_index_path,
        colocalisation_path=cfg.l2g.colocalisation_path,
        gold_standard_curation_path=cfg.l2g.gold_standard_curation_path,
        gene_interactions_path=cfg.l2g.gene_interactions_path,
        hyperparameters=cfg.l2g.hyperparameters,
        etl=cfg.l2g.defaults,
    )

    # Initialise and run step
    step = instantiate(config=struct_cfg, _target_="otg.l2g.LocusToGeneStep")
    step.run()

    # working - TODO: follow this example to populate the classes from the config https://github.com/facebookresearch/hydra/blob/main/examples/instantiate/object/my_app.py


if __name__ == "__main__":
    main()
