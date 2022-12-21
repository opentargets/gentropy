"""CLI for OTG."""
from __future__ import annotations

import hydra
from hydra.utils import instantiate
from omegaconf import OmegaConf

from otg.config import Config, register_configs

register_configs()


@hydra.main(config_path="config", version_base=None, config_name="config")
def my_app(cfg: Config) -> None:
    """OTG ETL CLI.

    Args:
        cfg (Config): hydra configuration object
    """
    # Print config
    print(OmegaConf.to_yaml(cfg))
    # Initialise ETL session
    # etl = instantiate(cfg.etl)
    # Initialise and run step
    step = instantiate(cfg.step)
    step.run()


if __name__ == "__main__":
    my_app()
